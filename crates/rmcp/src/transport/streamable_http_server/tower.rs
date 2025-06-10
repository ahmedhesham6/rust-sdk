use std::{collections::HashMap, io, net::SocketAddr, sync::Arc, time::Duration};

use futures::Stream;
use http::{Request, Response, StatusCode};
use hyper::service::make_service_fn;
use hyper::{Body, Server};
use tokio::sync::{RwLock, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::Instrument;

use super::session::{EventId, SessionHandle, SessionWorker, StreamableHttpMessageReceiver};
use crate::{
    model::ClientJsonRpcMessage,
    transport::common::{
        http_header::{HEADER_LAST_EVENT_ID, HEADER_SESSION_ID},
        tower::{DEFAULT_AUTO_PING_INTERVAL, SessionId, session_id},
    },
};

/// Shared session manager
type SessionManager = Arc<RwLock<HashMap<String, SessionHandle>>>;

/// Application state
#[derive(Clone)]
struct App {
    session_manager: SessionManager,
    transport_tx: mpsc::UnboundedSender<SessionWorker>,
    sse_ping_interval: Duration,
}

impl App {
    pub fn new(sse_ping_interval: Duration) -> (Self, mpsc::UnboundedReceiver<SessionWorker>) {
        let (transport_tx, transport_rx) = mpsc::unbounded_channel();
        (
            Self {
                session_manager: Default::default(),
                transport_tx,
                sse_ping_interval,
            },
            transport_rx,
        )
    }
}

/// Convert our message receiver into an SSE stream
fn receiver_as_stream(
    receiver: StreamableHttpMessageReceiver,
) -> impl Stream<Item = Result<hyper::sse::Event, io::Error>> {
    ReceiverStream::new(receiver.inner).map(|message| {
        let payload = serde_json::to_string(&message.message)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(hyper::sse::Event::default()
            .event("message")
            .data(payload)
            .id(message.event_id.to_string()))
    })
}

/// Handler for POST /
async fn post_handler(app: Arc<App>, mut req: Request<Body>) -> Result<Response<Body>, Infallible> {
    use futures::StreamExt;
    // Extract headers and JSON body
    let headers = req.headers().clone();
    let bytes = hyper::body::to_bytes(req.into_body())
        .await
        .unwrap_or_default();
    let mut message: ClientJsonRpcMessage = match serde_json::from_slice(&bytes) {
        Ok(msg) => msg,
        Err(e) => {
            let resp = Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Body::from(format!("bad json: {}", e)))
                .unwrap();
            return Ok(resp);
        }
    };

    if let Some(raw) = headers.get(HEADER_SESSION_ID) {
        let sid = raw.to_str().unwrap_or_default().to_string();
        tracing::debug!(session_id = %sid, ?message, "new client message");
        let handle = {
            let sm = app.session_manager.read().await;
            sm.get(&sid).cloned()
        };
        if let Some(session) = handle {
            // attach headers/extensions if needed...
            match &message {
                ClientJsonRpcMessage::Request(_) | ClientJsonRpcMessage::BatchRequest(_) => {
                    let receiver = match session.establish_request_wise_channel().await {
                        Ok(r) => r,
                        Err(e) => {
                            let resp = Response::builder()
                                .status(StatusCode::INTERNAL_SERVER_ERROR)
                                .body(Body::from(format!(
                                    "fail to establish request channel: {}",
                                    e
                                )))
                                .unwrap();
                            return Ok(resp);
                        }
                    };
                    let stream = ReceiverStream::new(receiver.inner).map(|msg| {
                        let data = serde_json::to_string(&msg.message)
                            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                        Ok(hyper::sse::Event::default()
                            .event("message")
                            .data(data)
                            .id(msg.event_id.to_string()))
                    });
                    // Respond as SSE
                    let body =
                        Body::wrap_stream(hyper::sse::Sse::new(stream).keep_alive(
                            hyper::sse::KeepAlive::new().interval(app.sse_ping_interval),
                        ));
                    let resp = Response::builder()
                        .status(StatusCode::OK)
                        .header("content-type", "text/event-stream")
                        .body(body)
                        .unwrap();
                    return Ok(resp);
                }
                _ => {
                    let result = session.push_message(message, None).await;
                    if result.is_err() {
                        let resp = Response::builder()
                            .status(StatusCode::GONE)
                            .body(Body::from("session terminated"))
                            .unwrap();
                        return Ok(resp);
                    }
                    let resp = Response::builder()
                        .status(StatusCode::ACCEPTED)
                        .body(Body::empty())
                        .unwrap();
                    return Ok(resp);
                }
            }
        }
        // session not found
        let resp = Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("session not found"))
            .unwrap();
        return Ok(resp);
    }

    // Initialize new session
    let session_id = session_id();
    let (session, transport) =
        super::session::create_session(session_id.clone(), Default::default());
    let _ = app.transport_tx.send(transport);

    let init_resp = match session.initialize(message).await {
        Ok(r) => r,
        Err(e) => {
            let resp = Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(Body::from(format!("fail to initialize: {}", e)))
                .unwrap();
            return Ok(resp);
        }
    };
    let mut resp = Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_string(&init_resp).unwrap()))
        .unwrap();
    resp.headers_mut()
        .insert(HEADER_SESSION_ID, session_id.parse().unwrap());
    app.session_manager
        .write()
        .await
        .insert(session_id, session);
    Ok(resp)
}

/// Handler for GET /
async fn get_handler(app: Arc<App>, req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let headers = req.headers();
    let sid_opt = headers.get(HEADER_SESSION_ID).and_then(|v| v.to_str().ok());
    if let Some(sid) = sid_opt {
        let last_event = headers
            .get(HEADER_LAST_EVENT_ID)
            .and_then(|v| v.to_str().ok());
        let session = {
            let sm = app.session_manager.read().await;
            sm.get(sid).cloned()
        };
        if let Some(session) = session {
            let receiver = if let Some(le) = last_event {
                match le.parse::<EventId>() {
                    Ok(id) => session.resume(id).await.map_err(|e| {
                        eprintln!("resume error {}", e);
                        StatusCode::INTERNAL_SERVER_ERROR
                    }),
                    Err(_) => Err(StatusCode::BAD_REQUEST),
                }
            } else {
                session
                    .establish_common_channel()
                    .await
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
            };
            if let Ok(rx) = receiver {
                let stream = receiver_as_stream(rx);
                let body = Body::wrap_stream(
                    hyper::sse::Sse::new(stream)
                        .keep_alive(hyper::sse::KeepAlive::new().interval(app.sse_ping_interval)),
                );
                let resp = Response::builder()
                    .status(StatusCode::OK)
                    .header("content-type", "text/event-stream")
                    .body(body)
                    .unwrap();
                return Ok(resp);
            }
        }
        let resp = Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap();
        return Ok(resp);
    }
    let resp = Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .body(Body::from("missing session id"))
        .unwrap();
    Ok(resp)
}

/// Handler for DELETE /
async fn delete_handler(app: Arc<App>, req: Request<Body>) -> Result<Response<Body>, Infallible> {
    let headers = req.headers();
    if let Some(raw) = headers.get(HEADER_SESSION_ID) {
        let sid = raw.to_str().unwrap_or_default();
        let session_opt = app.session_manager.write().await.remove(sid);
        if let Some(session) = session_opt {
            let _ = session.close().await;
            let resp = Response::builder()
                .status(StatusCode::ACCEPTED)
                .body(Body::empty())
                .unwrap();
            return Ok(resp);
        }
        let resp = Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::empty())
            .unwrap();
        return Ok(resp);
    }
    let resp = Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .body(Body::from("missing session id"))
        .unwrap();
    Ok(resp)
}

/// RouterService implementing Tower's Service trait
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

#[derive(Clone)]
struct RouterService {
    app: Arc<App>,
}

impl tower::Service<Request<Body>> for RouterService {
    type Response = Response<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let app = self.app.clone();
        let method = req.method().clone();
        let path = req.uri().path().to_string();
        Box::pin(async move {
            match (method, path.as_str()) {
                (http::Method::POST, "/") => post_handler(app, req).await,
                (http::Method::GET, "/") => get_handler(app, req).await,
                (http::Method::DELETE, "/") => delete_handler(app, req).await,
                _ => Ok(Response::builder()
                    .status(StatusCode::NOT_FOUND)
                    .body(Body::empty())
                    .unwrap()),
            }
        })
    }
}

/// Server configuration
#[derive(Debug, Clone)]
pub struct StreamableHttpServerConfig {
    pub bind: SocketAddr,
    pub ct: CancellationToken,
    pub sse_keep_alive: Option<Duration>,
}

impl Default for StreamableHttpServerConfig {
    fn default() -> Self {
        Self {
            bind: "[::]:80".parse().unwrap(),
            ct: CancellationToken::new(),
            sse_keep_alive: None,
        }
    }
}

/// Main server struct
pub struct StreamableHttpServer {
    transport_rx: mpsc::UnboundedReceiver<SessionWorker>,
    pub config: StreamableHttpServerConfig,
}

impl StreamableHttpServer {
    /// Start the server and return the handle
    pub async fn serve_with_config(config: StreamableHttpServerConfig) -> io::Result<Self> {
        let (app, transport_rx) =
            App::new(config.sse_keep_alive.unwrap_or(DEFAULT_AUTO_PING_INTERVAL));
        let app = Arc::new(app);
        let ct_child = config.ct.clone();

        // spawn transport loop
        tokio::spawn(async move {
            let mut server = StreamableHttpServer {
                transport_rx,
                config: config.clone(),
            };
            while let Some(transport) = server.transport_rx.recv().await {
                let service = (|| crate::service::MyServiceProvider::new())(); // adapt to your provider
                let ct = ct_child.child_token();
                tokio::spawn(async move {
                    let server = service.serve_with_ct(transport, ct).await.unwrap();
                    let _ = server.waiting().await;
                });
            }
        });

        // setup Hyper
        let make_svc = make_service_fn(move |_| {
            let router = RouterService { app: app.clone() };
            async move { Ok::<_, Infallible>(router) }
        });

        let server = Server::bind(&config.bind)
            .serve(make_svc)
            .with_graceful_shutdown(async move {
                config.ct.cancelled().await;
                tracing::info!("streamable http server cancelled");
            });

        tokio::spawn(
            server.instrument(tracing::info_span!("streamable-http-server", bind = %config.bind)),
        );
        Ok(StreamableHttpServer {
            transport_rx: app.transport_tx.subscribe(),
            config,
        })
    }

    /// Cancel the server
    pub fn cancel(&self) {
        self.config.ct.cancel();
    }

    /// Receive next transport
    pub async fn next_transport(&mut self) -> Option<SessionWorker> {
        self.transport_rx.recv().await
    }
}
