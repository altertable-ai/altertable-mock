use tonic::{
    Request, Status,
    service::{Interceptor, InterceptorLayer},
};
use tower_http::trace::MakeSpan;
use tracing::debug_span;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct CorrelationID(pub String);

pub fn make_span<B>() -> impl MakeSpan<B> + Clone {
    move |req: &http::Request<B>| {
        let id = req
            .extensions()
            .get::<CorrelationID>()
            .map(|id| id.0.clone());

        debug_span!("request", correlation_id = ?id)
    }
}

pub fn layer() -> InterceptorLayer<impl Interceptor + Clone> {
    InterceptorLayer::new(interceptor)
}

#[allow(clippy::result_large_err)]
pub fn interceptor(mut req: Request<()>) -> Result<Request<()>, Status> {
    let id = match req.metadata().get("x-correlation-id") {
        Some(id) => id
            .to_str()
            .map_err(|_| Status::invalid_argument("Invalid correlation header encoding"))?
            .to_owned(),

        None => Uuid::new_v4().to_string(),
    };

    req.extensions_mut().insert(CorrelationID(id));

    Ok(req)
}
