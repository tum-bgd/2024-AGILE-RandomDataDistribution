use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};

/// A common error type that can be used throughout the API.
///
/// Can be returned in a `Result` from an API handler function.
///
/// For convenience, this represents both API errors as well as internal recoverable errors,
/// and maps them to appropriate status codes along with at least a minimally useful error
/// message in a plain text body, or a JSON body in the case of `UnprocessableEntity`.
#[derive(thiserror::Error, Debug)]
pub enum AppError {
    /// Return `404 Not Found`
    #[error("request path not found")]
    NotFound,

    /// Return `500 Internal Server Error` on a `anyhow::Error`.
    ///
    /// Via the generated `From<anyhow::Error> for Error` impl, this allows the
    /// use of `?` in handler functions to automatically convert `anyhow::Error` into a response.
    #[error("an internal server error occurred")]
    Anyhow(#[from] anyhow::Error),
}

impl AppError {
    fn status_code(&self) -> StatusCode {
        match self {
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::Anyhow(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

// Tell axum how to convert `AppError` into a response.
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        if let AppError::Anyhow(ref e) = self {
            tracing::error!("Generic error: {:?}", e);
        }

        (self.status_code(), self.to_string()).into_response()
    }
}
