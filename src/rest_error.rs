use axum::{Json, http::StatusCode, response::{IntoResponse, Response}};
use serde_json::json;
use tonic::{Code, Status};

pub fn status_to_response(s: Status) -> Response {
    let http = match s.code() {
        Code::InvalidArgument => 400,
        Code::Unauthenticated => 401,
        Code::PermissionDenied => 403,
        Code::NotFound => 404,
        Code::AlreadyExists | Code::Aborted => 409,
        Code::FailedPrecondition => 412,
        Code::Unimplemented => 501,
        _ => 500,
    };
    let status_str = format!("{:?}", s.code()).to_uppercase();
    (
        StatusCode::from_u16(http).unwrap(),
        Json(json!({
            "error": {
                "code": http,
                "message": s.message(),
                "status": status_str,
            }
        })),
    )
        .into_response()
}

pub fn bad_request(msg: &str) -> Response {
    status_to_response(Status::invalid_argument(msg))
}

pub fn not_found(msg: &str) -> Response {
    status_to_response(Status::not_found(msg))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn body_to_json(resp: Response) -> serde_json::Value {
        let bytes = hyper::body::to_bytes(resp.into_body()).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn invalid_argument_maps_to_400_with_envelope() {
        let resp = status_to_response(Status::invalid_argument("bad payload"));
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let v = body_to_json(resp).await;
        assert_eq!(v["error"]["code"], 400);
        assert_eq!(v["error"]["status"], "INVALIDARGUMENT");
        assert_eq!(v["error"]["message"], "bad payload");
    }

    #[tokio::test]
    async fn not_found_maps_to_404() {
        let resp = status_to_response(Status::not_found("nope"));
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn unimplemented_maps_to_501() {
        let resp = status_to_response(Status::unimplemented("soon"));
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
    }
}
