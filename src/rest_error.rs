use axum::{
    Json,
    http::{StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
};
use prost::Message;
use serde_json::json;
use tonic::{Code, Status};

pub const PROTOBUF_CONTENT_TYPE: &str = "application/x-protobuf";

/// `google.rpc.Status`, the error body on the protobuf wire.
#[derive(Clone, PartialEq, Message)]
pub struct RpcStatus {
    #[prost(int32, tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub message: String,
}

fn http_status(code: Code) -> StatusCode {
    let http = match code {
        Code::InvalidArgument => 400,
        Code::Unauthenticated => 401,
        Code::PermissionDenied => 403,
        Code::NotFound => 404,
        Code::AlreadyExists | Code::Aborted => 409,
        Code::FailedPrecondition => 412,
        Code::Unimplemented => 501,
        _ => 500,
    };
    StatusCode::from_u16(http).unwrap()
}

/// The error as the Google clients expect it on the protobuf wire: the
/// mapped HTTP status with a serialized `google.rpc.Status` body.
pub fn status_to_protobuf_response(s: Status) -> Response {
    let body = RpcStatus {
        code: s.code() as i32,
        message: s.message().to_string(),
    };
    (
        http_status(s.code()),
        [(CONTENT_TYPE, PROTOBUF_CONTENT_TYPE)],
        body.encode_to_vec(),
    )
        .into_response()
}

pub fn status_to_response(s: Status) -> Response {
    let http = http_status(s.code());
    let status_str = match s.code() {
        Code::Ok => "OK",
        Code::Cancelled => "CANCELLED",
        Code::Unknown => "UNKNOWN",
        Code::InvalidArgument => "INVALID_ARGUMENT",
        Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
        Code::NotFound => "NOT_FOUND",
        Code::AlreadyExists => "ALREADY_EXISTS",
        Code::PermissionDenied => "PERMISSION_DENIED",
        Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
        Code::FailedPrecondition => "FAILED_PRECONDITION",
        Code::Aborted => "ABORTED",
        Code::OutOfRange => "OUT_OF_RANGE",
        Code::Unimplemented => "UNIMPLEMENTED",
        Code::Internal => "INTERNAL",
        Code::Unavailable => "UNAVAILABLE",
        Code::DataLoss => "DATA_LOSS",
        Code::Unauthenticated => "UNAUTHENTICATED",
    };
    (
        http,
        Json(json!({
            "error": {
                "code": http.as_u16(),
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
        assert_eq!(v["error"]["status"], "INVALID_ARGUMENT");
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
