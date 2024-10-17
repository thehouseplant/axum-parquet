use axum::{extract::Extension, http::StatusCode, Json};
use serde_json::json;
use std::sync::Arc;

use crate::{db::Database, models::Record};

pub async fn get_records_handler(
    Extension(db): Extension<Arc<Database>>,
) -> Result<Json<Vec<Record>>, (StatusCode, String)> {
    match db.get_records() {
        Ok(records) => Ok(Json(records)),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_Sstring())),
    }
}

pub async fn add_record_handler(
    Extension(db): Extension<Arc<Database>>,
    Json(payload): Json<Record>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, String)> {
    match db.add_record(payload.clone()) {
        Ok(_) => Ok((
            StatusCode::CREATED,
            Json(json!({"status": "success", "record": payload})),
        )),
        Err(e) => Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}
