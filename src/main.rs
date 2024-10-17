mod db;
mod handlers;
mod models;

use axum::{
    routing::{get, post},
    Router,
};
use db::Database;
use handlers::{add_record_handler, get_records_handler};
use std::net::SocketAddr;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    // Initialize the database
    let db = Arc::new(Database::new("data.parquet"));
    db.initialize().expect("Failed to initialize database");

    // Define the server routes
    let app = Router::new()
        .route(
            "/records",
            get(get_records_handler).post(add_record_handler),
        )
        .layer(Extension(db));

    // Set the server address
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    println!("Server running at http://{}", addr);

    // Run the server
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}
