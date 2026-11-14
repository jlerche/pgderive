//! Pgderive control plane HTTP API.
use axum::{
    Json, Router,
    extract::Path,
    http::{HeaderMap, StatusCode},
    routing::get,
};
use serde::Serialize;

#[derive(Serialize)]
struct OrganizationContext {
    organization_id: String,
    user_id: String,
}

async fn organization_context(
    Path(organization_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<OrganizationContext>, StatusCode> {
    let user_id = headers
        .get("x-pgderive-user-id")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    if headers
        .get("x-pgderive-organization-id")
        .and_then(|value| value.to_str().ok())
        != Some(organization_id.as_str())
    {
        return Err(StatusCode::FORBIDDEN);
    }

    Ok(Json(OrganizationContext {
        organization_id,
        user_id: user_id.to_owned(),
    }))
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let app = Router::new().route(
        "/api/v1/organizations/{organization_id}",
        get(organization_context),
    );
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3001").await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            if let Err(error) = tokio::signal::ctrl_c().await {
                eprintln!("could not install shutdown handler: {error}");
            }
        })
        .await
}
