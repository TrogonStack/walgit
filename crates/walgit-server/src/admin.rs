//! Admin endpoints: `PUT /{owner}/{repo}` (create), `DELETE /{owner}/{repo}`
//! (delete manifest + prefix objects), `GET /` (list repos, text/plain).

use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use walgit_git::ObjectFormat;

use crate::AppState;
use crate::error::ApiError;
use crate::repo::RepoRoute;

/// `PUT /{owner}/{repo}` — create repo. 201 on new, 409 if it exists.
pub async fn create(
    st: &AppState,
    route: &RepoRoute,
    headers: &HeaderMap,
    query: &str,
) -> Result<Response, ApiError> {
    let _principal = st
        .auth
        .require_write(headers)
        .await
        .map_err(ApiError::from)?;
    let format = match query
        .split('&')
        .find_map(|part| part.strip_prefix("object_format="))
    {
        Some("sha256") => ObjectFormat::Sha256,
        Some("sha1") => ObjectFormat::Sha1,
        Some(other) => {
            return Err(ApiError::BadRequest(format!(
                "unsupported object format: {other}"
            )));
        }
        None => ObjectFormat::from(st.cfg.git.object_format),
    };
    match st.registry.create(&route.id, format).await {
        Ok(_h) => Ok((StatusCode::CREATED, "created").into_response()),
        Err(walgit_wal::WalError::AlreadyExists) => {
            Ok((StatusCode::CONFLICT, "already exists").into_response())
        }
        Err(e) => Err(ApiError::from(e)),
    }
}

/// `DELETE /{owner}/{repo}` — admin-only deletion of the manifest and every object under the repo prefix.
pub async fn delete(
    st: &AppState,
    route: &RepoRoute,
    headers: &HeaderMap,
) -> Result<Response, ApiError> {
    let _principal = st
        .auth
        .require_admin(headers)
        .await
        .map_err(ApiError::from)?;
    st.registry
        .delete(&route.id)
        .await
        .map_err(ApiError::from)?;
    Ok((StatusCode::NO_CONTENT, "").into_response())
}

/// `GET /` — list repos as text/plain, one `owner/name` per line.
pub async fn list_repos(st: &AppState, headers: &HeaderMap) -> Result<Response, ApiError> {
    let _ = st
        .auth
        .require_read(headers)
        .await
        .map_err(ApiError::from)?;
    let repos = st.registry.list().await.map_err(ApiError::from)?;
    let body = repos
        .into_iter()
        .map(|r| r.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    Ok((
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; charset=utf-8",
        )],
        body,
    )
        .into_response())
}
