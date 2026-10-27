//! The NATS service API (`$SRV` micro protocol) in place of an HTTP listener:
//! `<prefix>.events.catch-up` takes `{"repo":"owner/name"}` and answers with the catch-up report;
//! `$SRV.PING|INFO|STATS` come with the protocol (stats carry this publisher's counters).

use std::str::FromStr;
use std::sync::Arc;

use async_nats::service::ServiceExt;
use async_nats::service::error::Error as ServiceError;
use bytes::Bytes;
use futures::StreamExt;
use walgit_git::RepoId;

use crate::publisher::Publisher;
use crate::subject::SubjectPrefix;

pub const SERVICE_NAME: &str = "walgit-events";

#[derive(serde::Deserialize)]
struct CatchUpRequest {
    repo: String,
}

fn failure(code: usize, status: &impl ToString) -> ServiceError {
    ServiceError {
        status: status.to_string(),
        code,
    }
}

async fn handle(publisher: &Publisher, payload: &[u8]) -> Result<Bytes, ServiceError> {
    let req: CatchUpRequest =
        serde_json::from_slice(payload).map_err(|e| failure(400, &format!("request: {e}")))?;
    let id = RepoId::from_str(&req.repo).map_err(|e| failure(400, &e))?;
    match publisher.catch_up_if_present(&id).await {
        Ok(Some(report)) => serde_json::to_vec(&report)
            .map(Bytes::from)
            .map_err(|e| failure(500, &e)),
        Ok(None) => Err(failure(404, &format!("no repository {id}"))),
        Err(e) => Err(failure(500, &format!("{e:#}"))),
    }
}

/// Serve until the endpoint closes. Instances share requests through the service queue group.
pub async fn serve(
    client: async_nats::Client,
    prefix: &SubjectPrefix,
    queue_group: &str,
    publisher: Arc<Publisher>,
) -> anyhow::Result<()> {
    let stats = publisher.clone();
    let service = client
        .service_builder()
        .description("walgit ref events into JetStream")
        .queue_group(queue_group)
        .stats_handler(move |_endpoint, _stats| stats.counters.snapshot())
        .start(SERVICE_NAME, env!("CARGO_PKG_VERSION"))
        .await
        .map_err(|e| anyhow::anyhow!("starting the {SERVICE_NAME} service: {e}"))?;
    let mut endpoint = service
        .group(prefix.child("events"))
        .endpoint("catch-up")
        .await
        .map_err(|e| anyhow::anyhow!("registering catch-up: {e}"))?;
    tracing::info!(subject = %prefix.child("events.catch-up"), "service ready");
    while let Some(request) = endpoint.next().await {
        let publisher = publisher.clone();
        tokio::spawn(async move {
            let answer = handle(&publisher, &request.message.payload).await;
            if let Err(e) = request.respond(answer).await {
                tracing::warn!(error = %e, "catch-up reply failed");
            }
        });
    }
    Ok(())
}
