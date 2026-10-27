//! The NATS service API (`$SRV` micro protocol) in place of an HTTP listener:
//! `<prefix>.events.catch-up` takes `{"repo":"owner/name"}` and answers with the catch-up report;
//! `$SRV.PING|INFO|STATS` come with the protocol (stats carry this publisher's counters).

use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use async_nats::service::ServiceExt;
use async_nats::service::error::Error as ServiceError;
use bytes::Bytes;
use futures::StreamExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use walgit_git::RepoId;

use crate::publisher::Publisher;
use crate::subject::SubjectPrefix;

pub const SERVICE_NAME: &str = "walgit-events";

/// Catch-up requests in flight at once. They serialize on the publisher, so more than this only
/// queues payloads behind notification-driven catch-up.
const MAX_PENDING_REQUESTS: usize = 64;

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

fn admit(slots: &Arc<Semaphore>) -> Result<OwnedSemaphorePermit, ServiceError> {
    slots.clone().try_acquire_owned().map_err(|_| {
        failure(
            503,
            &format!("busy: {MAX_PENDING_REQUESTS} catch-up requests already pending; retry"),
        )
    })
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

/// A registered service: the handle keeps `$SRV` answering, the endpoint carries the requests.
pub struct Registered {
    _service: async_nats::service::Service,
    endpoint: async_nats::service::endpoint::Endpoint,
}

/// Register the service and its endpoint. Returns once the server has processed the
/// subscriptions, so a request sent afterwards finds a responder. Instances share requests
/// through the service queue group.
pub async fn register(
    client: &async_nats::Client,
    prefix: &SubjectPrefix,
    queue_group: &str,
    publisher: &Arc<Publisher>,
) -> anyhow::Result<Registered> {
    let stats = publisher.clone();
    let service = client
        .service_builder()
        .description("walgit ref events into JetStream")
        .queue_group(queue_group)
        .stats_handler(move |_endpoint, _stats| stats.counters.snapshot())
        .start(SERVICE_NAME, env!("CARGO_PKG_VERSION"))
        .await
        .map_err(|e| anyhow::anyhow!("starting the {SERVICE_NAME} service: {e}"))?;
    let endpoint = service
        .group(prefix.child("events"))
        .endpoint("catch-up")
        .await
        .map_err(|e| anyhow::anyhow!("registering catch-up: {e}"))?;
    client.flush().await?;
    tracing::info!(subject = %prefix.child("events.catch-up"), "service ready");
    Ok(Registered {
        _service: service,
        endpoint,
    })
}

/// Serve until the endpoint closes.
pub async fn serve(registered: Registered, publisher: Arc<Publisher>) -> anyhow::Result<()> {
    let Registered {
        _service,
        mut endpoint,
    } = registered;
    let slots = Arc::new(Semaphore::new(MAX_PENDING_REQUESTS));
    while let Some(request) = endpoint.next().await {
        let permit = match admit(&slots) {
            Ok(permit) => permit,
            Err(busy) => {
                publisher.counters.refused.fetch_add(1, Ordering::Relaxed);
                if let Err(e) = request.respond(Err(busy)).await {
                    tracing::warn!(error = %e, "catch-up reply failed");
                }
                continue;
            }
        };
        let publisher = publisher.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let answer = handle(&publisher, &request.message.payload).await;
            if let Err(e) = request.respond(answer).await {
                tracing::warn!(error = %e, "catch-up reply failed");
            }
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_beyond_the_backlog_are_refused_until_one_finishes() {
        let slots = Arc::new(Semaphore::new(MAX_PENDING_REQUESTS));
        let mut held: Vec<_> = (0..MAX_PENDING_REQUESTS)
            .map(|_| admit(&slots).unwrap())
            .collect();
        let busy = admit(&slots).unwrap_err();
        assert_eq!(busy.code, 503);
        assert!(busy.status.starts_with("busy"), "{}", busy.status);
        held.pop();
        assert!(admit(&slots).is_ok());
    }
}
