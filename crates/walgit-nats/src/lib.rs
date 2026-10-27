//! `walgit nats`: the WAL's `ref` events (`docs/EVENTS.md`) published into NATS `JetStream`.

pub mod auth;
pub mod message;
pub mod subject;
#[cfg(feature = "testing")]
pub mod testing;

use std::str::FromStr;

use anyhow::Context;
use walgit_config::NatsEventsConfig;

use crate::auth::NatsAuth;

/// Connect to `[events.nats] urls` with `auth`. Only the method is logged, never the secret.
pub async fn connect(cfg: &NatsEventsConfig, auth: &NatsAuth) -> anyhow::Result<async_nats::Client> {
    let addrs = cfg
        .urls
        .iter()
        .map(|u| async_nats::ServerAddr::from_str(u).with_context(|| format!("events.nats.urls: {u}")))
        .collect::<anyhow::Result<Vec<_>>>()?;
    tracing::info!(urls = ?cfg.urls, auth = auth.description(), "connecting to NATS");
    auth.connect_options()
        .await?
        .name("walgit")
        .connect(addrs)
        .await
        .with_context(|| format!("connecting to NATS (auth: {})", auth.description()))
}
