//! `walgit nats`: the WAL's `ref` events (`docs/EVENTS.md`) published into NATS `JetStream`.

pub mod env;
pub mod message;
pub mod subject;
#[cfg(feature = "testing")]
pub mod testing;

use anyhow::Context;
use walgit_config::NatsEventsConfig;

use crate::env::{NatsEnv, Servers};

/// Connect to `NATS_URL`, or `[events.nats] urls` when it is unset, as `env` says. Only the
/// methods and the URLs without their credentials are logged, never a secret.
pub async fn connect(cfg: &NatsEventsConfig, env: &NatsEnv) -> anyhow::Result<async_nats::Client> {
    let (urls, source) = match &env.servers {
        Some(urls) => (urls, crate::env::ENV_NATS_URL),
        None => (&cfg.urls, "events.nats.urls"),
    };
    let servers = Servers::parse(source, urls)?;
    let url_credentials = servers.credentials.as_ref();
    let auth = env.description(url_credentials);
    tracing::info!(urls = ?servers.shown(), source, %auth, "connecting to NATS");
    env.connect_options(url_credentials)
        .await?
        .name("walgit")
        .connect(servers.addrs)
        .await
        .with_context(|| format!("connecting to NATS (auth: {auth})"))
}
