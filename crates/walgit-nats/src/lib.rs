//! `walgit nats`: the WAL's `ref` events (`docs/EVENTS.md`) published into NATS `JetStream`.

pub mod env;
pub mod message;
pub mod subject;
#[cfg(feature = "testing")]
pub mod testing;

use std::str::FromStr;

use anyhow::Context;
use walgit_config::NatsEventsConfig;

use crate::env::NatsEnv;

/// Connect to `NATS_URL`, or `[events.nats] urls` when it is unset, as `env` says. Only the
/// methods are logged, never a secret.
pub async fn connect(cfg: &NatsEventsConfig, env: &NatsEnv) -> anyhow::Result<async_nats::Client> {
    let (urls, source) = match &env.servers {
        Some(urls) => (urls, crate::env::ENV_NATS_URL),
        None => (&cfg.urls, "events.nats.urls"),
    };
    let addrs = urls
        .iter()
        .map(|u| async_nats::ServerAddr::from_str(u).with_context(|| format!("{source}: {u}")))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let auth = env.description();
    tracing::info!(?urls, source, %auth, "connecting to NATS");
    env.connect_options()
        .await?
        .name("walgit")
        .connect(addrs)
        .await
        .with_context(|| format!("connecting to NATS (auth: {auth})"))
}
