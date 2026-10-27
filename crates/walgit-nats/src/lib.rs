//! `walgit nats`: the WAL's `ref` events (`docs/EVENTS.md`) published into NATS `JetStream`,
//! with the cursor in the stream.
//! The event model is shared with the webhook bridge through `walgit-events`.

pub mod env;
pub mod message;
pub mod publisher;
pub mod stream_shape;
pub mod subject;
#[cfg(feature = "testing")]
pub mod testing;

use anyhow::Context;
use async_nats::jetstream::{self, stream};
use walgit_config::{Config, NatsEventsConfig};

use crate::env::{NatsEnv, Servers};
use crate::subject::SubjectPrefix;

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

/// The stream holding the events, provisioned by the operator and shaped as the publisher needs
/// ([`stream_shape::problems`]), so a wrong stream fails at boot.
pub async fn open_stream(
    js: &jetstream::Context,
    cfg: &NatsEventsConfig,
    prefix: &SubjectPrefix,
) -> anyhow::Result<stream::Stream> {
    let stream = js.get_stream(&cfg.stream).await.with_context(|| {
        format!(
            "stream {} (walgit nats never creates it; provision it as docs/EVENTS.md shows)",
            cfg.stream
        )
    })?;
    let problems = stream_shape::problems(&stream.cached_info().config, prefix);
    if !problems.is_empty() {
        let problems: Vec<String> = problems.iter().map(ToString::to_string).collect();
        anyhow::bail!(
            "stream {} is not usable: {} (provision it as docs/EVENTS.md shows)",
            cfg.stream,
            problems.join("; ")
        );
    }
    Ok(stream)
}

/// The config the publisher's registry runs on. This process never applies a push itself, so a
/// wake-up must always see the manifest as it is in the bucket (`freshness_ttl` would let it
/// skip the GET and miss the push it was woken for), and it only reads manifest and log, so
/// pack prefetch would pull bytes nobody reads. Both hold whatever `[wal]` says. Its local copies
/// live under `<cache.dir>/.nats` (no repository owner starts with `.`), so a `walgit serve` on the
/// same `walgit.toml` never shares a working directory with it.
pub fn publisher_config(cfg: &Config) -> Config {
    let mut cfg = cfg.clone();
    cfg.wal.freshness_ttl = std::time::Duration::ZERO;
    cfg.wal.prefetch_packs = false;
    cfg.cache.dir = cfg.cache.dir.join(".nats");
    cfg
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_publisher_never_shares_local_copies_with_serve() {
        let serve = Config::default();
        let repo = walgit_git::RepoId::new("acme", "repo").unwrap();
        let nats = publisher_config(&serve);
        assert_eq!(nats.cache.dir, serve.cache.dir.join(".nats"));
        assert_ne!(repo.local_dir(&nats.cache.dir), repo.local_dir(&serve.cache.dir));
        assert!(walgit_git::RepoId::new(".nats", "repo").is_err(), "no owner can claim the directory");
    }
}
