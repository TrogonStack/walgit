//! `walgit-nats`: the WAL's `ref` events (`docs/EVENTS.md`) published into NATS `JetStream`, with
//! the cursor in the stream, wake-ups over NATS and a `$SRV` service API instead of HTTP.
//! The event model is shared with the webhook bridge through `walgit-events`.

pub mod env;
pub mod message;
pub mod publisher;
pub mod service;
pub mod stream_shape;
pub mod subject;
#[cfg(feature = "testing")]
pub mod testing;
pub mod wake;

use std::sync::Arc;

use anyhow::Context;
use async_nats::jetstream::{self, stream};
use walgit_config::{Config, NatsConfig, NatsEventsConfig};

use crate::env::{NatsEnv, Servers};
use crate::publisher::Publisher;
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
            "stream {} (walgit-nats never creates it; provision it as docs/EVENTS.md shows)",
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

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
                return;
            }
            Err(e) => tracing::warn!(error = %e, "no SIGTERM handler"),
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

/// The full config the publisher's registry runs on, built from the sections this binary reads
/// (the rest stay at their defaults, never consulted). This process never applies a push itself,
/// so a wake-up must always see the manifest as it is in the bucket (`freshness_ttl` would let it
/// skip the GET and miss the push it was woken for), and it only reads manifest and log, so
/// pack prefetch would pull bytes nobody reads. Both hold whatever `[wal]` says. Its local copies
/// live under `<cache.dir>/.nats` (no repository owner starts with `.`), so a `walgit serve` on the
/// same `walgit.toml` never shares a working directory with it.
pub fn publisher_config(cfg: &NatsConfig) -> Config {
    let mut registry = Config {
        store: cfg.store.clone(),
        cache: cfg.cache.clone(),
        wal: cfg.wal.clone(),
        telemetry: cfg.telemetry.clone(),
        ..Config::default()
    };
    registry.wal.freshness_ttl = std::time::Duration::ZERO;
    registry.wal.prefetch_packs = false;
    registry.cache.dir = registry.cache.dir.join(".nats");
    registry
}

/// `walgit-nats`: publish until SIGTERM/ctrl-c, or fail when a worker stops.
pub async fn run(cfg: NatsConfig) -> anyhow::Result<()> {
    let nats = &cfg.events.nats;
    let prefix = SubjectPrefix::new(&nats.subject_prefix)?;
    let registry_cfg = Arc::new(publisher_config(&cfg));
    let store = walgit_store::open_store(&registry_cfg).await?;
    let store_prefix = registry_cfg.store_prefix();
    let registry = walgit_wal::Registry::new(store, registry_cfg);

    let client = crate::connect(nats, &NatsEnv::from_process_env()?).await?;
    let js = async_nats::jetstream::new(client.clone());
    let stream = crate::open_stream(&js, nats, &prefix).await?;
    let publisher = Publisher::new(registry, js, stream, prefix.clone(), nats.start);
    let sub = crate::wake::subscribe(&client, nats.notify_subject.clone(), nats.queue_group.clone())
        .await?;
    let registered =
        crate::service::register(&client, &prefix, &nats.queue_group, &publisher).await?;
    tracing::info!(stream = %nats.stream, prefix = %prefix, start = ?nats.start, "walgit-nats ready");

    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn(crate::wake::notify_loop(sub, store_prefix, publisher.clone()));
    tasks.spawn(crate::service::serve(registered, publisher.clone()));
    tasks.spawn({
        let publisher = publisher.clone();
        let every = cfg.events.sweep_interval;
        async move {
            crate::wake::sweep_loop(publisher, every).await;
            std::future::pending::<anyhow::Result<()>>().await
        }
    });

    tokio::select! {
        () = shutdown_signal() => tracing::info!("shutting down"),
        Some(done) = tasks.join_next() => {
            // A loop ending is a failure: the subscription or the service closed under us.
            done.context("worker panicked")?.context("worker stopped")?;
            anyhow::bail!("a worker stopped unexpectedly");
        }
    }
    tasks.shutdown().await;
    client.flush().await.ok();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_publisher_never_shares_local_copies_with_serve() {
        let serve = Config::default();
        let nats_cfg = NatsConfig { cache: serve.cache.clone(), ..NatsConfig::default() };
        let repo = walgit_git::RepoId::new("acme", "repo").unwrap();
        let nats = publisher_config(&nats_cfg);
        assert_eq!(nats.cache.dir, serve.cache.dir.join(".nats"));
        assert_ne!(repo.local_dir(&nats.cache.dir), repo.local_dir(&serve.cache.dir));
        assert!(walgit_git::RepoId::new(".nats", "repo").is_err(), "no owner can claim the directory");
    }

    #[test]
    fn the_registry_config_keeps_what_the_operator_set_and_adapts_the_rest() {
        let mut cfg = NatsConfig::default();
        cfg.store.bucket = "acme".into();
        cfg.store.prefix = "p".into();
        cfg.wal.freshness_ttl = std::time::Duration::from_mins(10);
        cfg.wal.prefetch_packs = true;
        cfg.wal.max_batch = 3;
        let registry = publisher_config(&cfg);
        assert_eq!(registry.store_prefix(), "p/");
        assert_eq!(registry.store.bucket, "acme");
        assert_eq!(registry.wal.max_batch, 3);
        assert_eq!(registry.wal.freshness_ttl, std::time::Duration::ZERO);
        assert!(!registry.wal.prefetch_packs);
    }
}
