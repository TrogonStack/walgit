//! `walgit nats`: the WAL's `ref` events (`docs/EVENTS.md`) published into NATS `JetStream`, with
//! the cursor in the stream, wake-ups over NATS and a `$SRV` service API instead of HTTP.
//! The event model is shared with the webhook bridge through `walgit-events`.

pub mod message;
pub mod publisher;
pub mod service;
pub mod subject;
#[cfg(feature = "testing")]
pub mod testing;
pub mod wake;

use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context;
use async_nats::jetstream::{self, stream};
use walgit_config::{Config, NatsEventsConfig};

use crate::publisher::Publisher;
use crate::subject::SubjectPrefix;

/// Connect as `[events.nats]` says. The credentials path is read, never logged.
pub async fn connect(cfg: &NatsEventsConfig) -> anyhow::Result<async_nats::Client> {
    let addrs = cfg
        .urls
        .iter()
        .map(|u| async_nats::ServerAddr::from_str(u).with_context(|| format!("events.nats.urls: {u}")))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut options = async_nats::ConnectOptions::new().name("walgit");
    if let Some(path) = &cfg.credentials_file {
        options = options
            .credentials_file(path)
            .await
            .context("events.nats.credentials_file")?;
    }
    options.connect(addrs).await.context("connecting to NATS")
}

/// The stream holding the events: created when missing (file storage, `<prefix>.ref.>`,
/// `duplicate_window`) if `ensure_stream`, otherwise it must exist.
pub async fn open_stream(
    js: &jetstream::Context,
    cfg: &NatsEventsConfig,
    prefix: &SubjectPrefix,
) -> anyhow::Result<stream::Stream> {
    if cfg.ensure_stream {
        js.get_or_create_stream(stream::Config {
            name: cfg.stream.clone(),
            subjects: vec![prefix.stream_filter()],
            storage: stream::StorageType::File,
            duplicate_window: cfg.duplicate_window,
            ..stream::Config::default()
        })
        .await
        .with_context(|| format!("ensuring stream {}", cfg.stream))
    } else {
        js.get_stream(&cfg.stream)
            .await
            .with_context(|| format!("stream {} (events.nats.ensure_stream is off)", cfg.stream))
    }
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

/// `walgit nats`: publish until SIGTERM/ctrl-c, or fail when a worker stops.
pub async fn run(cfg: Arc<Config>) -> anyhow::Result<()> {
    let nats = &cfg.events.nats;
    let prefix = SubjectPrefix::new(&nats.subject_prefix)?;
    let store = walgit_store::open_store(&cfg).await?;
    let registry = walgit_wal::Registry::new(store, cfg.clone());

    let client = crate::connect(nats).await?;
    let js = async_nats::jetstream::new(client.clone());
    let stream = crate::open_stream(&js, nats, &prefix).await?;
    let publisher = Publisher::new(registry, js, stream, prefix.clone(), nats.start);
    tracing::info!(stream = %nats.stream, prefix = %prefix, start = ?nats.start, "walgit-nats ready");

    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn(crate::wake::notify_loop(
        client.clone(),
        nats.notify_subject.clone(),
        nats.queue_group.clone(),
        cfg.store_prefix(),
        publisher.clone(),
    ));
    tasks.spawn({
        let (client, queue_group, publisher) =
            (client.clone(), nats.queue_group.clone(), publisher.clone());
        async move { crate::service::serve(client, &prefix, &queue_group, publisher).await }
    });
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
