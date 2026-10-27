//! `walgit-nats`: the WAL's `ref` events (`docs/EVENTS.md`) published into NATS `JetStream`, with
//! the cursor in the stream, wake-ups over NATS and a `$SRV` service API instead of HTTP.
//! The event model is shared with the webhook bridge through `walgit-events`.

pub mod message;
pub mod publisher;
pub mod service;
pub mod subject;
pub mod wake;

use std::str::FromStr;

use anyhow::Context;
use async_nats::jetstream::{self, stream};
use walgit_config::NatsEventsConfig;

use crate::subject::SubjectPrefix;

/// Connect as `[events.nats]` says. The credentials path is read, never logged.
pub async fn connect(cfg: &NatsEventsConfig) -> anyhow::Result<async_nats::Client> {
    let addrs = cfg
        .urls
        .iter()
        .map(|u| async_nats::ServerAddr::from_str(u).with_context(|| format!("events.nats.urls: {u}")))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let mut options = async_nats::ConnectOptions::new().name("walgit-nats");
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
