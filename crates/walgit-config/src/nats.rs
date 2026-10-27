//! `walgit-nats`'s configuration: the sections of `walgit.toml` it reads, under the same
//! `WALGIT__` environment prefix as the server. Everything else in a shared file is skipped.

use std::time::Duration;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::{CacheConfig, Settings, StoreConfig, TelemetryConfig, WalConfig};

pub const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_mins(5);

/// What `walgit-nats --config` reads: `[store]`, `[cache]`, `[wal]`, `[telemetry]`, and under
/// `[events]` only `sweep_interval` and `[events.nats]`. The server's sections and the webhook
/// keys of `[events]` are skipped, neither parsed nor validated, so one `walgit.toml` serves both
/// binaries and an invalid `[server.auth]` cannot stop this one from booting.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct NatsConfig {
    pub store: StoreConfig,
    pub cache: CacheConfig,
    pub wal: WalConfig,
    pub telemetry: TelemetryConfig,
    pub events: SinkEventsConfig,
}

/// The part of `[events]` the `JetStream` sink reads.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct SinkEventsConfig {
    /// Catch-all sweep over every repo, the backstop behind bucket notifications. `0` = off.
    #[serde(with = "humantime_serde")]
    pub sweep_interval: Duration,
    pub nats: NatsEventsConfig,
}

impl Default for SinkEventsConfig {
    fn default() -> Self {
        SinkEventsConfig {
            sweep_interval: DEFAULT_SWEEP_INTERVAL,
            nats: NatsEventsConfig::default(),
        }
    }
}

impl Settings for NatsConfig {
    const FOREIGN: &'static [&'static [&'static str]] = &[
        &["server"],
        &["packs"],
        &["refs"],
        &["packfile_uri"],
        &["maintenance"],
        &["placement"],
        &["lfs"],
        &["git"],
        &["upstream"],
        &["events", "webhook_url"],
        &["events", "webhook_secret"],
    ];

    fn telemetry(&self) -> &TelemetryConfig {
        &self.telemetry
    }

    fn validate(&self) -> Result<()> {
        self.store.validate()?;
        self.wal.validate()?;
        self.events.nats.validate()
    }
}

/// Where a cold repository (no message on its subject yet) starts publishing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum NatsStart {
    /// The oldest readable seq (`min_seq - 1`), as the webhook bridge does.
    #[default]
    Oldest,
    /// Nothing older than the head the publisher first sees.
    Head,
}

/// `[events.nats]`: `walgit-nats` publishes the bridge's `ref` events into `JetStream`.
/// The resume cursor is the last message on each repository's subject, so there is no cursor
/// object in the bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct NatsEventsConfig {
    /// Server URLs (`nats://host:4222`); the client connects to the first reachable one.
    pub urls: Vec<String>,
    /// The `JetStream` stream holding the events (subjects `<subject_prefix>.ref.>`).
    pub stream: String,
    /// Events go to `<subject_prefix>.ref.<owner>.<repo>`.
    pub subject_prefix: String,
    /// Core subject bucket notifications are published to (S3 event JSON, `{"key"}` or `{"repo"}`).
    pub notify_subject: String,
    /// Queue group of the notify subscription and the service endpoints: instances share the work.
    pub queue_group: String,
    pub start: NatsStart,
}

impl Default for NatsEventsConfig {
    fn default() -> Self {
        NatsEventsConfig {
            urls: vec!["nats://127.0.0.1:4222".into()],
            stream: "WALGIT_EVENTS".into(),
            subject_prefix: "walgit".into(),
            notify_subject: "walgit.notify".into(),
            queue_group: "walgit-events".into(),
            start: NatsStart::Oldest,
        }
    }
}

impl NatsEventsConfig {
    pub fn validate(&self) -> Result<()> {
        anyhow::ensure!(!self.urls.is_empty(), "events.nats.urls must not be empty");
        anyhow::ensure!(
            self.urls.iter().all(|u| !u.trim().is_empty()),
            "events.nats.urls must not contain empty entries"
        );
        let plain = |s: &str| {
            !s.is_empty()
                && !s
                    .chars()
                    .any(|c| c.is_whitespace() || c.is_control() || matches!(c, '*' | '>'))
        };
        let dotted = |s: &str| plain(s) && s.split('.').all(|t| !t.is_empty());
        anyhow::ensure!(
            plain(&self.stream) && !self.stream.contains(['.', '/', '\\']),
            "events.nats.stream must be a stream name (no '.', '/', '\\', '*', '>' or whitespace)"
        );
        anyhow::ensure!(
            dotted(&self.subject_prefix),
            "events.nats.subject_prefix must be dot-separated tokens (no '*', '>' or whitespace)"
        );
        anyhow::ensure!(
            dotted(&self.notify_subject),
            "events.nats.notify_subject must be a literal subject (no '*', '>' or whitespace)"
        );
        anyhow::ensure!(
            plain(&self.queue_group),
            "events.nats.queue_group must not be empty or contain '*', '>' or whitespace"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Config;

    fn vars(pairs: &[(&str, &str)]) -> impl Iterator<Item = (String, String)> + use<> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect::<Vec<_>>()
            .into_iter()
    }

    #[test]
    fn the_example_files_load_as_they_are() {
        for text in [
            include_str!("../../../walgit.example.toml"),
            include_str!("../../../walgit.standalone.toml"),
        ] {
            NatsConfig::parse(text).unwrap();
        }
    }

    #[test]
    fn a_small_file_with_only_its_sections_loads() {
        let c = NatsConfig::parse(
            r#"
[store]
bucket = "b"

[events]
sweep_interval = "0s"

[events.nats]
urls = ["nats://a:4222", "nats://b:4222"]
start = "head"
subject_prefix = "acme.git"
"#,
        )
        .unwrap();
        assert_eq!(c.store.bucket, "b");
        assert_eq!(c.events.sweep_interval, Duration::ZERO);
        assert_eq!(c.events.nats.start, NatsStart::Head);
        assert_eq!(c.events.nats.urls.len(), 2);
    }

    #[test]
    fn it_defaults_to_the_servers_defaults() {
        let c = NatsConfig::parse("").unwrap();
        let server = Config::parse("").unwrap();
        assert_eq!(c.events.sweep_interval, server.events.sweep_interval);
        assert_eq!(c.events.nats.stream, "WALGIT_EVENTS");
        assert_eq!(c.events.nats.start, NatsStart::Oldest);
    }

    #[test]
    fn every_server_section_is_either_read_or_skipped() {
        let mut server = Config::default();
        server.events.webhook_url = Some("https://hooks.example.com/walgit".into());
        server.events.webhook_secret = Some("s".into());
        let text = toml::to_string(&server).unwrap();
        let c = NatsConfig::parse(&text).unwrap();
        assert_eq!(c.store.bucket, server.store.bucket);
    }

    #[test]
    fn server_only_sections_are_not_validated() {
        let c = NatsConfig::parse(
            r#"
[server.auth]
mode = "oidc"
issuer = "nonsense"
[server.tls]
mode = "files"
[placement]
maintain = ["a/b"]
not_a_key = 1
[events]
webhook_url = "ftp://x"
unknown_to_nobody = 1
"#,
        );
        let err = c.unwrap_err();
        assert!(format!("{err:#}").contains("unknown_to_nobody"), "{err:#}");
        NatsConfig::parse(
            "[server.auth]\nmode = \"oidc\"\nissuer = \"nonsense\"\n[events]\nwebhook_url = \"ftp://x\"\n",
        )
        .unwrap();
    }

    #[test]
    fn a_typo_inside_a_section_it_reads_fails() {
        for text in [
            "[events.nats]\nstrem = \"X\"\n",
            "[store]\nbuckt = \"x\"\n",
            "[wal]\nnope = 1\n",
        ] {
            assert!(NatsConfig::parse(text).is_err(), "{text}");
        }
        let err = NatsConfig::parse("[events.nats]\nstrem = \"X\"\n").unwrap_err();
        assert!(format!("{err:#}").contains("strem"), "{err:#}");
    }

    #[test]
    fn its_own_sections_are_validated() {
        for bad in [
            "subject_prefix = \"a.*\"",
            "subject_prefix = \"a..b\"",
            "notify_subject = \"x.>\"",
            "stream = \"a.b\"",
            "queue_group = \"\"",
            "urls = []",
        ] {
            let err = NatsConfig::parse(&format!("[events.nats]\n{bad}\n")).unwrap_err();
            assert!(err.to_string().contains("events.nats"), "{bad}: {err}");
        }
        assert!(NatsConfig::parse("[store]\nbucket = \"\"\n").is_err());
    }

    #[test]
    fn env_overrides_apply_and_foreign_ones_are_skipped() {
        let mut c = NatsConfig::default();
        let ignored = c
            .apply_env_report(vars(&[
                ("WALGIT__EVENTS__NATS__STREAM", "ACME"),
                ("WALGIT__STORE__BUCKET", "env-bucket"),
                ("WALGIT__EVENTS__NATS__START", "head"),
                ("WALGIT__SERVER__LISTEN", "0.0.0.0:1"),
                ("WALGIT__SERVER__AUTH__MODE", "none"),
                ("WALGIT__EVENTS__WEBHOOK_URL", "https://h.example.com"),
                ("WALGIT__EVENTS__NATS__NOPE", "1"),
                ("PORT", "9999"),
            ]))
            .unwrap();
        assert_eq!(c.events.nats.stream, "ACME");
        assert_eq!(c.store.bucket, "env-bucket");
        assert_eq!(c.events.nats.start, NatsStart::Head);
        assert_eq!(ignored.len(), 1, "{ignored:?}");
        assert!(ignored[0].0.ends_with("NATS__NOPE"));
    }
}
