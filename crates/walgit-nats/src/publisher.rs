//! The `JetStream` publisher: the WAL → `<prefix>.ref.<owner>.<repo>`.
//!
//! `catch_up(repo)`: the cursor is the stream. The last message on the repo's subject names the
//! WAL seq (`Walgit-Seq`) and ref it reached; the publisher reads the log from that entry
//! (inclusive: one entry can yield several events and a crash mid-entry must not drop the rest),
//! drops the events the stream already holds, and publishes the remainder one at a time, waiting
//! for each `PubAck`. The first publish of a catch-up carries
//! `Nats-Expected-Last-Subject-Sequence`, so a second publisher that got there first makes this one
//! stop (the next wake-up retries); every message carries `Nats-Msg-Id` (`<repo>:<seq>:<ref>`) so
//! `JetStream` dedups within its window. Consumers still dedup on `(repo, _walgit.seq, ref_name)`.
//!
//! The cursor is the sink's state, not walgit's (D50): wiping walgit loses nothing, wiping the
//! stream republishes the WAL window (`events.nats.start` decides where a cold repo begins).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Context;
use async_nats::jetstream::{
    self,
    context::PublishErrorKind,
    stream::{LastRawMessageErrorKind, Stream},
};
use walgit_config::NatsStart;
use walgit_events::refs_from_entries;
use walgit_git::RepoId;

use crate::message::{EventMessage, LastPublished, MsgId, after_last_published};
use crate::subject::{RepoSubject, SubjectPrefix};

/// What one `catch_up` did.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct CatchUp {
    pub repo: String,
    /// The WAL seq the stream had reached for this repository (or the cold start point).
    pub from_seq: u64,
    pub head_seq: u64,
    /// Messages published now (`JetStream`-deduplicated ones included).
    pub emitted: usize,
    /// `[first, last]` seqs folded into a checkpoint before they were read: not emitted, counted;
    /// consumers backfill from the WAL.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gap: Option<[u64; 2]>,
    /// Another publisher advanced the subject first; this catch-up stopped and the next retries.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub conflict: bool,
}

#[derive(Debug, Default)]
pub struct Counters {
    pub published: AtomicU64,
    pub deduplicated: AtomicU64,
    pub gap_entries: AtomicU64,
    pub conflicts: AtomicU64,
    pub rewinds: AtomicU64,
    pub sweep_found: AtomicU64,
}

impl Counters {
    pub fn snapshot(&self) -> serde_json::Value {
        let n = |c: &AtomicU64| c.load(Ordering::Relaxed);
        serde_json::json!({
            "published": n(&self.published),
            "deduplicated": n(&self.deduplicated),
            "gap_entries": n(&self.gap_entries),
            "conflicts": n(&self.conflicts),
            "rewinds": n(&self.rewinds),
            "sweep_found": n(&self.sweep_found),
        })
    }
}

pub struct Publisher {
    registry: Arc<walgit_wal::Registry>,
    js: jetstream::Context,
    stream: Stream,
    prefix: SubjectPrefix,
    start: NatsStart,
    serial: tokio::sync::Mutex<()>,
    /// Where each repository was fully published up to by this process: the head and the stream's
    /// last message for its subject. A wake-up that finds both unchanged spares the log read. The
    /// stream half is what notices a purged or recreated stream. Never a source of truth.
    done: std::sync::Mutex<HashMap<RepoId, (u64, Option<LastPublished>)>>,
    /// `start = "head"`: the head a cold repository was first seen at.
    cold_marks: std::sync::Mutex<HashMap<RepoId, u64>>,
    pub counters: Counters,
}

fn locked<T>(m: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

impl Publisher {
    pub fn new(
        registry: Arc<walgit_wal::Registry>,
        js: jetstream::Context,
        stream: Stream,
        prefix: SubjectPrefix,
        start: NatsStart,
    ) -> Arc<Self> {
        Arc::new(Publisher {
            registry,
            js,
            stream,
            prefix,
            start,
            serial: tokio::sync::Mutex::new(()),
            done: std::sync::Mutex::default(),
            cold_marks: std::sync::Mutex::default(),
            counters: Counters::default(),
        })
    }

    pub fn registry(&self) -> &Arc<walgit_wal::Registry> {
        &self.registry
    }

    async fn last_published(&self, subject: &RepoSubject) -> anyhow::Result<Option<LastPublished>> {
        match self
            .stream
            .get_last_raw_message_by_subject(subject.as_str())
            .await
        {
            Ok(m) => {
                let last = LastPublished::from_headers(m.sequence, &m.headers);
                anyhow::ensure!(
                    last.is_some(),
                    "last message on {subject} (stream seq {}) lacks Walgit-Seq / Nats-Msg-Id",
                    m.sequence
                );
                Ok(last)
            }
            Err(e) if matches!(e.kind(), LastRawMessageErrorKind::NoMessageFound) => Ok(None),
            Err(e) => Err(e).with_context(|| format!("reading the last message on {subject}")),
        }
    }

    /// Publish everything committed after what the stream holds for `id`.
    pub async fn catch_up(&self, id: &RepoId) -> anyhow::Result<CatchUp> {
        let _serial = self.serial.lock().await;
        let handle = self.registry.open(id).await?;
        let manifest = {
            let guard = handle.sync_refs().await?;
            let m = handle.manifest();
            drop(guard);
            m
        };
        let head = manifest.head_seq;
        // Entries below `min_seq` are folded into a checkpoint and no longer in the manifest's
        // log window; `min_seq - 1` is the newest seq that is lost to us.
        let readable_from = manifest.min_seq.saturating_sub(1);
        let lowest = readable_from + 1;

        let mut report = CatchUp {
            repo: id.to_string(),
            from_seq: head,
            head_seq: head,
            ..CatchUp::default()
        };

        // One cheap request on every wake-up: the stream, not this process, says what is published.
        let subject = RepoSubject::new(&self.prefix, id);
        let last = self.last_published(&subject).await?;
        if locked(&self.done)
            .get(id)
            .is_some_and(|(done_head, done_last)| *done_head == head && *done_last == last)
        {
            return Ok(report);
        }

        // The stream is ahead of the WAL: the repository was deleted and created again (or the log
        // rewound). What is on the subject belongs to the old incarnation, so it names no cursor;
        // its stream sequence still guards the first publish.
        let rewound = last.as_ref().is_some_and(|l| l.id.seq() > head);
        if rewound {
            self.counters.rewinds.fetch_add(1, Ordering::Relaxed);
            locked(&self.cold_marks).remove(id);
            tracing::warn!(repo = %id, published = last.as_ref().map_or(0, |l| l.id.seq()), head,
                "the stream is ahead of the WAL: the repository was recreated; publishing it as a new one");
        }
        let basis = if rewound { None } else { last.as_ref() };

        // First seq to read; `resume` is the entry the stream may hold only part of.
        let (first, resume) = match basis {
            Some(l) if l.id.seq() >= lowest => (l.id.seq(), Some(l)),
            Some(l) => {
                // The newest published entry is already folded away: what follows it up to the
                // log window is lost to us.
                let published = l.id.seq();
                if published < readable_from {
                    report.gap = Some([published + 1, readable_from]);
                    self.counters
                        .gap_entries
                        .fetch_add(readable_from - published, Ordering::Relaxed);
                    tracing::warn!(repo = %id, first = published + 1, last = readable_from,
                        "entries folded into a checkpoint before they were published; consumers backfill from the WAL");
                }
                (lowest, None)
            }
            None => match self.start {
                NatsStart::Oldest => (lowest, None),
                NatsStart::Head => {
                    let mark = *locked(&self.cold_marks).entry(id.clone()).or_insert(head);
                    (mark.saturating_add(1).max(lowest), None)
                }
            },
        };
        report.from_seq = resume.map_or(first.saturating_sub(1), |l| l.id.seq());

        let mut last_sent: Option<MsgId> = None;
        if first <= head {
            let entries = handle.read_log(&manifest, first, Some(head)).await?;
            let mut events = Vec::new();
            refs_from_entries(id, &entries, &mut events);
            if let Some(l) = resume {
                events = after_last_published(events, l);
            }
            // Whatever is on the subject now (a gap leaves older messages there too).
            let mut expected = last.as_ref().map_or(0, |l| l.stream_seq);
            for (i, event) in events.iter().enumerate() {
                last_sent = MsgId::of(event);
                let mut msg = EventMessage::new(subject.clone(), event)?;
                if i == 0 {
                    msg = msg.expecting_last_subject_sequence(expected);
                }
                let (msg_id, subj) = (msg.id.clone(), msg.subject.to_string());
                let ack = self
                    .js
                    .send_publish(subj, msg.into_publish())
                    .await
                    .with_context(|| format!("publishing {msg_id}"))?;
                match ack.await {
                    Ok(ack) => {
                        expected = ack.sequence;
                        report.emitted += 1;
                        if ack.duplicate {
                            self.counters.deduplicated.fetch_add(1, Ordering::Relaxed);
                        } else {
                            self.counters.published.fetch_add(1, Ordering::Relaxed);
                        }
                        // One structured line per published event, as the bridge logs.
                        if let Ok(body) = serde_json::to_string(event) {
                            tracing::info!(target: "walgit::event",
                                event_type = "ref", repo = %event.repo, event = %body, "event");
                        }
                    }
                    Err(e) if e.kind() == PublishErrorKind::WrongLastSequence => {
                        self.counters.conflicts.fetch_add(1, Ordering::Relaxed);
                        report.conflict = true;
                        tracing::warn!(repo = %id, "another publisher advanced {subject}; stopping this catch-up, the next wake-up retries");
                        return Ok(report);
                    }
                    Err(e) => return Err(e).with_context(|| format!("PubAck for {msg_id}")),
                }
            }
        }

        // Done only when the stream confirms its last message is the one just sent: a broker
        // that swallowed it as a duplicate of a purged message (the dedup table outlives a purge)
        // leaves the repository open, and the retry lands once the dedup window has passed.
        let settled = if report.emitted > 0 {
            self.last_published(&subject).await?
        } else {
            last
        };
        let confirmed = last_sent
            .as_ref()
            .is_none_or(|sent| settled.as_ref().is_some_and(|s| s.id == *sent));
        if confirmed {
            locked(&self.done).insert(id.clone(), (head, settled));
        }
        if report.emitted > 0 {
            tracing::info!(repo = %id, from = report.from_seq, head, emitted = report.emitted, "published");
        }
        Ok(report)
    }

    /// `catch_up` that treats a repository deleted since its notification as nothing to do.
    pub async fn catch_up_if_present(&self, id: &RepoId) -> anyhow::Result<Option<CatchUp>> {
        match self.catch_up(id).await {
            Err(e)
                if matches!(
                    e.downcast_ref::<walgit_wal::WalError>(),
                    Some(walgit_wal::WalError::NotFound)
                ) =>
            {
                Ok(None)
            }
            r => r.map(Some),
        }
    }

    /// Every repository (a `list` + one conditional manifest GET each): the backstop behind the
    /// notifications and the health check. `warn_found`: finding work here means they are not
    /// flowing (the startup sweep legitimately finds work and passes `false`).
    pub async fn sweep(&self, warn_found: bool) {
        let repos = match self.registry.list().await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "sweep list failed");
                return;
            }
        };
        for id in repos {
            match self.catch_up_if_present(&id).await {
                Ok(Some(c)) if c.emitted > 0 && warn_found => {
                    self.counters
                        .sweep_found
                        .fetch_add(c.emitted as u64, Ordering::Relaxed);
                    tracing::warn!(repo = %id, emitted = c.emitted,
                        "sweep found unpublished entries: are the bucket notifications flowing to the notify subject?");
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(repo = %id, error = %format!("{e:#}"), "sweep catch-up failed"),
            }
        }
    }
}
