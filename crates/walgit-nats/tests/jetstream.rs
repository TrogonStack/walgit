// Test fixtures use panics to fail the test, including shared helper functions.
#![allow(clippy::indexing_slicing, clippy::unwrap_used, clippy::expect_used)]

//! The `JetStream` publisher against a real `nats-server -js` and the in-memory walgit store:
//! the stream is the cursor (resume without republishing) and crash-mid-entry completion.
//! Skips (passes) when `nats-server` is not on PATH.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_nats::jetstream;
use walgit_config::{Config, NatsStart};
use walgit_events::refs_from_entries;
use walgit_git::{ObjectFormat, RepoId};
use walgit_nats::env::NatsEnv;
use walgit_nats::message::EventMessage;
use walgit_nats::publisher::Publisher;
use walgit_nats::subject::{RepoSubject, SubjectPrefix};
use walgit_nats::testing::{NatsServer, provision_stream, start_nats};
use walgit_proto::v1::{RefTransaction, RefUpdate};
use async_nats::jetstream::message::PublishMessage;

const ZERO: &str = "0000000000000000000000000000000000000000";
const A: &str = "1111111111111111111111111111111111111111";
const B: &str = "2222222222222222222222222222222222222222";
const C: &str = "3333333333333333333333333333333333333333";

struct Rig {
    cfg: Arc<Config>,
    store: Arc<walgit_store::memory::MemoryStore>,
    registry: Arc<walgit_wal::Registry>,
    client: async_nats::Client,
    js: jetstream::Context,
    prefix: SubjectPrefix,
    window: Duration,
    _cache: tempfile::TempDir,
}

impl Rig {
    async fn new(nats: &NatsServer, start: NatsStart) -> Self {
        Self::with_window(nats, start, Duration::from_hours(1)).await
    }

    /// `window`: the provisioned stream's `duplicate_window`.
    async fn with_window(nats: &NatsServer, start: NatsStart, window: Duration) -> Self {
        let rig = Self::unprovisioned(nats, start, window).await;
        rig.provision().await;
        rig
    }

    /// A rig whose stream the test provisions itself.
    async fn unprovisioned(nats: &NatsServer, start: NatsStart, window: Duration) -> Self {
        let cache = tempfile::tempdir().unwrap();
        let mut cfg = Config::default();
        cfg.store.backend = walgit_config::StoreBackend::Memory;
        cfg.cache.dir = cache.path().to_path_buf();
        cfg.events.nats.urls = vec![nats.url.clone()];
        cfg.events.nats.start = start;
        let cfg = Arc::new(cfg);
        let store = Arc::new(walgit_store::memory::MemoryStore::new());
        let registry = walgit_wal::Registry::new(store.clone(), cfg.clone());
        let client = walgit_nats::connect(&cfg.events.nats, &NatsEnv::default()).await.unwrap();
        let js = jetstream::ContextBuilder::new()
            .timeout(Duration::from_secs(3))
            .build(client.clone());
        let prefix = SubjectPrefix::new(&cfg.events.nats.subject_prefix).unwrap();
        Rig {
            cfg,
            store,
            registry,
            client,
            js,
            prefix,
            window,
            _cache: cache,
        }
    }

    async fn provision(&self) {
        provision_stream(&self.js, &self.cfg.events.nats, self.window).await;
    }

    /// A publisher with no memory of earlier ones, as after a restart.
    async fn publisher(&self, start: NatsStart) -> Arc<Publisher> {
        let stream = walgit_nats::open_stream(&self.js, &self.cfg.events.nats, &self.prefix)
            .await
            .unwrap();
        Publisher::new(
            self.registry.clone(),
            self.js.clone(),
            stream,
            self.prefix.clone(),
            start,
        )
    }

    /// What `walgit nats` runs: its own registry over the same store (it never sees the
    /// writer's pushes except through the bucket) on the operator's `[wal]` settings, as
    /// `run` adapts them. Returns the registry's cache dir guard too.
    async fn operator_publisher(
        &self,
        operator: impl FnOnce(&mut Config),
    ) -> (Arc<Publisher>, Arc<walgit_wal::Registry>, tempfile::TempDir) {
        let cache = tempfile::tempdir().unwrap();
        let mut cfg = (*self.cfg).clone();
        cfg.cache.dir = cache.path().to_path_buf();
        operator(&mut cfg);
        let registry = walgit_wal::Registry::new(
            self.store.clone(),
            Arc::new(walgit_nats::publisher_config(&cfg)),
        );
        let stream = walgit_nats::open_stream(&self.js, &self.cfg.events.nats, &self.prefix)
            .await
            .unwrap();
        let publisher = Publisher::new(
            registry.clone(),
            self.js.clone(),
            stream,
            self.prefix.clone(),
            NatsStart::Oldest,
        );
        (publisher, registry, cache)
    }

    async fn push(&self, id: &RepoId, updates: &[(&str, &str, &str)]) -> u64 {
        let handle = self.registry.open(id).await.unwrap();
        let txn = RefTransaction {
            updates: updates
                .iter()
                .map(|(name, old, new)| RefUpdate {
                    name: (*name).into(),
                    old_oid: (*old).into(),
                    new_oid: (*new).into(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        let meta = HashMap::from([
            ("principal".to_string(), "alice".to_string()),
            ("request_id".to_string(), "rid".to_string()),
        ]);
        let r = handle.publish_ref_update(txn, meta).await.unwrap();
        assert!(r.seq > 0, "push rejected: {:?}", r.per_ref);
        r.seq
    }

    /// The events (one per ref) that WAL entry `seq` yields.
    async fn events_of(&self, id: &RepoId, seq: u64) -> Vec<walgit_events::RefEvent> {
        let handle = self.registry.open(id).await.unwrap();
        drop(handle.sync_refs().await.unwrap());
        let manifest = handle.manifest();
        let entries = handle.read_log(&manifest, seq, Some(seq)).await.unwrap();
        let mut events = Vec::new();
        refs_from_entries(id, &entries, &mut events);
        events
    }

    /// Publish a message on the repo's subject as walgit would, but for a ref the entry does not
    /// contain: the stream's last message for the subject then names something `catch_up` cannot
    /// find in the entry, so it holds another incarnation and the repository is published again
    /// from the oldest readable entry. Returns its stream sequence.
    async fn inject(&self, id: &RepoId, seq: u64, ref_name: &str) -> u64 {
        let msg = PublishMessage::build()
            .payload(format!(r#"{{"old":"{ZERO}","new":"{A}"}}"#).into())
            .message_id(format!("{id}:{seq}:{ref_name}"))
            .header("Walgit-Seq", seq.to_string().as_str())
            .header("Walgit-Repo", id.to_string().as_str());
        let subject = RepoSubject::new(&self.prefix, id).to_string();
        self.js
            .send_publish(subject, msg)
            .await
            .unwrap()
            .await
            .unwrap()
            .sequence
    }

    /// `Nats-Msg-Id` of every message in the stream, in stream order.
    async fn ids(&self) -> Vec<String> {
        let stream = self.stream().await;
        let state = stream.get_info().await.unwrap().state;
        let mut out = Vec::new();
        if state.messages == 0 {
            return out;
        }
        for seq in state.first_sequence..=state.last_sequence {
            if let Ok(raw) = stream.get_raw_message(seq).await {
                out.push(
                    raw.headers
                        .get("Nats-Msg-Id")
                        .map(|v| v.as_str().to_string())
                        .unwrap_or_default(),
                );
            }
        }
        out
    }

    async fn wait_connected(&self) {
        let t0 = Instant::now();
        while self.client.connection_state() != async_nats::connection::State::Connected {
            assert!(t0.elapsed() < Duration::from_secs(20), "client did not reconnect");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn stream(&self) -> jetstream::stream::Stream {
        self.js
            .get_stream(&self.cfg.events.nats.stream)
            .await
            .unwrap()
    }

    async fn message_count(&self) -> u64 {
        self.stream().await.get_info().await.unwrap().state.messages
    }

}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_is_the_cursor() {
    let Some(nats) = start_nats().await else { return };
    let rig = Rig::new(&nats, NatsStart::Oldest).await;
    let id = RepoId::new("acme.io", "mono.repo").unwrap();
    rig.registry.create(&id, ObjectFormat::Sha1).await.unwrap();
    rig.push(&id, &[("refs/heads/main", ZERO, A)]).await;
    rig.push(&id, &[("refs/heads/main", A, B)]).await;

    let publisher = rig.publisher(NatsStart::Oldest).await;
    let r = publisher.catch_up(&id).await.unwrap();
    assert_eq!((r.from_seq, r.head_seq, r.emitted, r.conflict), (0, 2, 2, false));
    assert_eq!(rig.message_count().await, 2);

    let stream = rig.stream().await;
    let first = stream.get_raw_message(1).await.unwrap();
    assert_eq!(first.subject.as_str(), "walgit.ref.acme%2Eio.mono%2Erepo");
    assert_eq!(first.headers.get("Walgit-Seq").unwrap().as_str(), "1");
    assert_eq!(first.headers.get("Walgit-Repo").unwrap().as_str(), "acme.io/mono.repo");
    assert_eq!(
        first.headers.get("Nats-Msg-Id").unwrap().as_str(),
        "acme.io/mono.repo:1:refs/heads/main"
    );
    let body: serde_json::Value = serde_json::from_slice(&first.payload).unwrap();
    assert_eq!(body["action"], "create");
    assert_eq!(body["old"], ZERO);
    assert_eq!(body["new"], A);
    assert_eq!(body["repo"], "acme.io/mono.repo");
    assert_eq!(body["pusher"], "alice");
    assert_eq!(body["_walgit"]["seq"], "1");
    let second = stream.get_raw_message(2).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&second.payload).unwrap();
    assert_eq!((body["action"].as_str(), body["_walgit"]["seq"].as_str()), (Some("update"), Some("2")));

    // Restart: a publisher with no memory resumes from the stream and publishes nothing.
    let restarted = rig.publisher(NatsStart::Oldest).await;
    let r = restarted.catch_up(&id).await.unwrap();
    assert_eq!((r.from_seq, r.emitted), (2, 0));
    assert_eq!(rig.message_count().await, 2, "resume must not republish");

    // New pushes are published, and only those.
    rig.push(&id, &[("refs/heads/main", B, ZERO)]).await;
    let r = restarted.catch_up(&id).await.unwrap();
    assert_eq!((r.from_seq, r.head_seq, r.emitted), (2, 3, 1));
    assert_eq!(rig.message_count().await, 3);
    let third = stream.get_raw_message(3).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&third.payload).unwrap();
    assert_eq!((body["action"].as_str(), body["new"].as_str()), (Some("delete"), Some(ZERO)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_mid_entry_finishes_that_entry_only() {
    let Some(nats) = start_nats().await else { return };
    let rig = Rig::new(&nats, NatsStart::Oldest).await;
    let id = RepoId::new("t", "r").unwrap();
    rig.registry.create(&id, ObjectFormat::Sha1).await.unwrap();
    rig.push(&id, &[("refs/heads/main", ZERO, A)]).await;
    let publisher = rig.publisher(NatsStart::Oldest).await;
    assert_eq!(publisher.catch_up(&id).await.unwrap().emitted, 1);

    // One entry, three refs; the "crash" leaves only the first event in the stream.
    let seq = rig
        .push(
            &id,
            &[
                ("refs/heads/a", ZERO, B),
                ("refs/heads/b", ZERO, B),
                ("refs/tags/c", ZERO, C),
            ],
        )
        .await;
    let handle = rig.registry.open(&id).await.unwrap();
    drop(handle.sync_refs().await.unwrap());
    let manifest = handle.manifest();
    let entries = handle.read_log(&manifest, seq, Some(seq)).await.unwrap();
    let mut events = Vec::new();
    refs_from_entries(&id, &entries, &mut events);
    assert_eq!(events.len(), 3);
    let msg = EventMessage::new(RepoSubject::new(&rig.prefix, &id), &events[0]).unwrap();
    let subject = msg.subject.to_string();
    rig.js
        .send_publish(subject, msg.into_publish())
        .await
        .unwrap()
        .await
        .unwrap();
    assert_eq!(rig.message_count().await, 2);

    let restarted = rig.publisher(NatsStart::Oldest).await;
    let r = restarted.catch_up(&id).await.unwrap();
    assert_eq!((r.from_seq, r.emitted), (seq, 2), "the rest of the entry, nothing twice");
    assert_eq!(rig.message_count().await, 4);
    let last = rig.stream().await.get_raw_message(4).await.unwrap();
    assert_eq!(
        last.headers.get("Nats-Msg-Id").unwrap().as_str(),
        format!("t/r:{seq}:refs/tags/c")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn head_start_publishes_nothing_older() {
    let Some(nats) = start_nats().await else { return };
    let rig = Rig::new(&nats, NatsStart::Head).await;
    let id = RepoId::new("t", "r").unwrap();
    rig.registry.create(&id, ObjectFormat::Sha1).await.unwrap();
    rig.push(&id, &[("refs/heads/main", ZERO, A)]).await;
    let publisher = rig.publisher(NatsStart::Head).await;
    assert_eq!(publisher.catch_up(&id).await.unwrap().emitted, 0);
    assert_eq!(rig.message_count().await, 0);
    rig.push(&id, &[("refs/heads/main", A, B)]).await;
    let r = publisher.catch_up(&id).await.unwrap();
    assert_eq!(r.emitted, 1);
    let m = rig.stream().await.get_raw_message(1).await.unwrap();
    assert_eq!(m.headers.get("Walgit-Seq").unwrap().as_str(), "2");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_publisher_that_moved_first_stops_this_one() {
    let Some(nats) = start_nats().await else { return };
    let rig = Rig::new(&nats, NatsStart::Oldest).await;
    let id = RepoId::new("t", "r").unwrap();
    rig.registry.create(&id, ObjectFormat::Sha1).await.unwrap();
    rig.push(&id, &[("refs/heads/main", ZERO, A)]).await;
    let publisher = rig.publisher(NatsStart::Oldest).await;
    publisher.catch_up(&id).await.unwrap();

    // The subject moves (another instance published a different entry) between our read of the
    // last message and our first publish: emulate by publishing a foreign message afterwards,
    // with a stale expectation.
    rig.push(&id, &[("refs/heads/main", A, B)]).await;
    let handle = rig.registry.open(&id).await.unwrap();
    drop(handle.sync_refs().await.unwrap());
    let manifest = handle.manifest();
    let entries = handle.read_log(&manifest, 2, Some(2)).await.unwrap();
    let mut events = Vec::new();
    refs_from_entries(&id, &entries, &mut events);
    let stale = EventMessage::new(RepoSubject::new(&rig.prefix, &id), &events[0])
        .unwrap()
        .expecting_last_subject_sequence(0);
    let subject = stale.subject.to_string();
    let err = rig
        .js
        .send_publish(subject, stale.into_publish())
        .await
        .unwrap()
        .await
        .unwrap_err();
    assert_eq!(err.kind(), jetstream::context::PublishErrorKind::WrongLastSequence);
    assert_eq!(rig.message_count().await, 1);
}

// ---------------------------------------------------------------------------------------------
// Dedup, races, wipes, gaps and outages.
// ---------------------------------------------------------------------------------------------

async fn repo_with_three_events(rig: &Rig, owner: &str, name: &str) -> RepoId {
    let id = RepoId::new(owner, name).unwrap();
    rig.registry.create(&id, ObjectFormat::Sha1).await.unwrap();
    rig.push(&id, &[("refs/heads/main", ZERO, A)]).await;
    rig.push(&id, &[("refs/heads/a", ZERO, B), ("refs/heads/b", ZERO, B)]).await;
    id
}

fn msg_id(id: &RepoId, seq: u64, name: &str) -> String {
    format!("{id}:{seq}:{name}")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broker_dedup_absorbs_a_republish_inside_the_window() {
    let Some(nats) = start_nats().await else { return };
    let rig = Rig::new(&nats, NatsStart::Oldest).await;
    let id = repo_with_three_events(&rig, "t", "r").await;
    let first = rig.publisher(NatsStart::Oldest).await;
    assert_eq!(first.catch_up(&id).await.unwrap().emitted, 3);

    // A hand-built republish of an existing Nats-Msg-Id is acknowledged as a duplicate.
    let event = rig.events_of(&id, 1).await.remove(0);
    let msg = EventMessage::new(RepoSubject::new(&rig.prefix, &id), &event).unwrap();
    let subject = msg.subject.to_string();
    let ack = rig.js.send_publish(subject, msg.into_publish()).await.unwrap().await.unwrap();
    assert!(ack.duplicate, "same Nats-Msg-Id inside the window must be a duplicate");
    assert_eq!(rig.message_count().await, 3);

    // The real path: the last message on the subject names a ref the entry does not contain, so
    // the stream holds another incarnation and the whole repository is republished.
    rig.inject(&id, 2, "refs/heads/not-in-the-entry").await;
    assert_eq!(rig.message_count().await, 4);
    let restarted = rig.publisher(NatsStart::Oldest).await;
    let r = restarted.catch_up(&id).await.unwrap();
    assert_eq!((r.from_seq, r.emitted, r.conflict), (0, 3, false), "{r:?}");
    let counters = restarted.counters.snapshot();
    assert_eq!(counters["deduplicated"], 3, "every republished event was a broker duplicate");
    assert_eq!(counters["published"], 0);
    assert_eq!(rig.message_count().await, 4, "the stream did not grow");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_publishers_leave_each_event_exactly_once() {
    let Some(nats) = start_nats().await else { return };
    let rig = Rig::new(&nats, NatsStart::Oldest).await;
    let mut expected = Vec::new();
    let mut ids = Vec::new();
    for round in 0..6 {
        for n in 0..3 {
            let id = RepoId::new("race", format!("r{round}x{n}").as_str()).unwrap();
            rig.registry.create(&id, ObjectFormat::Sha1).await.unwrap();
            for seq in 1..=4u64 {
                let a = format!("refs/heads/a{seq}");
                let b = format!("refs/heads/b{seq}");
                rig.push(&id, &[(a.as_str(), ZERO, A), (b.as_str(), ZERO, B)]).await;
                expected.push(msg_id(&id, seq, &a));
                expected.push(msg_id(&id, seq, &b));
            }
            ids.push(id);
        }
    }
    let one = rig.publisher(NatsStart::Oldest).await;
    let two = rig.publisher(NatsStart::Oldest).await;
    let mut reports = Vec::new();
    for batch in ids.chunks(3) {
        let (a, b) = (batch[0].clone(), batch[1].clone());
        let c = batch[2].clone();
        let runs = tokio::join!(
            one.catch_up(&a),
            two.catch_up(&a),
            one.catch_up(&b),
            two.catch_up(&b),
            two.catch_up(&c),
            one.catch_up(&c),
        );
        for r in [runs.0, runs.1, runs.2, runs.3, runs.4, runs.5] {
            reports.push(r.expect("a lost race is a conflict or a dedup, never an error"));
        }
    }
    let mut seen = rig.ids().await;
    let total = seen.len();
    seen.sort();
    seen.dedup();
    assert_eq!(seen.len(), total, "a (repo, seq, ref) landed twice");
    expected.sort();
    assert_eq!(seen, expected, "every event exactly once");

    let (one, two) = (one.counters.snapshot(), two.counters.snapshot());
    let sum = |key: &str| one[key].as_u64().unwrap() + two[key].as_u64().unwrap();
    assert_eq!(sum("published"), total as u64, "each landed message was counted once");
    let conflicts = reports.iter().filter(|r| r.conflict).count() as u64;
    assert_eq!(sum("conflicts"), conflicts);
    assert!(conflicts + sum("deduplicated") > 0, "the publishers never actually raced");
    eprintln!(
        "race: {conflicts} conflicts, {} deduplicated over {} catch-ups",
        sum("deduplicated"),
        reports.len()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn after_the_dedup_window_a_republish_lands_twice() {
    // At-least-once, documented: JetStream only dedups inside `duplicate_window`, so a consumer
    // must dedup on (repo, _walgit.seq, ref_name) itself.
    let Some(nats) = start_nats().await else { return };
    let rig = Rig::with_window(&nats, NatsStart::Oldest, Duration::from_millis(200)).await;
    let id = repo_with_three_events(&rig, "t", "r").await;
    assert_eq!(rig.publisher(NatsStart::Oldest).await.catch_up(&id).await.unwrap().emitted, 3);
    rig.inject(&id, 2, "refs/heads/not-in-the-entry").await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let restarted = rig.publisher(NatsStart::Oldest).await;
    let r = restarted.catch_up(&id).await.unwrap();
    assert_eq!(r.emitted, 3);
    let counters = restarted.counters.snapshot();
    assert_eq!((counters["published"].as_u64(), counters["deduplicated"].as_u64()), (Some(3), Some(0)));
    assert_eq!(rig.message_count().await, 7, "every republished event landed a second time");
    let ids = rig.ids().await;
    for (seq, name) in [(1, "refs/heads/main"), (2, "refs/heads/a"), (2, "refs/heads/b")] {
        let id = msg_id(&id, seq, name);
        assert_eq!(ids.iter().filter(|i| **i == id).count(), 2, "{id} must be in the stream twice");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wiped_stream_is_republished_on_the_next_catch_up() {
    let Some(nats) = start_nats().await else { return };
    let rig = Rig::new(&nats, NatsStart::Oldest).await;
    let id = repo_with_three_events(&rig, "t", "r").await;
    let publisher = rig.publisher(NatsStart::Oldest).await;
    assert_eq!(publisher.catch_up(&id).await.unwrap().emitted, 3);
    let original = rig.ids().await;

    // Purged: JetStream keeps its duplicate table across a purge, so inside `duplicate_window`
    // the republish is acknowledged as duplicates and lands nothing. PINNED known limitation; the
    // repository stays open (not recorded as done) so it heals once the window has passed, see
    // `a_purged_stream_heals_once_the_dedup_window_has_passed`.
    rig.stream().await.purge().await.unwrap();
    assert_eq!(rig.message_count().await, 0);
    let r = publisher.catch_up(&id).await.unwrap();
    assert_eq!(r.emitted, 3, "{r:?}");
    assert_eq!(publisher.counters.snapshot()["deduplicated"], 3, "swallowed as duplicates of purged messages");
    assert_eq!(rig.message_count().await, 0, "PINNED: a purge inside the window loses the republish");

    // Deleted: nothing to publish into (an error, not a silent skip) ...
    rig.js.delete_stream(&rig.cfg.events.nats.stream).await.unwrap();
    assert!(publisher.catch_up(&id).await.is_err());
    // ... and recreated, the window is republished again.
    rig.provision().await;
    let r = publisher.catch_up(&id).await.unwrap();
    assert_eq!(r.emitted, 3, "a recreated stream is republished: {r:?}");
    assert_eq!(rig.ids().await, original);

    // A steady state still spares the log read and publishes nothing.
    assert_eq!(publisher.catch_up(&id).await.unwrap().emitted, 0);
    assert_eq!(rig.message_count().await, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_purged_stream_heals_once_the_dedup_window_has_passed() {
    let Some(nats) = start_nats().await else { return };
    let rig = Rig::with_window(&nats, NatsStart::Oldest, Duration::from_millis(200)).await;
    let id = repo_with_three_events(&rig, "t", "r").await;
    let publisher = rig.publisher(NatsStart::Oldest).await;
    assert_eq!(publisher.catch_up(&id).await.unwrap().emitted, 3);
    let original = rig.ids().await;

    rig.stream().await.purge().await.unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let r = publisher.catch_up(&id).await.unwrap();
    assert_eq!(r.emitted, 3, "{r:?}");
    assert_eq!(rig.ids().await, original, "the same running publisher republished the window");
    assert_eq!(publisher.catch_up(&id).await.unwrap().emitted, 0, "and then it is steady");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn entries_folded_before_publishing_are_a_counted_gap() {
    let Some(nats) = start_nats().await else { return };
    let rig = Rig::new(&nats, NatsStart::Oldest).await;
    let id = RepoId::new("t", "r").unwrap();
    rig.registry.create(&id, ObjectFormat::Sha1).await.unwrap();
    rig.push(&id, &[("refs/heads/main", ZERO, A)]).await;
    let publisher = rig.publisher(NatsStart::Oldest).await;
    assert_eq!(publisher.catch_up(&id).await.unwrap().emitted, 1);

    rig.push(&id, &[("refs/heads/main", A, B)]).await;
    rig.push(&id, &[("refs/heads/main", B, C)]).await;
    rig.push(&id, &[("refs/heads/main", C, A)]).await;
    let cp = rig.registry.open(&id).await.unwrap().write_checkpoint().await.unwrap();
    assert_eq!(cp.seq, 4, "seqs 2..=4 are folded away before they were published");
    rig.push(&id, &[("refs/heads/main", A, B)]).await;

    let r = publisher.catch_up(&id).await.unwrap();
    assert_eq!(r.gap, Some([2, 4]), "{r:?}");
    assert_eq!((r.head_seq, r.emitted), (5, 1), "the readable entry is still published");
    assert_eq!(publisher.counters.snapshot()["gap_entries"], 3);
    assert_eq!(rig.ids().await, [msg_id(&id, 1, "refs/heads/main"), msg_id(&id, 5, "refs/heads/main")]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_gap_is_counted_once_while_nothing_moves_the_subject() {
    let Some(nats) = start_nats().await else { return };
    let rig = Rig::new(&nats, NatsStart::Oldest).await;
    let id = RepoId::new("t", "r").unwrap();
    rig.registry.create(&id, ObjectFormat::Sha1).await.unwrap();
    rig.push(&id, &[("refs/heads/main", ZERO, A)]).await;
    let publisher = rig.publisher(NatsStart::Oldest).await;
    assert_eq!(publisher.catch_up(&id).await.unwrap().emitted, 1);

    rig.push(&id, &[("refs/heads/main", A, B)]).await;
    rig.push(&id, &[("refs/heads/main", B, C)]).await;
    let handle = rig.registry.open(&id).await.unwrap();
    handle.write_checkpoint().await.unwrap();
    let r = publisher.catch_up(&id).await.unwrap();
    assert_eq!((r.gap, r.emitted), (Some([2, 3]), 0), "{r:?}");

    handle.publish_settings("", "test", "moves the head without a ref event").await.unwrap();
    let r = publisher.catch_up(&id).await.unwrap();
    assert!(r.head_seq > 3, "{r:?}");
    assert_eq!((r.gap, r.emitted), (None, 0), "the same gap is not reported again: {r:?}");
    assert_eq!(publisher.counters.snapshot()["gap_entries"], 2);

    rig.push(&id, &[("refs/heads/main", C, A)]).await;
    let r = publisher.catch_up(&id).await.unwrap();
    assert_eq!((r.gap, r.emitted), (None, 1), "{r:?}");
    assert_eq!(publisher.counters.snapshot()["gap_entries"], 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_outage_mid_catch_up_loses_and_duplicates_nothing() {
    const REFS: usize = 1500;
    let Some(mut nats) = start_nats().await else { return };
    let rig = Rig::new(&nats, NatsStart::Oldest).await;
    let id = RepoId::new("t", "big").unwrap();
    rig.registry.create(&id, ObjectFormat::Sha1).await.unwrap();
    let names: Vec<String> = (0..REFS).map(|i| format!("refs/heads/b{i:04}")).collect();
    let updates: Vec<(&str, &str, &str)> = names.iter().map(|n| (n.as_str(), ZERO, A)).collect();
    // One entry with many refs: a single catch_up has plenty to publish one ack at a time.
    rig.push(&id, &updates).await;

    let publisher = rig.publisher(NatsStart::Oldest).await;
    let running = tokio::spawn({
        let (publisher, id) = (publisher.clone(), id.clone());
        async move { publisher.catch_up(&id).await }
    });
    while rig.message_count().await < 20 {
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    nats.stop();
    let outcome = running.await.unwrap();
    let error = outcome.expect_err("the server went away mid catch-up");
    eprintln!("outage error: {error:#}");

    nats.restart().await;
    rig.wait_connected().await;
    let held = rig.message_count().await;
    assert!(held >= 20 && held < REFS as u64, "partial progress survived the restart: {held}");
    let r = publisher.catch_up(&id).await.unwrap();
    assert_eq!(r.emitted as u64, REFS as u64 - held, "exactly the rest: {r:?}");
    let mut ids = rig.ids().await;
    assert_eq!(ids.len(), REFS);
    let expected: Vec<String> = names.iter().map(|n| msg_id(&id, 1, n)).collect();
    assert_eq!(ids, expected, "in order, nothing skipped, nothing twice");
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), REFS);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn known_limitation_head_start_skips_pushes_made_while_the_publisher_was_down() {
    // PINNED: docs/EVENTS.md says `start = "head"` publishes only pushes made after the publisher
    // first saw the repository. The mark is in memory, so a restart before the repository's first
    // event treats everything pushed while the publisher was down as "before". If this test fails
    // because the behavior changed, update the `start` paragraph of docs/EVENTS.md with it.
    let Some(nats) = start_nats().await else { return };
    let rig = Rig::new(&nats, NatsStart::Head).await;
    let id = RepoId::new("t", "r").unwrap();
    rig.registry.create(&id, ObjectFormat::Sha1).await.unwrap();
    rig.push(&id, &[("refs/heads/main", ZERO, A)]).await;
    let first = rig.publisher(NatsStart::Head).await;
    assert_eq!(first.catch_up(&id).await.unwrap().emitted, 0, "head start: history is not published");

    // The publisher goes down; these two pushes happen while it is gone.
    drop(first);
    rig.push(&id, &[("refs/heads/main", A, B)]).await;
    rig.push(&id, &[("refs/heads/main", B, C)]).await;

    let restarted = rig.publisher(NatsStart::Head).await;
    assert_eq!(restarted.catch_up(&id).await.unwrap().emitted, 0, "seqs 2 and 3 are skipped forever");
    rig.push(&id, &[("refs/heads/main", C, A)]).await;
    assert_eq!(restarted.catch_up(&id).await.unwrap().emitted, 1);
    assert_eq!(rig.ids().await, [msg_id(&id, 4, "refs/heads/main")]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_foreign_message_on_a_repo_subject_fails_every_catch_up_with_a_clear_error() {
    // PINNED operational risk: a message without Walgit-Seq / Nats-Msg-Id as the subject's last
    // message makes every catch-up for that repository fail until it is removed from the stream.
    let Some(nats) = start_nats().await else { return };
    let rig = Rig::new(&nats, NatsStart::Oldest).await;
    let id = RepoId::new("t", "r").unwrap();
    rig.registry.create(&id, ObjectFormat::Sha1).await.unwrap();
    rig.push(&id, &[("refs/heads/main", ZERO, A)]).await;
    let publisher = rig.publisher(NatsStart::Oldest).await;
    rig.js.publish("walgit.ref.t.r", "hello".into()).await.unwrap().await.unwrap();
    for _ in 0..3 {
        let error = format!("{:#}", publisher.catch_up(&id).await.unwrap_err());
        assert!(
            error.contains("walgit.ref.t.r") && error.contains("lacks Walgit-Seq / Nats-Msg-Id"),
            "the error must say what is wrong: {error}"
        );
    }
    assert_eq!(rig.message_count().await, 1, "nothing was published on top of it");

    // Other repositories are unaffected.
    let other = RepoId::new("t", "other").unwrap();
    rig.registry.create(&other, ObjectFormat::Sha1).await.unwrap();
    rig.push(&other, &[("refs/heads/main", ZERO, A)]).await;
    assert_eq!(publisher.catch_up(&other).await.unwrap().emitted, 1);
}

fn pack_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "pack") {
                found.push(path);
            }
        }
    }
    found
}

/// A push carrying a real pack (one commit), so the repository has pack bytes in the bucket.
async fn push_with_pack(rig: &Rig, id: &RepoId) {
    let work = tempfile::tempdir().unwrap();
    let git = |args: &[&str]| {
        let out = std::process::Command::new("git")
            .args(["-c", "user.name=T", "-c", "user.email=t@t", "-c", "commit.gpgsign=false"])
            .args(args)
            .current_dir(work.path())
            .output()
            .unwrap();
        assert!(out.status.success(), "git {args:?}: {}", String::from_utf8_lossy(&out.stderr));
        String::from_utf8(out.stdout).unwrap().trim().to_string()
    };
    git(&["init", "-q"]);
    std::fs::write(work.path().join("f"), "x").unwrap();
    git(&["add", "."]);
    git(&["commit", "-q", "-m", "c"]);
    let head = git(&["rev-parse", "HEAD"]);
    let pack = {
        use std::io::Write as _;
        let mut child = std::process::Command::new("git")
            .args(["pack-objects", "--stdout", "--revs"])
            .current_dir(work.path())
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(format!("{head}\n").as_bytes()).unwrap();
        child.wait_with_output().unwrap().stdout
    };
    let handle = rig.registry.open(id).await.unwrap();
    let ingested = handle
        .local()
        .ingest_pack(
            std::io::Cursor::new(pack),
            walgit_git::IngestOptions { fsck: false, max_bytes: None, thin: false },
        )
        .await
        .unwrap();
    let txn = RefTransaction {
        updates: vec![RefUpdate {
            name: "refs/heads/main".into(),
            old_oid: String::new(),
            new_oid: head,
            ..Default::default()
        }],
        atomic: true,
        ..Default::default()
    };
    let r = handle.publish_push(ingested, txn, HashMap::new()).await.unwrap();
    assert!(r.seq > 0, "push rejected: {:?}", r.per_ref);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_publisher_never_prefetches_packs_whatever_the_operator_configured() {
    let Some(nats) = start_nats().await else { return };
    let rig = Rig::new(&nats, NatsStart::Oldest).await;
    let id = RepoId::new("t", "r").unwrap();
    rig.registry.create(&id, ObjectFormat::Sha1).await.unwrap();
    push_with_pack(&rig, &id).await;

    let (publisher, _registry, cache) = rig
        .operator_publisher(|c| c.wal.prefetch_packs = true)
        .await;
    assert_eq!(publisher.catch_up(&id).await.unwrap().emitted, 1);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(pack_files(cache.path()), Vec::<std::path::PathBuf>::new(), "no pack bytes pulled");

    // Control: the same operator config without the adaptation does prefetch.
    let control_cache = tempfile::tempdir().unwrap();
    let mut operator = (*rig.cfg).clone();
    operator.cache.dir = control_cache.path().to_path_buf();
    operator.wal.prefetch_packs = true;
    let plain = walgit_wal::Registry::new(rig.store.clone(), Arc::new(operator));
    let handle = plain.open(&id).await.unwrap();
    drop(handle.sync_refs().await.unwrap());
    let t0 = Instant::now();
    while pack_files(control_cache.path()).is_empty() {
        assert!(t0.elapsed() < Duration::from_secs(10), "the control never prefetched");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_missing_stream_fails_clearly_and_is_never_created() {
    let Some(nats) = start_nats().await else { return };
    let rig = Rig::unprovisioned(&nats, NatsStart::Oldest, Duration::from_hours(1)).await;
    let err = walgit_nats::open_stream(&rig.js, &rig.cfg.events.nats, &rig.prefix).await.unwrap_err();
    let text = format!("{err:#}");
    assert!(
        text.contains("WALGIT_EVENTS") && text.contains("never creates it"),
        "the error must name the stream and say who provisions it: {text}"
    );
    assert!(rig.js.get_stream("WALGIT_EVENTS").await.is_err(), "nothing was created");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_misshapen_stream_fails_at_boot_and_is_never_altered() {
    let Some(nats) = start_nats().await else { return };
    let rig = Rig::unprovisioned(&nats, NatsStart::Oldest, Duration::from_hours(1)).await;
    rig.js
        .create_stream(jetstream::stream::Config {
            name: "WALGIT_EVENTS".into(),
            subjects: vec!["walgit.ref.*".into()],
            storage: jetstream::stream::StorageType::Memory,
            ..Default::default()
        })
        .await
        .unwrap();

    let err = walgit_nats::open_stream(&rig.js, &rig.cfg.events.nats, &rig.prefix).await.unwrap_err();
    let text = format!("{err:#}");
    assert!(
        text.contains("WALGIT_EVENTS")
            && text.contains("storage is Memory")
            && text.contains("do not capture walgit.ref.>")
            && text.contains("docs/EVENTS.md"),
        "every shortfall and the way out: {text}"
    );

    let config = rig.stream().await.cached_info().config.clone();
    assert_eq!(config.storage, jetstream::stream::StorageType::Memory, "never altered");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_existing_stream_with_other_settings_is_reused_not_clobbered() {
    let Some(nats) = start_nats().await else { return };
    let rig = Rig::unprovisioned(&nats, NatsStart::Oldest, Duration::from_hours(1)).await;
    let existing = jetstream::stream::Config {
        name: "WALGIT_EVENTS".into(),
        subjects: vec!["walgit.ref.>".into(), "other.>".into()],
        max_messages: 1000,
        duplicate_window: Duration::from_secs(30),
        ..Default::default()
    };
    rig.js.create_stream(existing).await.unwrap();

    let stream = walgit_nats::open_stream(&rig.js, &rig.cfg.events.nats, &rig.prefix)
        .await
        .unwrap();
    let config = stream.cached_info().config.clone();
    assert_eq!(config.max_messages, 1000);
    assert_eq!(config.duplicate_window, Duration::from_secs(30));
    assert_eq!(config.subjects, ["walgit.ref.>", "other.>"]);

    let id = RepoId::new("t", "r").unwrap();
    rig.registry.create(&id, ObjectFormat::Sha1).await.unwrap();
    rig.push(&id, &[("refs/heads/main", ZERO, A)]).await;
    let publisher = rig.publisher(NatsStart::Oldest).await;
    assert_eq!(publisher.catch_up(&id).await.unwrap().emitted, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unparsable_server_url_fails_to_connect_naming_the_setting() {
    let cfg = walgit_config::NatsEventsConfig {
        urls: vec!["nats://bad host:not-a-port".into()],
        ..Default::default()
    };
    let err = walgit_nats::connect(&cfg, &NatsEnv::default()).await.unwrap_err();
    assert!(format!("{err:#}").contains("events.nats.urls"), "{err:#}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tags_and_deletes_carry_their_ref_type_and_zero_oids() {
    let Some(nats) = start_nats().await else { return };
    let rig = Rig::new(&nats, NatsStart::Oldest).await;
    let id = RepoId::new("t", "r").unwrap();
    rig.registry.create(&id, ObjectFormat::Sha1).await.unwrap();
    rig.push(&id, &[("refs/heads/main", ZERO, A), ("refs/tags/v1", ZERO, B)]).await;
    rig.push(&id, &[("refs/heads/main", A, ZERO), ("refs/tags/v1", B, ZERO)]).await;
    rig.push(&id, &[("refs/notes/x", ZERO, C)]).await;
    let publisher = rig.publisher(NatsStart::Oldest).await;
    assert_eq!(publisher.catch_up(&id).await.unwrap().emitted, 5);

    let stream = rig.stream().await;
    let mut seen = Vec::new();
    for n in 1..=5 {
        let raw = stream.get_raw_message(n).await.unwrap();
        let b: serde_json::Value = serde_json::from_slice(&raw.payload).unwrap();
        let get = |k: &str| b[k].as_str().unwrap().to_string();
        seen.push((get("action"), get("ref_type"), get("ref_name"), get("old"), get("new")));
    }
    let row = |a: &str, t: &str, r: &str, o: &str, n: &str| {
        (a.to_string(), t.to_string(), r.to_string(), o.to_string(), n.to_string())
    };
    assert_eq!(
        seen,
        [
            row("create", "branch", "refs/heads/main", ZERO, A),
            row("create", "tag", "refs/tags/v1", ZERO, B),
            row("delete", "branch", "refs/heads/main", A, ZERO),
            row("delete", "tag", "refs/tags/v1", B, ZERO),
            row("create", "", "refs/notes/x", ZERO, C),
        ]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_sequential_pushes_land_in_seq_order_across_catch_ups() {
    let Some(nats) = start_nats().await else { return };
    let rig = Rig::new(&nats, NatsStart::Oldest).await;
    let id = RepoId::new("t", "r").unwrap();
    rig.registry.create(&id, ObjectFormat::Sha1).await.unwrap();
    let publisher = rig.publisher(NatsStart::Oldest).await;
    let oid = |i: usize| format!("{i:040x}");
    let mut prev = ZERO.to_string();
    let mut expected = Vec::new();
    for i in 1..=30usize {
        let next = oid(i);
        let seq = rig.push(&id, &[("refs/heads/main", &prev, &next)]).await;
        expected.push(msg_id(&id, seq, "refs/heads/main"));
        prev = next;
        if i % 7 == 0 {
            publisher.catch_up(&id).await.unwrap();
        }
    }
    publisher.catch_up(&id).await.unwrap();
    assert_eq!(rig.ids().await, expected);
}

async fn published_then_recreated(rig: &Rig, publisher: &Publisher) -> RepoId {
    let id = RepoId::new("t", "r").unwrap();
    rig.registry.create(&id, ObjectFormat::Sha1).await.unwrap();
    let mut prev = ZERO.to_string();
    for i in 1..=3usize {
        let next = format!("{i:040x}");
        rig.push(&id, &[("refs/heads/main", &prev, &next)]).await;
        prev = next;
    }
    assert_eq!(publisher.catch_up(&id).await.unwrap().emitted, 3);
    rig.registry.delete(&id).await.unwrap();
    rig.registry.create(&id, ObjectFormat::Sha1).await.unwrap();
    id
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recreated_repository_is_published_as_a_new_one() {
    let Some(nats) = start_nats().await else { return };
    let rig = Rig::new(&nats, NatsStart::Oldest).await;
    let publisher = rig.publisher(NatsStart::Oldest).await;
    let id = published_then_recreated(&rig, &publisher).await;
    let seq = rig.push(&id, &[("refs/heads/fresh", ZERO, A)]).await;
    assert_eq!(seq, 1, "the new incarnation starts again at seq 1");

    let r = publisher.catch_up(&id).await.unwrap();
    assert_eq!((r.head_seq, r.emitted, r.conflict), (1, 1, false), "{r:?}");
    assert_eq!(publisher.counters.snapshot()["rewinds"], 1);
    let ids = rig.ids().await;
    assert_eq!(ids.len(), 4);
    assert_eq!(ids.last().unwrap(), &msg_id(&id, 1, "refs/heads/fresh"));
    let again = publisher.catch_up(&id).await.unwrap();
    assert_eq!(again.emitted, 0, "steady afterwards, not rewound again");
    assert_eq!(publisher.counters.snapshot()["rewinds"], 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recreated_repository_that_caught_up_with_the_old_seqs_is_still_published_whole() {
    let Some(nats) = start_nats().await else { return };
    let rig = Rig::new(&nats, NatsStart::Oldest).await;
    let publisher = rig.publisher(NatsStart::Oldest).await;
    let id = published_then_recreated(&rig, &publisher).await;
    let mut prev = ZERO.to_string();
    for i in 1..=4usize {
        let next = format!("{:040x}", 0x100 + i);
        rig.push(&id, &[("refs/heads/new", &prev, &next)]).await;
        prev = next;
    }

    let r = publisher.catch_up(&id).await.unwrap();
    assert_eq!((r.from_seq, r.head_seq, r.emitted, r.conflict), (0, 4, 4, false), "{r:?}");
    assert_eq!(publisher.counters.snapshot()["rewinds"], 1);
    let ids = rig.ids().await;
    let fresh: Vec<_> = (1..=4).map(|seq| msg_id(&id, seq, "refs/heads/new")).collect();
    assert_eq!(ids[3..], fresh[..], "the new incarnation from its first entry");
    let restarted = rig.publisher(NatsStart::Oldest).await;
    assert_eq!(restarted.catch_up(&id).await.unwrap().emitted, 0, "its own last message is a cursor");
    assert_eq!(restarted.counters.snapshot()["rewinds"], 0);
}

async fn head_start_marked_then_recreated(rig: &Rig, publisher: &Publisher) -> RepoId {
    let id = RepoId::new("t", "r").unwrap();
    rig.registry.create(&id, ObjectFormat::Sha1).await.unwrap();
    for name in ["refs/heads/a", "refs/heads/b", "refs/heads/c"] {
        rig.push(&id, &[(name, ZERO, A)]).await;
    }
    assert_eq!(publisher.catch_up(&id).await.unwrap().emitted, 0, "first sighting at head 3");
    rig.registry.delete(&id).await.unwrap();
    id
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deleted_repository_under_head_start_forgets_its_mark() {
    let Some(nats) = start_nats().await else { return };
    let rig = Rig::new(&nats, NatsStart::Head).await;
    let publisher = rig.publisher(NatsStart::Head).await;
    let id = head_start_marked_then_recreated(&rig, &publisher).await;
    assert!(publisher.catch_up_if_present(&id).await.unwrap().is_none());
    rig.registry.create(&id, ObjectFormat::Sha1).await.unwrap();

    rig.push(&id, &[("refs/heads/before", ZERO, A)]).await;
    assert_eq!(publisher.catch_up(&id).await.unwrap().emitted, 0, "first sighting at head 1");
    rig.push(&id, &[("refs/heads/after", ZERO, B)]).await;
    let r = publisher.catch_up(&id).await.unwrap();
    assert_eq!((r.emitted, r.conflict), (1, false), "{r:?}");
    assert_eq!(rig.ids().await, [msg_id(&id, 2, "refs/heads/after")]);
    assert_eq!(publisher.counters.snapshot()["rewinds"], 0, "a deletion it saw is no rewind");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recreation_under_head_start_seen_only_as_a_lower_head_resets_the_mark() {
    let Some(nats) = start_nats().await else { return };
    let rig = Rig::new(&nats, NatsStart::Head).await;
    let publisher = rig.publisher(NatsStart::Head).await;
    let id = head_start_marked_then_recreated(&rig, &publisher).await;
    rig.registry.create(&id, ObjectFormat::Sha1).await.unwrap();

    rig.push(&id, &[("refs/heads/before", ZERO, A)]).await;
    assert_eq!(publisher.catch_up(&id).await.unwrap().emitted, 0, "the new head is the mark");
    assert_eq!(publisher.counters.snapshot()["rewinds"], 1);
    rig.push(&id, &[("refs/heads/after", ZERO, B)]).await;
    let r = publisher.catch_up(&id).await.unwrap();
    assert_eq!((r.emitted, r.conflict), (1, false), "{r:?}");
    assert_eq!(rig.ids().await, [msg_id(&id, 2, "refs/heads/after")]);
    assert_eq!(publisher.counters.snapshot()["rewinds"], 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recreated_repository_under_head_start_publishes_pushes_after_its_first_sighting() {
    let Some(nats) = start_nats().await else { return };
    let rig = Rig::new(&nats, NatsStart::Head).await;
    let publisher = rig.publisher(NatsStart::Head).await;
    let id = RepoId::new("t", "r").unwrap();
    rig.registry.create(&id, ObjectFormat::Sha1).await.unwrap();
    assert_eq!(publisher.catch_up(&id).await.unwrap().emitted, 0, "first sighting at head 0");
    let mut prev = ZERO.to_string();
    for i in 1..=3usize {
        let next = format!("{i:040x}");
        rig.push(&id, &[("refs/heads/main", &prev, &next)]).await;
        prev = next;
    }
    assert_eq!(publisher.catch_up(&id).await.unwrap().emitted, 3);
    rig.registry.delete(&id).await.unwrap();
    rig.registry.create(&id, ObjectFormat::Sha1).await.unwrap();

    rig.push(&id, &[("refs/heads/before", ZERO, A)]).await;
    let r = publisher.catch_up(&id).await.unwrap();
    assert_eq!(r.emitted, 0, "head start: what the new incarnation held when first seen is skipped");
    assert_eq!(publisher.counters.snapshot()["rewinds"], 1);

    rig.push(&id, &[("refs/heads/after", ZERO, B)]).await;
    let r = publisher.catch_up(&id).await.unwrap();
    assert_eq!((r.emitted, r.conflict), (1, false), "{r:?}");
    assert_eq!(rig.ids().await.last().unwrap(), &msg_id(&id, 2, "refs/heads/after"));
    assert_eq!(publisher.counters.snapshot()["rewinds"], 1, "the same rewind is not counted twice");
    assert_eq!(publisher.catch_up(&id).await.unwrap().emitted, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recreated_repository_colliding_with_the_old_ids_heals_after_the_dedup_window() {
    // PINNED: the new incarnation's first event has the old incarnation's Nats-Msg-Id
    // (`t/r:1:refs/heads/main`), so inside the window the broker swallows it, the catch-up is not
    // confirmed and the repository stays open until the window has passed.
    let Some(nats) = start_nats().await else { return };
    let rig = Rig::with_window(&nats, NatsStart::Oldest, Duration::from_secs(2)).await;
    let publisher = rig.publisher(NatsStart::Oldest).await;
    let id = published_then_recreated(&rig, &publisher).await;
    rig.push(&id, &[("refs/heads/main", ZERO, A)]).await;

    let r = publisher.catch_up(&id).await.unwrap();
    assert_eq!((r.emitted, r.conflict), (1, false));
    assert_eq!(publisher.counters.snapshot()["deduplicated"], 1);
    assert_eq!(rig.message_count().await, 3, "swallowed as a duplicate of the old one");

    tokio::time::sleep(Duration::from_secs(4)).await;
    let r = publisher.catch_up(&id).await.unwrap();
    assert_eq!((r.emitted, r.conflict), (1, false), "{r:?}");
    assert_eq!(rig.message_count().await, 4, "it landed once the window had passed");
    assert_eq!(publisher.catch_up(&id).await.unwrap().emitted, 0);
}
