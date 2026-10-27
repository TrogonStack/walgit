// Test fixtures use panics to fail the test, including shared helper functions.
#![allow(clippy::indexing_slicing, clippy::unwrap_used, clippy::expect_used)]

//! The `JetStream` publisher against a real `nats-server -js` and the in-memory walgit store:
//! the stream is the cursor (resume without republishing), crash-mid-entry completion, the
//! wake-ups and the service API. Skips (passes) when `nats-server` is not on PATH.

use std::collections::HashMap;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_nats::jetstream;
use futures::StreamExt;
use walgit_config::{Config, NatsStart};
use walgit_events::refs_from_entries;
use walgit_git::{ObjectFormat, RepoId};
use walgit_nats::message::EventMessage;
use walgit_nats::publisher::Publisher;
use walgit_nats::subject::{RepoSubject, SubjectPrefix};
use walgit_proto::v1::{RefTransaction, RefUpdate};

const ZERO: &str = "0000000000000000000000000000000000000000";
const A: &str = "1111111111111111111111111111111111111111";
const B: &str = "2222222222222222222222222222222222222222";
const C: &str = "3333333333333333333333333333333333333333";

struct NatsServer {
    child: Child,
    url: String,
    _store: tempfile::TempDir,
}

impl Drop for NatsServer {
    fn drop(&mut self) {
        self.child.kill().ok();
        self.child.wait().ok();
    }
}

async fn start_nats() -> Option<NatsServer> {
    if !Command::new("nats-server")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
    {
        eprintln!("skipping: nats-server is not available on PATH");
        return None;
    }
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let store = tempfile::tempdir().unwrap();
    let child = Command::new("nats-server")
        .args(["-js", "-a", "127.0.0.1", "-p", &port.to_string(), "-sd"])
        .arg(store.path())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let url = format!("nats://127.0.0.1:{port}");
    let server = NatsServer {
        child,
        url,
        _store: store,
    };
    let t0 = Instant::now();
    while async_nats::connect(&server.url).await.is_err() {
        assert!(t0.elapsed() < Duration::from_secs(15), "nats-server did not start");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Some(server)
}

struct Rig {
    cfg: Arc<Config>,
    registry: Arc<walgit_wal::Registry>,
    client: async_nats::Client,
    js: jetstream::Context,
    prefix: SubjectPrefix,
    _cache: tempfile::TempDir,
}

impl Rig {
    async fn new(nats: &NatsServer, start: NatsStart) -> Self {
        let cache = tempfile::tempdir().unwrap();
        let mut cfg = Config::default();
        cfg.store.backend = walgit_config::StoreBackend::Memory;
        cfg.cache.dir = cache.path().to_path_buf();
        cfg.events.nats.urls = vec![nats.url.clone()];
        cfg.events.nats.start = start;
        let cfg = Arc::new(cfg);
        let registry = walgit_wal::Registry::new(
            Arc::new(walgit_store::memory::MemoryStore::new()),
            cfg.clone(),
        );
        let client = walgit_nats::connect(&cfg.events.nats).await.unwrap();
        let js = jetstream::new(client.clone());
        let prefix = SubjectPrefix::new(&cfg.events.nats.subject_prefix).unwrap();
        Rig {
            cfg,
            registry,
            client,
            js,
            prefix,
            _cache: cache,
        }
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

    async fn stream(&self) -> jetstream::stream::Stream {
        self.js
            .get_stream(&self.cfg.events.nats.stream)
            .await
            .unwrap()
    }

    async fn message_count(&self) -> u64 {
        self.stream().await.get_info().await.unwrap().state.messages
    }

    async fn wait_for_messages(&self, n: u64) {
        let t0 = Instant::now();
        while self.message_count().await < n {
            assert!(t0.elapsed() < Duration::from_secs(10), "timed out waiting for {n} messages");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn notifications_and_the_service_api_wake_the_publisher() {
    let Some(nats) = start_nats().await else { return };
    let rig = Rig::new(&nats, NatsStart::Oldest).await;
    let id = RepoId::new("t", "r").unwrap();
    rig.registry.create(&id, ObjectFormat::Sha1).await.unwrap();
    let publisher = rig.publisher(NatsStart::Oldest).await;

    let nats_cfg = &rig.cfg.events.nats;
    let notify = tokio::spawn(walgit_nats::wake::notify_loop(
        rig.client.clone(),
        nats_cfg.notify_subject.clone(),
        nats_cfg.queue_group.clone(),
        rig.cfg.store_prefix(),
        publisher.clone(),
    ));
    let service = tokio::spawn({
        let (client, prefix, publisher) =
            (rig.client.clone(), rig.prefix.clone(), publisher.clone());
        let queue_group = nats_cfg.queue_group.clone();
        async move { walgit_nats::service::serve(client, &prefix, &queue_group, publisher).await }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;

    // An S3 bucket notification, as the object store publishes it.
    rig.push(&id, &[("refs/heads/main", ZERO, A)]).await;
    let s3 = serde_json::json!({"Records": [{"eventName": "s3:ObjectCreated:Put",
        "s3": {"object": {"key": "repos/t/r/manifest.pb"}}}]});
    rig.client
        .publish(nats_cfg.notify_subject.clone(), s3.to_string().into())
        .await
        .unwrap();
    rig.wait_for_messages(1).await;

    // {"repo": ...} through the same subject.
    rig.push(&id, &[("refs/heads/main", A, B)]).await;
    rig.client
        .publish(nats_cfg.notify_subject.clone(), r#"{"repo":"t/r"}"#.into())
        .await
        .unwrap();
    rig.wait_for_messages(2).await;

    // The service endpoint.
    rig.push(&id, &[("refs/heads/main", B, C)]).await;
    let reply = rig
        .client
        .request("walgit.events.catch-up", r#"{"repo":"t/r"}"#.into())
        .await
        .unwrap();
    let report: serde_json::Value = serde_json::from_slice(&reply.payload).unwrap();
    assert_eq!((report["repo"].as_str(), report["emitted"].as_u64()), (Some("t/r"), Some(1)));
    assert_eq!(rig.message_count().await, 3);

    let missing = rig
        .client
        .request("walgit.events.catch-up", r#"{"repo":"t/none"}"#.into())
        .await
        .unwrap();
    let code = missing.headers.as_ref().and_then(|h| h.get("Nats-Service-Error-Code"));
    assert_eq!(code.map(|c| c.as_str().to_string()), Some("404".into()));

    // The micro protocol's built-ins answer too.
    let mut pings = rig.client.subscribe("inbox.ping").await.unwrap();
    rig.client
        .publish_with_reply("$SRV.PING.walgit-events", "inbox.ping", "".into())
        .await
        .unwrap();
    let pong = tokio::time::timeout(Duration::from_secs(5), pings.next())
        .await
        .unwrap()
        .unwrap();
    let pong: serde_json::Value = serde_json::from_slice(&pong.payload).unwrap();
    assert_eq!(pong["name"], "walgit-events");

    notify.abort();
    service.abort();
}
