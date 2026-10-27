// Test fixtures use panics to fail the test, including shared helper functions.
#![allow(clippy::indexing_slicing, clippy::unwrap_used)]

//! Events (docs/EVENTS.md, `walgit nats`): real `git push`es against the harness server, published
//! into a real `nats-server -js` by a `walgit_nats` publisher over the same registry. Skips (passes)
//! when `nats-server` is not on PATH.
mod harness;

use std::sync::Arc;
use std::time::{Duration, Instant};

use async_nats::jetstream;
use futures::StreamExt;
use harness::{Server, TestRepo, git_in};
use walgit_config::{NatsEventsConfig, NatsStart};
use walgit_git::RepoId;
use walgit_nats::publisher::Publisher;
use walgit_nats::subject::SubjectPrefix;
use walgit_nats::testing::{NatsServer, start_nats};

type TestResult = anyhow::Result<()>;

const ZERO_OID: &str = "0000000000000000000000000000000000000000";

struct Msg {
    subject: String,
    seq: String,
    repo: String,
    dedup_id: String,
    body: serde_json::Value,
}

struct Rig {
    server: Server,
    nats_cfg: NatsEventsConfig,
    client: async_nats::Client,
    js: jetstream::Context,
    prefix: SubjectPrefix,
    _nats: NatsServer,
}

impl Rig {
    async fn new() -> anyhow::Result<Option<Self>> {
        let Some(nats) = start_nats().await else {
            return Ok(None);
        };
        Ok(Some(Self::around(nats, Server::start().await?).await?))
    }

    async fn around(nats: NatsServer, server: Server) -> anyhow::Result<Self> {
        let nats_cfg = NatsEventsConfig {
            urls: vec![nats.url.clone()],
            ..NatsEventsConfig::default()
        };
        let client = walgit_nats::connect(&nats_cfg, &walgit_nats::env::NatsEnv::default()).await?;
        let js = jetstream::new(client.clone());
        let prefix = SubjectPrefix::new(&nats_cfg.subject_prefix)?;
        Ok(Rig {
            server,
            nats_cfg,
            client,
            js,
            prefix,
            _nats: nats,
        })
    }

    /// A publisher with no memory of earlier ones, as after a restart.
    async fn publisher(&self) -> anyhow::Result<Arc<Publisher>> {
        let stream = walgit_nats::open_stream(&self.js, &self.nats_cfg, &self.prefix).await?;
        Ok(Publisher::new(
            self.server.state.registry.clone(),
            self.js.clone(),
            stream,
            self.prefix.clone(),
            NatsStart::Oldest,
        ))
    }

    async fn stream(&self) -> jetstream::stream::Stream {
        self.js.get_stream(&self.nats_cfg.stream).await.unwrap()
    }

    async fn message_count(&self) -> u64 {
        self.stream().await.get_info().await.unwrap().state.messages
    }

    async fn wait_for_messages(&self, n: u64) {
        let t0 = Instant::now();
        loop {
            let have = self.message_count().await;
            if have >= n {
                return;
            }
            assert!(
                t0.elapsed() < Duration::from_secs(10),
                "timed out waiting for {n} messages; have {have}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Every message in the stream, in stream order.
    async fn messages(&self) -> Vec<Msg> {
        let stream = self.stream().await;
        let count = stream.get_info().await.unwrap().state.messages;
        let mut out = Vec::new();
        for stream_seq in 1..=count {
            let raw = stream.get_raw_message(stream_seq).await.unwrap();
            let header = |name: &str| raw.headers.get(name).unwrap().as_str().to_string();
            out.push(Msg {
                subject: raw.subject.to_string(),
                seq: header("Walgit-Seq"),
                repo: header("Walgit-Repo"),
                dedup_id: header("Nats-Msg-Id"),
                body: serde_json::from_slice(&raw.payload).unwrap(),
            });
        }
        out
    }

    fn spawn_notify_loop(&self, publisher: &Arc<Publisher>) -> tokio::task::JoinHandle<anyhow::Result<()>> {
        tokio::spawn(walgit_nats::wake::notify_loop(
            self.client.clone(),
            self.nats_cfg.notify_subject.clone(),
            self.nats_cfg.queue_group.clone(),
            self.server.state.cfg.store_prefix(),
            publisher.clone(),
        ))
    }
}

/// A working clone whose `origin` is `owner/repo` on the server (created first).
async fn clone_of(server: &Server, owner: &str, repo: &str) -> anyhow::Result<TestRepo> {
    server.put_repo(owner, repo).await?;
    let src = TestRepo::synthetic(1, 1)?;
    git_in(&src, &["commit", "--allow-empty", "-m", "a"])?;
    git_in(&src, &["branch", "-M", "main"])?;
    git_in(&src, &["remote", "add", "origin", &server.repo_url(owner, repo)])?;
    Ok(src)
}

/// Let a notified catch-up (a queue subscription, no reply) settle: the loop is subscribed once a
/// published probe reaches it, which a short sleep approximates for a fresh subscription.
async fn subscribed() {
    tokio::time::sleep(Duration::from_millis(500)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pushes_become_events_with_the_right_shape() -> TestResult {
    let Some(rig) = Rig::new().await? else { return Ok(()) };
    let id = RepoId::new("t", "r")?;
    let src = clone_of(&rig.server, "t", "r").await?;
    let publisher = rig.publisher().await?;

    // New branch.
    git_in(&src, &["push", "origin", "main"])?;
    let main_v1 = git_in(&src, &["rev-parse", "HEAD"])?.trim().to_string();
    let r = publisher.catch_up(&id).await?;
    assert_eq!((r.from_seq, r.head_seq, r.emitted, r.conflict), (0, 1, 1, false));
    let msgs = rig.messages().await;
    assert_eq!(msgs.len(), 1);
    let m = &msgs[0];
    assert_eq!(m.subject, "walgit.ref.t.r");
    assert_eq!((m.seq.as_str(), m.repo.as_str()), ("1", "t/r"));
    assert_eq!(m.dedup_id, "t/r:1:refs/heads/main");
    assert_eq!(m.body["action"], "create");
    assert_eq!(m.body["ref_type"], "branch");
    assert_eq!(m.body["ref_name"], "refs/heads/main");
    assert_eq!(m.body["old"], ZERO_OID, "create carries the zero OID, never empty");
    assert_eq!(m.body["new"], main_v1);
    assert_eq!(m.body["pusher"], "anon");
    assert_eq!(m.body["repo"], "t/r");
    assert_eq!(m.body["_walgit"]["seq"], "1");

    // A no-op push writes no WAL entry and so no event.
    git_in(&src, &["push", "origin", "main"])?;
    let r = publisher.catch_up(&id).await?;
    assert_eq!((r.head_seq, r.emitted), (1, 0));
    assert_eq!(rig.message_count().await, 1);

    // Fast-forward.
    git_in(&src, &["commit", "--allow-empty", "-m", "b"])?;
    git_in(&src, &["push", "origin", "main"])?;
    let main_v2 = git_in(&src, &["rev-parse", "HEAD"])?.trim().to_string();
    assert_eq!(publisher.catch_up(&id).await?.emitted, 1);
    let m = &rig.messages().await[1];
    assert_eq!((m.body["action"].as_str(), m.seq.as_str()), (Some("update"), "2"));
    assert_eq!((m.body["old"].as_str(), m.body["new"].as_str()), (Some(main_v1.as_str()), Some(main_v2.as_str())));
    assert_eq!(m.dedup_id, "t/r:2:refs/heads/main");

    // Lightweight and annotated tags.
    git_in(&src, &["tag", "light"])?;
    git_in(&src, &["tag", "-a", "-m", "annotated", "v1"])?;
    git_in(&src, &["push", "origin", "light"])?;
    git_in(&src, &["push", "origin", "v1"])?;
    assert_eq!(publisher.catch_up(&id).await?.emitted, 2);
    let msgs = rig.messages().await;
    for (m, name) in msgs[2..].iter().zip(["refs/tags/light", "refs/tags/v1"]) {
        assert_eq!(m.body["action"], "create", "{name}");
        assert_eq!(m.body["ref_type"], "tag", "{name}");
        assert_eq!(m.body["ref_name"], name);
        assert_eq!(m.body["old"], ZERO_OID);
    }
    assert_eq!(msgs[2].body["new"], main_v2, "a lightweight tag points at the commit");
    assert_ne!(msgs[3].body["new"], main_v2, "an annotated tag points at the tag object");

    // Branch delete.
    git_in(&src, &["push", "origin", "main:refs/heads/topic"])?;
    git_in(&src, &["push", "origin", ":refs/heads/topic"])?;
    assert_eq!(publisher.catch_up(&id).await?.emitted, 2);
    let msgs = rig.messages().await;
    assert_eq!(msgs[4].body["action"], "create");
    let del = &msgs[5];
    assert_eq!(del.body["action"], "delete");
    assert_eq!(del.body["ref_type"], "branch");
    assert_eq!(del.body["ref_name"], "refs/heads/topic");
    assert_eq!(del.body["old"], main_v2);
    assert_eq!(del.body["new"], ZERO_OID);

    // Every message names its own subject, stream order is WAL order.
    assert!(msgs.iter().all(|m| m.subject == "walgit.ref.t.r"));
    let seqs: Vec<u64> = msgs.iter().map(|m| m.seq.parse().unwrap()).collect();
    assert!(seqs.windows(2).all(|w| w[0] < w[1]), "{seqs:?}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_atomic_multi_ref_push_is_one_message_per_ref_in_order() -> TestResult {
    let Some(rig) = Rig::new().await? else { return Ok(()) };
    let id = RepoId::new("t", "r")?;
    let src = clone_of(&rig.server, "t", "r").await?;
    git_in(&src, &["branch", "feature"])?;
    git_in(&src, &["tag", "v1"])?;
    git_in(
        &src,
        &["push", "--atomic", "origin", "main", "feature", "refs/tags/v1"],
    )?;

    let publisher = rig.publisher().await?;
    let r = publisher.catch_up(&id).await?;
    assert_eq!((r.head_seq, r.emitted), (1, 3), "one entry, three refs");
    let msgs = rig.messages().await;
    assert_eq!(msgs.len(), 3);
    assert!(msgs.iter().all(|m| m.seq == "1"), "all share the entry's seq");
    let mut names: Vec<&str> = msgs
        .iter()
        .map(|m| m.body["ref_name"].as_str().unwrap())
        .collect();
    let ids: Vec<&str> = msgs.iter().map(|m| m.dedup_id.as_str()).collect();
    for (m, name) in msgs.iter().zip(&names) {
        assert_eq!(m.dedup_id, format!("t/r:1:{name}"));
    }
    assert_eq!(ids.len(), ids.iter().collect::<std::collections::BTreeSet<_>>().len());
    names.sort_unstable();
    assert_eq!(names, ["refs/heads/feature", "refs/heads/main", "refs/tags/v1"]);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dotted_repository_name_is_encoded_in_the_subject() -> TestResult {
    let Some(rig) = Rig::new().await? else { return Ok(()) };
    let id = RepoId::new("acme.io", "mono.repo")?;
    let src = clone_of(&rig.server, "acme.io", "mono.repo").await?;
    git_in(&src, &["push", "origin", "main"])?;
    let publisher = rig.publisher().await?;
    assert_eq!(publisher.catch_up(&id).await?.emitted, 1);
    let m = &rig.messages().await[0];
    assert_eq!(m.subject, "walgit.ref.acme%2Eio.mono%2Erepo");
    assert_eq!(m.repo, "acme.io/mono.repo");
    assert_eq!(m.dedup_id, "acme.io/mono.repo:1:refs/heads/main");
    assert_eq!(m.body["repo"], "acme.io/mono.repo");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bucket_notifications_wake_the_publisher_without_a_catch_up_call() -> TestResult {
    let Some(rig) = Rig::new().await? else { return Ok(()) };
    let src = clone_of(&rig.server, "t", "r").await?;
    let publisher = rig.publisher().await?;
    let notify = rig.spawn_notify_loop(&publisher);
    subscribed().await;

    // An S3 bucket notification naming the manifest, as the object store publishes it.
    git_in(&src, &["push", "origin", "main"])?;
    let s3 = serde_json::json!({"Records": [{"eventName": "s3:ObjectCreated:Put",
        "s3": {"object": {"key": "repos/t/r/manifest.pb"}}}]});
    rig.client
        .publish(rig.nats_cfg.notify_subject.clone(), s3.to_string().into())
        .await?;
    rig.wait_for_messages(1).await;

    // `{"repo": ...}` through the same subject.
    git_in(&src, &["commit", "--allow-empty", "-m", "b"])?;
    git_in(&src, &["push", "origin", "main"])?;
    rig.client
        .publish(rig.nats_cfg.notify_subject.clone(), r#"{"repo":"t/r"}"#.into())
        .await?;
    rig.wait_for_messages(2).await;

    // A notification for something that is not a manifest names nothing.
    git_in(&src, &["commit", "--allow-empty", "-m", "c"])?;
    git_in(&src, &["push", "origin", "main"])?;
    let other = serde_json::json!({"key": "repos/t/r/wal/x.pack"});
    rig.client
        .publish(rig.nats_cfg.notify_subject.clone(), other.to_string().into())
        .await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(rig.message_count().await, 2);

    let msgs = rig.messages().await;
    assert_eq!(
        msgs.iter().map(|m| m.seq.as_str()).collect::<Vec<_>>(),
        ["1", "2"]
    );
    notify.abort();
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_publisher_resumes_from_the_stream() -> TestResult {
    let Some(rig) = Rig::new().await? else { return Ok(()) };
    let id = RepoId::new("t", "r")?;
    let src = clone_of(&rig.server, "t", "r").await?;
    git_in(&src, &["push", "origin", "main"])?;
    git_in(&src, &["commit", "--allow-empty", "-m", "b"])?;
    git_in(&src, &["push", "origin", "main"])?;
    assert_eq!(rig.publisher().await?.catch_up(&id).await?.emitted, 2);
    assert_eq!(rig.message_count().await, 2);

    let restarted = rig.publisher().await?;
    let r = restarted.catch_up(&id).await?;
    assert_eq!((r.from_seq, r.head_seq, r.emitted), (2, 2, 0));
    assert_eq!(rig.message_count().await, 2, "resume must not republish");

    git_in(&src, &["commit", "--allow-empty", "-m", "c"])?;
    git_in(&src, &["push", "origin", "main"])?;
    let r = restarted.catch_up(&id).await?;
    assert_eq!((r.from_seq, r.head_seq, r.emitted), (2, 3, 1));
    assert_eq!(rig.message_count().await, 3, "exactly one new message");
    assert_eq!(rig.messages().await[2].seq, "3");

    // A third process that never saw any of it publishes nothing more.
    assert_eq!(rig.publisher().await?.catch_up(&id).await?.emitted, 0);
    assert_eq!(rig.message_count().await, 3);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_catch_up_endpoint_reports_a_real_repository() -> TestResult {
    let Some(rig) = Rig::new().await? else { return Ok(()) };
    let src = clone_of(&rig.server, "t", "r").await?;
    git_in(&src, &["push", "origin", "main"])?;
    let publisher = rig.publisher().await?;
    let service = tokio::spawn({
        let (client, prefix, publisher) = (rig.client.clone(), rig.prefix.clone(), publisher.clone());
        let queue_group = rig.nats_cfg.queue_group.clone();
        async move { walgit_nats::service::serve(client, &prefix, &queue_group, publisher).await }
    });
    subscribed().await;

    let reply = rig
        .client
        .request("walgit.events.catch-up", r#"{"repo":"t/r"}"#.into())
        .await?;
    let report: serde_json::Value = serde_json::from_slice(&reply.payload)?;
    assert_eq!(report["repo"], "t/r");
    assert_eq!((report["from_seq"].as_u64(), report["head_seq"].as_u64()), (Some(0), Some(1)));
    assert_eq!(report["emitted"], 1);
    assert!(report.get("conflict").is_none(), "{report}");
    assert_eq!(rig.message_count().await, 1);

    // Asked again, nothing is new.
    let reply = rig
        .client
        .request("walgit.events.catch-up", r#"{"repo":"t/r"}"#.into())
        .await?;
    let report: serde_json::Value = serde_json::from_slice(&reply.payload)?;
    assert_eq!((report["from_seq"].as_u64(), report["emitted"].as_u64()), (Some(1), Some(0)));

    let missing = rig
        .client
        .request("walgit.events.catch-up", r#"{"repo":"t/none"}"#.into())
        .await?;
    let code = missing.headers.as_ref().and_then(|h| h.get("Nats-Service-Error-Code"));
    assert_eq!(code.map(|c| c.as_str().to_string()), Some("404".into()));

    let mut stats = rig.client.subscribe("inbox.stats").await?;
    rig.client
        .publish_with_reply("$SRV.STATS.walgit-events", "inbox.stats", "".into())
        .await?;
    let stats = tokio::time::timeout(Duration::from_secs(5), stats.next())
        .await?
        .expect("stats reply");
    let stats: serde_json::Value = serde_json::from_slice(&stats.payload)?;
    assert_eq!(stats["name"], "walgit-events");
    service.abort();
    Ok(())
}

type Captured = Arc<std::sync::Mutex<Vec<serde_json::Value>>>;

/// The webhook sink's target: records every event the bridge delivers.
async fn webhook() -> (String, Captured) {
    let captured: Captured = Arc::default();
    let app = axum::Router::new().route(
        "/events",
        axum::routing::post({
            let captured = captured.clone();
            move |axum::Json(batch): axum::Json<Vec<serde_json::Value>>| {
                let captured = captured.clone();
                async move {
                    captured.lock().unwrap().extend(batch);
                    axum::http::StatusCode::OK
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    (format!("http://{addr}/events"), captured)
}

/// The identity of an event as a consumer of either sink sees it.
fn identity(event: &serde_json::Value) -> String {
    let field = |name: &str| event[name].as_str().unwrap_or_default().to_string();
    format!(
        "{} seq={} {} {} {} -> {}",
        field("repo"),
        event["_walgit"]["seq"].as_str().unwrap_or_default(),
        field("ref_name"),
        field("action"),
        field("old"),
        field("new"),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_webhook_bridge_and_the_jetstream_publisher_agree() -> TestResult {
    let Some(nats) = start_nats().await else { return Ok(()) };
    let (url, captured) = webhook().await;
    let server = Server::start_with_tweak(|c| {
        c.events.webhook_url = Some(url.clone());
        c.events.sweep_interval = Duration::ZERO;
    })
    .await?;
    let rig = Rig::around(nats, server).await?;
    let bridge = rig.server.state.bridge.clone().expect("bridge enabled");
    let id = RepoId::new("t", "r")?;
    let src = clone_of(&rig.server, "t", "r").await?;

    git_in(&src, &["push", "origin", "main"])?;
    git_in(&src, &["commit", "--allow-empty", "-m", "b"])?;
    git_in(&src, &["push", "origin", "main"])?;
    git_in(&src, &["tag", "light"])?;
    git_in(&src, &["tag", "-a", "-m", "annotated", "v1"])?;
    git_in(&src, &["push", "origin", "light", "v1"])?;
    git_in(&src, &["branch", "feature"])?;
    git_in(&src, &["commit", "--allow-empty", "-m", "c"])?;
    git_in(&src, &["push", "--atomic", "origin", "main", "feature"])?;
    git_in(&src, &["push", "origin", "--delete", "feature", "light"])?;

    let sent = bridge.catch_up(&id).await?;
    let published = rig.publisher().await?.catch_up(&id).await?;
    assert_eq!(sent.emitted, published.emitted, "{sent:?} {published:?}");
    assert_eq!(sent.head_seq, published.head_seq);

    let webhook_events: Vec<String> = captured.lock().unwrap().iter().map(identity).collect();
    let stream_events: Vec<String> = rig.messages().await.iter().map(|m| identity(&m.body)).collect();
    assert!(webhook_events.len() >= 8, "the scenario produced events: {webhook_events:#?}");
    assert_eq!(stream_events, webhook_events, "both sinks, same events, same order");
    // The wire body is one and the same, not just the identity fields.
    let bodies: Vec<serde_json::Value> = rig.messages().await.into_iter().map(|m| m.body).collect();
    assert_eq!(bodies, *captured.lock().unwrap());
    Ok(())
}
