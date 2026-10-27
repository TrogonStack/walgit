//! Wake-ups: a bucket notification on the notify subject, and the periodic sweep.
//! Both only ever call `catch_up`, so both are idempotent.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use walgit_events::{manifest_repo, notified_keys};
use walgit_git::RepoId;

use crate::publisher::Publisher;

/// The repositories a notification body names: an S3 event (`Records[].s3.object.key`, what
/// `MinIO` and rustfs publish to NATS natively), a GCS envelope, `{"key": "..."}` or
/// `{"repo": "owner/name"}`. Anything else names nothing.
pub fn notified_repos(payload: &[u8], store_prefix: &str) -> BTreeSet<RepoId> {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(payload) else {
        return BTreeSet::new();
    };
    notified_keys(&value, store_prefix)
        .iter()
        .filter_map(|k| manifest_repo(k, store_prefix))
        .collect()
}

/// Subscribe to `subject` in `queue_group` (instances share the notifications) until the
/// subscription ends. A failed catch-up is logged, not retried: core NATS does not redeliver, and
/// the sweep is the backstop.
pub async fn notify_loop(
    client: async_nats::Client,
    subject: String,
    queue_group: String,
    store_prefix: String,
    publisher: Arc<Publisher>,
) -> anyhow::Result<()> {
    let mut sub = client.queue_subscribe(subject.clone(), queue_group).await?;
    tracing::info!(%subject, "listening for bucket notifications");
    while let Some(msg) = sub.next().await {
        for id in notified_repos(&msg.payload, &store_prefix) {
            if let Err(e) = publisher.catch_up_if_present(&id).await {
                tracing::warn!(repo = %id, error = %format!("{e:#}"), "notified catch-up failed; the sweep retries");
            }
        }
    }
    Ok(())
}

/// One sweep at startup (quiet: it legitimately finds work), then every `every` (`0` = never again).
pub async fn sweep_loop(publisher: Arc<Publisher>, every: Duration) {
    publisher.sweep(false).await;
    if every.is_zero() {
        return;
    }
    let mut tick = tokio::time::interval(every);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    tick.tick().await;
    loop {
        tick.tick().await;
        publisher.sweep(true).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_notification_shape_names_the_repo() {
        let key = "pre/repos/acme/mono/manifest.pb";
        for body in [
            serde_json::json!({"Records": [{"eventName": "s3:ObjectCreated:Put", "s3": {"object": {"key": key}}}]}),
            serde_json::json!({"key": key}),
            serde_json::json!({"repo": "acme/mono"}),
        ] {
            let repos = notified_repos(body.to_string().as_bytes(), "pre/");
            let names: Vec<_> = repos.iter().map(ToString::to_string).collect();
            assert_eq!(names, ["acme/mono"], "{body}");
        }
        assert!(notified_repos(b"not json", "pre/").is_empty());
        let other = serde_json::json!({"key": "pre/repos/acme/mono/wal/x.pack"});
        assert!(notified_repos(other.to_string().as_bytes(), "pre/").is_empty());
    }

    fn names(body: &serde_json::Value, prefix: &str) -> Vec<String> {
        notified_repos(body.to_string().as_bytes(), prefix)
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    #[test]
    fn an_s3_event_with_several_records_names_each_repo_once() {
        let rec = |name: &str, key: &str| {
            serde_json::json!({"eventName": name, "s3": {"object": {"key": key}}})
        };
        let body = serde_json::json!({"Records": [
            rec("s3:ObjectCreated:Put", "repos/a/one/manifest.pb"),
            rec("s3:ObjectCreated:CompleteMultipartUpload", "repos/b/two/manifest.pb"),
            rec("ObjectCreated:Put", "repos/c/three/manifest.pb"),
            rec("s3:ObjectCreated:Put", "repos/a/one/manifest.pb"),
            rec("s3:ObjectRemoved:Delete", "repos/d/gone/manifest.pb"),
            rec("s3:ObjectCreated:Put", "repos/e/four/log/1.pb"),
            rec("s3:ObjectCreated:Put", "repos/a%2Dx/five/manifest.pb"),
        ]});
        assert_eq!(names(&body, ""), ["a/one", "a-x/five", "b/two", "c/three"]);
    }

    #[test]
    fn a_gcs_envelope_counts_only_finalize_events() {
        let env = |event: &str, id: &str| {
            serde_json::json!({"message": {"attributes": {"eventType": event, "objectId": id}}})
        };
        assert_eq!(
            names(&env("OBJECT_FINALIZE", "repos/a/one/manifest.pb"), ""),
            ["a/one"]
        );
        for event in ["OBJECT_DELETE", "OBJECT_ARCHIVE", "OBJECT_METADATA_UPDATE", ""] {
            assert!(names(&env(event, "repos/a/one/manifest.pb"), "").is_empty(), "{event}");
        }
    }

    #[test]
    fn the_store_prefix_scopes_what_is_notified() {
        let key = |k: &str| serde_json::json!({"key": k});
        assert_eq!(names(&key("pre/fix/repos/a/one/manifest.pb"), "pre/fix/"), ["a/one"]);
        assert!(names(&key("repos/a/one/manifest.pb"), "pre/fix/").is_empty());
        assert!(names(&key("other/repos/a/one/manifest.pb"), "pre/fix/").is_empty());
        assert_eq!(names(&serde_json::json!({"repo": "a/one"}), "pre/fix/"), ["a/one"]);
        let s3 = serde_json::json!({"Records": [{"eventName": "s3:ObjectCreated:Put",
            "s3": {"object": {"key": "pre/fix/repos/a/one/manifest.pb"}}}]});
        assert_eq!(names(&s3, "pre/fix/"), ["a/one"]);
        assert!(names(&s3, "").is_empty());
    }
}
