//! `ref` event shapes and the WAL → event conversion. Contract:
//! `docs/EVENTS.md`. Shared by the producers (the webhook bridge in
//! `walgit-server`, the `JetStream` publisher in `walgit-nats`): they tail each
//! repo's WAL from a durable cursor and convert committed PUSH / `REF_UPDATE`
//! entries with [`refs_from_entries`]. Nothing on the push path knows events
//! exist, and neither does the WAL crate (D32).
//!
//! Wire facts consumers rely on (the doc is normative, this is the code):
//! `old`/`new` are always full zero OIDs on create/delete, never empty;
//! dedup key = `(repo, _walgit.seq, ref_name)`; order by `_walgit.seq`.

#![allow(clippy::indexing_slicing)]

use serde::Serialize;
use walgit_git::RepoId;
use walgit_proto::v1::{EntryKind, LogEntry};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Action {
    Create,
    Update,
    Delete,
}

impl Action {
    pub fn as_str(self) -> &'static str {
        match self {
            Action::Create => "create",
            Action::Update => "update",
            Action::Delete => "delete",
        }
    }
}

fn absent(oid: &str) -> bool {
    oid.is_empty() || oid.bytes().all(|b| b == b'0')
}

/// `create` / `update` / `delete` from the oid pair; `None` = no-op
/// (`0→0`, `X→X`) which must not emit.
pub fn classify(old: &str, new: &str) -> Option<Action> {
    match (absent(old), absent(new)) {
        (true, true) => None,
        (true, false) => Some(Action::Create),
        (false, true) => Some(Action::Delete),
        (false, false) if old == new => None,
        (false, false) => Some(Action::Update),
    }
}

/// The zero OID of the same length as `other` (sha1: 40, sha256: 64). The
/// WAL records "absent" as `""` (import-built txns) or the wire's zero OID
/// (pushes); the event always carries the zero OID.
fn zero_like(other: &str) -> String {
    "0".repeat(other.len())
}

pub fn ref_type(name: &str) -> &'static str {
    if name.starts_with("refs/heads/") {
        "branch"
    } else if name.starts_with("refs/tags/") {
        "tag"
    } else {
        ""
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct WalgitExt {
    pub schema_version: u32,
    /// WAL seq of the committed entry, as a string (uint64 convention).
    pub seq: String,
    /// `push` | `ref_update`.
    pub entry_kind: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub request_id: String,
}

/// Top level byte-compatible with the common `RefEvent` shape; walgit adds `repo`
/// and `_walgit`.
#[derive(Debug, Clone, Serialize)]
pub struct RefEvent {
    pub action: Action,
    pub ref_type: &'static str,
    pub ref_name: String,
    pub old: String,
    pub new: String,
    pub pusher: String,
    pub correlation_id: String,
    pub repo: String,
    #[serde(rename = "_walgit")]
    pub walgit: WalgitExt,
}

impl RefEvent {
    /// Message key (`<repo>/<ref_name>`): per-ref order where a transport keeps it.
    pub fn key(&self) -> String {
        format!("{}/{}", self.repo, self.ref_name)
    }
    /// Transport headers (`x_walgit_<name>`): filtering without parsing bodies.
    /// Always a strict subset of the body.
    pub fn headers(&self) -> [(&'static str, &str); 7] {
        [
            ("repo", &self.repo),
            ("action", self.action.as_str()),
            ("ref_type", self.ref_type),
            ("ref_name", &self.ref_name),
            ("pusher", &self.pusher),
            ("seq", &self.walgit.seq),
            ("correlation_id", &self.correlation_id),
        ]
    }
}

/// `ref` events for the PUSH / `REF_UPDATE` entries in `entries`, in seq order.
pub fn refs_from_entries(repo: &RepoId, entries: &[LogEntry], out: &mut Vec<RefEvent>) {
    let repo = repo.to_string();
    for entry in entries {
        let entry_kind = match EntryKind::try_from(entry.kind) {
            Ok(EntryKind::Push) => "push",
            Ok(EntryKind::RefUpdate) => "ref_update",
            // COMPACT / CHECKPOINT never move refs.
            _ => continue,
        };
        let Some(txn) = &entry.txn else { continue };
        let pusher = entry.meta.get("principal").cloned().unwrap_or_default();
        let request_id = entry.meta.get("request_id").cloned().unwrap_or_default();
        for u in &txn.updates {
            if !u.new_symbolic_target.is_empty() {
                continue; // HEAD retarget: no oid transition to announce
            }
            let Some(action) = classify(&u.old_oid, &u.new_oid) else {
                continue;
            };
            let (old, new) = match action {
                Action::Create => (zero_like(&u.new_oid), u.new_oid.clone()),
                Action::Delete => (u.old_oid.clone(), zero_like(&u.old_oid)),
                Action::Update => (u.old_oid.clone(), u.new_oid.clone()),
            };
            out.push(RefEvent {
                action,
                ref_type: ref_type(&u.name),
                ref_name: u.name.clone(),
                old,
                new,
                pusher: pusher.clone(),
                correlation_id: request_id.clone(),
                repo: repo.clone(),
                walgit: WalgitExt {
                    schema_version: 1,
                    seq: entry.seq.to_string(),
                    entry_kind: entry_kind.to_string(),
                    request_id: request_id.clone(),
                },
            });
        }
    }
}

/// The bucket object keys a notification body names, `store.prefix` included as the
/// notifications carry it. Store-agnostic.
pub fn notified_keys(v: &serde_json::Value, store_prefix: &str) -> Vec<String> {
    let mut keys = Vec::new();
    // GCS → Pub/Sub push envelope.
    let attrs = &v["message"]["attributes"];
    if attrs["eventType"] == "OBJECT_FINALIZE"
        && let Some(k) = attrs["objectId"].as_str()
    {
        keys.push(k.to_string());
    }
    // S3 event notification (also what MinIO/rustfs/Ceph emit; MinIO spells the name `s3:ObjectCreated:*`).
    if let Some(records) = v["Records"].as_array() {
        for r in records {
            if r["eventName"]
                .as_str()
                .is_some_and(|e| e.strip_prefix("s3:").unwrap_or(e).starts_with("ObjectCreated"))
                && let Some(k) = r["s3"]["object"]["key"].as_str()
            {
                keys.push(percent_decode(k));
            }
        }
    }
    // Plain shapes for your own glue.
    if let Some(k) = v["key"].as_str() {
        keys.push(k.to_string());
    }
    // A repository id names no object: use the key its manifest has in this bucket.
    if let Some(r) = v["repo"].as_str() {
        keys.push(format!("{store_prefix}repos/{r}/manifest.pb"));
    }
    keys
}

/// S3 URL-encodes keys in notifications (`+` is a space, `%XX` a byte).
fn percent_decode(key: &str) -> String {
    let bytes = key.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while let Some(&b) = bytes.get(i) {
        match b {
            b'+' => out.push(b' '),
            b'%' => {
                let hex = bytes
                    .get(i + 1..i + 3)
                    .and_then(|h| std::str::from_utf8(h).ok())
                    .and_then(|h| u8::from_str_radix(h, 16).ok());
                if let Some(decoded) = hex {
                    out.push(decoded);
                    i += 2;
                } else {
                    out.push(b);
                }
            }
            _ => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// `prefix/repos/<o>/<r>/manifest.pb` → `o/r`: the repository whose commit point
/// `object` is. `None` for every other key.
pub fn manifest_repo(object: &str, store_prefix: &str) -> Option<RepoId> {
    let rel = object
        .strip_prefix(store_prefix)?
        .strip_prefix("repos/")?
        .strip_suffix("/manifest.pb")?;
    let (owner, name) = rel.split_once('/')?;
    if name.contains('/') {
        return None;
    }
    RepoId::new(owner, name).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn classify_shapes() {
        assert_eq!(classify("", ""), None);
        assert_eq!(classify("0000", "0000"), None);
        assert_eq!(classify("", "abc"), Some(Action::Create));
        assert_eq!(classify("abc", ""), Some(Action::Delete));
        assert_eq!(classify("abc", "abc"), None);
        assert_eq!(classify("abc", "def"), Some(Action::Update));
        assert_eq!(ref_type("refs/heads/main"), "branch");
        assert_eq!(ref_type("refs/tags/v1"), "tag");
        assert_eq!(ref_type("refs/notes/commits"), "");
    }

    /// The golden body shape (docs/EVENTS.md). Field order and names are the
    /// contract; changing this test is changing the wire.
    #[test]
    fn ref_event_golden() {
        let ev = RefEvent {
            action: Action::Update,
            ref_type: "branch",
            ref_name: "refs/heads/main".into(),
            old: "48a0637".into(),
            new: "cb38da1".into(),
            pusher: "alice@example.com".into(),
            correlation_id: "d1f916f7".into(),
            repo: "acme/monorepo".into(),
            walgit: WalgitExt {
                schema_version: 1,
                seq: "42".into(),
                entry_kind: "push".into(),
                request_id: "d1f916f7".into(),
            },
        };
        let got = serde_json::to_string(&ev).unwrap();
        let want = concat!(
            "{\"action\":\"update\",\"ref_type\":\"branch\",\"ref_name\":\"refs/heads/main\",",
            "\"old\":\"48a0637\",\"new\":\"cb38da1\",\"pusher\":\"alice@example.com\",",
            "\"correlation_id\":\"d1f916f7\",\"repo\":\"acme/monorepo\",",
            "\"_walgit\":{\"schema_version\":1,\"seq\":\"42\",\"entry_kind\":\"push\",",
            "\"request_id\":\"d1f916f7\"}}"
        );
        assert_eq!(got, want);
        assert_eq!(ev.key(), "acme/monorepo/refs/heads/main");
    }

    fn log_entry(kind: EntryKind, name: &str, old: &str, new: &str, sym: &str) -> LogEntry {
        let mut meta = HashMap::new();
        meta.insert("principal".into(), "alice".into());
        meta.insert("request_id".into(), "cid".into());
        LogEntry {
            seq: 9,
            kind: kind as i32,
            txn: Some(walgit_proto::v1::RefTransaction {
                updates: vec![walgit_proto::v1::RefUpdate {
                    name: name.into(),
                    old_oid: old.into(),
                    new_oid: new.into(),
                    new_symbolic_target: sym.into(),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            meta,
            ..Default::default()
        }
    }

    #[test]
    fn manifest_object_names() {
        let prefix = "prefix/";
        let id = manifest_repo("prefix/repos/acme/monorepo/manifest.pb", prefix).unwrap();
        assert_eq!(id.to_string(), "acme/monorepo");
        // Every body shape names the same repository under a non-empty prefix.
        for body in [
            serde_json::json!({"repo": "acme/monorepo"}),
            serde_json::json!({"key": "prefix/repos/acme/monorepo/manifest.pb"}),
            serde_json::json!({"message": {"attributes": {
                "eventType": "OBJECT_FINALIZE",
                "objectId": "prefix/repos/acme/monorepo/manifest.pb"}}}),
            serde_json::json!({"Records": [{"eventName": "ObjectCreated:Put",
                "s3": {"object": {"key": "prefix/repos/acme/monorepo/manifest.pb"}}}]}),
            serde_json::json!({"Records": [{"eventName": "s3:ObjectCreated:Put",
                "s3": {"object": {"key": "prefix/repos/acme/monorepo/manifest.pb"}}}]}),
        ] {
            let ids: Vec<_> = notified_keys(&body, prefix)
                .iter()
                .filter_map(|k| manifest_repo(k, prefix))
                .map(|id| id.to_string())
                .collect();
            assert_eq!(ids, ["acme/monorepo"], "{body}");
        }
        for other in [
            "repos/t/r/manifest.pb",
            "prefix/repos/t/r/wal/abc.pack",
            "prefix/repos/t/r/events/cursor.json",
            "walgit-go/repos/t/r/manifest.pb",
            "prefix/repos/t/manifest.pb",
            "prefix/repos/t/r/x/manifest.pb",
        ] {
            assert!(manifest_repo(other, prefix).is_none(), "{other}");
        }
    }

    #[test]
    fn s3_keys_are_percent_decoded() {
        assert_eq!(percent_decode("a%2Fb+c%zz%4"), "a/b c%zz%4");
    }

    const SHA: &str = "4a06219d6a5ba9d3e3d1a8e1e2b6f0c9d8e7f6a5";

    #[test]
    fn conversion_emits_ref_skips_compact_noop_symbolic() {
        let repo = walgit_git::RepoId::new("t", "r").unwrap();
        let mut out = Vec::new();
        refs_from_entries(
            &repo,
            &[
                log_entry(EntryKind::Compact, "refs/heads/main", "", SHA, ""),
                log_entry(EntryKind::Push, "refs/heads/main", SHA, SHA, ""),
                log_entry(EntryKind::Push, "HEAD", "", "", "refs/heads/main"),
                log_entry(EntryKind::Push, "refs/heads/main", "", SHA, ""),
                log_entry(EntryKind::Push, "refs/heads/main", SHA, &"0".repeat(40), ""),
            ],
            &mut out,
        );
        assert_eq!(out.len(), 2, "compact/noop/symbolic must not emit");
        let r = &out[0];
        assert_eq!(r.action, Action::Create);
        assert_eq!(r.ref_name, "refs/heads/main");
        assert_eq!(
            r.old,
            "0".repeat(40),
            "absent old is the zero OID, never empty"
        );
        assert_eq!(r.new, SHA);
        assert_eq!(
            (r.pusher.as_str(), r.correlation_id.as_str()),
            ("alice", "cid")
        );
        assert_eq!((r.repo.as_str(), r.walgit.seq.as_str()), ("t/r", "9"));
        assert_eq!(r.walgit.entry_kind, "push");
        let d = &out[1];
        assert_eq!(d.action, Action::Delete);
        assert_eq!(
            (d.old.as_str(), d.new.as_str()),
            (SHA, "0".repeat(40).as_str())
        );
    }
}
