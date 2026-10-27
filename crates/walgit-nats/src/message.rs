//! One `RefEvent` as a `JetStream` message: body = the webhook's event JSON, headers
//! `Walgit-Seq`, `Walgit-Repo` and `Nats-Msg-Id = <repo>:<seq>:<ref_name>`.

use std::fmt;

use async_nats::header::NATS_MESSAGE_ID;
use async_nats::{HeaderMap, jetstream::message::PublishMessage};
use bytes::Bytes;
use walgit_events::RefEvent;

use crate::subject::RepoSubject;

pub const HEADER_SEQ: &str = "Walgit-Seq";
pub const HEADER_REPO: &str = "Walgit-Repo";

/// `<repo>:<seq>:<ref_name>`: `JetStream`'s dedup key and, with the repository, the consumer's.
/// A repository id never contains `:` and a seq is a number, so the first two colons delimit
/// the ref name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MsgId {
    repo: String,
    seq: u64,
    ref_name: String,
}

impl MsgId {
    pub fn new(repo: &str, seq: u64, ref_name: &str) -> Self {
        MsgId {
            repo: repo.to_string(),
            seq,
            ref_name: ref_name.to_string(),
        }
    }

    pub fn of(event: &RefEvent) -> Option<Self> {
        Some(MsgId::new(
            &event.repo,
            event.walgit.seq.parse().ok()?,
            &event.ref_name,
        ))
    }

    pub fn parse(raw: &str) -> Option<Self> {
        let mut parts = raw.splitn(3, ':');
        let repo = parts.next()?;
        let seq = parts.next()?.parse().ok()?;
        let ref_name = parts.next()?;
        Some(MsgId::new(repo, seq, ref_name))
    }

    pub fn seq(&self) -> u64 {
        self.seq
    }

    pub fn ref_name(&self) -> &str {
        &self.ref_name
    }
}

impl fmt::Display for MsgId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}:{}", self.repo, self.seq, self.ref_name)
    }
}

/// The last event on a repository's subject, as the stream remembers it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LastPublished {
    /// Stream sequence of the message (the `Nats-Expected-Last-Subject-Sequence` token).
    pub stream_seq: u64,
    pub id: MsgId,
    /// The ref's `old` and `new` from the body: a seq names an entry only within one incarnation
    /// of the repository, so these tell its entry apart from a recreated one's at the same seq.
    pub old: String,
    pub new: String,
}

impl LastPublished {
    pub fn from_message(stream_seq: u64, headers: &HeaderMap, payload: &[u8]) -> Option<Self> {
        let id = MsgId::parse(headers.get(NATS_MESSAGE_ID)?.as_str())?;
        let seq: u64 = headers.get(HEADER_SEQ)?.as_str().parse().ok()?;
        let body: serde_json::Value = serde_json::from_slice(payload).ok()?;
        let oid = |key: &str| body.get(key)?.as_str().map(str::to_owned);
        (seq == id.seq()).then_some(LastPublished {
            stream_seq,
            id,
            old: oid("old")?,
            new: oid("new")?,
        })
    }

    /// Whether the entry read at this message's seq yielded the event it carries. `false` means
    /// the stream holds another incarnation of the repository.
    pub fn published_from(&self, events: &[RefEvent]) -> bool {
        let seq = self.id.seq().to_string();
        events.iter().any(|e| {
            e.walgit.seq == seq
                && e.ref_name == self.id.ref_name()
                && e.old == self.old
                && e.new == self.new
        })
    }
}

/// The events of `last`'s entry that were not published yet, and everything after it. One WAL
/// entry can yield several events, so a crash mid-entry leaves a tail to finish; resuming from the
/// entry (inclusive) and dropping what the stream already holds makes it exact.
pub fn after_last_published(events: Vec<RefEvent>, last: &LastPublished) -> Vec<RefEvent> {
    let seq = last.id.seq().to_string();
    let published = events
        .iter()
        .position(|e| e.walgit.seq == seq && e.ref_name == last.id.ref_name());
    match published {
        Some(i) => events.into_iter().skip(i + 1).collect(),
        None => events,
    }
}

pub struct EventMessage {
    pub subject: RepoSubject,
    pub id: MsgId,
    publish: PublishMessage,
}

impl EventMessage {
    pub fn new(subject: RepoSubject, event: &RefEvent) -> anyhow::Result<Self> {
        let id = MsgId::of(event)
            .ok_or_else(|| anyhow::anyhow!("event seq {:?} is not a number", event.walgit.seq))?;
        let publish = PublishMessage::build()
            .payload(Bytes::from(serde_json::to_vec(event)?))
            .message_id(id.to_string())
            .header(HEADER_SEQ, event.walgit.seq.as_str())
            .header(HEADER_REPO, event.repo.as_str());
        Ok(EventMessage {
            subject,
            id,
            publish,
        })
    }

    /// Fails the publish unless the subject's last message is `stream_seq` (`0` = none yet).
    #[must_use]
    pub fn expecting_last_subject_sequence(mut self, stream_seq: u64) -> Self {
        self.publish = self.publish.expected_last_subject_sequence(stream_seq);
        self
    }

    pub fn into_publish(self) -> PublishMessage {
        self.publish
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use walgit_events::{Action, WalgitExt};

    fn event(seq: u64, ref_name: &str) -> RefEvent {
        RefEvent {
            action: Action::Update,
            ref_type: "branch",
            ref_name: ref_name.into(),
            old: "a".into(),
            new: "b".into(),
            pusher: "p".into(),
            correlation_id: "c".into(),
            repo: "acme/repo".into(),
            walgit: WalgitExt {
                schema_version: 1,
                seq: seq.to_string(),
                entry_kind: "push".into(),
                request_id: "c".into(),
            },
        }
    }

    #[test]
    fn msg_id_round_trips_with_colons_in_the_ref() {
        let id = MsgId::new("acme/repo", 42, "refs/heads/a:b");
        assert_eq!(id.to_string(), "acme/repo:42:refs/heads/a:b");
        assert_eq!(MsgId::parse(&id.to_string()), Some(id));
        assert_eq!(MsgId::parse("acme/repo:x:refs/heads/a"), None);
        assert_eq!(MsgId::parse("acme/repo:1"), None);
    }

    #[test]
    fn resume_drops_only_what_the_stream_already_holds() {
        let events = vec![
            event(5, "refs/heads/a"),
            event(5, "refs/heads/b"),
            event(5, "refs/heads/c"),
            event(6, "refs/heads/a"),
        ];
        let last = |seq, name: &str| LastPublished {
            stream_seq: 9,
            id: MsgId::new("acme/repo", seq, name),
            old: "a".into(),
            new: "b".into(),
        };
        let names = |v: Vec<RefEvent>| {
            v.into_iter()
                .map(|e| format!("{}:{}", e.walgit.seq, e.ref_name))
                .collect::<Vec<_>>()
        };
        // Crash after b: c and the next entry remain.
        assert_eq!(
            names(after_last_published(events.clone(), &last(5, "refs/heads/b"))),
            ["5:refs/heads/c", "6:refs/heads/a"]
        );
        // The whole entry was published.
        assert_eq!(
            names(after_last_published(events.clone(), &last(5, "refs/heads/c"))),
            ["6:refs/heads/a"]
        );
        // Nothing new at all.
        assert!(after_last_published(events.clone(), &last(6, "refs/heads/a")).is_empty());
        // The message is not among the events (entry rewritten or folded): publish everything.
        assert_eq!(
            after_last_published(events.clone(), &last(5, "refs/heads/zzz")).len(),
            4
        );
    }

    #[test]
    fn the_last_message_is_read_back_from_its_headers_and_body() {
        let ev = event(7, "refs/heads/main");
        let mut headers = HeaderMap::new();
        headers.insert(NATS_MESSAGE_ID, "acme/repo:7:refs/heads/main");
        headers.insert(HEADER_SEQ, "7");
        let body = serde_json::to_vec(&ev).unwrap();
        let last = LastPublished::from_message(3, &headers, &body).unwrap();
        assert_eq!((last.stream_seq, last.id.seq(), last.id.ref_name()), (3, 7, "refs/heads/main"));
        assert_eq!((last.old.as_str(), last.new.as_str()), ("a", "b"));

        assert_eq!(LastPublished::from_message(3, &headers, b"{}"), None, "no oids in the body");
        headers.insert(HEADER_SEQ, "8");
        assert_eq!(LastPublished::from_message(3, &headers, &body), None, "seq headers disagree");
    }

    #[test]
    fn only_the_entry_it_came_from_yields_the_last_message() {
        let last = LastPublished {
            stream_seq: 9,
            id: MsgId::new("acme/repo", 5, "refs/heads/main"),
            old: "a".into(),
            new: "b".into(),
        };
        assert!(last.published_from(&[event(5, "refs/heads/x"), event(5, "refs/heads/main")]));
        let mut recreated = event(5, "refs/heads/main");
        recreated.new = "c".into();
        assert!(!last.published_from(&[recreated]), "same seq and ref, another push");
        assert!(!last.published_from(&[event(5, "refs/heads/other")]));
        assert!(!last.published_from(&[event(6, "refs/heads/main")]));
        assert!(!last.published_from(&[]), "an entry without ref events");
    }

    #[test]
    fn message_carries_the_contract_headers() {
        let prefix = crate::subject::SubjectPrefix::new("walgit").unwrap();
        let repo = walgit_git::RepoId::new("acme", "repo").unwrap();
        let ev = event(7, "refs/heads/main");
        let msg = EventMessage::new(RepoSubject::new(&prefix, &repo), &ev).unwrap();
        assert_eq!(msg.id.to_string(), "acme/repo:7:refs/heads/main");
        assert_eq!(msg.subject.as_str(), "walgit.ref.acme.repo");
    }
}
