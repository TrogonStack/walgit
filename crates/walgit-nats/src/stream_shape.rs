//! What `walgit-nats` needs from the stream it publishes into. The operator provisions the stream
//! (`docs/EVENTS.md`); walgit only checks it, so it never needs the permission to create one.

use std::fmt;

use async_nats::jetstream::stream;

use crate::subject::SubjectPrefix;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamProblem {
    /// Memory storage loses the events, and with them the cursor, on a server restart.
    NotFileStorage(stream::StorageType),
    /// Interest or work-queue retention removes the messages the cursor is read from.
    NotLimitsRetention(stream::RetentionPolicy),
    /// No subject of the stream captures `<prefix>.ref.>`.
    SubjectsMissFilter {
        filter: String,
        subjects: Vec<String>,
    },
    /// The publisher relies on `Nats-Msg-Id` deduplication.
    NoDuplicateWindow,
}

impl fmt::Display for StreamProblem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StreamProblem::NotFileStorage(s) => write!(f, "storage is {s:?}, want file"),
            StreamProblem::NotLimitsRetention(r) => write!(f, "retention is {r:?}, want limits"),
            StreamProblem::SubjectsMissFilter { filter, subjects } => {
                write!(f, "subjects {subjects:?} do not capture {filter}")
            }
            StreamProblem::NoDuplicateWindow => write!(f, "duplicate window is 0, want one"),
        }
    }
}

/// Every way `config` falls short of what the publisher needs; empty when it is usable.
pub fn problems(config: &stream::Config, prefix: &SubjectPrefix) -> Vec<StreamProblem> {
    let mut problems = Vec::new();
    if config.storage != stream::StorageType::File {
        problems.push(StreamProblem::NotFileStorage(config.storage));
    }
    if config.retention != stream::RetentionPolicy::Limits {
        problems.push(StreamProblem::NotLimitsRetention(config.retention));
    }
    let filter = prefix.stream_filter();
    if !config.subjects.iter().any(|s| captures(s, &filter)) {
        problems.push(StreamProblem::SubjectsMissFilter {
            filter,
            subjects: config.subjects.clone(),
        });
    }
    if config.duplicate_window.is_zero() {
        problems.push(StreamProblem::NoDuplicateWindow);
    }
    problems
}

/// Whether every subject `filter` matches is also matched by `subject`.
fn captures(subject: &str, filter: &str) -> bool {
    let mut wanted = filter.split('.');
    for token in subject.split('.') {
        if token == ">" {
            return wanted.next().is_some();
        }
        match wanted.next() {
            Some(">") | None => return false,
            Some(w) if token != "*" && token != w => return false,
            Some(_) => {}
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn prefix() -> SubjectPrefix {
        SubjectPrefix::new("walgit").unwrap()
    }

    fn usable() -> stream::Config {
        stream::Config {
            name: "WALGIT_EVENTS".into(),
            subjects: vec!["walgit.ref.>".into()],
            storage: stream::StorageType::File,
            duplicate_window: Duration::from_hours(1),
            ..Default::default()
        }
    }

    #[test]
    fn a_file_limits_stream_capturing_the_filter_is_usable() {
        assert_eq!(problems(&usable(), &prefix()), []);
    }

    #[test]
    fn wider_subjects_capture_the_filter() {
        for subject in ["walgit.ref.>", "walgit.>", "walgit.*.>", ">", "*.ref.>"] {
            assert!(captures(subject, "walgit.ref.>"), "{subject}");
        }
        for subject in [
            "walgit.ref.*",
            "walgit.ref.a.>",
            "walgit.ref",
            "other.>",
            "walgit.*",
            "walgit.ref.*.*",
        ] {
            assert!(!captures(subject, "walgit.ref.>"), "{subject}");
        }
    }

    #[test]
    fn every_shortfall_is_reported_at_once() {
        let config = stream::Config {
            subjects: vec!["walgit.ref.*".into(), "other.>".into()],
            storage: stream::StorageType::Memory,
            retention: stream::RetentionPolicy::WorkQueue,
            duplicate_window: Duration::ZERO,
            ..usable()
        };
        assert_eq!(
            problems(&config, &prefix()),
            [
                StreamProblem::NotFileStorage(stream::StorageType::Memory),
                StreamProblem::NotLimitsRetention(stream::RetentionPolicy::WorkQueue),
                StreamProblem::SubjectsMissFilter {
                    filter: "walgit.ref.>".into(),
                    subjects: vec!["walgit.ref.*".into(), "other.>".into()],
                },
                StreamProblem::NoDuplicateWindow,
            ]
        );
    }
}
