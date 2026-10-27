//! Subjects of the `JetStream` events: `<prefix>.ref.<owner>.<repo>`.
//!
//! A NATS subject token may not contain `.`, `*`, `>` or whitespace, and a repository
//! name may contain `.` (`RepoId` allows `[A-Za-z0-9._-]`). Each of owner and repo is
//! therefore one [`Token`]: every byte outside `[A-Za-z0-9_-]` becomes `%XX` (uppercase hex of
//! the UTF-8 byte, `%` itself included), so `acme.io` is `acme%2Eio` and the encoding is
//! reversible token by token.

use std::fmt::{self, Write as _};

use walgit_git::RepoId;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SubjectError {
    #[error("subject prefix {0:?} must be dot-separated tokens without '*', '>' or whitespace")]
    InvalidPrefix(String),
    #[error("{0:?} is not a repository subject under this prefix")]
    NotARepoSubject(String),
}

/// One encoded subject token (never empty, never contains `.`, `*`, `>` or whitespace).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token(String);

impl Token {
    pub fn encode(raw: &str) -> Self {
        let mut out = String::with_capacity(raw.len());
        for b in raw.bytes() {
            if b.is_ascii_alphanumeric() || b == b'_' || b == b'-' {
                out.push(char::from(b));
            } else {
                let _ = write!(out, "%{b:02X}");
            }
        }
        Token(out)
    }

    /// `None` when `encoded` is not what [`Token::encode`] produces.
    pub fn decode(encoded: &str) -> Option<String> {
        let bytes = encoded.as_bytes();
        let mut out = Vec::with_capacity(bytes.len());
        let mut i = 0;
        while let Some(&b) = bytes.get(i) {
            match b {
                b'%' => {
                    let hex = bytes.get(i + 1..i + 3)?;
                    if !hex
                        .iter()
                        .all(|h| h.is_ascii_digit() || (b'A'..=b'F').contains(h))
                    {
                        return None;
                    }
                    out.push(u8::from_str_radix(std::str::from_utf8(hex).ok()?, 16).ok()?);
                    i += 3;
                }
                _ if b.is_ascii_alphanumeric() || b == b'_' || b == b'-' => {
                    out.push(b);
                    i += 1;
                }
                _ => return None,
            }
        }
        if out.is_empty() {
            return None;
        }
        // One subject per repository: a byte escaped where `encode` keeps it is refused.
        String::from_utf8(out)
            .ok()
            .filter(|raw| Token::encode(raw).0 == encoded)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The configured `events.nats.subject_prefix`: dot-separated, literal tokens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubjectPrefix(String);

impl SubjectPrefix {
    pub fn new(raw: &str) -> Result<Self, SubjectError> {
        let literal = |t: &str| {
            !t.is_empty()
                && !t
                    .chars()
                    .any(|c| c.is_whitespace() || c.is_control() || matches!(c, '*' | '>'))
        };
        if raw.split('.').all(literal) {
            Ok(SubjectPrefix(raw.to_string()))
        } else {
            Err(SubjectError::InvalidPrefix(raw.to_string()))
        }
    }

    /// `<prefix>.ref.>`: what the stream captures.
    pub fn stream_filter(&self) -> String {
        format!("{}.ref.>", self.0)
    }

    /// `<prefix>.<endpoint group>` for the service API.
    pub fn child(&self, name: &str) -> String {
        format!("{}.{name}", self.0)
    }

    fn ref_root(&self) -> String {
        format!("{}.ref.", self.0)
    }
}

impl fmt::Display for SubjectPrefix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// `<prefix>.ref.<owner>.<repo>`: one repository's events, in WAL order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepoSubject(String);

impl RepoSubject {
    pub fn new(prefix: &SubjectPrefix, repo: &RepoId) -> Self {
        RepoSubject(format!(
            "{}{}.{}",
            prefix.ref_root(),
            Token::encode(repo.owner()),
            Token::encode(repo.name())
        ))
    }

    pub fn parse(prefix: &SubjectPrefix, subject: &str) -> Result<RepoId, SubjectError> {
        let err = || SubjectError::NotARepoSubject(subject.to_string());
        let rest = subject.strip_prefix(&prefix.ref_root()).ok_or_else(err)?;
        let (owner, name) = rest.split_once('.').ok_or_else(err)?;
        let owner = Token::decode(owner).ok_or_else(err)?;
        let name = Token::decode(name).ok_or_else(err)?;
        RepoId::new(owner, name).map_err(|_| err())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RepoSubject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prefix() -> SubjectPrefix {
        SubjectPrefix::new("walgit").unwrap()
    }

    #[test]
    fn dots_are_escaped_and_round_trip() {
        let repo = RepoId::new("acme.io", "mono.repo-v2_x").unwrap();
        let subject = RepoSubject::new(&prefix(), &repo);
        assert_eq!(subject.as_str(), "walgit.ref.acme%2Eio.mono%2Erepo-v2_x");
        assert_eq!(RepoSubject::parse(&prefix(), subject.as_str()).unwrap(), repo);
    }

    #[test]
    fn token_never_carries_a_nats_metacharacter() {
        let t = Token::encode("a.b*c>d e\t%é/");
        assert!(!t.as_str().contains(['.', '*', '>', ' ', '\t', '/']));
        assert_eq!(t.as_str(), "a%2Eb%2Ac%3Ed%20e%09%25%C3%A9%2F");
        assert_eq!(Token::decode(t.as_str()).unwrap(), "a.b*c>d e\t%é/");
    }

    #[test]
    fn decode_refuses_what_encode_never_emits() {
        for bad in ["", "a.b", "a%2", "a%zz", "a%2e", "a b", "a*", "%FF", "%41cme", "a%5Fb", "a%2Db"] {
            assert_eq!(Token::decode(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn parse_refuses_foreign_subjects() {
        for bad in [
            "walgit.ref.acme",
            "walgit.ref.acme.repo.extra",
            "other.ref.acme.repo",
            "walgit.refs.acme.repo",
            "walgit.ref..repo",
            "walgit.ref.%2E%2E.repo",
            "walgit.ref.%61cme.repo",
        ] {
            assert!(RepoSubject::parse(&prefix(), bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn prefix_validation_and_filter() {
        assert_eq!(prefix().stream_filter(), "walgit.ref.>");
        let p = SubjectPrefix::new("acme.git").unwrap();
        assert_eq!(p.stream_filter(), "acme.git.ref.>");
        assert_eq!(p.child("events"), "acme.git.events");
        for bad in ["", ".a", "a.", "a..b", "a.*", "a.>", "a b"] {
            assert!(SubjectPrefix::new(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn odd_names_round_trip_and_never_carry_a_metacharacter() {
        let names = [
            "a", "-", "_", "a.b.c", "*", ">", "%", "%2E", "%%", "a b", "tab\tnl\n", "é", "日本語",
            "emoji\u{1F600}", "a/b\\c", "\u{0}", "..", "*.>", "Z%zz",
        ];
        for raw in names {
            let t = Token::encode(raw);
            assert!(!t.as_str().is_empty(), "{raw:?}");
            assert!(
                t.as_str()
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'%')),
                "{raw:?} -> {t}"
            );
            assert_eq!(Token::decode(t.as_str()).as_deref(), Some(raw), "{raw:?}");
        }
        let mut seen = std::collections::HashSet::new();
        assert!(names.iter().all(|n| seen.insert(Token::encode(n).to_string())), "injective");
    }

    #[test]
    fn every_single_byte_string_round_trips() {
        for c in (1u32..0x250).filter_map(char::from_u32) {
            let raw = c.to_string();
            let t = Token::encode(&raw);
            assert_eq!(Token::decode(t.as_str()).as_deref(), Some(raw.as_str()), "{c:?}");
            assert!(!t.as_str().contains(['.', '*', '>']), "{c:?}");
        }
    }
}
