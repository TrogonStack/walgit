//! How `walgit nats` authenticates to NATS, resolved from the standard `NATS_*` environment
//! variables so secrets never sit in `walgit.toml`.

use std::fmt;
use std::path::PathBuf;

use anyhow::Context;

pub const ENV_NATS_CREDS: &str = "NATS_CREDS";
pub const ENV_NATS_NKEY: &str = "NATS_NKEY";
pub const ENV_NATS_USER: &str = "NATS_USER";
pub const ENV_NATS_PASSWORD: &str = "NATS_PASSWORD";
pub const ENV_NATS_TOKEN: &str = "NATS_TOKEN";

/// NATS authentication method.
///
/// When resolved from environment variables, priority order is:
/// 1. Credentials file (`NATS_CREDS`)
/// 2. `NKey` seed (`NATS_NKEY`)
/// 3. User/Password (`NATS_USER` + `NATS_PASSWORD`)
/// 4. Token (`NATS_TOKEN`)
/// 5. No auth
#[derive(Clone)]
pub enum NatsAuth {
    Credentials(PathBuf),
    NKey(String),
    UserPassword { user: String, password: String },
    Token(String),
    None,
}

/// `NKey`, `password`, and `Token` are bearer credentials and must not be logged.
impl fmt::Debug for NatsAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Credentials(path) => f.debug_tuple("Credentials").field(path).finish(),
            Self::NKey(_) => f.debug_tuple("NKey").field(&"<redacted>").finish(),
            Self::UserPassword { user, .. } => f
                .debug_struct("UserPassword")
                .field("user", user)
                .field("password", &"<redacted>")
                .finish(),
            Self::Token(_) => f.debug_tuple("Token").field(&"<redacted>").finish(),
            Self::None => f.write_str("None"),
        }
    }
}

impl NatsAuth {
    pub fn description(&self) -> &'static str {
        match self {
            Self::Credentials(_) => "credentials file",
            Self::NKey(_) => "NKey",
            Self::UserPassword { .. } => "user/password",
            Self::Token(_) => "token",
            Self::None => "none",
        }
    }

    /// Resolve from the process environment.
    pub fn from_process_env() -> Self {
        Self::from_env(|name| std::env::var(name).ok())
    }

    /// Resolve through `var`, in the priority order of [`NatsAuth`]. An empty variable is unset.
    pub fn from_env(var: impl Fn(&str) -> Option<String>) -> Self {
        let var = |name: &str| var(name).filter(|v| !v.is_empty());
        if let Some(path) = var(ENV_NATS_CREDS) {
            return Self::Credentials(PathBuf::from(path));
        }
        if let Some(seed) = var(ENV_NATS_NKEY) {
            return Self::NKey(seed);
        }
        if let (Some(user), Some(password)) = (var(ENV_NATS_USER), var(ENV_NATS_PASSWORD)) {
            return Self::UserPassword { user, password };
        }
        if let Some(token) = var(ENV_NATS_TOKEN) {
            return Self::Token(token);
        }
        Self::None
    }

    pub(crate) async fn connect_options(&self) -> anyhow::Result<async_nats::ConnectOptions> {
        Ok(match self {
            Self::Credentials(path) => {
                async_nats::ConnectOptions::with_credentials_file(path.clone())
                    .await
                    .with_context(|| format!("{ENV_NATS_CREDS}: {}", path.display()))?
            }
            Self::NKey(seed) => async_nats::ConnectOptions::with_nkey(seed.clone()),
            Self::UserPassword { user, password } => {
                async_nats::ConnectOptions::with_user_and_password(user.clone(), password.clone())
            }
            Self::Token(token) => async_nats::ConnectOptions::with_token(token.clone()),
            Self::None => async_nats::ConnectOptions::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn resolve(vars: &[(&str, &str)]) -> NatsAuth {
        let vars: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        NatsAuth::from_env(|name| vars.get(name).cloned())
    }

    #[test]
    fn no_variables_mean_no_auth() {
        assert!(matches!(resolve(&[]), NatsAuth::None));
    }

    #[test]
    fn credentials_take_priority() {
        let auth = resolve(&[
            ("NATS_CREDS", "/path/to/creds"),
            ("NATS_NKEY", "some-nkey"),
            ("NATS_TOKEN", "some-token"),
        ]);
        assert!(
            matches!(auth, NatsAuth::Credentials(p) if p == std::path::Path::new("/path/to/creds"))
        );
    }

    #[test]
    fn nkey_over_user_password_and_token() {
        let auth = resolve(&[
            ("NATS_NKEY", "my-nkey"),
            ("NATS_USER", "user"),
            ("NATS_PASSWORD", "pass"),
            ("NATS_TOKEN", "tok"),
        ]);
        assert!(matches!(auth, NatsAuth::NKey(k) if k == "my-nkey"));
    }

    #[test]
    fn user_password_over_token() {
        let auth = resolve(&[
            ("NATS_USER", "user"),
            ("NATS_PASSWORD", "pass"),
            ("NATS_TOKEN", "tok"),
        ]);
        assert!(matches!(
            auth,
            NatsAuth::UserPassword { user, password } if user == "user" && password == "pass"
        ));
    }

    #[test]
    fn token_when_nothing_else() {
        assert!(
            matches!(resolve(&[("NATS_TOKEN", "my-token")]), NatsAuth::Token(t) if t == "my-token")
        );
    }

    #[test]
    fn user_without_password_is_no_auth() {
        assert!(matches!(resolve(&[("NATS_USER", "user")]), NatsAuth::None));
        assert!(matches!(
            resolve(&[("NATS_PASSWORD", "pass")]),
            NatsAuth::None
        ));
    }

    #[test]
    fn empty_variables_are_unset() {
        let auth = resolve(&[
            ("NATS_CREDS", ""),
            ("NATS_NKEY", ""),
            ("NATS_USER", "u"),
            ("NATS_PASSWORD", "p"),
            ("NATS_TOKEN", "t"),
        ]);
        assert!(matches!(auth, NatsAuth::UserPassword { .. }), "{auth:?}");
        let auth = resolve(&[("NATS_USER", "u"), ("NATS_PASSWORD", ""), ("NATS_TOKEN", "t")]);
        assert!(matches!(auth, NatsAuth::Token(_)), "{auth:?}");
        assert!(matches!(resolve(&[("NATS_TOKEN", "")]), NatsAuth::None));
    }

    #[test]
    fn description_matches_variant() {
        assert_eq!(
            NatsAuth::Credentials("/a".into()).description(),
            "credentials file"
        );
        assert_eq!(NatsAuth::NKey("k".into()).description(), "NKey");
        assert_eq!(
            NatsAuth::UserPassword {
                user: "u".into(),
                password: "p".into()
            }
            .description(),
            "user/password"
        );
        assert_eq!(NatsAuth::Token("t".into()).description(), "token");
        assert_eq!(NatsAuth::None.description(), "none");
    }

    #[test]
    fn debug_redacts_credentials() {
        let nkey = format!("{:?}", NatsAuth::NKey("super-secret-nkey".into()));
        assert!(!nkey.contains("super-secret-nkey"));
        assert!(nkey.contains("<redacted>"));

        let user_password = format!(
            "{:?}",
            NatsAuth::UserPassword {
                user: "alice".into(),
                password: "super-secret-password".into(),
            }
        );
        assert!(user_password.contains("alice"));
        assert!(!user_password.contains("super-secret-password"));
        assert!(user_password.contains("<redacted>"));

        let token = format!("{:?}", NatsAuth::Token("super-secret-token".into()));
        assert!(!token.contains("super-secret-token"));
        assert!(token.contains("<redacted>"));
    }

    #[test]
    fn debug_shows_non_secret_variants() {
        let creds = format!(
            "{:?}",
            NatsAuth::Credentials(PathBuf::from("/etc/nats/app.creds"))
        );
        assert!(creds.contains("/etc/nats/app.creds"));
        assert_eq!(format!("{:?}", NatsAuth::None), "None");
    }
}
