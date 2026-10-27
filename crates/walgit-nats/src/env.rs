//! How `walgit nats` reaches NATS, read from the same `NATS_*` environment variables the `nats` CLI
//! reads and combined the way it combines them, so one shell environment drives both and secrets
//! never sit in `walgit.toml`.

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;

pub const ENV_NATS_URL: &str = "NATS_URL";
pub const ENV_NATS_USER: &str = "NATS_USER";
pub const ENV_NATS_PASSWORD: &str = "NATS_PASSWORD";
pub const ENV_NATS_TOKEN: &str = "NATS_TOKEN";
pub const ENV_NATS_CREDS: &str = "NATS_CREDS";
pub const ENV_NATS_NKEY: &str = "NATS_NKEY";
pub const ENV_NATS_JWT: &str = "NATS_JWT";
pub const ENV_NATS_SEED: &str = "NATS_SEED";
pub const ENV_NATS_CERT: &str = "NATS_CERT";
pub const ENV_NATS_KEY: &str = "NATS_KEY";
pub const ENV_NATS_CA: &str = "NATS_CA";
pub const ENV_NATS_CONTEXT: &str = "NATS_CONTEXT";
pub const ENV_NATS_SOCKS_PROXY: &str = "NATS_SOCKS_PROXY";

/// A value that must never reach a log line.
#[derive(Clone, PartialEq, Eq)]
pub struct Secret(String);

impl Secret {
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// Who the connection authenticates as. At most one applies, picked in the `nats` CLI's order.
#[derive(Clone, Debug)]
pub enum Identity {
    UserPassword { user: String, password: Secret },
    Credentials(PathBuf),
    NKeySeedFile(PathBuf),
    JwtAndSeed { jwt: Secret, seed: Secret },
    BearerJwt(Secret),
}

impl Identity {
    fn description(&self) -> &'static str {
        match self {
            Self::UserPassword { .. } => "user/password",
            Self::Credentials(_) => "credentials file",
            Self::NKeySeedFile(_) => "nkey seed file",
            Self::JwtAndSeed { .. } => "user JWT and seed",
            Self::BearerJwt(_) => "bearer JWT",
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ClientTls {
    pub certificate: Option<(PathBuf, PathBuf)>,
    pub ca: Option<PathBuf>,
}

/// Everything the `NATS_*` variables say about the connection.
#[derive(Clone, Debug, Default)]
pub struct NatsEnv {
    pub servers: Option<Vec<String>>,
    pub identity: Option<Identity>,
    pub token: Option<Secret>,
    pub tls: ClientTls,
}

impl NatsEnv {
    pub fn from_process_env() -> anyhow::Result<Self> {
        Self::from_env(|name| std::env::var(name).ok())
    }

    /// Resolve through `var` as `nats` does: an empty variable is unset, `NATS_USER` without
    /// `NATS_PASSWORD` is a token, and the identity is the first of user/password, `NATS_CREDS`,
    /// `NATS_NKEY`, `NATS_JWT` with `NATS_SEED`, `NATS_JWT` alone. A token travels with any of them.
    /// `NATS_CONTEXT` and `NATS_SOCKS_PROXY` are refused: ignoring them would connect somewhere or
    /// somehow other than `nats` does with the same environment.
    pub fn from_env(var: impl Fn(&str) -> Option<String>) -> anyhow::Result<Self> {
        let var = |name: &str| var(name).filter(|v| !v.is_empty());
        for unsupported in [ENV_NATS_CONTEXT, ENV_NATS_SOCKS_PROXY] {
            anyhow::ensure!(
                var(unsupported).is_none(),
                "{unsupported} is set but walgit nats does not support it; unset it and use the other NATS_* variables"
            );
        }
        let home = var("HOME");
        let location = |name: &str| -> anyhow::Result<Option<PathBuf>> {
            var(name)
                .map(|v| file_location(name, &v, home.as_deref()))
                .transpose()
        };

        let servers = var(ENV_NATS_URL).map(|urls| {
            urls.split(',')
                .map(str::trim)
                .filter(|u| !u.is_empty())
                .map(str::to_owned)
                .collect::<Vec<_>>()
        });
        let servers = servers.filter(|s| !s.is_empty());

        let (user, password) = (var(ENV_NATS_USER), var(ENV_NATS_PASSWORD));
        let (token, user) = match (user, &password) {
            (Some(user), None) => (Some(user), None),
            (user, _) => (var(ENV_NATS_TOKEN), user),
        };

        let (jwt, seed) = (var(ENV_NATS_JWT), var(ENV_NATS_SEED));
        let identity = if let (Some(user), Some(password)) = (user, password) {
            Some(Identity::UserPassword {
                user,
                password: Secret(password),
            })
        } else if let Some(path) = location(ENV_NATS_CREDS)? {
            Some(Identity::Credentials(path))
        } else if let Some(path) = location(ENV_NATS_NKEY)? {
            Some(Identity::NKeySeedFile(path))
        } else if let Some(jwt) = jwt {
            let jwt = Secret(literal(ENV_NATS_JWT, &jwt, home.as_deref())?);
            match seed {
                Some(seed) => Some(Identity::JwtAndSeed {
                    jwt,
                    seed: Secret(literal(ENV_NATS_SEED, &seed, home.as_deref())?),
                }),
                None => Some(Identity::BearerJwt(jwt)),
            }
        } else {
            None
        };

        let certificate = match (location(ENV_NATS_CERT)?, location(ENV_NATS_KEY)?) {
            (Some(cert), Some(key)) => Some((cert, key)),
            _ => None,
        };
        Ok(Self {
            servers,
            identity,
            token: token.map(Secret),
            tls: ClientTls {
                certificate,
                ca: location(ENV_NATS_CA)?,
            },
        })
    }

    /// The methods in use, for logs and errors. Never contains a secret.
    pub fn description(&self) -> String {
        let mut parts: Vec<&str> = self.identity.iter().map(Identity::description).collect();
        if self.token.is_some() {
            parts.push("token");
        }
        if self.tls.certificate.is_some() {
            parts.push("client certificate");
        }
        if parts.is_empty() {
            "none".to_owned()
        } else {
            parts.join(" + ")
        }
    }

    pub(crate) async fn connect_options(&self) -> anyhow::Result<async_nats::ConnectOptions> {
        let mut opts = async_nats::ConnectOptions::new();
        opts = match &self.identity {
            None => opts,
            Some(Identity::UserPassword { user, password }) => {
                opts.user_and_password(user.clone(), password.expose().to_owned())
            }
            Some(Identity::Credentials(path)) => opts
                .credentials_file(path)
                .await
                .with_context(|| format!("{ENV_NATS_CREDS}: {}", path.display()))?,
            Some(Identity::NKeySeedFile(path)) => {
                let contents = tokio::fs::read_to_string(path)
                    .await
                    .with_context(|| format!("{ENV_NATS_NKEY}: {}", path.display()))?;
                let seed = seed_in(&contents).with_context(|| {
                    format!("{ENV_NATS_NKEY}: {} holds no nkey seed", path.display())
                })?;
                opts.nkey(seed)
            }
            Some(Identity::JwtAndSeed { jwt, seed }) => {
                let key = Arc::new(
                    nkeys::KeyPair::from_seed(seed.expose())
                        .map_err(|e| anyhow::anyhow!("{ENV_NATS_SEED}: {e}"))?,
                );
                opts.jwt(jwt.expose().to_owned(), move |nonce| {
                    let key = key.clone();
                    async move { key.sign(&nonce).map_err(async_nats::AuthError::new) }
                })
            }
            Some(Identity::BearerJwt(jwt)) => {
                opts.jwt(jwt.expose().to_owned(), |_| async { Ok(Vec::new()) })
            }
        };
        if let Some(token) = &self.token {
            opts = opts.token(token.expose().to_owned());
        }
        if let Some((cert, key)) = &self.tls.certificate {
            opts = opts.add_client_certificate(cert.clone(), key.clone());
        }
        if let Some(ca) = &self.tls.ca {
            opts = opts.add_root_certificates(ca.clone());
        }
        Ok(opts)
    }
}

/// A URI scheme as Go's `url.Parse` sees one: at least two characters before the first `:`.
fn scheme(value: &str) -> Option<&str> {
    let (scheme, _) = value.split_once(':')?;
    let mut chars = scheme.chars();
    let valid = chars.next().is_some_and(|c| c.is_ascii_alphabetic())
        && chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'));
    (valid && scheme.len() >= 2).then_some(scheme)
}

/// A `nats` LOCATION that names a file: a bare path or `file://`, with `~/` expanded.
fn file_location(name: &str, value: &str, home: Option<&str>) -> anyhow::Result<PathBuf> {
    let path = match scheme(value) {
        None => value,
        Some(s) if s.eq_ignore_ascii_case("file") => value.get(s.len() + 3..).unwrap_or_default(),
        Some(s) => anyhow::bail!("{name}: the {s}:// scheme is not supported; give a file path"),
    };
    Ok(match (path.strip_prefix("~/"), home) {
        (Some(rest), Some(home)) => PathBuf::from(home).join(rest),
        _ => PathBuf::from(path),
    })
}

/// A value that is itself the secret, unless it names a file with `file://`.
fn literal(name: &str, value: &str, home: Option<&str>) -> anyhow::Result<String> {
    if scheme(value).is_none() {
        return Ok(value.to_owned());
    }
    let path = file_location(name, value, home)?;
    let contents =
        std::fs::read_to_string(&path).with_context(|| format!("{name}: {}", path.display()))?;
    Ok(contents.trim_end_matches(['\r', '\n']).to_owned())
}

/// The seed in an nkey file, bare or inside the decorated block `nsc` writes.
fn seed_in(contents: &str) -> Option<String> {
    contents
        .lines()
        .map(str::trim)
        .find(|line| nkeys::KeyPair::from_seed(line).is_ok())
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::Path;

    use super::*;

    fn resolve(vars: &[(&str, &str)]) -> anyhow::Result<NatsEnv> {
        let vars: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        NatsEnv::from_env(|name| vars.get(name).cloned())
    }

    fn identity(vars: &[(&str, &str)]) -> Option<Identity> {
        resolve(vars).unwrap().identity
    }

    #[test]
    fn no_variables_mean_no_auth_and_the_configured_servers() {
        let env = resolve(&[]).unwrap();
        assert!(env.identity.is_none() && env.token.is_none() && env.servers.is_none());
        assert_eq!(env.description(), "none");
    }

    #[test]
    fn nats_url_is_a_comma_separated_server_list() {
        let env = resolve(&[("NATS_URL", "nats://a:4222, nats://b:4222,")]).unwrap();
        assert_eq!(env.servers.unwrap(), ["nats://a:4222", "nats://b:4222"]);
        assert!(resolve(&[("NATS_URL", " , ")]).unwrap().servers.is_none());
    }

    #[test]
    fn user_and_password_come_first() {
        let id = identity(&[
            ("NATS_USER", "u"),
            ("NATS_PASSWORD", "p"),
            ("NATS_CREDS", "/c"),
            ("NATS_NKEY", "/n"),
            ("NATS_JWT", "j"),
        ]);
        assert!(matches!(id, Some(Identity::UserPassword { user, password })
            if user == "u" && password.expose() == "p"));
    }

    #[test]
    fn a_user_without_a_password_is_a_token_and_beats_nats_token() {
        let env = resolve(&[("NATS_USER", "tok-as-user"), ("NATS_TOKEN", "tok")]).unwrap();
        assert!(env.identity.is_none());
        assert_eq!(env.token.unwrap().expose(), "tok-as-user");
    }

    #[test]
    fn a_password_without_a_user_is_ignored() {
        let env = resolve(&[("NATS_PASSWORD", "p"), ("NATS_TOKEN", "tok")]).unwrap();
        assert!(env.identity.is_none());
        assert_eq!(env.token.unwrap().expose(), "tok");
    }

    #[test]
    fn creds_then_nkey_then_jwt() {
        let all = [
            ("NATS_CREDS", "/c"),
            ("NATS_NKEY", "/n"),
            ("NATS_JWT", "j"),
            ("NATS_SEED", "s"),
        ];
        assert!(matches!(identity(&all), Some(Identity::Credentials(p)) if p == Path::new("/c")));
        assert!(
            matches!(identity(&all[1..]), Some(Identity::NKeySeedFile(p)) if p == Path::new("/n"))
        );
        assert!(
            matches!(identity(&all[2..]), Some(Identity::JwtAndSeed { jwt, seed })
            if jwt.expose() == "j" && seed.expose() == "s")
        );
        assert!(matches!(identity(&all[2..3]), Some(Identity::BearerJwt(j)) if j.expose() == "j"));
        assert!(identity(&all[3..]).is_none(), "a seed alone is no identity");
    }

    #[test]
    fn a_token_travels_with_the_identity() {
        let env = resolve(&[("NATS_CREDS", "/c"), ("NATS_TOKEN", "tok")]).unwrap();
        assert!(matches!(env.identity, Some(Identity::Credentials(_))));
        assert_eq!(env.token.as_ref().unwrap().expose(), "tok");
        assert_eq!(env.description(), "credentials file + token");
    }

    #[test]
    fn locations_are_paths_or_file_uris_with_home_expanded() {
        let id = identity(&[("NATS_CREDS", "file:///etc/nats/a.creds")]);
        assert!(
            matches!(id, Some(Identity::Credentials(p)) if p == Path::new("/etc/nats/a.creds"))
        );
        let id = identity(&[("NATS_NKEY", "~/keys/u.nk"), ("HOME", "/home/w")]);
        assert!(
            matches!(id, Some(Identity::NKeySeedFile(p)) if p == Path::new("/home/w/keys/u.nk"))
        );
        let err = resolve(&[("NATS_CREDS", "op://vault/item")]).unwrap_err();
        assert!(format!("{err}").contains("NATS_CREDS"), "{err}");
    }

    #[test]
    fn jwt_and_seed_may_name_a_file() {
        let dir = tempfile::tempdir().unwrap();
        let seed = dir.path().join("seed");
        std::fs::write(&seed, "SUSEED\n").unwrap();
        let uri = format!("file://{}", seed.display());
        let id = identity(&[("NATS_JWT", "j"), ("NATS_SEED", &uri)]);
        assert!(matches!(id, Some(Identity::JwtAndSeed { seed, .. }) if seed.expose() == "SUSEED"));
    }

    #[test]
    fn tls_needs_both_certificate_and_key_and_the_ca_stands_alone() {
        let env = resolve(&[("NATS_CERT", "/c.pem"), ("NATS_CA", "/ca.pem")]).unwrap();
        assert!(env.tls.certificate.is_none());
        assert_eq!(env.tls.ca, Some(Path::new("/ca.pem").to_path_buf()));
        let env = resolve(&[("NATS_CERT", "/c.pem"), ("NATS_KEY", "/k.pem")]).unwrap();
        assert_eq!(
            env.tls.certificate,
            Some(("/c.pem".into(), "/k.pem".into()))
        );
    }

    #[test]
    fn empty_variables_are_unset() {
        let env = resolve(&[
            ("NATS_URL", ""),
            ("NATS_CREDS", ""),
            ("NATS_NKEY", ""),
            ("NATS_USER", "u"),
            ("NATS_PASSWORD", "p"),
            ("NATS_CONTEXT", ""),
        ])
        .unwrap();
        assert!(env.servers.is_none());
        assert!(matches!(env.identity, Some(Identity::UserPassword { .. })));
        let env = resolve(&[("NATS_USER", "u"), ("NATS_PASSWORD", "")]).unwrap();
        assert_eq!(env.token.unwrap().expose(), "u");
    }

    #[test]
    fn variables_that_would_change_the_route_are_refused() {
        for name in ["NATS_CONTEXT", "NATS_SOCKS_PROXY"] {
            let err = resolve(&[(name, "x")]).unwrap_err();
            assert!(format!("{err}").contains(name), "{err}");
        }
    }

    #[test]
    fn the_seed_is_found_bare_or_decorated() {
        let seed = nkeys::KeyPair::new_user().seed().unwrap();
        assert_eq!(
            seed_in(&format!("{seed}\n")).as_deref(),
            Some(seed.as_str())
        );
        let decorated =
            format!("-----BEGIN USER NKEY SEED-----\n{seed}\n------END USER NKEY SEED------\n");
        assert_eq!(seed_in(&decorated).as_deref(), Some(seed.as_str()));
        assert!(seed_in("not a seed").is_none());
    }

    #[test]
    fn debug_never_shows_a_secret() {
        let env = resolve(&[
            ("NATS_USER", "alice"),
            ("NATS_PASSWORD", "super-secret-password"),
            ("NATS_TOKEN", "super-secret-token"),
        ])
        .unwrap();
        let shown = format!("{env:?}");
        assert!(shown.contains("alice"));
        assert!(!shown.contains("super-secret"), "{shown}");
        let env = resolve(&[("NATS_JWT", "secret-jwt"), ("NATS_SEED", "secret-seed")]).unwrap();
        assert!(!format!("{env:?}").contains("secret-"));
    }
}
