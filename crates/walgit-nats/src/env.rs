//! How `walgit nats` reaches NATS, read from the same `NATS_*` environment variables the `nats` CLI
//! reads and combined the way it combines them, so one shell environment drives both and secrets
//! never sit in `walgit.toml`.

use std::borrow::Cow;
use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
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
#[derive(Clone, Default)]
pub struct NatsEnv {
    pub servers: Option<Vec<String>>,
    pub identity: Option<Identity>,
    pub token: Option<Secret>,
    pub tls: ClientTls,
}

impl fmt::Debug for NatsEnv {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let servers = self
            .servers
            .as_ref()
            .map(|urls| urls.iter().map(|url| redacted(url)).collect::<Vec<_>>());
        f.debug_struct("NatsEnv")
            .field("servers", &servers)
            .field("identity", &self.identity)
            .field("token", &self.token)
            .field("tls", &self.tls)
            .finish()
    }
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
                    seed: Secret(seed_value(literal(ENV_NATS_SEED, &seed, home.as_deref())?)),
                }),
                None => Some(Identity::BearerJwt(jwt)),
            }
        } else {
            None
        };

        let certificate = match (location(ENV_NATS_CERT)?, location(ENV_NATS_KEY)?) {
            (Some(cert), Some(key)) => Some((cert, key)),
            (None, None) => None,
            _ => anyhow::bail!("{ENV_NATS_CERT} and {ENV_NATS_KEY} must be set together"),
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

    /// The identity that still applies once a server URL's credentials took the user and password.
    fn identity_beside(&self, url: Option<&UrlCredentials>) -> Option<&Identity> {
        match (&self.identity, url) {
            (Some(Identity::UserPassword { .. }), Some(_)) => None,
            (identity, _) => identity.as_ref(),
        }
    }

    /// The methods in use, for logs and errors. Never contains a secret.
    pub fn description(&self, url: Option<&UrlCredentials>) -> String {
        let mut parts: Vec<&str> = url.iter().map(|c| c.description()).collect();
        parts.extend(self.identity_beside(url).map(Identity::description));
        if url.is_none() && self.token.is_some() {
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

    /// As `nats` does, credentials in a server URL replace the user, password and token.
    pub(crate) async fn connect_options(
        &self,
        url: Option<&UrlCredentials>,
    ) -> anyhow::Result<async_nats::ConnectOptions> {
        let mut opts = async_nats::ConnectOptions::new();
        opts = match self.identity_beside(url) {
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
        opts = match (url, &self.token) {
            (Some(UrlCredentials::UserPassword { user, password }), _) => {
                opts.user_and_password(user.clone(), password.expose().to_owned())
            }
            (Some(UrlCredentials::Token(token)), _) | (None, Some(token)) => {
                opts.token(token.expose().to_owned())
            }
            (None, None) => opts,
        };
        if let Some((cert, key)) = &self.tls.certificate {
            opts = opts.add_client_certificate(cert.clone(), key.clone());
        }
        if let Some(ca) = &self.tls.ca {
            opts = opts.add_root_certificates(ca.clone());
        }
        if self.tls.certificate.is_some() || self.tls.ca.is_some() {
            opts = opts.require_tls(true);
        }
        Ok(opts)
    }
}

/// Credentials written into a server URL as `user:password@` or `token@`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UrlCredentials {
    UserPassword { user: String, password: Secret },
    Token(Secret),
}

impl UrlCredentials {
    fn description(&self) -> &'static str {
        match self {
            Self::UserPassword { .. } => "URL user/password",
            Self::Token(_) => "URL token",
        }
    }
}

/// The servers to dial, with the credentials taken out of their URLs.
#[derive(Debug)]
pub struct Servers {
    pub addrs: Vec<async_nats::ServerAddr>,
    pub credentials: Option<UrlCredentials>,
}

impl Servers {
    /// Parse `urls` from `source`. Servers that carry credentials must carry the same ones, and
    /// an error never repeats them.
    pub fn parse(source: &str, urls: &[String]) -> anyhow::Result<Self> {
        let mut credentials: Option<UrlCredentials> = None;
        let mut addrs = Vec::with_capacity(urls.len());
        for u in urls {
            let (addr, found) = async_nats::ServerAddr::from_str(u)
                .map_err(anyhow::Error::from)
                .and_then(take_credentials)
                .with_context(|| format!("{source}: {}", redacted(u)))?;
            if let Some(found) = found {
                match &credentials {
                    Some(seen) => anyhow::ensure!(
                        *seen == found,
                        "{source}: the servers carry different credentials"
                    ),
                    None => credentials = Some(found),
                }
            }
            addrs.push(addr);
        }
        Ok(Self { addrs, credentials })
    }

    /// The URLs as dialled, which hold no credentials.
    pub fn shown(&self) -> Vec<&str> {
        self.addrs
            .iter()
            .map(async_nats::ServerAddr::as_url_str)
            .collect()
    }
}

fn take_credentials(
    addr: async_nats::ServerAddr,
) -> anyhow::Result<(async_nats::ServerAddr, Option<UrlCredentials>)> {
    let decode = |s: &str| {
        percent_encoding::percent_decode_str(s)
            .decode_utf8()
            .map(Cow::into_owned)
    };
    let credentials = match (addr.username(), addr.password()) {
        (None, None) => return Ok((addr, None)),
        (user, Some(password)) => UrlCredentials::UserPassword {
            user: decode(user.unwrap_or_default())?,
            password: Secret(decode(password)?),
        },
        (Some(token), None) => UrlCredentials::Token(Secret(decode(token)?)),
    };
    let mut url = addr.into_inner();
    url.set_password(None)
        .and_then(|()| url.set_username(""))
        .map_err(|()| anyhow::anyhow!("the URL cannot carry credentials"))?;
    Ok((async_nats::ServerAddr::from_url(url)?, Some(credentials)))
}

/// `url` with whatever precedes its last `@` hidden. A password may hold an unencoded `/`, `?`
/// or `#`, so the authority's end cannot be trusted to bound it.
fn redacted(url: &str) -> String {
    let (scheme, rest) = url.split_once("://").map_or(("", url), |(s, r)| (s, r));
    match rest.rsplit_once('@') {
        Some((_, host)) if scheme.is_empty() => format!("<redacted>@{host}"),
        Some((_, host)) => format!("{scheme}://<redacted>@{host}"),
        None => url.to_owned(),
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
        Some(s) if s.eq_ignore_ascii_case("file") => value
            .get(..7)
            .filter(|prefix| prefix.eq_ignore_ascii_case("file://"))
            .and_then(|_| value.get(7..))
            .with_context(|| format!("{name}: write a file URI as file://<path>"))?,
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

/// A seed given bare or in the decorated block `nsc` writes; anything else is left for the key
/// parser to reject.
fn seed_value(value: String) -> String {
    seed_in(&value).unwrap_or(value)
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
        assert_eq!(env.description(None), "none");
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
        assert_eq!(env.description(None), "credentials file + token");
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
        for short in ["file:/etc/a.creds", "file:relative"] {
            let err = resolve(&[("NATS_CREDS", short)]).unwrap_err();
            assert!(format!("{err}").contains("file://"), "{err}");
        }
        let id = identity(&[("NATS_CREDS", "FILE:///etc/a.creds")]);
        assert!(matches!(id, Some(Identity::Credentials(p)) if p == Path::new("/etc/a.creds")));
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
    fn a_seed_file_may_hold_the_decorated_block() {
        let dir = tempfile::tempdir().unwrap();
        let seed = nkeys::KeyPair::new_user().seed().unwrap();
        let path = dir.path().join("user.nk");
        std::fs::write(
            &path,
            format!("-----BEGIN USER NKEY SEED-----\n{seed}\n------END USER NKEY SEED------\n"),
        )
        .unwrap();
        let uri = format!("file://{}", path.display());
        let id = identity(&[("NATS_JWT", "j"), ("NATS_SEED", &uri)]);
        assert!(matches!(id, Some(Identity::JwtAndSeed { seed: s, .. }) if s.expose() == seed));
    }

    fn servers(urls: &[&str]) -> anyhow::Result<Servers> {
        let urls: Vec<String> = urls.iter().map(|u| (*u).to_owned()).collect();
        Servers::parse("NATS_URL", &urls)
    }

    #[test]
    fn credentials_leave_the_url_and_are_decoded() {
        let s = servers(&["nats://al%40ce:p%3Ass@a:4222", "nats://b:4222"]).unwrap();
        assert_eq!(s.shown(), ["nats://a:4222", "nats://b:4222"]);
        assert!(
            matches!(&s.credentials, Some(UrlCredentials::UserPassword { user, password })
            if user == "al@ce" && password.expose() == "p:ss")
        );
        let s = servers(&["nats://tok@a:4222"]).unwrap();
        assert_eq!(s.shown(), ["nats://a:4222"]);
        assert!(matches!(&s.credentials, Some(UrlCredentials::Token(t)) if t.expose() == "tok"));
        assert!(servers(&["nats://a:4222"]).unwrap().credentials.is_none());
    }

    #[test]
    fn servers_must_agree_on_their_credentials() {
        let err = servers(&["nats://u:p@a:4222", "nats://u:q@b:4222"]).unwrap_err();
        assert!(!format!("{err:#}").contains(":p@"), "{err:#}");
        assert!(servers(&["nats://u:p@a:4222", "nats://b:4222", "nats://u:p@c:4222"]).is_ok());
    }

    #[test]
    fn a_url_that_fails_to_parse_is_shown_without_its_credentials() {
        let err = servers(&["nats://u:hidden@[bad"]).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("NATS_URL: nats://<redacted>@[bad"), "{msg}");
        assert!(!msg.contains("hidden"), "{msg}");
        for url in ["nats://u:hid#den@h:4222", "nats://u:hid/den@h:4222", "nats://u:hid?den@h:4222"] {
            let err = servers(&[url]).unwrap_err();
            let msg = format!("{err:#}");
            assert!(msg.contains("nats://<redacted>@h:4222"), "{msg}");
            assert!(!msg.contains("hid"), "{msg}");
            let env = resolve(&[("NATS_URL", url)]).unwrap();
            assert!(!format!("{env:?}").contains("hid"), "{env:?}");
        }
    }

    #[test]
    fn url_credentials_replace_the_user_password_and_token() {
        let url = UrlCredentials::Token(Secret("t".into()));
        let env = resolve(&[
            ("NATS_USER", "u"),
            ("NATS_PASSWORD", "p"),
            ("NATS_TOKEN", "x"),
        ])
        .unwrap();
        assert_eq!(env.description(Some(&url)), "URL token");
        let env = resolve(&[("NATS_CREDS", "/c"), ("NATS_TOKEN", "x")]).unwrap();
        assert_eq!(env.description(Some(&url)), "URL token + credentials file");
    }

    #[test]
    fn tls_needs_both_certificate_and_key_and_the_ca_stands_alone() {
        for half in [("NATS_CERT", "/c.pem"), ("NATS_KEY", "/k.pem")] {
            let err = resolve(&[half, ("NATS_CA", "/ca.pem")]).unwrap_err();
            assert!(format!("{err:#}").contains("must be set together"), "{err:#}");
        }
        let env = resolve(&[("NATS_CA", "/ca.pem")]).unwrap();
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
        let env = resolve(&[("NATS_URL", "nats://bob:secret-pass@a:4222,nats://secret-token@b:4222")]).unwrap();
        let shown = format!("{env:?}");
        assert!(shown.contains("a:4222") && shown.contains("b:4222"), "{shown}");
        assert!(!shown.contains("secret-") && !shown.contains("bob"), "{shown}");
    }
}
