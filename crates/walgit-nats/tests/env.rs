//! `walgit-nats` connects with whatever the `NATS_*` variables resolve to, against a real
//! `nats-server` that requires it.

use std::time::Duration;

use walgit_config::NatsEventsConfig;
use walgit_nats::env::NatsEnv;
use walgit_nats::testing::{NatsServer, start_nats_with};

fn cfg(nats: &NatsServer) -> NatsEventsConfig {
    NatsEventsConfig {
        urls: vec![nats.url.clone()],
        ..NatsEventsConfig::default()
    }
}

#[allow(clippy::expect_used)]
fn env(vars: &[(&str, &str)]) -> NatsEnv {
    let vars: Vec<(String, String)> = vars
        .iter()
        .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
        .collect();
    NatsEnv::from_env(|name| vars.iter().find(|(k, _)| k == name).map(|(_, v)| v.clone()))
        .expect("the variables resolve")
}

async fn connects(nats: &NatsServer, auth: &NatsEnv) -> bool {
    let attempt = tokio::time::timeout(Duration::from_secs(5), async {
        let client = walgit_nats::connect(&cfg(nats), auth).await?;
        client.flush().await?;
        anyhow::Ok(())
    });
    matches!(attempt.await, Ok(Ok(())))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_token_from_the_environment_authenticates() {
    let Some(nats) = start_nats_with(&["--auth", "s3cret-token"]).await else {
        return;
    };
    assert!(connects(&nats, &env(&[("NATS_TOKEN", "s3cret-token")])).await);
    assert!(!connects(&nats, &env(&[("NATS_TOKEN", "wrong")])).await);
    assert!(!connects(&nats, &env(&[])).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_user_and_password_from_the_environment_authenticate() {
    let Some(nats) = start_nats_with(&["--user", "walgit", "--pass", "s3cret-pass"]).await else {
        return;
    };
    assert!(
        connects(
            &nats,
            &env(&[("NATS_USER", "walgit"), ("NATS_PASSWORD", "s3cret-pass")])
        )
        .await
    );
    assert!(
        !connects(
            &nats,
            &env(&[("NATS_USER", "walgit"), ("NATS_PASSWORD", "wrong")])
        )
        .await
    );
    assert!(!connects(&nats, &env(&[("NATS_USER", "walgit")])).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_user_without_a_password_is_a_token_as_in_the_nats_cli() {
    let Some(nats) = start_nats_with(&["--auth", "s3cret-token"]).await else {
        return;
    };
    assert!(connects(&nats, &env(&[("NATS_USER", "s3cret-token")])).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nats_url_wins_over_the_configured_servers() {
    let Some(nats) = start_nats_with(&[]).await else {
        return;
    };
    let unreachable = NatsEventsConfig {
        urls: vec!["nats://127.0.0.1:1".into()],
        ..NatsEventsConfig::default()
    };
    let client = tokio::time::timeout(
        Duration::from_secs(5),
        walgit_nats::connect(&unreachable, &env(&[("NATS_URL", &nats.url)])),
    )
    .await
    .expect("connects without trying the configured server")
    .unwrap();
    client.flush().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_nkey_seed_file_from_the_environment_authenticates() {
    let user = nkeys::KeyPair::new_user();
    let other = nkeys::KeyPair::new_user();
    let dir = tempfile::tempdir().unwrap();
    let conf = dir.path().join("nats.conf");
    std::fs::write(
        &conf,
        format!(
            "authorization {{ users = [ {{ nkey: {} }} ] }}\n",
            user.public_key()
        ),
    )
    .unwrap();
    let Some(nats) = start_nats_with(&["-c", conf.to_str().unwrap()]).await else {
        return;
    };
    let seed_file = |name: &str, key: &nkeys::KeyPair| {
        let path = dir.path().join(name);
        std::fs::write(&path, format!("{}\n", key.seed().unwrap())).unwrap();
        path.to_str().unwrap().to_owned()
    };
    let (user_file, other_file) = (seed_file("user.nk", &user), seed_file("other.nk", &other));
    assert!(connects(&nats, &env(&[("NATS_NKEY", &user_file)])).await);
    assert!(!connects(&nats, &env(&[("NATS_NKEY", &other_file)])).await);
    assert!(
        !connects(&nats, &env(&[("NATS_NKEY", &user.seed().unwrap())])).await,
        "NATS_NKEY is a seed file, never the seed itself"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_credentials_file_wins_and_a_missing_one_fails_naming_the_variable() {
    let Some(nats) = start_nats_with(&["--auth", "s3cret-token"]).await else {
        return;
    };
    let auth = env(&[
        ("NATS_CREDS", "/nonexistent/walgit.creds"),
        ("NATS_TOKEN", "s3cret-token"),
    ]);
    assert!(matches!(
        auth.identity,
        Some(walgit_nats::env::Identity::Credentials(_))
    ));
    let err = walgit_nats::connect(&cfg(&nats), &auth).await.unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("NATS_CREDS") && msg.contains("/nonexistent/walgit.creds"),
        "{msg}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_connection_names_the_method_but_never_the_secret() {
    let Some(nats) = start_nats_with(&["--auth", "s3cret-token"]).await else {
        return;
    };
    let auth = env(&[("NATS_TOKEN", "leaked-if-printed")]);
    let err = tokio::time::timeout(
        Duration::from_secs(5),
        walgit_nats::connect(&cfg(&nats), &auth),
    )
    .await
    .expect("a rejected credential fails fast")
    .unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("auth: token"), "{msg}");
    assert!(!msg.contains("leaked-if-printed"), "{msg}");
    assert!(!format!("{auth:?}").contains("leaked-if-printed"));
}

fn with_userinfo(nats: &NatsServer, userinfo: &str) -> String {
    nats.url.replacen("://", &format!("://{userinfo}@"), 1)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn credentials_in_nats_url_replace_the_user_password_and_token() {
    let Some(nats) = start_nats_with(&["--user", "walgit", "--pass", "s3cret-pass"]).await else {
        return;
    };
    let url = with_userinfo(&nats, "walgit:s3cret-pass");
    let wrong = [("NATS_USER", "walgit"), ("NATS_PASSWORD", "wrong")];
    assert!(connects(&nats, &env(&[("NATS_URL", &url), wrong[0], wrong[1]])).await);
    assert!(
        !connects(
            &nats,
            &env(&[("NATS_URL", &with_userinfo(&nats, "walgit:wrong"))])
        )
        .await
    );

    let Some(nats) = start_nats_with(&["--auth", "s3cret-token"]).await else {
        return;
    };
    let url = with_userinfo(&nats, "s3cret-token");
    assert!(connects(&nats, &env(&[("NATS_URL", &url), ("NATS_TOKEN", "wrong")])).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_connection_never_shows_the_credentials_in_the_url() {
    let Some(nats) = start_nats_with(&["--auth", "s3cret-token"]).await else {
        return;
    };
    let auth = env(&[("NATS_URL", &with_userinfo(&nats, "leaked-if-printed"))]);
    let err = tokio::time::timeout(
        Duration::from_secs(5),
        walgit_nats::connect(&cfg(&nats), &auth),
    )
    .await
    .expect("a rejected credential fails fast")
    .unwrap_err();
    let msg = format!("{err:#}");
    assert!(msg.contains("auth: URL token"), "{msg}");
    assert!(!msg.contains("leaked-if-printed"), "{msg}");
}

/// A CA and a server certificate for 127.0.0.1 it signed, as PEM files in `dir`.
#[allow(clippy::expect_used)]
fn tls_files(dir: &std::path::Path) -> (String, String, String) {
    let ca_key = rcgen::KeyPair::generate().expect("ca key");
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new()).expect("ca params");
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca = ca_params.self_signed(&ca_key).expect("ca");
    let issuer = rcgen::Issuer::new(ca_params, ca_key);
    let key = rcgen::KeyPair::generate().expect("server key");
    let cert = rcgen::CertificateParams::new(vec!["127.0.0.1".to_owned()])
        .expect("server params")
        .signed_by(&key, &issuer)
        .expect("server certificate");
    let write = |name: &str, pem: String| {
        let path = dir.join(name);
        std::fs::write(&path, pem).expect("write pem");
        path.to_string_lossy().into_owned()
    };
    (
        write("ca.pem", ca.pem()),
        write("server.pem", cert.pem()),
        write("server-key.pem", key.serialize_pem()),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_ca_requires_tls_as_in_the_nats_cli() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let dir = tempfile::tempdir().unwrap();
    let (ca, cert, key) = tls_files(dir.path());
    let Some(plain) = start_nats_with(&[]).await else {
        return;
    };
    assert!(connects(&plain, &env(&[])).await);
    assert!(
        !connects(&plain, &env(&[("NATS_CA", &ca)])).await,
        "a plaintext server is refused, never used without TLS"
    );
    let Some(tls) = start_nats_with(&["--tls", "--tlscert", &cert, "--tlskey", &key]).await else {
        return;
    };
    assert!(connects(&tls, &env(&[("NATS_CA", &ca)])).await, "the CA verifies the server");
}
