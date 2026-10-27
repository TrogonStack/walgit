//! A throwaway `nats-server -js` for tests (feature `testing`). Shared by this crate's tests and
//! `walgit-server`'s, so neither copies the launcher.

use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

pub struct NatsServer {
    child: Child,
    port: u16,
    pub url: String,
    store: tempfile::TempDir,
}

impl NatsServer {
    fn spawn(port: u16, store: &std::path::Path) -> Child {
        #[allow(clippy::expect_used)]
        Command::new("nats-server")
            .args(["-js", "-a", "127.0.0.1", "-p", &port.to_string(), "-sd"])
            .arg(store)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn nats-server")
    }

    /// Kill the server at once (no drain), leaving its store dir for [`NatsServer::restart`].
    pub fn stop(&mut self) {
        self.child.kill().ok();
        self.child.wait().ok();
    }

    /// Start the server again on the same port and store dir: streams survive, clients reconnect.
    ///
    /// # Panics
    /// When the server does not accept connections within 15 s.
    pub async fn restart(&mut self) {
        self.stop();
        self.child = Self::spawn(self.port, self.store.path());
        wait_ready(&self.url).await;
    }
}

async fn wait_ready(url: &str) {
    let t0 = Instant::now();
    while async_nats::connect(url).await.is_err() {
        assert!(
            t0.elapsed() < Duration::from_secs(15),
            "nats-server did not start"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

impl Drop for NatsServer {
    fn drop(&mut self) {
        self.child.kill().ok();
        self.child.wait().ok();
    }
}

/// Start `nats-server -js` on a free loopback port with a temp store dir. `None` (after saying so
/// on stderr) when the binary is not on PATH, so tests skip instead of failing.
///
/// # Panics
/// When the server cannot be spawned or does not accept connections within 15 s.
#[allow(clippy::expect_used)]
pub async fn start_nats() -> Option<NatsServer> {
    if !Command::new("nats-server")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
    {
        eprintln!("skipping: nats-server is not available on PATH");
        return None;
    }
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("free port")
        .local_addr()
        .expect("local addr")
        .port();
    let store = tempfile::tempdir().expect("tempdir");
    let child = NatsServer::spawn(port, store.path());
    let server = NatsServer {
        child,
        port,
        url: format!("nats://127.0.0.1:{port}"),
        store,
    };
    wait_ready(&server.url).await;
    Some(server)
}
