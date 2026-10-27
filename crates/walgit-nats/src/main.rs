//! `walgit-nats --config walgit.toml`: publish the WAL's ref events into `JetStream` (`docs/EVENTS.md`).

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use tracing_subscriber::EnvFilter;
use walgit_config::{Config, LogFormat};
use walgit_nats::publisher::Publisher;
use walgit_nats::subject::SubjectPrefix;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser)]
#[command(name = "walgit-nats", version, about = "walgit ref events into NATS JetStream")]
struct Cli {
    /// Path to the configuration file.
    #[arg(long, env = "WALGIT_CONFIG", default_value = "walgit.toml")]
    config: PathBuf,
}

fn init_tracing(cfg: &Config) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(&cfg.telemetry.log_filter));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    match cfg.telemetry.log_format {
        LogFormat::Json => builder.json().init(),
        LogFormat::Pretty => builder.pretty().init(),
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
                return;
            }
            Err(e) => tracing::warn!(error = %e, "no SIGTERM handler"),
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

async fn run(cfg: Arc<Config>) -> anyhow::Result<()> {
    let nats = &cfg.events.nats;
    let prefix = SubjectPrefix::new(&nats.subject_prefix)?;
    let store = walgit_store::open_store(&cfg).await?;
    let registry = walgit_wal::Registry::new(store, cfg.clone());

    let client = walgit_nats::connect(nats).await?;
    let js = async_nats::jetstream::new(client.clone());
    let stream = walgit_nats::open_stream(&js, nats, &prefix).await?;
    let publisher = Publisher::new(registry, js, stream, prefix.clone(), nats.start);
    tracing::info!(stream = %nats.stream, prefix = %prefix, start = ?nats.start, "walgit-nats ready");

    let mut tasks = tokio::task::JoinSet::new();
    tasks.spawn(walgit_nats::wake::notify_loop(
        client.clone(),
        nats.notify_subject.clone(),
        nats.queue_group.clone(),
        cfg.store_prefix(),
        publisher.clone(),
    ));
    tasks.spawn({
        let (client, queue_group, publisher) =
            (client.clone(), nats.queue_group.clone(), publisher.clone());
        async move { walgit_nats::service::serve(client, &prefix, &queue_group, publisher).await }
    });
    tasks.spawn({
        let publisher = publisher.clone();
        let every = cfg.events.sweep_interval;
        async move {
            walgit_nats::wake::sweep_loop(publisher, every).await;
            std::future::pending::<anyhow::Result<()>>().await
        }
    });

    tokio::select! {
        () = shutdown_signal() => tracing::info!("shutting down"),
        Some(done) = tasks.join_next() => {
            // A loop ending is a failure: the subscription or the service closed under us.
            done.context("worker panicked")?.context("worker stopped")?;
            anyhow::bail!("a worker stopped unexpectedly");
        }
    }
    tasks.shutdown().await;
    client.flush().await.ok();
    Ok(())
}

fn main() -> anyhow::Result<()> {
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("a rustls crypto provider is already installed"))?;
    let cli = Cli::parse();
    let cfg = if cli.config.exists() {
        Config::load(&cli.config)?
    } else {
        eprintln!(
            "walgit-nats: config file {} not found (pass --config PATH or WALGIT_CONFIG; `--config /dev/null` for defaults + WALGIT__ env)",
            cli.config.display()
        );
        std::process::exit(2);
    };
    init_tracing(&cfg);
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run(Arc::new(cfg)))
}
