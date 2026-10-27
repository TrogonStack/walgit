//! `walgit-nats --config walgit.toml`: the WAL's `ref` events published into NATS `JetStream`
//! (D50, D51), without the server or the web UI. `--config` may name the file `walgit serve`
//! reads or a smaller one holding only `[store]`, `[cache]`, `[wal]`, `[events]` and `[telemetry]`.

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Parser)]
#[command(
    name = "walgit-nats",
    version = walgit_telemetry::BUILD_SHA,
    about = "Publish walgit's ref events into NATS JetStream"
)]
struct Cli {
    /// Path to the configuration file.
    #[arg(long, env = "WALGIT_CONFIG", default_value = "walgit.toml")]
    config: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let cfg = walgit_telemetry::startup(&cli.config, "walgit-nats");
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(walgit_nats::run(Arc::new(cfg)))
}
