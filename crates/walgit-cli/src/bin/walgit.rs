//! `walgit`: the full CLI (serve | compact | repo | wal | nats | synth | import | mirror | config).
fn main() -> anyhow::Result<()> {
    walgit_cli::main()
}
