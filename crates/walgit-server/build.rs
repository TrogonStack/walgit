//! Make `cargo build`/`cargo test` work in a fresh checkout without running the
//! web build. `rust-embed` requires `../../web/dist` to exist at compile time;
//! when the SPA has not been built (`just web-build`) we drop in a placeholder
//! `index.html` so the server compiles and the HTML routes still answer 200.
//! A real `pnpm run build` overwrites the placeholder.

use std::fs;
use std::path::Path;

const PLACEHOLDER: &str = "<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\"><title>walgit</title></head>\n\
<body><p>walgit web UI is not built in this binary. Run <code>just web-build</code> (vite via pnpm) and rebuild.</p></body></html>\n";

fn main() -> std::io::Result<()> {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let dist = manifest.join("../../web/dist");
    println!("cargo:rerun-if-changed={}", dist.display());
    let index = dist.join("index.html");
    if !index.exists() {
        fs::create_dir_all(&dist)?;
        fs::write(&index, PLACEHOLDER)?;
        println!(
            "cargo:warning=web/dist was missing; wrote a placeholder index.html (run `just web-build` for the real UI)"
        );
    }
    Ok(())
}
