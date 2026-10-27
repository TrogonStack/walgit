fn main() {
    println!("cargo:rustc-env=WALGIT_BUILD_SHA={}", build_sha());
}

/// Build identity for `/healthz` (`version`) and `walgit --version`: the commit
/// the binary was built from. A container or package build may pass it as
/// `WALGIT_BUILD_SHA` (an archived source tree has no `.git`); a checkout
/// falls back to `git rev-parse --short=12 HEAD`; otherwise "dev".
fn build_sha() -> String {
    println!("cargo:rerun-if-env-changed=WALGIT_BUILD_SHA");
    if let Ok(s) = std::env::var("WALGIT_BUILD_SHA") {
        let s = s.trim().to_string();
        if !s.is_empty() {
            return s;
        }
    }
    std::process::Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "dev".to_string())
}
