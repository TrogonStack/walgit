#!/usr/bin/env bash
# Fetch the pinned TLA+ tools jar atomically and verify its release checksum.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
JAR="$ROOT/target/tla2tools.jar"
VERSION="v1.8.0"
# v1.8.0 is a rolling upstream asset, not an immutable release. This pin
# identifies 2ef2af8c767b76e5db5cba2878f027ac07aad36c (2026-10-23). A mismatch fails closed: inspect
# MANIFEST.MF, upstream commit history and the release asset digest before
# changing it. Never accept an unverified replacement automatically.
SHA256="d4a12081bb7acd49d6d6158e818d8d69f8eb3f9a1f140cb39d20d50c4ce77f44"
URL="https://github.com/tlaplus/tlaplus/releases/download/$VERSION/tla2tools.jar"

command -v java >/dev/null || { echo "TLA+: Java 11+ is required" >&2; exit 2; }
if command -v sha256sum >/dev/null; then
  SHA_CHECK=(sha256sum -c -)
elif command -v shasum >/dev/null; then
  SHA_CHECK=(shasum -a 256 -c -)
else
  echo "TLA+: sha256sum or shasum is required" >&2
  exit 2
fi

check_sha256() {
  printf '%s  %s\n' "$SHA256" "$1" | "${SHA_CHECK[@]}" >/dev/null 2>&1
}

valid_jar() {
  [[ -f "$JAR" ]] && check_sha256 "$JAR"
}

valid_jar && exit 0
mkdir -p "$(dirname "$JAR")"

# Sibling worktrees usually hold a pin-matching jar already; copying one is
# faster than the download and keeps every checkout on this machine working
# when the rolling upstream asset moves ahead of the pin. Still pin-verified:
# a sibling can seed bytes, never trust.
for sibling in "$ROOT"/../*/target/tla2tools.jar; do
  [[ -f "$sibling" && "$sibling" != "$JAR" ]] || continue
  if check_sha256 "$sibling"; then
    echo "TLA+: seeding $VERSION from $(cd "$(dirname "$sibling")/.." && pwd)"
    tmp="$(mktemp "${JAR}.tmp.XXXXXX")"
    cp "$sibling" "$tmp" && mv "$tmp" "$JAR"
    valid_jar && exit 0
  fi
done

command -v curl >/dev/null || { echo "TLA+: curl is required to fetch $VERSION" >&2; exit 2; }
tmp="$(mktemp "${JAR}.tmp.XXXXXX")"
trap 'rm -f "$tmp"' EXIT

echo "TLA+: downloading $VERSION to $JAR"
curl -fsSL -o "$tmp" "$URL"
if ! check_sha256 "$tmp"; then
  echo "TLA+: checksum mismatch for $VERSION — the rolling upstream asset has" >&2
  echo "TLA+: likely moved past the pin; see the provenance recipe in $0" >&2
  exit 1
fi
mv "$tmp" "$JAR"
trap - EXIT
