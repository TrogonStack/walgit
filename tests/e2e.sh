#!/usr/bin/env bash
# tests/e2e.sh — end-to-end test: real git vs. walgit server.
#
# Phase 1 (memory backend): build release, start `walgit serve` with a generated
# config on a random port, then exercise the full git surface:
#   synth → PUT repo → push → clone → fetch → ls-remote → partial clone →
#   tag → delete → git fsck.
#
# Phase 2 (S3/rustfs, when WALGIT_TEST_S3_ENDPOINT is set): run two instances
# backed by the same rustfs bucket; push to A, clone from B; assert both see
# the same refs and pass fsck.
#
# Usage:
#   tests/e2e.sh
#   WALGIT_TEST_S3_ENDPOINT=http://localhost:9000 tests/e2e.sh
#   WALGIT=/absolute/path/to/freshly/built/walgit tests/e2e.sh
#   WALGIT_NATS=/absolute/path/to/freshly/built/walgit-nats (phase 3; default target/release/walgit-nats)
#
# Phase 3 (S3/rustfs plus `nats-server` and the `nats` CLI on PATH): `walgit-nats` as a process next to
# `walgit serve`; real pushes become JetStream events; a restart republishes nothing; wake-ups work.
#
# Requires: cargo, git, curl, python3 (for random port), ripgrep (S3 maintenance receipt).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
WALGIT="${WALGIT:-}"
WALGIT_NATS="${WALGIT_NATS:-}"

# --- helpers -----------------------------------------------------------------

red()   { printf '\033[31m%s\033[0m\n' "$*"; }
green() { printf '\033[32m%s\033[0m\n' "$*"; }
bold()  { printf '\033[1m%s\033[0m\n' "$*"; }

step()  { bold ">>> $*"; }
pass()  { green "  ok: $*"; }
fail()  { red "  FAIL: $*"; exit 1; }

rand_port() {
    python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()'
}

# Auth support: when WALGIT_AUTH_TOKEN is set (a serverless host), use it for all
# curl and git operations. a cloud load balancer requires an identity token.
AUTH_CURL_ARGS=()
GIT_AUTH_ARGS=()
if [[ -n "${WALGIT_E2E_BASE_URL:-}" ]]; then
    source "$SCRIPT_DIR/lib-auth.sh"
    walgit_auth_setup "$WALGIT_E2E_BASE_URL" || exit 1
fi

# ${ARR[@]+"${ARR[@]}"}: bash 3.2 (macOS /bin/bash) calls an empty array expansion an unbound
# variable under set -u. "${ARR[@]:-}" would pass a blank argument, which curl and git reject.
# Wrapper for curl that adds auth headers.
curl_auth() { curl ${AUTH_CURL_ARGS[@]+"${AUTH_CURL_ARGS[@]}"} "$@"; }
# Wrapper for git that adds auth headers.
git_auth() { git ${GIT_AUTH_ARGS[@]+"${GIT_AUTH_ARGS[@]}"} "$@"; }

wait_http() {
    local url="$1" max="${2:-30}"
    for ((i=0; i<max; i++)); do
        if curl -sf ${AUTH_CURL_ARGS[@]+"${AUTH_CURL_ARGS[@]}"} "$url" >/dev/null 2>&1; then return 0; fi
        sleep 1
    done
    return 1
}

# --- setup -------------------------------------------------------------------

step "e2e: walgit end-to-end test"

if [[ -z "$WALGIT" ]]; then
    step "building walgit (release)..."
    (cd "$ROOT" && cargo build -p walgit-cli -p walgit-nats --release)
    WALGIT="$ROOT/target/release/walgit"
fi
if [[ -z "$WALGIT_NATS" ]]; then
    WALGIT_NATS="$ROOT/target/release/walgit-nats"
fi
[[ -x "$WALGIT" ]] || fail "WALGIT must name an executable built from this checkout"

TMP="$(mktemp -d)"
PIDS=()
# "${PIDS[@]:-}": bash 3.2 (macOS /bin/bash) calls an empty array expansion an unbound variable
# under set -u, which aborts the EXIT trap before rm -rf.
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; rm -rf "$TMP"; }
trap cleanup EXIT

if [[ -z "${WALGIT_E2E_BASE_URL:-}" ]]; then
    # Local tests must neither depend on nor change the developer's Git config.
    export GIT_CONFIG_GLOBAL="$TMP/gitconfig" GIT_CONFIG_NOSYSTEM=1
    cat > "$GIT_CONFIG_GLOBAL" <<EOF
[user]
name = walgit-e2e
email = e2e@example.invalid
EOF
fi

PORT="$(rand_port)"

if [[ -n "${WALGIT_E2E_BASE_URL:-}" ]]; then
    # Remote mode: target an already-running server (e.g. a serverless host).
    BASE_URL="$WALGIT_E2E_BASE_URL"
    step "remote mode: targeting $BASE_URL"
    # a cloud load balancer intercepts /healthz; use /readyz for remote health checks.
    if ! wait_http "$BASE_URL/readyz" 30; then
        fail "remote server at $BASE_URL is not healthy"
    fi
    pass "remote server is ready"
else
    BASE_URL="http://127.0.0.1:${PORT}"
    step "phase 1: memory backend on port $PORT"

    # Generate a config for the memory backend.
    cat > "$TMP/walgit.toml" <<EOF
[server]
listen = "127.0.0.1:${PORT}"
[store]
backend = "memory"
bucket = "walgit-e2e"
[cache]
dir = "$TMP/cache"
[wal]
freshness_ttl = "0s"
[packs]
enabled = false
[lfs]
enabled = false
EOF

    step "starting walgit serve (memory)..."
    RUST_LOG=info "$WALGIT" --config "$TMP/walgit.toml" serve > "$TMP/server-mem.log" 2>&1 &
    SERVER_PID=$!
    PIDS+=("$SERVER_PID")
    sleep 2

    if ! wait_http "$BASE_URL/healthz" 15; then
        fail "server did not become ready"
    fi
    pass "server is ready"
fi

# --- synth -------------------------------------------------------------------

step "synth: generate synthetic repo (size s, seed 12345)"
SYNTH_DIR="$TMP/synth"
# synth reads nothing from the config (walgit-cli/src/lib.rs:493 passes only out/size/seed) and
# $TMP/walgit.toml exists in local mode only, so ask for defaults the way the CLI documents.
"$WALGIT" --config /dev/null synth --out "$SYNTH_DIR" --size s --seed 12345
pass "synth completed"

# Verify with git fsck.
(cd "$SYNTH_DIR" && git fsck --full --strict) >/dev/null 2>&1 || fail "synth repo failed fsck"
pass "synth repo passes git fsck"

# --- create repo via PUT -----------------------------------------------------

REPO="test/e2e-$(date +%s)-$$"
step "PUT $BASE_URL/$REPO.git (create repo)"
HTTP_CODE=$(curl_auth -sf -X PUT "$BASE_URL/$REPO.git" -w '%{http_code}' -o /dev/null || true)
if [[ "$HTTP_CODE" != "201" && "$HTTP_CODE" != "200" && "$HTTP_CODE" != "409" ]]; then
    fail "PUT repo returned $HTTP_CODE (expected 201/200/409)"
fi
pass "repo ready (HTTP $HTTP_CODE)"

# --- push --------------------------------------------------------------------

step "push: clone synth → push to walgit"
WORK="$TMP/work"
git clone -q "$SYNTH_DIR" "$WORK"
(cd "$WORK" && git remote remove origin 2>/dev/null || true)
(cd "$WORK" && git remote add origin "$BASE_URL/$REPO.git")
(cd "$WORK" && git_auth push -q origin main) || fail "push main failed"
pass "push main"

# --- clone -------------------------------------------------------------------

step "clone: from walgit"
CLONE="$TMP/clone"
git_auth clone -q "$BASE_URL/$REPO.git" "$CLONE" || fail "clone failed"
pass "clone"

# Compare HEAD.
HEAD_PUSH=$(cd "$WORK" && git rev-parse main)
HEAD_CLONE=$(cd "$CLONE" && git rev-parse main)
[[ "$HEAD_PUSH" == "$HEAD_CLONE" ]] || fail "HEAD mismatch: push=$HEAD_PUSH clone=$HEAD_CLONE"
pass "HEAD matches: ${HEAD_PUSH:0:12}"

(cd "$CLONE" && git fsck --full) >/dev/null 2>&1 || fail "clone failed fsck"
pass "clone passes git fsck"

# --- fetch -------------------------------------------------------------------

step "fetch: new commit → push → fetch"
(cd "$WORK" && git commit -q --allow-empty -m "e2e fetch test")
(cd "$WORK" && git_auth push -q origin main) || fail "second push failed"
(cd "$CLONE" && git_auth fetch -q origin) || fail "fetch failed"
HEAD2_PUSH=$(cd "$WORK" && git rev-parse main)
HEAD2_CLONE=$(cd "$CLONE" && git rev-parse origin/main)
[[ "$HEAD2_PUSH" == "$HEAD2_CLONE" ]] || fail "fetch HEAD mismatch"
pass "fetch sees new commit: ${HEAD2_PUSH:0:12}"

# --- ls-remote ---------------------------------------------------------------

step "ls-remote"
LS=$(git_auth ls-remote "$BASE_URL/$REPO.git" refs/heads/main 2>/dev/null | awk '{print $1}')
[[ "$LS" == "$HEAD2_PUSH" ]] || fail "ls-remote mismatch: $LS vs $HEAD2_PUSH"
pass "ls-remote matches"

# --- partial clone -----------------------------------------------------------

step "partial clone (--filter=blob:none)"
PARTIAL="$TMP/partial"
git_auth clone -q --filter=blob:none "$BASE_URL/$REPO.git" "$PARTIAL" || fail "partial clone failed"
pass "partial clone"
(cd "$PARTIAL" && git fsck --full) >/dev/null 2>&1 || fail "partial clone failed fsck"

# --- tag ---------------------------------------------------------------------

step "tag: push annotated tag"
(cd "$WORK" && git tag -a -m "e2e tag" v1.0.0)
(cd "$WORK" && git_auth push -q origin v1.0.0) || fail "tag push failed"
pass "tag pushed"

TAG_LS=$(git_auth ls-remote "$BASE_URL/$REPO.git" refs/tags/v1.0.0 2>/dev/null | awk '{print $1}')
[[ -n "$TAG_LS" ]] || fail "tag not found in ls-remote"
pass "tag visible: ${TAG_LS:0:12}^{}"

# --- delete ref --------------------------------------------------------------

step "delete: remove remote tag"
(cd "$WORK" && git_auth push -q origin :refs/tags/v1.0.0) || fail "tag delete failed"
TAG_GONE=$(git_auth ls-remote "$BASE_URL/$REPO.git" refs/tags/v1.0.0 2>/dev/null | wc -l)
[[ "$TAG_GONE" -eq 0 ]] || fail "tag still present after delete"
pass "tag deleted"

# --- for-each-ref diff -------------------------------------------------------

step "for-each-ref diff (push vs clone)"
REFS_WORK=$(cd "$WORK" && git for-each-ref --format='%(refname) %(objectname)' refs/heads/)
REFS_CLONE=$(cd "$CLONE" && git for-each-ref --format='%(refname) %(objectname)' refs/remotes/origin/ | sed '/^refs\/remotes\/origin\/HEAD /d')
# Normalize: strip refs/remotes/origin/ prefix from clone refs.
REFS_CLONE_NORM=$(echo "$REFS_CLONE" | sed 's|refs/remotes/origin/|refs/heads/|g')
[[ "$REFS_WORK" == "$REFS_CLONE_NORM" ]] || fail "branch refs differ between push and clone"
pass "refs match exactly"

# --- delete repo -------------------------------------------------------------

step "DELETE repo"
HTTP_CODE=$(curl_auth -sf -X DELETE "$BASE_URL/$REPO.git" -w '%{http_code}' -o /dev/null || true)
if [[ "$HTTP_CODE" != "200" && "$HTTP_CODE" != "204" ]]; then
    fail "DELETE repo returned $HTTP_CODE"
fi
pass "repo deleted (HTTP $HTTP_CODE)"

# --- phase 2: S3/rustfs (optional, local only) -------------------------------

# Kill the memory server (not in remote mode — no local server was started).
if [[ -z "${WALGIT_E2E_BASE_URL:-}" ]]; then
    kill "$SERVER_PID" 2>/dev/null || true
fi

# S3 phase only makes sense for local testing, not remote (a serverless host) mode.
if [[ -z "${WALGIT_E2E_BASE_URL:-}" && -n "${WALGIT_TEST_S3_ENDPOINT:-}" ]]; then
    step "phase 2: S3 backend ($WALGIT_TEST_S3_ENDPOINT)"

    BUCKET="${WALGIT_TEST_BUCKET:-walgit-test}"
    PORT_A="$(rand_port)"
    PORT_B="$(rand_port)"
    BASE_A="http://127.0.0.1:${PORT_A}"
    BASE_B="http://127.0.0.1:${PORT_B}"

    cat > "$TMP/cfg-s3.toml" <<EOF
[server]
listen = "127.0.0.1:0"
[store]
backend = "s3"
bucket = "$BUCKET"
[store.s3]
endpoint = "$WALGIT_TEST_S3_ENDPOINT"
region = "us-east-1"
force_path_style = true
[cache]
dir = "$TMP/cache-s3"
[wal]
freshness_ttl = "0s"
[packs]
enabled = false
[lfs]
enabled = false
EOF

    # Start instance A
    RUST_LOG=info env "WALGIT__SERVER__LISTEN=127.0.0.1:${PORT_A}" "WALGIT__CACHE__DIR=$TMP/cache-s3-a" \
        "$WALGIT" --config "$TMP/cfg-s3.toml" serve > "$TMP/server-a.log" 2>&1 &
    SERVER_A_PID=$!
    PIDS+=("$SERVER_A_PID")
    sleep 2
    wait_http "$BASE_A/healthz" 15 || fail "instance A not ready"
    pass "instance A ready on $PORT_A"

    # Start instance B
    RUST_LOG=info env "WALGIT__SERVER__LISTEN=127.0.0.1:${PORT_B}" "WALGIT__CACHE__DIR=$TMP/cache-s3-b" \
        "$WALGIT" --config "$TMP/cfg-s3.toml" serve > "$TMP/server-b.log" 2>&1 &
    SERVER_B_PID=$!
    PIDS+=("$SERVER_B_PID")
    sleep 2
    wait_http "$BASE_B/healthz" 15 || fail "instance B not ready"
    pass "instance B ready on $PORT_B"

    REPO2="test/e2e-cross-$(date +%s)-$$"
    step "S3: create repo on A"
    curl_auth -sf -X PUT "$BASE_A/$REPO2.git" -o /dev/null || fail "create on A"
    pass "repo created on A"

    step "S3: push to A"
    git clone -q "$SYNTH_DIR" "$TMP/work-s3"
    (cd "$TMP/work-s3" && git remote set-url origin "$BASE_A/$REPO2.git")
    (cd "$TMP/work-s3" && git_auth push -q origin main) || fail "push to A"
    pass "pushed to A"

    step "S3: conserving maintenance from an independent cache"
    WALGIT__PACKS__ENABLED=true WALGIT__CACHE__DIR="$TMP/cache-maintenance" \
        "$WALGIT" --config "$TMP/cfg-s3.toml" compact "$REPO2" --base --once \
        > "$TMP/maintenance.log" 2>&1 || { cat "$TMP/maintenance.log"; fail "maintenance failed"; }
    # The CLI can log per-repository failures while returning success. Require its
    # completed checkpoint receipt as well as its exit status.
    cat "$TMP/maintenance.log"
    rg -q '; checkpoint at seq [0-9]+' "$TMP/maintenance.log" || fail "maintenance did not finish"
    pass "conserving maintenance completed"

    step "S3: clone from B (cross-instance consistency)"
    git_auth clone -q "$BASE_B/$REPO2.git" "$TMP/clone-s3" || fail "clone from B"
    HEAD_A=$(cd "$TMP/work-s3" && git rev-parse main)
    HEAD_B=$(cd "$TMP/clone-s3" && git rev-parse main)
    [[ "$HEAD_A" == "$HEAD_B" ]] || fail "cross-instance HEAD mismatch: A=$HEAD_A B=$HEAD_B"
    pass "cross-instance consistency: ${HEAD_A:0:12}"

    (cd "$TMP/clone-s3" && git fsck --full) >/dev/null 2>&1 || fail "S3 clone failed fsck"
    pass "S3 clone passes fsck"

    # Leave no test repos behind (the S3 pair is local, but keep the habit).
    curl_auth -s -X DELETE "$BASE_A/$REPO2.git" -o /dev/null || true

    step "S3: for-each-ref diff"
    REFS_A=$(cd "$TMP/work-s3" && git for-each-ref --format='%(refname) %(objectname)' refs/heads/)
    REFS_B=$(cd "$TMP/clone-s3" && git for-each-ref --format='%(refname) %(objectname)' refs/remotes/origin/ | sed '/^refs\/remotes\/origin\/HEAD /d; s|refs/remotes/origin/|refs/heads/|g')
    [[ "$REFS_A" == "$REFS_B" ]] || fail "branch refs differ between S3 instances"
    pass "refs match exactly"

    kill "$SERVER_A_PID" "$SERVER_B_PID" 2>/dev/null || true
fi

# --- phase 3: `walgit-nats` as processes (optional, local only) --------------

# Real processes end to end: nats-server -js, `walgit serve` and `walgit-nats` sharing the rustfs
# bucket, real git pushes, assertions through the `nats` CLI. The first walgit-nats is woken by the
# sweep (and one notification); after a kill and restart the sweep is off, so a push there can only
# arrive through the notify subject.
if [[ -z "${WALGIT_E2E_BASE_URL:-}" && -n "${WALGIT_TEST_S3_ENDPOINT:-}" ]]; then
    if ! command -v nats-server >/dev/null 2>&1 || ! command -v nats >/dev/null 2>&1; then
        step "phase 3: walgit-nats (skipped: nats-server and the nats CLI must be on PATH)"
    elif [[ ! -x "$WALGIT_NATS" ]]; then
        fail "phase 3 needs the walgit-nats binary: build it (cargo build -p walgit-nats --release) or set WALGIT_NATS"
    else
        step "phase 3: walgit-nats against nats-server -js ($WALGIT_TEST_S3_ENDPOINT)"
        export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-walgit-dev}" AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-walgit-dev-secret}"

        NATS_PORT="$(rand_port)"
        NATS_URL="nats://127.0.0.1:${NATS_PORT}"
        STREAM="WALGIT_E2E_$$"
        REPO3_NAME="nats-$(date +%s)-$$"
        REPO3="test/$REPO3_NAME"
        SUBJECT3="walgit.ref.test.$REPO3_NAME"
        PORT_N="$(rand_port)"
        BASE_N="http://127.0.0.1:${PORT_N}"

        mkdir -p "$TMP/nats-store"
        nats-server -js -a 127.0.0.1 -p "$NATS_PORT" -sd "$TMP/nats-store" > "$TMP/nats-server.log" 2>&1 &
        PIDS+=("$!")
        for ((i=0; i<30; i++)); do nats -s "$NATS_URL" rtt >/dev/null 2>&1 && break; sleep 1; done
        nats -s "$NATS_URL" rtt >/dev/null 2>&1 || fail "nats-server did not start"
        pass "nats-server ready on $NATS_PORT"
        nats -s "$NATS_URL" stream add "$STREAM" --subjects 'walgit.ref.>' --storage file \
            --retention limits --dupe-window 1h --defaults >/dev/null || fail "provisioning stream $STREAM"

        # A fresh store prefix keeps the repositories earlier runs left in the bucket out of the stream.
        cat > "$TMP/cfg-nats.toml" <<EOF
[server]
listen = "127.0.0.1:${PORT_N}"
[store]
backend = "s3"
bucket = "${WALGIT_TEST_BUCKET:-walgit-test}"
prefix = "e2e-nats-$$-$(date +%s)/"
[store.s3]
endpoint = "$WALGIT_TEST_S3_ENDPOINT"
region = "us-east-1"
force_path_style = true
[cache]
dir = "$TMP/cache-nats"
[wal]
freshness_ttl = "0s"
[packs]
enabled = false
[lfs]
enabled = false
[events]
sweep_interval = "2s"
[events.nats]
urls = ["$NATS_URL"]
stream = "$STREAM"
EOF

        nats_count() {
            nats -s "$NATS_URL" stream info "$STREAM" -j 2>/dev/null \
                | python3 -c 'import json,sys; print(json.load(sys.stdin)["state"]["messages"])' 2>/dev/null || echo 0
        }
        wait_nats_count() {
            local want="$1" max="${2:-20}"
            for ((i=0; i<max; i++)); do
                [[ "$(nats_count)" -ge "$want" ]] && return 0
                sleep 1
            done
            return 1
        }
        # One line per message: subject action ref_type ref_name walgit_seq nats_msg_id
        nats_events() {
            local n count
            count="$(nats_count)"
            for ((n=1; n<=count; n++)); do
                nats -s "$NATS_URL" stream get "$STREAM" "$n" -j | python3 -c '
import base64, json, sys
m = json.load(sys.stdin)
body = json.loads(base64.b64decode(m["data"]))
hdrs = base64.b64decode(m["hdrs"]).decode().splitlines()
mid = next(l.split(": ", 1)[1] for l in hdrs if l.startswith("Nats-Msg-Id:"))
print(m["subject"], body["action"], body["ref_type"], body["ref_name"], body["_walgit"]["seq"], mid)'
            done
        }
        start_walgit_nats() {
            local log="$1"; shift
            : > "$log"
            RUST_LOG=info env "WALGIT__CACHE__DIR=$TMP/cache-nats-pub" "$@" \
                "$WALGIT_NATS" --config "$TMP/cfg-nats.toml" > "$log" 2>&1 &
            NATS_PUB_PID=$!
            PIDS+=("$NATS_PUB_PID")
            for ((i=0; i<30; i++)); do
                kill -0 "$NATS_PUB_PID" 2>/dev/null || { cat "$log"; fail "walgit-nats exited"; }
                grep -q "service ready" "$log" && return 0
                sleep 1
            done
            cat "$log"; fail "walgit-nats did not become ready"
        }

        RUST_LOG=info env "WALGIT__CACHE__DIR=$TMP/cache-nats-serve" \
            "$WALGIT" --config "$TMP/cfg-nats.toml" serve > "$TMP/server-nats.log" 2>&1 &
        SERVER_N_PID=$!
        PIDS+=("$SERVER_N_PID")
        wait_http "$BASE_N/healthz" 15 || fail "walgit serve (nats phase) not ready"
        start_walgit_nats "$TMP/walgit-nats-1.log"
        pass "walgit serve on $PORT_N and walgit-nats ready"

        curl_auth -sf -X PUT "$BASE_N/$REPO3.git" -o /dev/null || fail "create $REPO3"
        git clone -q "$SYNTH_DIR" "$TMP/work-nats"
        (cd "$TMP/work-nats" && git remote set-url origin "$BASE_N/$REPO3.git")

        step "nats: create pushed, woken by a notification on walgit.notify"
        (cd "$TMP/work-nats" && git push -q origin main) || fail "push main"
        nats -s "$NATS_URL" pub walgit.notify "{\"repo\":\"$REPO3\"}" >/dev/null
        wait_nats_count 1 10 || fail "no event for the first push after a notification"
        pass "first event arrived"

        step "nats: update, tag and delete, found by the 2s sweep"
        (cd "$TMP/work-nats" && git commit -q --allow-empty -m "e2e nats update" && git push -q origin main) || fail "push update"
        (cd "$TMP/work-nats" && git tag -a -m "e2e nats tag" v1.0.0 && git push -q origin v1.0.0) || fail "push tag"
        (cd "$TMP/work-nats" && git push -q origin :refs/tags/v1.0.0) || fail "delete tag"
        wait_nats_count 4 30 || fail "expected 4 events, have $(nats_count)"
        sleep 5
        [[ "$(nats_count)" -eq 4 ]] || fail "sweeps republished: $(nats_count) messages, expected 4"
        EVENTS="$(nats_events)"
        echo "$EVENTS"
        [[ "$(echo "$EVENTS" | sed -n 1p)" == "$SUBJECT3 create branch refs/heads/main 1 $REPO3:1:refs/heads/main" ]] || fail "event 1"
        [[ "$(echo "$EVENTS" | sed -n 2p)" == "$SUBJECT3 update branch refs/heads/main 2 $REPO3:2:refs/heads/main" ]] || fail "event 2"
        [[ "$(echo "$EVENTS" | sed -n 3p)" == "$SUBJECT3 create tag refs/tags/v1.0.0 3 $REPO3:3:refs/tags/v1.0.0" ]] || fail "event 3"
        [[ "$(echo "$EVENTS" | sed -n 4p)" == "$SUBJECT3 delete tag refs/tags/v1.0.0 4 $REPO3:4:refs/tags/v1.0.0" ]] || fail "event 4"
        pass "create, update, tag create and delete on $SUBJECT3, in WAL order"

        step "nats: kill walgit-nats, restart (sweep off): no duplicates, a new push arrives by notification"
        kill "$NATS_PUB_PID" 2>/dev/null || true
        wait "$NATS_PUB_PID" 2>/dev/null || true
        start_walgit_nats "$TMP/walgit-nats-2.log" WALGIT__EVENTS__SWEEP_INTERVAL=0s
        sleep 3
        [[ "$(nats_count)" -eq 4 ]] || fail "restart republished: $(nats_count) messages, expected 4"
        pass "restart left the stream at 4 messages"

        (cd "$TMP/work-nats" && git commit -q --allow-empty -m "e2e nats after restart" && git push -q origin main) || fail "push after restart"
        sleep 3
        [[ "$(nats_count)" -eq 4 ]] || fail "sweep is off but the push was published without a wake-up"
        nats -s "$NATS_URL" pub walgit.notify "{\"repo\":\"$REPO3\"}" >/dev/null
        wait_nats_count 5 10 || fail "no event after the notification following the restart"
        sleep 3
        [[ "$(nats_count)" -eq 5 ]] || fail "expected exactly 5 messages, have $(nats_count)"
        LAST="$(nats_events | tail -n 1)"
        [[ "$LAST" == "$SUBJECT3 update branch refs/heads/main 5 $REPO3:5:refs/heads/main" ]] || fail "event 5: $LAST"
        pass "post-restart push published exactly once"

        step "nats: \$SRV catch-up endpoint"
        REPORT="$(nats -s "$NATS_URL" request walgit.events.catch-up "{\"repo\":\"$REPO3\"}" --raw 2>/dev/null || true)"
        echo "$REPORT"
        echo "$REPORT" | python3 -c '
import json, sys
r = json.load(sys.stdin)
assert r["repo"] == sys.argv[1] and r["head_seq"] == 5 and r["emitted"] == 0, r' "$REPO3" || fail "catch-up report"
        pass "catch-up reports head 5, nothing to emit"

        step "nats: SIGTERM makes walgit-nats exit 0 promptly"
        kill -TERM "$NATS_PUB_PID"
        for ((i=0; i<50; i++)); do
            kill -0 "$NATS_PUB_PID" 2>/dev/null || break
            sleep 0.1
        done
        if kill -0 "$NATS_PUB_PID" 2>/dev/null; then
            cat "$TMP/walgit-nats-2.log"; fail "walgit-nats still running 5s after SIGTERM"
        fi
        NATS_RC=0
        wait "$NATS_PUB_PID" 2>/dev/null || NATS_RC=$?
        [[ "$NATS_RC" -eq 0 ]] || { cat "$TMP/walgit-nats-2.log"; fail "walgit-nats exited $NATS_RC after SIGTERM"; }
        grep -q "shutting down" "$TMP/walgit-nats-2.log" || fail "no shutdown line after SIGTERM"
        pass "SIGTERM: walgit-nats exited 0 within 5s"

        kill "$SERVER_N_PID" 2>/dev/null || true
        curl_auth -s -X DELETE "$BASE_N/$REPO3.git" -o /dev/null || true
    fi
fi

# --- done --------------------------------------------------------------------

step "e2e: all checks passed"
green "================================================"
green "  walgit e2e: PASS"
green "================================================"
