# Events

Context: **the normative WAL-to-webhook event contract**, for anyone changing ref-event shapes, the events
bridge, delivery, or consumer semantics (D32). The golden tests in `crates/walgit-server/src/{events,bridge}.rs`
and `tests/events.rs` are its executable form (same discipline as `docs/POLICY.md`).

## Principle

**Events are produced from the WAL by one small service — the bridge (`Role::Events`) — never by the push
path.** It tails each repo's log from a durable per-repo cursor, converts committed entries, POSTs them to your
webhook, advances the cursor. So an event is delivered iff its entry is durable, a crash can't lose one, the lag is
`head_seq − cursor`, and no writer — any serving host, a push broker, the CLI, an import — contains event code:
every writer is covered because the WAL is what is read.

Invariants:

1. Nothing **ever gates a push**: the bridge is another process reading the bucket. A down webhook adds zero
   milliseconds to receive-pack; it adds lag, which is a metric.
2. No no-op events. `old == new` (and `0→0`) emits nothing.
3. No lost events: the cursor advances only after the webhook answered 2xx. Duplicates are possible
   (at-least-once) and carry a deterministic dedup key. What *can* happen is a **gap when the bridge lags behind
   log retention** (entries folded into a checkpoint before they were read): counted (`events_bridge_gap_total`)
   and warned, never silently repaired — consumers backfill from the WAL.
4. One producer, one instance.

Only `ref` events exist. Not events: push denials and auth failures (metrics + logs), LFS, compaction/checkpoints
(already WAL entries), repo/policy admin (no consumer; the HTTP request log has the principal).

## `ref` event

```json
{
  "action": "update",
  "ref_type": "branch",
  "ref_name": "refs/heads/main",
  "old": "48a0637…",
  "new": "cb38da1…",
  "pusher": "alice@example.com",
  "correlation_id": "d1f916f7-…",
  "repo": "acme/monorepo",
  "_walgit": { "schema_version": 1, "seq": "42", "entry_kind": "push", "request_id": "d1f916f7-…" }
}
```

- `action` — `create` / `update` / `delete`. Force is not a wire action (consumers derive it).
- `old` / `new` — **always the full zero OID on create/delete, never `""`** (40 chars sha1, 64 sha256).
- `ref_type` — `branch` (`refs/heads/`), `tag` (`refs/tags/`), `""` otherwise.
- `pusher` — the authenticated principal in the log entry's `meta` (`X-Walgit-Principal` on forwarded pushes).
- `correlation_id` / `_walgit.request_id` — the user-visible request id: the middleware honours an incoming
  `x-request-id` (else mints one), a front forwards it when it forwards receive-pack, `push_meta` stores it.
- `_walgit.seq` — the entry's WAL seq, a JSON **string** (uint64 convention). `entry_kind` — `push` |
  `ref_update`; consumers must not care.
- One event per ref update in the transaction; symbolic (HEAD) retargets and COMPACT / CHECKPOINT / SETTINGS
  entries emit nothing.

**Dedup key (normative): `(repo, _walgit.seq, ref_name)`.** **Order: by `seq` per repo.** Nothing else is promised.

## Delivery: the webhook

Each catch-up `POST`s one JSON **array** of events (a batch: everything in `(cursor, head_seq]`) to
`events.webhook_url` with:

```
Content-Type:        application/json
X-Walgit-Delivery:   <sha1 hex of the body>                 # the batch's id; safe to dedup on
X-Walgit-Signature:  sha256=<hex HMAC-SHA256(body, events.webhook_secret)>   # when a secret is configured
```

Answer 2xx to acknowledge; anything else (or a timeout, 10 s) leaves the cursor where it was and the bridge
retries the same range on the next wake-up. A consumer therefore sees at-least-once delivery of whole batches.
Verify the signature with a constant-time compare before parsing.

**Backfill contract (normative for consumers):** on any gap, read the WAL log from your last known seq
(`walgit wal ls`) and treat each PUSH / REF_UPDATE entry's ref transaction as the missed events. The webhook is
a latency optimization over polling the log; correctness never depends on it.

## The bridge

```
writers (any host, broker, CLI, import) ── manifest.pb CAS ──► bucket ──► notification ──► POST /_events/notify
   (no event code)                                                                               │
                                                                                                 ▼
                 the events host (roles=["events"], one instance):            catch_up(repo): cursor → manifest
                 + sweep every events.sweep_interval (backstop + health check)  → log (cursor, head] → ref events
                                                                               → webhook → CAS cursor → log line
```

`catch_up(repo)` = read `repos/<o>/<r>/events/cursor.json` → fresh manifest → log entries `(cursor, head_seq]` →
`ref` events → webhook → CAS the cursor to `head_seq`. A webhook error leaves the cursor; the next wake-up replays
the same range. A cold cursor starts at the oldest readable seq (`min_seq − 1`: everything still in the manifest's
log window is published once; pre-seed the cursor to skip history). Every published event is also one structured
log line (`event_type="ref"`). Metrics: `events_published_total{sink}`, `events_bridge_lag_entries{repo}`,
`events_bridge_gap_total{repo}`, `events_bridge_sweep_found_total`; alert on lag growth, any gap, any sweep-found.

Wake-ups (both idempotent; they only ever call `catch_up`):
- `POST /_events/notify` with a **bucket notification** naming a finalized `…/manifest.pb` — the commit point
  itself as the notification. Accepted bodies: a GCS Pub/Sub push envelope (`message.attributes.eventType =
  OBJECT_FINALIZE`, `objectId`), an S3 event notification (`Records[].eventName = ObjectCreated:*`,
  `s3.object.key`; MinIO, rustfs and Ceph emit the same shape), or your own glue's `{"key": "repos/o/r/manifest.pb"}`
  / `{"repo": "o/r"}`. Keys are bucket object keys as notifications carry them, `store.prefix` included
  (`{"key": "<prefix>/repos/o/r/manifest.pb"}`); `{"repo": …}` names the repository whatever the prefix.
  Everything else is acked and ignored; a webhook failure answers 503 so the notifier
  redelivers. Authenticated like every route (`require_read`): give the notifier a token.
- The sweep (`events.sweep_interval`, default 5 min): `list` + one conditional manifest GET per repo. Not needed
  for correctness; it is the backstop *and the health check* — a sweep that publishes anything means
  notifications are not flowing (`events_bridge_sweep_found_total`, warn). With no notifier at all, set the
  sweep to the latency you can live with.

```toml
[server]
roles = ["events"]            # or leave roles empty on a one-box install: every role, bridge included
[events]
webhook_url = "https://hooks.example.com/walgit"
webhook_secret = "…"          # env: WALGIT__EVENTS__WEBHOOK_SECRET
sweep_interval = "5m"
```

## Delivery: NATS JetStream (`walgit-nats`)

`walgit-nats` is a second sink for the same `ref` events, its own binary (`walgit-nats --config walgit.toml`,
`[events.nats]`) built from the `walgit-nats` crate. It depends on `walgit-events` (the shared event model) and
`walgit-telemetry` (config loading and logging), never on `walgit-server` or `walgit-cli`, so it carries no HTTP
server and no web UI. The webhook bridge is unchanged and the two can run side by side.
Like the bridge it is a reader of the WAL, never a step of a push (D32). It keeps its local copies under
`<cache.dir>/.nats`, so it can share a `walgit.toml` and a host with `walgit serve` without touching serve's.
`--config` (or `WALGIT_CONFIG`) may name that same file, or a second, smaller one.

**Which binary reads which sections.** Both binaries read one `walgit.toml` and the same `WALGIT__` environment
overrides, and each parses and validates only what it reads; the rest is skipped, not rejected. A typo inside a
section a binary reads still fails it.

| Binary | Reads | Skips |
|---|---|---|
| `walgit serve` / `walgit-server` | every section, `[events]` `webhook_url`, `webhook_secret`, `sweep_interval` | `[events.nats]` |
| `walgit-nats` | `[store]`, `[cache]`, `[wal]`, `[telemetry]`, `[events]` `sweep_interval`, `[events.nats]` | `[server]`, `[packs]`, `[refs]`, `[packfile_uri]`, `[maintenance]`, `[placement]`, `[lfs]`, `[git]`, `[upstream]`, `[events]` `webhook_url` and `webhook_secret` |

So an invalid `[server.auth]` does not stop `walgit-nats`, and a bad `[events.nats]` does not stop the server.
A `WALGIT__` variable for a section the binary skips (`WALGIT__SERVER__LISTEN` for `walgit-nats`,
`WALGIT__EVENTS__NATS__STREAM` for the server) is skipped without a warning, so one environment can configure
both; a key unknown to both is still warned about.

- **The operator provisions the stream; `walgit-nats` never creates it**, so its credentials need no
  stream-admin permission, only stream info and message reads on the stream and publishes on its subjects.
  The stream it expects, for the default `stream` and `subject_prefix`:

  ```sh
  nats stream add WALGIT_EVENTS --subjects 'walgit.ref.>' --storage file --retention limits --dupe-window 1h --defaults
  ```

  At boot it fails when the stream is missing or is not file storage with limits retention, a subject
  capturing `<subject_prefix>.ref.>` and a duplicate window, naming every shortfall. Limits, replicas and
  placement stay the operator's choice.
- **The stream is the cursor (D50).** For each repository the publisher reads the last message on the
  repository's subject, takes its `Walgit-Seq` header and resumes from that seq, inclusive. Events already in
  the stream are dropped, so a crash in the middle of a multi-ref entry completes only the rest of that entry. A repository with no message yet starts at `events.nats.start`: `oldest` (the oldest
  readable seq, as the bridge does) or `head` (only pushes made after the publisher first saw it).
- **Subjects.** `<subject_prefix>.ref.<owner>.<repo>`. Bytes outside `[A-Za-z0-9_-]` in a name are written
  `%XX`, so `acme.io/mono.repo` is `walgit.ref.acme%2Eio.mono%2Erepo` and the mapping is reversible.
  Subscribe to `walgit.ref.>` for everything, `walgit.ref.acme.>` for one owner.
- **Message.** The body is one `ref` event JSON object (not an array). Headers: `Walgit-Seq`, `Walgit-Repo`
  and `Nats-Msg-Id = <repo>:<seq>:<ref_name>`, which makes JetStream drop a republished event inside the
  stream's duplicate window. The publisher waits for every PubAck before the next message, so order within
  a repository is `_walgit.seq` order.
- **Concurrent publishers.** The first publish of a catch-up sets `Nats-Expected-Last-Subject-Sequence`.
  If another instance moved the subject first, this one stops, logs and lets the next wake-up retry from the
  new last message. Delivery is at least once; consumers still dedup on `(repo, _walgit.seq, ref_name)`.
- **Wake-ups.** A queue-group subscription on `events.nats.notify_subject` accepts the same notification
  shapes as `POST /_events/notify` (S3 event JSON, a GCS envelope, `{"key": ...}`, `{"repo": ...}`), and
  `events.sweep_interval` is the backstop with the same warning when it finds unpublished entries.
  Gap detection matches the bridge: a log gap is logged and the entries that are readable are published.
- **Service API.** `walgit-nats` registers a NATS micro service `walgit-events` (`$SRV.PING|INFO|STATS`, built in)
  with one endpoint, `<subject_prefix>.events.catch-up`. Request `{"repo": "o/r"}`, reply the catch-up report
  (`repo`, `from_seq`, `head_seq`, `emitted`, `conflict`); an unknown repository is a 404 service error.
  Gap, conflict and sweep counters are in the stats response. There is no HTTP listener.
- **Losing the stream.** Wiping the stream republishes the WAL window on the next wake-up; history older than
  the oldest readable entry is not in the WAL and is backfilled with `walgit wal ls`. Wiping walgit loses nothing.
  Deleting and recreating the stream republishes at once. A *purge* does not: JetStream keeps its duplicate
  table across a purge, so inside `duplicate_window` the republished events are acknowledged as duplicates and
  land nothing; the publisher leaves the repository open and the window's expiry heals it on a later wake-up.
  Prefer delete and recreate, or wait out the window.
- **`start = "head"` and restarts.** The first-seen head of a repository without a message is remembered in
  memory only. Pushes made while the publisher is down, before the repository's first event, are skipped for good
  after a restart (pinned by a test).
- **A foreign message** (no `Walgit-Seq` / `Nats-Msg-Id`, or a body without the event's `old`/`new`) as the last message on a repository's subject makes every
  catch-up for that repository fail, naming the subject and stream sequence, until it is removed.
- **Deleting and recreating a repository.** The manifest has no incarnation id, so the signal is the stream
  disagreeing with the WAL: the last message on a subject names a seq above the repository's `head_seq`, or the
  entry at its seq does not yield that message's event (same ref, `old` and `new`). The publisher then warns, counts it (`rewinds` in the service stats), ignores that message as a cursor (the first seq
  follows `events.nats.start`; with `"head"` the new incarnation's head at that first sighting is kept while the
  old message still tops the subject, so later pushes are published) and still guards the first publish with the subject's real last stream sequence, so
  it does not conflict. The new incarnation's `Nats-Msg-Id` (`<repo>:<seq>:<ref>`) can equal the old one's: inside
  `duplicate_window` the broker swallows it, the catch-up is not confirmed, the repository stays open and the
  event lands on the first wake-up after the window (pinned by a test). With `"head"` and nothing on the subject,
  a repository seen deleted forgets its first-seen head, and a head lower than that mark resets it. Limitation:
  an entry of the new incarnation identical to the old one's at the same seq (same ref, `old` and `new`) is taken
  for it. Consumers should treat a delete and recreate as a new stream.
  The webhook bridge has the same ambiguity from the other side: its cursor lives under the repository's prefix and
  is deleted with it, so the new incarnation is published from seq 1 and the dedup key `(repo, seq, ref_name)`
  collides with the old incarnation's events.
- **Config adaptation.** `walgit-nats` always revalidates the manifest (`wal.freshness_ttl` is forced to zero) and
  never prefetches packs (`wal.prefetch_packs` is forced off), whatever `[wal]` says, because it never applies a
  push itself and only reads manifest and log (`walgit_nats::publisher_config`, which builds the registry's
  config from `walgit_config::NatsConfig`).
- **Cost.** Every wake-up for a repository reads the last message on its subject (one JetStream request) before
  deciding there is nothing to do, so a wiped stream is noticed without a restart.

Each behaviour above has a test against a real `nats-server -js` (`crates/walgit-nats/tests/jetstream.rs`, and
`crates/walgit-server/tests/events_nats.rs` with real git pushes); `tests/e2e.sh` runs `walgit-nats` as a process.

| Behaviour | Test |
|---|---|
| Push becomes one message per ref with the bridge's event shape | `pushes_become_events_with_the_right_shape`, `an_atomic_multi_ref_push_is_one_message_per_ref_in_order` |
| The webhook bridge and `walgit-nats` emit the same events | `the_webhook_bridge_and_the_jetstream_publisher_agree` |
| Names are encoded in the subject | `a_dotted_repository_name_is_encoded_in_the_subject` |
| The stream is the cursor; a restart resumes from it | `stream_is_the_cursor`, `a_restarted_publisher_resumes_from_the_stream` |
| A crash mid-entry finishes that entry only | `a_crash_mid_entry_finishes_that_entry_only` |
| A NATS outage mid catch-up loses and duplicates nothing | `an_outage_mid_catch_up_loses_and_duplicates_nothing` |
| Broker dedup inside `duplicate_window`; none after it | `broker_dedup_absorbs_a_republish_inside_the_window`, `after_the_dedup_window_a_republish_lands_twice` |
| Concurrent publishers leave each event once | `a_second_publisher_that_moved_first_stops_this_one`, `racing_publishers_leave_each_event_exactly_once` |
| Losing the stream republishes the WAL window | `a_wiped_stream_is_republished_on_the_next_catch_up`, `a_purged_stream_heals_once_the_dedup_window_has_passed` |
| A log gap is counted, readable entries still published | `entries_folded_before_publishing_are_a_counted_gap` |
| A gap is counted once while nothing moves the subject | `a_gap_is_counted_once_while_nothing_moves_the_subject` |
| Bucket notifications wake it; junk is ignored | `bucket_notifications_wake_the_publisher_without_a_catch_up_call`, `notifications_that_name_nothing_are_ignored_and_the_loop_survives` |
| The sweep finds what nobody notified | `the_periodic_sweep_finds_a_push_nobody_notified` |
| Service API: catch-up, errors, stats | `notifications_and_the_service_api_wake_the_publisher`, `the_catch_up_endpoint_reports_a_real_repository`, `the_service_answers_bad_requests_and_reports_the_counters` |
| A NATS server restart keeps stream, subscriptions and service | `a_nats_server_restart_keeps_the_stream_the_subscriptions_and_the_service` |
| `start = "head"` | `head_start_publishes_nothing_older`, `known_limitation_head_start_skips_pushes_made_while_the_publisher_was_down` |
| A foreign message wedges the repository with a clear error | `a_foreign_message_on_a_repo_subject_fails_every_catch_up_with_a_clear_error` |
| A notification inside `wal.freshness_ttl` still publishes the push; no pack prefetch | `a_notification_inside_the_freshness_ttl_still_publishes_the_push`, `the_publisher_never_prefetches_packs_whatever_the_operator_configured` |
| Subject tokens round-trip odd names and never carry `.`, `*`, `>` | `odd_names_round_trip_and_never_carry_a_metacharacter`, `every_single_byte_string_round_trips` (unit, `subject.rs`) |
| Notification shapes: S3 multi-record, GCS finalize only, `store.prefix` | `an_s3_event_with_several_records_names_each_repo_once`, `a_gcs_envelope_counts_only_finalize_events`, `the_store_prefix_scopes_what_is_notified` (unit, `wake.rs`) |
| A missing stream fails clearly and is never created; a misshapen one fails at boot and is never altered; an existing one is reused as is | `a_missing_stream_fails_clearly_and_is_never_created`, `a_misshapen_stream_fails_at_boot_and_is_never_altered`, `an_existing_stream_with_other_settings_is_reused_not_clobbered` |
| A bad server URL fails to connect naming the setting | `an_unparsable_server_url_fails_to_connect_naming_the_setting` |
| Tags, deletes and non-branch refs carry `ref_type` and zero OIDs | `tags_and_deletes_carry_their_ref_type_and_zero_oids` |
| Many pushes land in seq order across catch-ups | `many_sequential_pushes_land_in_seq_order_across_catch_ups` |
| A deleted and recreated repository is detected and published as new; an id collision heals after the dedup window | `a_recreated_repository_is_published_as_a_new_one`, `a_recreated_repository_that_caught_up_with_the_old_seqs_is_still_published_whole`, `a_recreated_repository_under_head_start_publishes_pushes_after_its_first_sighting`, `a_deleted_repository_under_head_start_forgets_its_mark`, `a_recreation_under_head_start_seen_only_as_a_lower_head_resets_the_mark`, `a_recreated_repository_colliding_with_the_old_ids_heals_after_the_dedup_window` |
| The publisher never shares local copies with `walgit serve` | `the_publisher_never_shares_local_copies_with_serve` |
| SIGTERM exits 0 | `tests/e2e.sh` phase 3 |

Not covered: Prometheus metrics (the service stats are the only counters), multi-node JetStream clusters, and
the Containerfile / flake packaging.

```toml
[events]
sweep_interval = "5m"
[events.nats]
urls = ["nats://nats.internal:4222"]
start = "oldest"
```

Credentials are never in `walgit.toml`. `walgit-nats` reads the variables the `nats` CLI reads and combines
them the way it does, so a shell where `nats` works runs `walgit-nats` against the same server as the same
user. An empty variable counts as unset.

| Variable | Effect |
|---|---|
| `NATS_URL` | Comma-separated servers; replaces `urls` |
| `NATS_USER`, `NATS_PASSWORD` | User and password; `NATS_USER` alone is sent as a token, as in `nats` |
| `NATS_CREDS` | `.creds` file, a path or `file://` |
| `NATS_NKEY` | nkey seed file, a path or `file://` |
| `NATS_JWT`, `NATS_SEED` | User JWT signed with the seed; `NATS_JWT` alone is a bearer JWT. Each is the value itself or `file://` |
| `NATS_TOKEN` | Token, sent alongside any identity above |
| `NATS_CERT`, `NATS_KEY`, `NATS_CA` | TLS client certificate and key (both needed), and the CA chain |

The identity is the first of user and password, `NATS_CREDS`, `NATS_NKEY`, then `NATS_JWT`, the order `nats`
uses. A server URL written `user:password@host` or `token@host` replaces the user, password and token, as in
`nats`; servers that carry credentials must carry the same ones. `NATS_CONTEXT` and `NATS_SOCKS_PROXY` stop `walgit-nats` at startup: it reads no context files and has no
SOCKS dialer, and ignoring either would connect differently from `nats`. `NATS_TIMEOUT` and `NATS_COLOR` shape
the CLI's own requests and output and have no effect here. Only the methods and the URLs without their
credentials are logged, never a secret.

## Consumer checklist

1. Verify `X-Walgit-Signature` (if you set a secret), then parse the array.
2. Dedup on `(repo, _walgit.seq, ref_name)` (or on `X-Walgit-Delivery` per batch).
3. Order by `_walgit.seq` within a repo; do not assume order across repos.
4. On a gap alert from the bridge, backfill from `walgit wal ls <repo> --from <seq>`.
