# Changes Worker — Release Notes

---

## v1.0.0 — 2026-04-15

**Initial release.** A production-ready, async Python 3 processor for the Couchbase `_changes` feed.

### Features

- **Multi-source support** — Works with Sync Gateway, Capella App Services, and Couchbase Edge Server. Automatic compatibility handling (feed type fallbacks, timeout clamping, `_bulk_get` vs individual GETs).

- **Longpoll changes feed** — Configurable poll interval, channel filtering, `active_only`, `include_docs`, and `version_type` (rev/cv) support.

- **Feed throttling** — Consume large feeds (e.g., `since=0` with 100K+ docs) in configurable bite-sized batches via `throttle_feed`, with immediate back-to-back fetching until caught up.

- **CBL-style checkpoint management** — Checkpoints stored as `_local/` documents on Sync Gateway using the same key derivation as Couchbase Lite (`SHA1(client_id + URL + channels)`). Falls back to local `checkpoint.json` when the gateway is unreachable.

- **Output forwarding** — Forward processed documents to any HTTP endpoint (`PUT`/`DELETE` per doc) or to stdout for piping. Supports configurable retry with exponential backoff, halt-on-failure (freezes checkpoint), and reachability checks at startup.

- **Multiple output formats** — JSON (default), XML, form-encoded, msgpack, CBOR, BSON, and YAML. Startup validation blocks launch if the required library isn't installed.

- **Doc fetching** — When `include_docs=false`, fetches full document bodies via `_bulk_get` (SG/App Services) or fanned-out individual GETs (Edge Server), processed in configurable batches (`get_batch_number`).

- **Async concurrency control** — Parallel or sequential processing within each batch, with a configurable semaphore (`max_concurrent`). Checkpoint only advances after the entire batch completes.

- **Startup config validation** — Every setting validated against the selected `gateway.src` before the worker starts. Invalid combinations produce clear error messages and block startup; non-fatal issues log warnings.

- **Connection test mode** (`--test`) — Verifies server root, keyspace, `_changes` endpoint, checkpoint, and output endpoint reachability. Returns exit code 0/1 for CI and Docker health checks.

- **Dry run mode** — `processing.dry_run=true` processes the feed and logs what would be sent without actually sending anything.

- **Retryable HTTP** — Configurable retry with exponential backoff for both gateway and output requests. Separate retry configs for source vs destination.

- **Prometheus metrics endpoint** (`/_metrics`) — Built-in HTTP server exposing all operational metrics in Prometheus text exposition format:

  | Category | Metrics |
  |---|---|
  | **Process** | `uptime_seconds` |
  | **Poll loop** | `poll_cycles_total`, `poll_errors_total`, `last_poll_timestamp_seconds`, `last_batch_size` |
  | **Changes** | `changes_received_total`, `changes_processed_total`, `changes_filtered_total`, `changes_deleted_total`, `changes_removed_total` |
  | **Feed content** | `feed_deletes_seen_total`, `feed_removes_seen_total` (always counted, regardless of filter settings) |
  | **Data volume** | `bytes_received_total` (from `_changes` + `_bulk_get` + GETs), `bytes_output_total` (to downstream) |
  | **Doc fetching** | `docs_fetched_total` |
  | **Output** | `output_requests_total`, `output_errors_total`, `output_endpoint_up`, `output_requests_by_method_total{method=PUT\|DELETE}`, `output_errors_by_method_total{method=PUT\|DELETE}` |
  | **Response time** | `output_response_time_seconds` summary (p50, p90, p99, sum, count) |
  | **Checkpoint** | `checkpoint_saves_total`, `checkpoint_save_errors_total`, `checkpoint_seq` |
  | **Retries** | `retries_total` |

  All metrics labeled with `src` and `database` for multi-instance Grafana dashboards. Full reference with PromQL queries and alerting rules in [`metrics.html`](metrics.html).

- **Graceful shutdown** — Handles `SIGINT`/`SIGTERM`, completes current batch, saves checkpoint, and exits cleanly.

- **Docker support** — Includes `Dockerfile` for containerized deployment.

- **Logging** — Structured logging via Python stdlib with [icecream](https://github.com/gruns/icecream) debug tracing. Configurable log level (DEBUG/INFO/WARNING/ERROR).

### CLI

```
python changes_worker.py --config config.json          # Run the worker
python changes_worker.py --config config.json --test   # Test connectivity
python changes_worker.py --version                     # Print version
```

### Requirements

- Python 3.11+
- `aiohttp>=3.9`
- `icecream>=2.1`
- Optional: `msgpack`, `cbor2`, `pymongo` (bson), `pyyaml` for non-JSON output formats
