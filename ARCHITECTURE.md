# Code map

The host (`blohard-host`) has two optional roles. The store keeps content bytes, and the gateway
indexes chain history and serves the app's reads. [main.rs](src/main.rs) opens the state, starts the
background work, serves HTTP and shuts everything down in order. [server.rs](src/server.rs) builds
that state, and [routes.rs](src/routes.rs) registers the HTTP routes and their limits.

The [README](README.md#run-against-a-local-chain) shows how to run a host locally, and
[config.example.toml](config.example.toml) explains the configuration.

## Where to start for a change

| Change | Start here | Tests to look at |
|---|---|---|
| Add an endpoint or change its visitor limits | `ApiRoutes` and `router` in [routes.rs](src/routes.rs), response helpers in [http.rs](src/http.rs) | [paths.rs](src/http_tests/paths.rs), [budget.rs](src/http_tests/budget.rs) |
| Change configuration, startup or shutdown | [config.rs](src/config.rs), `Background` in [main.rs](src/main.rs), `open_state` in [server.rs](src/server.rs) | The config's unit tests, [lifecycle.rs](src/http_tests/lifecycle.rs) |
| Change feed filters, replies, paging or refresh results | [index/query.rs](src/index/query.rs), [index/changes.rs](src/index/changes.rs), HTTP handlers in [indexed.rs](src/indexed.rs) | [index/tests.rs](src/index/tests.rs), [indexed_reads.rs](src/http_tests/indexed_reads.rs) |
| Investigate a stalled index or change how chain history is read | [index/follow.rs](src/index/follow.rs), [index/journal.rs](src/index/journal.rs), [index/model.rs](src/index/model.rs) | [lifecycle.rs](src/http_tests/lifecycle.rs), [dense_blocks.rs](src/http_tests/dense_blocks.rs), [reorg_race.rs](src/http_tests/reorg_race.rs) |
| Change how upload requests are handled: memory, slots, deadlines or responses | [publish/http.rs](src/publish/http.rs) | Its unit tests, [uploads.rs](src/http_tests/uploads.rs), [lifecycle.rs](src/http_tests/lifecycle.rs) |
| Change upload proof, content limits or the promotion of held uploads | [publish.rs](src/publish.rs), [acceptance.rs](src/acceptance.rs), [watcher.rs](src/watcher.rs) | [admission.rs](src/http_tests/admission.rs), [upload_validation.rs](src/http_tests/upload_validation.rs), [held_acceptance.rs](src/http_tests/held_acceptance.rs) |
| Change copying from peers, body repair, or finding mentions and quotes | [peers.rs](src/peers.rs), [index/bodies.rs](src/index/bodies.rs), [index/references.rs](src/index/references.rs) | [peers/tests.rs](src/peers/tests.rs), [attachment_sweep.rs](src/http_tests/attachment_sweep.rs), [reference_tests.rs](src/index/reference_tests.rs) |
| Change reply checks, gas estimates or receipt reads | [services.rs](src/services.rs), chain access in [verifier.rs](src/verifier.rs) | [services.rs](src/http_tests/services.rs), [budget.rs](src/http_tests/budget.rs) |
| Change the shared journal settings or durability helpers | [journal.rs](src/journal.rs) | [index/tests.rs](src/index/tests.rs), [keyring/tests.rs](src/keyring/tests.rs), [lifecycle.rs](src/http_tests/lifecycle.rs) |
| Change the shared chain identifiers or block header reads | [chain.rs](src/chain.rs) | [index/tests.rs](src/index/tests.rs), [keyring/tests.rs](src/keyring/tests.rs), [budget.rs](src/http_tests/budget.rs), [reorg_race.rs](src/http_tests/reorg_race.rs) |
| Change request budgets or how the host reaches its RPC provider | [visitor.rs](src/visitor.rs), [rpc.rs](src/rpc.rs), [token_bucket.rs](src/token_bucket.rs) | Their unit tests, [budget.rs](src/http_tests/budget.rs), [uploads.rs](src/http_tests/uploads.rs) |
| Change verified names, avatars or recipient lookups | [names.rs](src/names.rs), [names/forward.rs](src/names/forward.rs) | [names/tests.rs](src/names/tests.rs) |
| Change link previews | [link_preview/http.rs](src/link_preview/http.rs) for who may ask, [mod.rs](src/link_preview/mod.rs) for fetching and caching, [metadata.rs](src/link_preview/metadata.rs) for reading pages | [link_preview/tests.rs](src/link_preview/tests.rs), unit tests in `http.rs` and `metadata.rs` |
| Change how previews are fetched: address checks and per-site limits | [outbound.rs](src/link_preview/outbound.rs) | [outbound/tests.rs](src/link_preview/outbound/tests.rs), [outbound/http_tests.rs](src/link_preview/outbound/http_tests.rs) |
| Change how this site's links appear in other apps | [share.rs](src/share.rs), [share_cache.rs](src/share_cache.rs) | Their unit tests, [web.rs](src/http_tests/web.rs) |
| Change the host's lists or bot declarations | [host_policy/http.rs](src/host_policy/http.rs) for request and signature checks, [host_policy.rs](src/host_policy.rs) for saving and snapshots, [bot_declarations.rs](src/bot_declarations.rs) for the declaration rules | [host_policy.rs](src/http_tests/host_policy.rs), the declaration unit tests |
| Change account-key holdings, holders or trade history | [keyring_http.rs](src/keyring_http.rs), [keyring/mod.rs](src/keyring/mod.rs) and its model, follower and journal | [keyring/tests.rs](src/keyring/tests.rs) |

The host runs whatever feed filter a client sends. Which filter each of the app's streams sends is
up to the app.

## Who owns what

- **Accepted chain facts.** `index/follow.rs` reads ranges of blocks, `index/journal.rs` writes them
  to disk in order, `index/model.rs` applies them to the in-memory index and `index/query.rs` reads
  from it. The HTTP layer stays in `indexed.rs`. For readiness and recovery, start with the index's
  [state transitions](INDEXING.md#51-state-transitions). [chain.rs](src/chain.rs) holds the
  identifiers and the block header reader shared by both indexes and the transaction services.
- **Content bytes.** [store.rs](src/store.rs) owns stored bodies and attachments, and
  [holding.rs](src/holding.rs) owns uploads that can't be served yet. `publish.rs` checks records
  from both uploads and peer copies, and `publish/http.rs` handles the upload requests themselves.
  `acceptance.rs` asks the index whether a message is accepted, or on a store-only host reads the
  chain directly, and `watcher.rs` promotes held uploads once their messages are accepted. Stored
  content and the index's journal follow different
  [durability rules](STORAGE.md#body-store-durability).
- **Account keys.** `keyring/` owns the market's model and journal, and `keyring_http.rs` handles
  its HTTP requests. It is separate from the Board's index; see its
  [storage rules](STORAGE.md#optional-keyring-journal). Both indexes use
  [journal.rs](src/journal.rs) for journal settings and for appending and syncing, but each keeps
  its own writer and recovery rules.
- **Host policy.** `host_policy.rs` owns the snapshots of the lists and of the bot declarations,
  their separate writers, and replacing the file safely. `host_policy/http.rs` checks requests and
  signatures and builds the shared answer. `bot_declarations.rs` owns the order of statements and
  the limits per account. Writers check the revision or the statement's time again before saving,
  and a write that was accepted finishes even if the caller disconnects.
- **Temporary work.** [flights.rs](src/flights.rs) lets callers share a task that keeps running on
  its own, and [expiring.rs](src/expiring.rs) bounds cached answers. Their callers own timeouts and
  retries. Receipt checks in `services.rs` and share-card builds in `share_cache.rs` behave
  differently when a reader leaves, so don't swap one mechanism for another without checking who
  owns the work after that.

[STORE_API.md](STORE_API.md) and [GATEWAY_API.md](GATEWAY_API.md) define what clients see,
[STORAGE.md](STORAGE.md) lists the host's state and what survives a restart, and
[INDEXING.md](INDEXING.md) describes the index's design. A change to outside fetching or to a
resource visitors share deserves a review that actively tries to break it, before it merges.

## Tests and checks

Most tests sit beside the code they test. [http_tests/mod.rs](src/http_tests/mod.rs) provides
`Setup` and `with_server`, which run the production state and routes on a temporary store and a real
loopback listener, with [mock_node.rs](src/mock_node.rs) answering for the chain. Add an HTTP test
next to the closest existing one. The lifecycle tests drive background work and restarts explicitly.

While editing, run one test at a time, for example:

```sh
cargo test -p blohard-host index::tests::feed_cursor_survives_new_posts_and_binds_filter_and_generation
```

Before sending a change, run what CI runs:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo test --workspace -- --ignored   # slow: waits for real timeouts and retries
```
