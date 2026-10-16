# blohard host API: content store

A host plays one or both of two roles. The **content store** keeps message bodies and their
attachments, checked against the chain, and serves them by hash. The **gateway** indexes accepted
chain history and serves application queries, transaction previews and receipts, and verified names.
A gateway can also serve static web files with `--web-dir`; a store-only host uses the built-in
page. The host exposes no Ethereum RPC proxy. Wallets use the separately configured public RPC
endpoint.

This document covers what every host shares, which is the request rules, the status document and the
upstream budget, and then the content store: the content format, publishing, fetching, and copying
content from peers. It is all that is needed to run or use a store-only host, for example to help
keep posts available. The gateway's additions are in [GATEWAY_API.md](GATEWAY_API.md).

The API is pre-beta: there is no compatibility layer for earlier request or content formats. This
document and [GATEWAY_API.md](GATEWAY_API.md) specify the host HTTP interface and the minimum
byte-validation and rendering rules a client needs. The [protocol spec][spec] is the authority for
contract calls and signatures, and for the record format and references (§6 and §7). The separate
relayer sends sponsored transactions; its routes are outside this host API.

[Requests](#request-and-trust-rules) · [Upstream budget](#shared-upstream-budget) ·
[Visitor admission](#visitor-admission) · [Status](#status) · [Content](#bodies-and-attachments) ·
[Publishing](#anchoring-and-publication) · [Fetching](#get-apicontenthash)

## Request and trust rules

- Send JSON request bodies with `Content-Type: application/json`, at most 64 KiB. Each request,
  including reading its body, must finish within 60 seconds; a timeout answers 408. A malformed or
  oversized JSON body may be refused by the HTTP framework before the host's own code runs, so that
  answer may not be JSON. A malformed path segment or query string gets the JSON error described
  below.
- Large numbers are **decimal strings**: indexed message and parent ids, block numbers, sequence
  numbers, reply counts, receipt block numbers and estimated gas. Keep them as 64-bit integers or
  BigInts, never as JavaScript Numbers. Chain ids are JSON numbers in the status document and
  decimal text in URL paths, but a reference's `target.chain` is a decimal string. Timestamps (Unix
  seconds), log indexes, page limits and the status document's settings and counts, including
  `start_block`, are JSON numbers. So is the `message_id` in an upload's answer, which can also be
  null. To identify a message exactly, for example during recovery, use the string id from a receipt
  or an indexed record.
- Addresses are 20 bytes (40 hex digits), and hashes and commitments are 32 bytes (64 hex digits),
  written with a `0x` prefix. Answers use lowercase; requests may use either case. A content path
  also accepts a hash without the prefix. Numbers in paths and query strings are unsigned decimal
  text.
- Where a field is documented as nullable, a request may leave it out or send null. To use the
  default of an array or boolean field, leave it out rather than sending null. Indexed JSON requests
  refuse unknown fields. Page cursors are opaque: send back exactly what the host returned, never
  one you built or edited.
- The host's own errors are JSON objects with a `code`, a stable snake_case reason for scripts, and
  an `error` string. On indexed and other application routes the `error` repeats the code; on
  content routes it explains the problem. A refused simulation may also carry the `revert` data as
  hex. Errors from the HTTP framework or a proxy, such as malformed input, oversized bodies and
  timeouts, may be plain text or empty, so check the status code before decoding and expect errors
  that are not JSON. A failed read never tells you whether a transaction happened. The relayer has
  its own rules for reporting outcomes.
- API answers default to `Cache-Control: no-store`. Successful content downloads may be kept by any
  cache for five minutes and must then be revalidated, since the host may take content down; the
  hash is their ETag. App pages, the widget, its loader and the demo are sent `no-cache`; hashed
  assets are immutable for a year, and a missing asset is a plain 404 with `no-store`. A 304 answer
  keeps the same policy.
- The status document and content downloads allow cross-origin reads from any origin (`*`). The
  application routes also allow GET, POST and OPTIONS from any origin, with `Content-Type` allowed
  in preflight requests. Publishing does not advertise cross-origin permission. Reads require no
  authentication. The anchoring rules and size limits protect storage, and admission limits bound
  upstream work. Operators may add per-IP limits at their public proxy.

A content hash verifies bytes, not chain history or query completeness. The host is trusted to
report accepted chain state and index it honestly, like an RPC provider. A client recomputes
immutable message commitments and content hashes, but those checks cannot prove that a host omitted
no posts. Comparing hosts and spot-checking a node are diagnostics, not a completeness proof.

### Shared upstream budget

The operator can configure:

```toml
[upstream]
calls_per_minute = 1200
reserved_for_ingestion = 300
```

These are also the defaults when the table is left out. Ingestion here means the gateway's chain
follower, which reads new blocks into its index. One token bucket covers all of this host's private
RPC calls, on every chain and for name lookups; it is not a separate allowance per endpoint or
client. It refills continuously, holds at most one minute's worth of tokens, and starts full. Calls
made for clients cannot spend the last `reserved_for_ingestion` tokens and are answered busy at
once. Ingestion may spend them, and when even those are gone it waits up to ten seconds for a new
token rather than being refused. So under a flood of client traffic the bucket hovers at the
reserve, and every new token goes to the follower. The reserve is a floor on the token balance,
**not** a fixed allocation of 300 calls a minute. When metering is on, it must be smaller than
`calls_per_minute`.

Name lookups, for display names and transfer recipients, stop earlier: halfway between the reserve
and a full bucket, in whole tokens (1,650 with a budget of 3,000 and a reserve of 300). Like other
client calls they are answered busy at once. So a flood of lookups leaves the headroom below that
line to other client work, such as checking uploads, receipts and replies, and to ingestion. It is
headroom names cannot spend, not an allocation of its own: whatever client work or ingestion asks
first spends it. A lookup that needs several calls can be refused partway when the balance sits at
the line, and is answered 503 `name_unavailable` like any lookup refused by the budget. Lookups
never pause an upstream for this.

The 32 upstream concurrency slots are split into 28 for client work and four for ingestion, each
with a two-second wait for a slot, so slow client calls cannot take the follower's slots. Name
lookups use client slots, but at most 24 lookups run at once, each making one call at a time, so
they always leave four. Checking the chains at startup uses ingestion's slots and token reserve;
checking the chain for a client's receipt request uses the client allowance. Running out of either
answers 503 `host_busy` on the receipt route, including while its index is starting or stopped.

`calls_per_minute = 0` turns this rate budget off, but not the concurrency limits or the bounds on
answers. Answers from a cache or the index spend no tokens. One request can need several RPC calls,
and the calls counted here are not a provider's weighted billing units, so this setting does not cap
a monthly bill. The separate relayer and other host processes have their own traffic. Clients should
handle 503 and back off, rather than treat the configured rate as their own allowance.

The transport also pauses a configured endpoint when it answers HTTP 429, HTTP 503 with a valid
`Retry-After`, JSON-RPC code 429, or code -32005 with an explicit request-rate message (such as
"rate limit exceeded", "too many requests" or "project ID request rate exceeded"). The code -32005 alone is not enough: providers also
use it for oversized log queries. `Retry-After` accepts seconds or an HTTP date, bounded to one
second through five minutes; without a valid value, the pause is two seconds. Calls during the
pause fail as retryable upstream errors before spending tokens or taking slots. A call already
waiting for a slot checks again before sending, and calls already sent may finish. The pause is
shared across classes using that exact configured endpoint URL; other endpoints and cached or
indexed reads keep working. It does not coordinate with other processes or replace their budgets.
Live chain reads, including upload verification, answer 503 `chain_read_unavailable` during this
pause, with a numeric `Retry-After` rounded up to the next second. Gateway routes that already allow cross-origin reads
expose this header to those clients. Name lookups also answer 503 without caching a partial or missing name;
they add no separate 30-second name-provider pause.

Both chain followers retain their checkpoint and retry a throttled read without shrinking its log
range or trying receipts. Query-size limits and ambiguous refusals still narrow the range and use
the single-block receipts fallback. A provider that never identifies its throttle may therefore
still be handled as a query refusal.

### Visitor admission

Each visitor has a budget for work that reads the chain live: reply admission checks, gas estimates
and transaction lookups share one. Uploads to `/api/publish` and name lookups that miss the cache
each have their own budget, slots and short queue. Message bodies have upload slots of their own
within the upload budget, so a visitor's slow attachments cannot hold up their posts. Indexed reads and cached names cost nothing from these budgets, nor does a content request for a record this host has. A content request naming a message for a record this host lacks spends from the reader's live-read budget first, since checking that message asks the chain before any peer is tried; past the budget it answers 429 without asking.

A refused request gets HTTP 429 `visitor_read_limit` with `Retry-After: 2`. The reference client
retries an upload refused this way with the same bytes and proof after that wait, at most three
times, before leaving it to background recovery. The host-wide upstream budget still applies and may
answer `host_busy`.

## Status

`GET /api/status` reports `verifier` (`eth-rpc`, or allow-all in development), `notice`, `chains`,
`store` and `gateway`. A role the host does not play is null. The status answers 200 while an index
is starting or backfilling, and 503 if an index is stopped, still with its usual body. `chains` are
listed in the order the operator configured them, and the reference client opens the first one by
default.

A store-only host has no chain follower, but it still verifies each chain's RPC endpoint at startup.
How a gateway's chains start, stop and report their index is described in
[GATEWAY_API.md](GATEWAY_API.md#status-index-and-app-fields), with a complete example.

Each chain reports:

| Field | Meaning |
|---|---|
| `id`, `name`, `contract` | Chain id, optional label and canonical Board address. |
| `public_rpc_url` | Optional wallet endpoint, absolute or relative to this host; never the private verifying URL. |
| `native_currency` | Native gas token metadata for wallets: `name`, `symbol`, and integer `decimals`. Defaults to Ether/ETH/18 when not configured. Older hosts omit it. |
| `start_block`, `confirmations` | The Board's deployment block, and how many confirmations a block needs before it is accepted. |

The other chain fields describe the chain's index, which is null without a gateway, and the
moderation settings apps use (`curated`, `blocklist`, `blocking`, `closed_gate`, `gates_pending` and
`key_gates`). They are described in [GATEWAY_API.md](GATEWAY_API.md#status-index-and-app-fields).

The top-level `upstream` field, null in development mode, counts this host's calls to its RPC
upstreams since it started, by class:
`{client: {calls, busy}, names: {calls, busy}, ingestion: {calls, busy}}`, where `names` counts name
lookups apart from other client work. `calls` were sent; `busy` were refused by the shared budget or
the concurrency limit, or suppressed by an endpoint pause before any call was made. A rising `client.busy` with a flat `ingestion.busy`
is the budget doing its job under client load; a rising `ingestion.busy` means the follower itself
is being held back.

`store` reports `max_content_size`, the largest body this host stores, header included (configured
per host, 4,000 bytes on the reference deployment and never above 1 MiB); `envelope`
(`{version:1}`); `attachments` (`{max_bytes,max_per_message,media_types}`, or null); `peers`; and
`holding` (`entries`, `bytes`, `max_bytes`, `max_entries`, `max_per_signer`, `max_bytes_per_signer`
and `ttl_secs`). A null list of attachment media types allows any type. The `gateway` object is
described in [GATEWAY_API.md](GATEWAY_API.md#status-index-and-app-fields). In development the
verifier is `allow-all (dev)`; in production it is `eth-rpc`.

## Bodies and attachments

A host holds two kinds of record ([spec §7][spec-7]), in two separate stores:

- A **body** is the record a message's on-chain `contentHash` names. The host checks that hash
  against the chain before storing it.
- An **attachment** is a record that a stored body references as `record:0x<hash>`. The host stores
  it only if it already holds the body of the message named in the request, and that body's first
  `max_per_message` references include the upload's hash. The chain vouches for the body, and the
  body for the attachment. Its size and media type must fit the published attachment policy; a null
  `media_types` allows any type.

Attachments are each host's choice. A host without a `[store.attachments]` section in its
configuration stores bodies only, and answers attachment uploads with `403`. `GET /api/status`
publishes whether they are on and their limits. This host's defaults are the spec's baseline
([§7][spec-7]): a 4,000-byte body, 4 attachments per message, 2 MiB each.

### Envelope bytes and safe interpretation

Every download and upload is the **whole envelope**: the header line and the payload together, not
just the payload. This UTF-8 sample ends with a newline after `Hello.`:

```text
version/1 text/plain
Hello.
```

Its exact bytes, written with escapes, are `version/1 text/plain\nHello.\n` (28 bytes), and its
keccak-256 is `0x1163378f19c56cdea3e4b7a722dd73e840f29e2a8350199877e1e48807e6c4a6`. This is the
`content_hash` the sample message records use in [GATEWAY_API.md](GATEWAY_API.md#indexed-reads).
Don't use SHA3-256: it is a different hash.

To interpret a record:

1. Limit the download size, then hash the raw bytes, including the header and every newline. Compare
   the result with the expected `content_hash` or `record:` hash before interpreting anything. Don't
   normalize the text before hashing.
2. Find the first LF byte (`0x0a`); the header before it is at most 128 bytes. A CR just before that
   LF is ignored **only when parsing the header**, never when hashing it.
3. Parse exactly `version/1`, one ASCII space, and a lowercase `type/subtype` without MIME
   parameters. Each part starts with a letter or digit, is at most 64 characters long, and uses
   letters, digits, `.`, `-` or `+`. An unsupported version or a malformed header makes the record
   unrecognized content, never raw HTML or an image.
4. Everything after the LF is the payload, and text is UTF-8. The HTTP `Content-Type` is only a
   convenience; the verified envelope is what counts. In particular, remove the envelope before
   handing image bytes to an image decoder.

How a client renders a `text/plain` body and its references is described in
[GATEWAY_API.md](GATEWAY_API.md#rendering-content).

## Anchoring and publication

An upload names its message with `chain_id` and either a `message_id` or the `tx_hash` of the
transaction that posts it. For an accepted message id, a gateway checks the body hash and author
against its index, and a store-only host asks the chain through its verifying RPC endpoint. For a
pending transaction, the host reads its receipt, or its authenticated `post` or `postBySig` call
data; an invalid signature takes no holding slot. A message not yet indexed can be checked live and
its bytes held, but a gateway that is backfilling or stopped never quietly substitutes an RPC answer
for its accepted index.

For a pending post signed by a contract wallet, a signature the wallet rejects answers 403. If the
RPC provider fails or answers nonsense while the host reads the wallet's code or checks its
signature, the upload answers 502; explicit provider throttles and this host's own upstream limits
answer 503. Provider throttles include `Retry-After`. None of these
failures hold any bytes. Keep the upload and retry it unchanged after a pause.

A transaction or receipt counts as missing or pending only when the RPC endpoint explicitly answers
null. Explicit provider throttles answer 503 with `Retry-After`; other provider errors and malformed
transaction or receipt fields answer 502. The background task
that promotes held uploads keeps the bytes and backs off on these failures; it never takes a missing
status or a malformed event to mean that the transaction reverted or did not post.

While a chain is starting or stopped, the host refuses every new upload for it with 503, before any
live check. While its index is backfilling, an upload by message id answers 503
`the chain index is unavailable; try again shortly`. So does an upload by the hash of a transaction
already mined, since it resolves to a message id. Such an upload takes no holding slot; the
publisher keeps the bytes and retries. An upload for a pending transaction is still checked through
that transaction and may be held. Content already stored can still be downloaded.

A body enters permanent storage only once its message reaches the configured accepted depth, that
is, enough confirmations. Until then the upload is held in memory, unserved, for at most 30 minutes.
The holding area takes at most 64 MiB and 4,096 records in all, and 16 records and 12 MiB per signer
on each chain, so one or two signers cannot fill it with uploads whose transactions never get mined.
One open issue: a pending `postBySig` is charged to the author its signature names, and that
signature is public calldata once the post is mined, so a stranger's transactions that carry an
author's old signed posts, if a provider reports them as pending, count against that author until
they expire. Whether a Base provider ever reports such a transaction as pending is unverified. A
held upload is dropped if its transaction reverts, if it does not match, or when it expires.

On a store-only host, each held body costs one Board read per new accepted head. A body found not
yet accepted is not read again until the accepted head moves, and a failed read backs that body off
from six seconds up to a minute, so a host full of held bodies does not spend a call on each one
every pass. A gateway reads its index instead.

Size limits for bodies and attachments apply before bytes are held as well as before they are
stored. When a held body is promoted, the host keeps holding it while its attachments are still
held. If promotion removes the body before an attachment that is still arriving can be held, that
upload answers 503 and must be retried; it is never acknowledged into a slot with no body. Reaching
the accepted depth is not the same as being safely on disk: content writes are batched and
best-effort, while the index journal is synced before its state is published.

Send the transaction first, then upload the exact bytes by transaction hash or accepted message id.
Keep the bytes until a host answers 200. Upload the body before its attachments, since an attachment
needs a body on the host that references it. The development flag `--allow-all` skips the anchoring
checks and must never be used on a public store.

A held upload is neither durable nor served. When the host is busy or behind, retry with a growing,
bounded wait, keeping the same signed transaction and request. Before treating a pending post as
done or failed, match the transaction receipt to what you sent. A failed answer to a resent request
cannot prove that an earlier attempt failed. The relayer, a separate service, has its own recovery
rules for the transactions it sends.

### `POST /api/publish?chain_id=C&message_id=M` or `…?chain_id=C&tx_hash=0x…`

Store a message body, or an attachment referenced by one.

**Request.** The raw record bytes. `chain_id` and `message_id` are unsigned 64-bit integers, and
`tx_hash` is 32 bytes of hex. Outside development mode, `chain_id` and one of the other two are
required. The host finds the content hash the message commits to, from the message or from the
transaction's call data. If it equals the upload's hash, the upload is that message's **body**, at
most `max_content_size`. Otherwise it is treated as an **attachment** of that message, at most
`attachments.max_bytes`, and the message's body must reference it and must already be on the host,
stored or held. Upload the body first, then its attachments, with the same query. If an upload's
connection drops or times out partway through the write, the write is not lost and the store is not
disturbed: the host finishes it in a task of its own, and only the answer goes unheard.

Uploads are read before they take a slot. The declared `Content-Length` picks one of two pools. An
upload declaring at most `max_content_size`, normally a message body, is small, and its bytes must
arrive within ten seconds. Any other upload, including one that declares no length, is large and has
the whole request timeout to arrive. While its bytes arrive, an upload holds only its visitor's own
upload lanes and the memory its buffer takes, charged to its pool as the buffer grows: 32 MiB in all
for small uploads and 64 MiB for large ones. Once every byte is in, it takes one of its pool's
slots, sixteen small or 32 large, for its checks and write. So a slow sender holds no shared slot,
and attachments cannot hold up message bodies. An upload answers 503
`the host is busy; try again shortly` at once when its pool's memory has no room for its next bytes,
or when no slot is free once its bytes are in. An incomplete or refused request gives back its
memory; once verification starts, the operation keeps its slot and its memory until it finishes,
even if the HTTP client disconnects or times out. This bounds the uploads buffered in memory,
separately from the holding area's limits.

**Response `200`.** The message is accepted and the record is in the store. It is served from now
on, and safely on disk after the store's next commit, a few seconds later; a crash before that
commit may lose it.

```json
{
  "hash": "0x1163378f19c56cdea3e4b7a722dd73e840f29e2a8350199877e1e48807e6c4a6", "stored": true,
  "held": false,
  "bytes": 28, "kind": "body"
}
```

| field | meaning |
|---|---|
| `hash` | keccak-256 of the bytes received. Compute it yourself and compare: a mismatch means the bytes were altered in transit or the host is faulty. |
| `stored` | `true` if the record was new; `false` if identical bytes were already there |
| `held` | `false` |
| `bytes` | the number of bytes received |
| `kind` | `"body"` or `"attachment"`: which store the record went to |

**Response `202`.** The record is valid, but its message is not accepted yet, or its transaction not
mined yet. It is held, not served, and will be promoted automatically once the message is accepted.
The client must keep its copy and retry until a `200` confirms storage: held bytes can expire, be
dropped after a revert, or be lost on restart.

```json
{
  "hash": "0x1163378f19c56cdea3e4b7a722dd73e840f29e2a8350199877e1e48807e6c4a6", "stored": false,
  "held": true,
  "bytes": 28, "kind": "body", "message_id": 9
}
```

`message_id` is the id the host has learned so far, or `null` while the transaction is pending.

Publishing the same bytes again is harmless: they always produce the same hash, and nothing changes
after the first time. Publishing again, with the same or a different valid proof, returns `200` with
`"stored": false`, or `202` again while the record is still held. Refused uploads write nothing. A
lost answer or a request timeout can follow a completed write, so retry with the same bytes and
proof. A store I/O error leaves that store unavailable until restart, and does not mean the write
completed.

**Errors**, roughly in the order the server checks them; the size and type checks on attachments
come after the proof:

| status | `code` | `error` | cause |
|---|---|---|---|
| `404` | `no_store` | `this host stores no content` | a gateway-only host: nothing is stored here (`store` is `null` in the status) |
| `400` | `malformed_request` | the parser's text | a malformed query, such as `chain_id=abc` |
| `400` | `empty_content` | `empty content` | zero-length body |
| `400` | `invalid_envelope` | `invalid content envelope: …` | the first line is not a valid `version/1 <media-type>` header (see [spec §6][spec-6]); the message says which rule failed |
| `413` | `too_large` | `content exceeds <n> bytes` | over the largest size this host accepts for anything (the attachment cap, or 1 MiB on a bodies-only host, the ceiling any body cap stays under). A declared length more than about 1 KiB over it is refused before the body is read. |
| `413` | `body_too_large` | `a message body exceeds <max_content_size> bytes` | the record is the named post's body and is over the body cap. The same bytes may be a fine attachment of another post, so a peer fetch refused this way backs off only fetches of these bytes as a body |
| `413` | `attachment_too_large` | `an attachment exceeds <max_bytes> bytes` | the record is the named post's attachment and is over the attachment cap. An attachment cap can be under the body cap, so a peer fetch refused this way backs off only fetches of these bytes as an attachment |
| `415` | `attachment_type_not_stored` | `this host stores attachments of these types only: …` | host policy (`media_types` in `[store.attachments]`): the attachment's declared media type is not one the host stores. The same bytes may be another post's body, so a peer fetch refused this way backs off only fetches of these bytes as an attachment |
| `400` | `proof_required` | `chain_id and either message_id or tx_hash are required: upload only after the post is sent` | a proof parameter is missing (enforced mode only) |
| `400` | `chain_not_served` | `chain <C> is not served by this host` | `chain_id` is not in the host's configuration; see `/api/status` for the list |
| `400` | `malformed_request` | `tx_hash must be 32 bytes of hex` | malformed `tx_hash` |
| `404` | `transaction_not_found` | `chain <C> has no transaction 0x…: send it first, then upload` | the chain's RPC does not know the transaction yet. Also what a lagging RPC returns for a transaction sent moments ago; see the retry guidance below. |
| `403` | `not_board_transaction` / `not_a_post` / `transaction_reverted` | `transaction 0x… is not sent to the board` / `… is not a post` / `… reverted` | the transaction cannot anchor anything on this board |
| `403` | `signature_invalid` | `transaction 0x… names an author whose signature does not verify` | a pending `postBySig` whose signature the named author did not make (checked as the Board will check it: the signature recovers to the author, or the author has code and accepts it through ERC-1271). Refused before it can take a holding slot in that author's name. |
| `403` | `not_anchored` | `not anchored on-chain: chain <C> has no message <M> (eth_call failed: …)` | the core contract reverted: no such message id at the block asked, which a lagging RPC also answers for a post mined moments ago; see the retry guidance below. A provider error is answered 502 instead, or 503 with `Retry-After` for a recognized rate throttle. |
| `403` | `host_blocked` | `posting blocked by host` | the host's policy blocks the post's author ([host blocks](GATEWAY_API.md#host-blocks)). Held uploads by a blocked author are dropped too. |
| `410` | `taken_down` | `this host has taken this content down` | the host's admin has taken these bytes down, so it accepts them under no post. Checked before anything is asked of the chain. |
| `410` | `message_gone` | `this post has been deleted or removed, so this host takes none of its content` | the post's author deleted it or its parent's author removed it ([spec §5.6][spec]), or a pending post is created already deleted. This covers its body and its attachments. A held upload whose post is deleted before it is accepted is dropped, and nothing is copied from peers for such a post. Another live post with the same bytes still stores them. |
| `403` | `not_message_body` | `this upload is not the message's body (the message commits to 0x…), and this host stores no attachments` | the upload does not hash to the message's content hash, and this host does not take attachments |
| `409` | `body_not_held` | `the body of chain <C> message <M> is not on this host yet: upload it before its attachments` (or `… pending message …` with a `tx_hash` proof) | the upload looks like an attachment, but the host has no body (stored or held) to check it against; also what wrong body bytes get on a host that stores attachments |
| `403` | `attachment_not_referenced` | `chain <C> message <M> does not reference 0x… within its first <K> record references` | the message's body does not list this record among the references this host honours (only `text/plain` bodies can carry references) |
| `429` | `signer_holding_limit` | `this signer already has <n> unconfirmed records held; wait for them to be confirmed` / `… <n> unconfirmed bytes held; …` | the holding area's per-signer bounds on records and bytes; retry once the earlier posts are accepted |
| `507` | `holding_full` | `the host is holding its maximum of <n> unconfirmed bytes; retry once the message is accepted` / `… maximum of <n> unconfirmed records; …` | the holding area is full, in bytes or in records; retry with the `message_id` once the message is accepted and the record goes straight to the store |
| `503` | `chain_read_unavailable` | `the chain provider is busy; try again shortly` | a recognized provider throttle or an active endpoint cooldown; numeric `Retry-After` gives the remaining wait, rounded up to seconds. Retry the same request after that wait. |
| `502` | `chain_read_unavailable` | `could not verify anchoring; try again shortly` | the chain's RPC endpoint was unreachable or returned something unintelligible. **The host could not check**; nothing is implied about the post. Retry later. RPC details are logged server-side, not included in the response. |
| `503` | `host_busy` | `the host is busy; try again shortly` | upload memory or slots, upstream concurrency or the configured RPC budget is exhausted. An upload is refused as soon as its pool's memory has no room for its next bytes, or once its bytes are in if no slot is free. Retry the same bytes/proof after backoff. |
| `408` | `upload_too_slow` | `the upload arrived too slowly; send it again` | an upload small enough for the small pool did not arrive within ten seconds; its memory is released. Send the same bytes again. |
| `400` | `upload_incomplete` | `the upload did not arrive in full; send it again` | the connection failed while the body was arriving |
| `503` | `body_not_held` | `the body is no longer held; retry the upload` | the provisional body left holding while attachment admission was in flight. Retry the same bytes/proof after backoff; no attachment slot was taken. |
| `503` | `index_unavailable` | `the chain index is unavailable; try again shortly` | the required index is starting, backfilling or stopped; no holding slot is taken. Retain the bytes and retry once the index is ready. |
| `500` | `store_failed` | `the content store failed; try again shortly` | the write failed and the store is unavailable until restart; the detail is logged server-side |

**Retry guidance.** Always retry with the same bytes and proof. Just after a post is sent, the
host's chain reads may not show it yet, so a `404` for the transaction, or a `403` saying the chain
has no such message, often clears within seconds; try a few more times before giving up. A `429` or
`503` asks you to come back later: wait the `Retry-After` seconds when the answer gives them, and
otherwise back off. The reference web client tries each of these up to three more times, 1, 2 and 3
seconds apart, then keeps the bytes for a later pass. A `Retry-After` from the host replaces those
delays, held between 2 and 30 seconds, and a `429` without one waits 2 seconds.

### `GET /api/content/{hash}`

Fetch a record, body or attachment, by hash.

**Optional query:** `chain_id` and `message_id`, naming the message that anchors the record, as its
body or as a record its body references. A host that lacks the record and has `peers` (below, and
`store.peers` in the status) then asks them for it, one after another, with a bare
`GET /api/content/{hash}` of its own. Before asking, it checks that the chain vouches for the hash:
the message's content hash equals it, or the message's body is on this host and references it. What
a peer returns is hashed, checked and admitted exactly like an upload to `/api/publish`, with the
same policies and holding, and then served. Without the query, a missing record is simply missing.
Peers are never asked with the query, so two hosts that list each other cannot pass a miss back and
forth. Clients that know the message, as every client reading a feed does, should name it.

Peer fetches in progress for the same hash and size limit share one task, including background
copies. At most eight distinct fetches run at once; a new one beyond that answers 503 instead of
waiting, though a duplicate can still join a fetch already running. Each peer gets five seconds, and
the whole fetch thirty; a peer's redirect is not followed. The position in the peer list is
remembered per hash and size limit for an hour, up to 10,000 entries, so a lookup that timed out
resumes with the peers not yet tried. A refusal of the bytes as one post's body or attachment, by
this host's body cap, attachment cap or attachment types, backs off asks for them in that role only;
a peer that lacks them backs off both. This schedule lives only in memory. A reader disconnecting
does not cancel the shared work; its slot is released when the fetch completes or times out.

A failed fetch backs off from thirty seconds to thirty minutes, doubling from the end of each
attempt. This schedule shares the bounded map of positions and applies to readers and background
sweeps alike, including after a reader disconnects. While it backs off, a missing record stays a 404
without asking peers again. Records stored locally need no peer slot and are always read first.

A peer copy that needs a gateway's index answers 503 while that chain's index is starting,
backfilling or stopped, before any peer is asked. This affects only misses that name a message on a
host with peers; bytes already stored are still served, and a miss that asks no peer stays a 404.

The reference index also asks peers for missing message bodies in the background, on the same retry
schedule as readers. Every sweep still checks for bodies stored locally, so a late upload can be
indexed at once. A successful peer fetch resets its backoff, and a restart or an evicted retry entry
starts it over.

On a host that both indexes and stores, with peers configured, this makes the host a copy of its
peers. The attachments of each body it analyses (the body's first `max_per_message` `record:`
references) are noted as missing. Once per process run, the host also walks every indexed message in
its own store, noting a body it has lost since analysing it and the attachments of each body it has.
Missing records are copied four per pass, taking turns and skipping those still backing off, so one
that no peer has holds up no other. All chains' sweeps share two of the eight peer fetch slots,
leaving the rest for readers; a copy waiting on a stalled peer can still hold up a pass for up to
thirty seconds. A record this host refuses for what it is, such as its type or size, or a body that
does not reference it, is dropped and not downloaded again for thirty minutes; a passing refusal,
such as a full holding area, is retried after thirty seconds. A post that was deleted or removed, or
whose author this host blocks, vouches for nothing, and nothing is fetched for it. When the post a
missing record was noted for can no longer vouch, the record is let go and the walk runs again, so
any other post still needing it notes it anew. Records this host has taken down are never fetched.
Missing records are kept in memory only, at most 100,000 per chain, and the walk after a restart
finds whatever is still missing. Junk references in enough posts could fill that limit, after which
new attachments wait for a restart. Hosts that index, store and list each other as peers therefore
back each other up.

**Response `200`.** The exact stored bytes, body or attachment, with these headers:

```
Content-Type: <the media type the record's envelope declares>
Cache-Control: public, max-age=300, must-revalidate
ETag: "0x<the record's hash>"
X-Content-Type-Options: nosniff
Content-Security-Policy: default-src 'none'; sandbox
```

The content type comes from the record's own header: `text/…` types get `; charset=utf-8`, and a
record without a header that parses is served as `application/octet-stream`. Records are untrusted
bytes uploaded by anyone with a post, so the last two headers stop a browser from guessing another
type or running anything in them; an uploaded HTML or SVG file stays inert if opened directly.
Because the URL contains the hash of the bytes, the bytes for a URL never change, so the hash is the
ETag. The host may still stop serving them, when its admin takes them down, so a cache may keep a
download for five minutes and must then ask again. A request whose `If-None-Match` names the ETag
gets `304 Not Modified` only while the host still serves the record; a takedown is checked first.

**Errors**

| status | `code` | `error` | cause |
|---|---|---|---|
| `400` | `malformed_request` | `malformed hash` | not 64 hex characters (optionally `0x`-prefixed) |
| `400` | `malformed_request` | the parser's text | a malformed query, such as `chain_id=abc` |
| `404` | `no_store` | `this host stores no content` | a gateway-only host (`store` is `null` in the status) |
| `404` | `content_not_found` | `unknown content hash` | nothing stored under that hash and nothing pulled: never published to this host, not vouched for by the message named, no peer had it, or pulled but only held because the message is not accepted yet |
| `410` | `taken_down` | `this host has taken this content down` | the host's admin has taken these exact bytes down ([takedowns](GATEWAY_API.md#takedowns)), under every post. Clients show it as removed by this host and do not retry. Another host may still serve it. |
| `503` | `chain_read_unavailable` | `the chain provider is busy; try again shortly` | a recognized provider throttle or an active endpoint cooldown; numeric `Retry-After` gives the remaining wait, rounded up to seconds. Retry the same request after that wait. |
| `502` | `chain_read_unavailable` | `could not verify anchoring; try again shortly` | the chain could not be asked whether the message named vouches for the hash; RPC details are logged server-side |
| `503` | `host_busy` | `the host is busy; try again shortly` | upstream concurrency/rate admission or the distinct-peer-fetch pool is exhausted; retry after a short pause |
| `503` | `index_unavailable` | `the chain index is unavailable; try again shortly` | peer-pull anchoring requires an index that is starting, backfilling or stopped; retry once ready |
| `500` | `store_failed` | `the content store failed; try again shortly` | the store could not be read; the detail is logged server-side |

A `404` says nothing about whether the message exists on chain, only that this host does not have
the bytes. The protocol lets any host serve a record ([spec §7][spec-7]), so try another host.

### `GET /` and everything else

API answers are `no-store` unless a route says otherwise: a content `404` means "not here yet",
while a successful content download may be cached for five minutes, then revalidated. Unknown
`/api/*` paths answer `404 {"error":"no such API endpoint","code":"not_found"}`. Without `--web-dir`
the host serves a small built-in page; with it, a gateway serves the web app
([GATEWAY_API.md](GATEWAY_API.md#serving-the-app)).

#### `GET /` (built-in page)

A small HTML page for publishing and reading by hand. It computes keccak-256 in the browser and
refuses to show fetched content that does not hash to the requested value, the same rule every
client should follow. It is not part of the API contract.

[spec-6]: https://github.com/blohard/protocol/blob/main/SPEC.md#6-records
[spec-7]: https://github.com/blohard/protocol/blob/main/SPEC.md#7-references-and-attachments
[spec]: https://github.com/blohard/protocol/blob/main/SPEC.md
