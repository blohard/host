# blohard host API: gateway

The gateway is the host role that indexes accepted chain history and serves application queries,
transaction previews and receipts, verified names, link previews, the account keyring and host
policy. With `--web-dir` it also serves the web app and its share pages.

This document builds on [STORE_API.md](STORE_API.md), which covers what every host shares: the
request and trust rules, the status document and the upstream budget. That document also defines the
content format and how content is published and fetched. A gateway without a store needs another
host for content.

[Walkthrough](#client-walkthrough) · [Snapshots](#accepted-snapshots-and-cursors) ·
[Status](#status-index-and-app-fields) · [Indexed reads](#indexed-reads) · [Changes](#changes) ·
[Transaction services](#transaction-services) · [Names](#names) · [Rendering](#rendering-content) ·
[The app](#serving-the-app) · [Keyring](#account-keyring) · [Link previews](#link-previews) ·
[Host policy](#host-policy)

## Client walkthrough

Paths below are relative to the chosen host's base URL. Example addresses, transactions, generations
and checkpoints are made up. The content hash and message commitment in the examples are real,
computed from the sample record in [Bodies and attachments](STORE_API.md#bodies-and-attachments).

1. **Discover.** Read `GET /api/status`. Choose a chain by its `id` and `contract`, and wait until
   its `index.state` is `ready` before making indexed reads. A gateway without a store needs another
   host for content. Keep the reader's own view state apart from what came from the host: cursors
   belong to this host, chain and generation.
2. **Load.** Send `POST /api/chains/{chain_id}/feed` with
   `{"filter":{"any":[{}]},"cursor":null,"limit":20}`. Validate each record, and remember the first
   page's `at` as the position the next refresh starts from.
3. **Read content.** Fetch `/api/content/{content_hash}?chain_id=C&message_id=M` from a store. Limit
   the download size, hash the complete bytes and compare the result with `content_hash`; only then
   parse the record and render it, if its type is supported. HTTP 404 means this host does not have
   the bytes yet, not that the message never existed. For a thread, read the selected message and
   its immediate parent with `message/{id}?ancestors=1`, then its children with `replies`.
4. **Page backwards.** Repeat the feed request with the same filter, sending the returned `next`
   unchanged as `cursor`. Keep going through empty pages while `next` is not null. A page's limit
   counts messages, not the conversation rows your client groups them into. Replies page from oldest
   to newest instead.
5. **Refresh.** Request `changes` with the saved generation and sequence as `after`, the same feed
   filter, and the ids of any messages and branches you keep on screen in its scope. Keep the first
   answer's whole `until` and send it with every later page. Move `after` to a page's `through` only
   after you have applied that page. Stop at `more:false`, and save `until` as the position the
   refresh reached. [Changes](#changes) explains the order of snapshots and gate updates. A
   background poll may count what a refresh would add, but it must not move the displayed view's
   cursor unless it keeps those changes to apply later.
6. **Recover.** On 409 `reset_required`, drop this view's host cursors and cached index data and
   reload; keep drafts, follows and mutes. Retry temporary failures with a growing, bounded wait.
   When a page fails, keep its cursor as it was. A later page's newer `at` must not let you skip
   refreshing earlier pages.

To publish, save the accepted checkpoint for recovery, check that the reply is allowed (`admission`
below), then sign and send the transaction through a wallet or relayer. The host never broadcasts
transactions. Upload the exact bytes you signed for, with the transaction hash, check the event in
the returned receipt, and keep the bytes until the store answers 200. The
[publication rules](STORE_API.md#anchoring-and-publication) cover uncertain outcomes.

## Gateway request rules

The [request and trust rules](STORE_API.md#request-and-trust-rules) apply to every gateway route. In
addition:

- Application queries are answered from memory and never start a scan of chain history over RPC.
  Gate previews, receipts and name lookups are bounded live reads. The host's concurrency limits and
  configured upstream budget can answer 503 before any RPC call is sent; those budgets are operator
  settings, not a request rate a client can count on.

## Accepted snapshots and cursors

Every indexed answer carries an `at`, the position in this host's index that the answer reflects:

```json
{
  "generation": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
  "sequence": "123",
  "checkpoint": {
    "block": "456", "hash": "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
  }
}
```

`checkpoint` is the last accepted block and its hash, and is null before any block is accepted.
`sequence` counts the index's changes: it advances for chain events and for facts the host derives,
such as references found when a body arrives late. It belongs to this chain's journal and
generation; it is not a block number and cannot be compared between hosts. Each answer is put
together under one read lock on the in-memory index, and its facts were written to the journal and
synced to disk before they became visible.

Only blocks at the configured accepted depth (`head - confirmations`) enter the index. The
operator's per-chain `poll_interval_secs` (default 1) sets the wait between polls once ingestion has
caught up, so a post can appear that long after it is accepted, plus processing time. A longer
interval still reads every block in between. Catching up (backfill) runs without pauses, and RPC
failures back off separately. This setting is not published and does not change how clients should
poll. There is no unconfirmed tail and no separate wait for finality.

If the host detects a contradiction in accepted history, it stops serving that index. During the
beta the operator must rebuild it, since there is no online rewind. At startup the host replays the
journal and checks its checkpoint against the node. Replacing the journal gives it a new generation,
which makes every saved cursor invalid.

A page cursor is bound to its generation, its query and the upper bound captured on the first page.
Send it back unchanged. New posts never move the boundary of an older page. A list page examines at
most 500 candidates and returns at most 50 items (20 by default), so an empty page whose `next` is
not null is **not** the end. A failed page advances nothing. A generation mismatch or an expired
change cursor answers 409 `reset_required`: clear that view and reload it. A missing chain or
message answers 404. An index that is starting, backfilling or stopped answers 503
`index_unavailable`.

Every deliberate refusal on these routes has a snake_case `code`, repeated as `error`. Besides the
two above, these answer 400: `page_limit_out_of_range`, `ancestor_limit_out_of_range`,
`too_many_feed_clauses`, `too_many_authors`, `too_many_lookup_subjects`,
`posts_have_no_parent_author`, `registry_not_indexed`, `invalid_change_boundary`,
`too_many_change_subjects` and `commitment_mismatch`. Any other failure inside a query is logged by
the host and answered 500 `index_query_failed`, without its details.

## Status: index and app fields

The [status document](STORE_API.md#status) also reports each chain's index, the moderation settings
apps use, and the gateway's own facts.

Each indexed chain starts on its own. Its follower, the task that reads new blocks into the index,
retries an RPC endpoint that does not answer, and checks the endpoint's chain id before the index
takes any update. A wrong chain id stops that chain; fix the configuration and restart.

If an index cannot open or replay its journal at startup, its chain is reported as stopped and the
host's other services still start. Repair the journal or configuration and restart; the host never
overwrites the journal or quietly rebuilds it. A proven mismatch with an accepted checkpoint stops
every index, because links between chains may depend on that history.

The follower also stops a chain, naming the block in its log, in these cases:

- A block holds more Board and registry events than the index takes (131,072), or its receipts
  exceed the 128 MiB limit on a receipts answer. The follower halves a range before falling back to
  a single block's receipts, and never indexes part of a block.
- An endpoint without `eth_getBlockReceipts` refuses a block as logs on the sixteenth consecutive
  poll. The block is retried for several minutes first, since an ambiguous refusal may be temporary; explicit throttles pause the endpoint without trying receipts.
- The provider keeps returning a range the index cannot accept as valid history: a log whose block
  or hash does not match its header, an event that does not decode, a post out of sequence, or a
  registry event that disagrees with history. Such a range is retried at first, since a
  reorganization or a lagging replica can look the same for a while, and the chain stops on the
  sixteenth consecutive rejection at the same block, with the block and the reason in the log.
  Transport failures neither count towards the sixteen nor reset the count; progress does.

For the first two, raise the limit or change the endpoint, then restart.

The header of a range's last block is read before its logs and compared again once the range is
assembled. So a block replaced while its logs were in flight is never committed under the
replacement's hash, even when the old block held no Board events; the range is read again on the
next poll. Temporary failures never stop a chain, and stored content stays downloadable in every
case.

While an index is stopped, the status answers 503 with its usual body. A client should accept that
body so the healthy chains stay usable. A proxy error or a malformed answer is still a failure.

Besides the identity fields in [STORE_API.md](STORE_API.md#status), each chain reports:

| Field | Meaning |
|---|---|
| `index` | Null without a gateway; otherwise `{at, state, accepted_head, messages, pending_bodies, pending_quotes}`. State is `starting` (the chain has not yet answered the follower's identity check), `backfilling`, `ready` or `stopped`. `accepted_head` is the most recently sampled head minus confirmations, as a decimal string or null before a successful poll. Compare it with `at.checkpoint.block` for ingestion lag; a ready index can still lag after becoming ready. `pending_quotes` counts unresolved quotes whose target chain this host serves; a quote of another chain waits unseen and resolves if that chain is configured later. |
| `curated` | Whether the host offers its curated feed and badges on this chain. Requires local policy and defaults to enabled; `chains.curated = false` disables it for a chain. Shared lists and blocking remain in the top-level `host_policy`. |
| `blocklist` | Optional `{registry, list}` naming each author’s display blocklist. Published directly from configuration, independently of gate validation. Use it for reply folding and notification filters. It does not identify a gate contract. |
| `blocking` | Optional verified `{gate, registry, list}` for the shared author-blocklist gate; `list` is its name. |
| `closed_gate` | Optional verified `ClosedReplyGate` address. Denies ordinary direct replies; the Board admits protected responses without calling it. Does not retract posts or add a historical display fold. No Board or market binding is needed for this stateless gate. |
| `gates_pending` | True while a configured gate (`blocking`, `key_gates` or `closed_gate`) has not validated yet, so it is omitted for now. The host validates again each minute; a client asks again rather than keep the gap. |
| `key_gates` | Optional `{holders, holders_blocklist}` with verified shared key-gate addresses for this Board; either address may be null. The combined gate uses `blocking`'s registry and list and is published only after both it and the shared blocklist gate pass validation. Clients re-apply recognized account-level key gates when displaying earlier replies. Per-message gates affect new replies only. |

`gateway` reports three booleans: `names`; `link_previews`, which needs a local store; and
`link_previews_ready`, which is true while the host's list of its own public addresses is current
(link previews never fetch from them). Readiness can be false during startup or a discovery outage
without turning the feature off or changing the status code, and cached previews stay available.

Each chain publishes `native_currency` (`name`, `symbol`, `decimals`) for wallet network
registration. Operators configure it under `[[chains]]`; omitting it defaults to Ether/ETH/18.
Clients talking to older hosts that omit this field may use the same default. This metadata
describes the native gas token, not AccountKeys pricing.

Example response (a host serving both roles):

```json
{
  "verifier": "eth-rpc", "notice": null,
  "chains": [
    {
      "id": 31337, "name": "example", "contract": "0x3333333333333333333333333333333333333333",
      "public_rpc_url": null, "start_block": 10, "confirmations": 2,
      "native_currency": {"name": "Ether", "symbol": "ETH", "decimals": 18},
      "index": {
        "at": {
          "generation": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
          "sequence": "123",
          "checkpoint": {
            "block": "456",
            "hash": "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
          }
        },
        "state": "ready", "messages": 9, "pending_bodies": 0, "pending_quotes": 0
      },
      "curated": false, "blocklist": null, "blocking": null, "key_gates": null, "closed_gate": null,
      "gates_pending": false
    }
  ],
  "store": {
    "max_content_size": 4000,
    "envelope": {"version": 1},
    "attachments": {"max_bytes": 2097152, "max_per_message": 4, "media_types": null},
    "peers": [],
    "holding": {
      "entries": 0, "bytes": 0, "max_bytes": 67108864, "max_entries": 4096, "max_per_signer": 16,
      "max_bytes_per_signer": 12582912,
      "ttl_secs": 1800
    }
  },
  "gateway": {"names": true, "link_previews": false, "link_previews_ready": false}
}
```

## Indexed reads

All paths below begin with `/api/chains/{chain_id}/`. `message` is a GET; the others take a JSON
body by POST. All of them are answered from memory, including list membership and reply counts.

Here a **record** means a message's indexed metadata, not its stored bytes:

```json
{
  "id": "9", "author": "0x1111111111111111111111111111111111111111", "parent": "7",
  "parent_author": "0x2222222222222222222222222222222222222222", "timestamp": 1788750000,
  "content_hash": "0x1163378f19c56cdea3e4b7a722dd73e840f29e2a8350199877e1e48807e6c4a6",
  "transaction": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "position": {"block": "100", "log": 2},
  "gate": "0x0000000000000000000000000000000000000000",
  "author_gate": "0x0000000000000000000000000000000000000000",
  "effective_gate": "0x0000000000000000000000000000000000000000",
  "commitment": "0x675c38a12776f31433ad4e42de1a0a0ab48b934f0007fa6d218579a8c10979e0", "replies": "0"
}
```

A record's `commitment = keccak256(abi.encode(author, parent, timestamp, contentHash))`, with
Solidity types `(address,uint64,uint32,bytes32)`. It covers only the fields that never change, not
the gates. `effective_gate` is the message's own gate when that is nonzero, and its author's gate
otherwise. `replies` counts live direct replies; deleted and removed ones don't count. When a
message is deleted or removed, its replies are never promoted into its parent's list or count.
Deleting a message does not erase its record's immutable fields, so it stays available as context.
Gate values mean:

| Address | Meaning |
|---|---|
| `0x0000000000000000000000000000000000000000` | Inherit the author's gate; if that is also zero, replies are open. |
| `0x0000000000000000000000000000000000000001` | TOMBSTONE: this message was deleted by its author. On an **author** gate it closes ordinary inherited replies; it does not delete the author's messages. |
| `0x0000000000000000000000000000000000000002` | REMOVED: this reply was removed by its immediate parent's author. |
| Any other address | A reply-gate contract. Use admission to check permission; an address alone is not an admission verdict. |

Tell whether a message was deleted or removed from its own `gate`, not from `effective_gate`. A
deleted or removed message never stands alone in a feed or a notification. It leaves its parent's
reply list and count together with its whole branch. Show its placeholder only at its own address,
or as context needed above a reply that survives. Opening its own page shows the placeholder and its
surviving direct replies. Never render its body.

The author of a live reply's immediate parent can always answer it, whatever the gates, blocklists
or account deactivation say. Every such answer is protected from removal and from automatic folding
by blocklists or key gates, even after an ancestor is deleted. Reader mutes and host blocks still
apply. Quotes don't count as answers here. Use `admission` for the Board's decision; an
`effective_gate` alone does not decide who may reply.

### `message/{id}?ancestors=N&commit=0x…`

The answer is `{at, message, ancestors}`. `N` defaults to zero and may be 0–32. Ancestors come
nearest first and stop at the root. If the optional commitment is given it must match, or the
request fails with 400 `commitment_mismatch`; it never returns a different post in its place.
Unknown ids, including zero, return 404. For `GET /api/chains/31337/message/9?ancestors=0`, a
complete answer is:

```json
{
  "at": {
    "generation": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    "sequence": "123",
    "checkpoint": {
      "block": "456", "hash": "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    }
  },
  "message": {
    "id": "9", "author": "0x1111111111111111111111111111111111111111", "parent": "7",
    "parent_author": "0x2222222222222222222222222222222222222222", "timestamp": 1788750000,
    "content_hash": "0x1163378f19c56cdea3e4b7a722dd73e840f29e2a8350199877e1e48807e6c4a6",
    "transaction": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "position": {"block": "100", "log": 2},
    "gate": "0x0000000000000000000000000000000000000000",
    "author_gate": "0x0000000000000000000000000000000000000000",
    "effective_gate": "0x0000000000000000000000000000000000000000",
    "commitment": "0x675c38a12776f31433ad4e42de1a0a0ab48b934f0007fa6d218579a8c10979e0",
    "replies": "0"
  },
  "ancestors": []
}
```

### `feed`

```json
{
  "filter": {"any": [{"authors": ["0x1111111111111111111111111111111111111111"]}]},
  "cursor": null, "limit": 20
}
```

A filter's clauses are listed in `any`, and a message matches if any clause matches it. Within a
clause, `authors`, `parent_authors` and `kind` (`all`, `posts` or `replies`) must all match, and an
empty address list means no restriction. An empty `any` matches nothing; the default filter matches
every message. A `posts` clause with `parent_authors` or `parent_members_of` is refused with 400
`posts_have_no_parent_author`: top-level posts record the zero address as their parent's author, so
such a clause could never select one and every page would scan for nothing. Deleted and removed
messages are never feed items; they are reached through their thread.

`filter.exclude_list` can name an indexed list as `{"registry":"0x…","owner":"0x…","list":"0x…"}`,
where the last field is the list's bytes32 id. Authors on that list are left out before the page is
chosen, within the same 500-candidate scan. The cursor includes this list as part of the filter. A
registry this host does not index is refused with 400 `registry_not_indexed`.

A clause can also name an indexed list, in the same form, as `members_of`, which keeps only messages
whose author is on the list now, or as `parent_members_of`, which keeps only replies whose parent's
author is on it. Given together, both must match. Membership is checked during the scan, within the
same 500-candidate budget, so a page of a list's posts usually takes one request; a list whose posts
are rarer than that among the chain's keeps paging. The cursor includes the list, and a registry
this host does not index is refused with 400 `registry_not_indexed`. A change to that list's
membership makes `changes` answer 409 `reset_required`, as for `exclude_list`: membership decides
which older messages belong, so the view must be read afresh.

A curated tab selects listed authors' posts and replies with `{"curated":true}`, alongside
`{"authors":[<reader>],"kind":"replies","parent_curated":true}` and
`{"parent_authors":[<reader>],"parent_curated":true}`. The last clause selects direct replies to the
reader only when the reader is curated. These clauses read the host policy, not an on-chain
registry. Every feed query leaves out host-blocked authors, and a block wins over curation. Feed
cursors include the policy revision. Clients compare the published revision to cue a fresh curated
read after a change, since policy edits are not chain events. The reader's own top-level posts get
no exception.

For reply notifications, use a `parent_authors` clause for the reader and exclude the reader's
blocklist, named in this chain's `blocklist` setting; the list id is the keccak256 of its UTF-8
name. Ordinary feeds and thread reads need not exclude it. This personalizes one query; nothing is
deleted from the shared index. A filter has at most eight clauses and 258 addresses in all: enough
for 256 followed authors, the reader, and replies to the reader.

The answer is `{at,items,next}`, newest first, without deleted and removed messages. Grouping,
follows, mutes and how many rows to show stay up to the client. For a feed with just the sample
message, the answer is:

```json
{
  "at": {
    "generation": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    "sequence": "123",
    "checkpoint": {
      "block": "456", "hash": "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    }
  },
  "items": [
    {
      "id": "9", "author": "0x1111111111111111111111111111111111111111", "parent": "7",
      "parent_author": "0x2222222222222222222222222222222222222222", "timestamp": 1788750000,
      "content_hash": "0x1163378f19c56cdea3e4b7a722dd73e840f29e2a8350199877e1e48807e6c4a6",
      "transaction": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "position": {"block": "100", "log": 2},
      "gate": "0x0000000000000000000000000000000000000000",
      "author_gate": "0x0000000000000000000000000000000000000000",
      "effective_gate": "0x0000000000000000000000000000000000000000",
      "commitment": "0x675c38a12776f31433ad4e42de1a0a0ab48b934f0007fa6d218579a8c10979e0",
      "replies": "0"
    }
  ],
  "next": null
}
```

When `next` is not null, it is an object with `generation`, `filter`, `ceiling` and `before`. The
two hashes bind it to this host's generation and to the normalized filter, and the numbers are
decimal strings. The `filter` hash in a cursor is not the request's filter object. Send the whole
object back as `cursor`. For the example author filter, a page that has examined message 9 and still
has older candidates can return this `next`, as one field of its answer:

```json
{
  "generation": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
  "filter": "0xd42a02b7c5e8cfe8c3fdf6f36a25667f24c2fd4c4912b0b9b047ec43c880917b", "ceiling": "9",
  "before": "9"
}
```

Send this object unchanged as the next request's `cursor`, with the same filter. `next:null`, as in
the complete answer above, means no older candidates remain in this series of pages.

### `replies`

Request:

```json
{"parent": "9", "cursor": null, "limit": 20}
```

The answer has `at`, the current `parent` record, `items` (direct replies, oldest first) and `next`.
Deleted and removed replies are left out, and their own replies are not promoted in their place.
Reading them directly still returns them, as context or as the parent. Here the sample reply has no
replies of its own:

```json
{
  "at": {
    "generation": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    "sequence": "123",
    "checkpoint": {
      "block": "456", "hash": "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    }
  },
  "parent": {
    "id": "9", "author": "0x1111111111111111111111111111111111111111", "parent": "7",
    "parent_author": "0x2222222222222222222222222222222222222222", "timestamp": 1788750000,
    "content_hash": "0x1163378f19c56cdea3e4b7a722dd73e840f29e2a8350199877e1e48807e6c4a6",
    "transaction": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "position": {"block": "100", "log": 2},
    "gate": "0x0000000000000000000000000000000000000000",
    "author_gate": "0x0000000000000000000000000000000000000000",
    "effective_gate": "0x0000000000000000000000000000000000000000",
    "commitment": "0x675c38a12776f31433ad4e42de1a0a0ab48b934f0007fa6d218579a8c10979e0",
    "replies": "0"
  },
  "items": [],
  "next": null
}
```

A cursor that is not null has `generation`, `parent`, `ceiling` and `after`, with ids as strings. It
is bound to the parent and to the highest reply id. Send it back unchanged.

### `notifications`

Request:

```json
{"account": "0x1111111111111111111111111111111111111111", "cursor": null, "limit": 20}
```

The answer has `at`, `items` and `next`: direct replies to the account's messages together with
indexed mentions and quotes of the account, by descending message id. A message that is both appears
once. Each item is a complete record with a `reference` field: `null` for a reply without an indexed
reference, or the mention or quote object described under `references`. A reference may only be
found after the reply, once its body arrives. A mention takes precedence over a quote as the reason.

The request accepts the same `exclude_list` as `references`. Deleted and removed messages and
excluded authors are left out before paging. The page limit is 1–50 items, and a page examines at
most 500 candidates, without building the whole history in memory. The cursor is bound to the
account, the exclusion list, the generation and the first upper id, and moves past the last
candidate examined. An empty page may still have a `next`.

To refresh, request `changes` with both `account` and this `feed` clause, giving the same exclusion
list in both: `{"parent_authors": ["<account>"], "kind": "replies"}`. Merge records and reference
reasons by message id, including references found after a reply was already shown. Across chains,
clients order these pages by timestamp, breaking ties the same way every time.

### `references`

Request:

```json
{"account": "0x1111111111111111111111111111111111111111", "cursor": null, "limit": 20}
```

The answer has `at`, `items` and `next`, newest first, without deleted and removed messages. Each
item has every field of a record, plus `reference`, which is one of these complete objects:

```json
{"kind": "mention", "account": "0x1111111111111111111111111111111111111111"}
```

```json
{
  "kind": "quote",
  "target": {
    "chain": "31337", "id": "9",
    "commit": "0x675c38a12776f31433ad4e42de1a0a0ab48b934f0007fa6d218579a8c10979e0"
  }
}
```

One item is returned per source message, even if it references the account more than once. A direct
mention takes precedence over a quote. `target` is the quoted message, while the item's own id and
hash identify the post that quotes it. The request also accepts `exclude_list`, with the same shape
and rules as `filter.exclude_list` on feeds. Notification clients send the reader's blocklist on the
source chain, even for quotes of a post on another chain. Filtering happens before the page is
chosen, and the associations stay indexed, so unblocking needs no rebuild. Cursors have the same
shape as feed cursors, bound to the account and the exclusion list.

The index takes at most eight mentions or post references from a `text/plain` body, counting
malformed and repeated ones, before removing duplicates. This limit is separate from the limit on
attachment previews. This query fetches no bodies.

A quote is associated only with an accepted target whose immutable commitment matches the one given.
A missing target is retried; a mismatch is recorded as rejected. Associations the host derives may
arrive long after the source post, so consume `changes` to find them; looking only at newer message
ids would miss late references.

### `state`

```json
{"ids": ["0"], "accounts": ["0x1111111111111111111111111111111111111111"], "memberships": []}
```

The answer is `{at,items,accounts,memberships}`, each in request order. An unknown message id gives
a null item. Accounts are `{address,gate,member}`, where `member` is this host's own curation, with
host blocks excluded, or null when the host has no local policy. The separate `memberships` array
reads on-chain lists, and its entries are booleans. A request has at most 50 entries across its
three arrays. Only registries this host indexes can be queried; another registry gets 400
`registry_not_indexed`. Arrays left out are empty. For the example request above, on a host without
a curated list:

```json
{
  "at": {
    "generation": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    "sequence": "123",
    "checkpoint": {
      "block": "456", "hash": "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    }
  },
  "items": [null],
  "accounts": [
    {
      "address": "0x1111111111111111111111111111111111111111",
      "gate": "0x0000000000000000000000000000000000000000", "member": null
    }
  ],
  "memberships": []
}
```

A membership request has `list`, the key shown below, and `member`, an address. The `memberships`
answer is a matching array of booleans, not a list of member records.

### `list`

Request, for a registry this host indexes:

```json
{
  "list": {
    "registry": "0x4444444444444444444444444444444444444444",
    "owner": "0x1111111111111111111111111111111111111111",
    "list": "0x5555555555555555555555555555555555555555555555555555555555555555"
  },
  "generation": null, "after": null, "limit": 20
}
```

The `list` key is `{registry,owner,list}`. Its inner `list` field is a bytes32 id, normally the
keccak256 of the list's UTF-8 name, not the name itself. `state.memberships` takes the same key. A
one-member answer is:

```json
{
  "at": {
    "generation": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    "sequence": "123",
    "checkpoint": {
      "block": "456", "hash": "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    }
  },
  "items": ["0x1111111111111111111111111111111111111111"],
  "next": null
}
```

Items are addresses in ascending order. A `next` that is not null is the last address returned: send
it as `after`, with `at.generation` as `generation`, on the next request. This walks the current
membership, not a frozen copy, so edits made meanwhile may need a refresh afterwards.

### `lookup`

Request:

```json
{
  "author": "0x1111111111111111111111111111111111111111",
  "content_hash": "0x1163378f19c56cdea3e4b7a722dd73e840f29e2a8350199877e1e48807e6c4a6",
  "parent": "7",
  "from_block": "90", "cursor": null, "limit": 20
}
```

For publication recovery, the answer has `at`, `items` and `next`, newest first, including deleted
messages. Records have the full indexed shape; an empty finished search is:

```json
{
  "at": {
    "generation": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    "sequence": "123",
    "checkpoint": {
      "block": "456", "hash": "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    }
  },
  "items": [],
  "next": null
}
```

`parent` is optional; leave it out or send null for any parent. `from_block` is inclusive and
defaults to zero. Capture an accepted checkpoint before signing and keep it with the signed request.
Use the same filters on every page. Cursors have the feed cursor's shape but are bound to this
lookup and can't be used for anything else.

After a finished search, the next one may start one block above the checkpoint captured on the first
page; if that was null, keep the original bound. A failed search keeps its cursor. With a
transaction hash, use `transaction/{hash}` instead and check its Posted event.

### `changes`

A request that refreshes only gates:

```json
{
  "generation": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
  "after": "123",
  "until": null,
  "scope": {
    "feed": null,
    "ids": [],
    "parents": [],
    "account": null,
    "gates": true
  }
}
```

To include content changes, set `scope.feed` to a feed filter, `ids` to the ids of messages you
keep, `parents` to the branches on screen, or `account` to the reader's address for references.
`feed` and `account` select nothing when left out or null, and id arrays left out are empty. `gates`
defaults to true. A request takes at most 4,096 ids and parents together, within the 64 KiB limit on
a request body.

Repeat the feed's `exclude_list` inside `scope.feed`, and for references pass the same list as
`scope.exclude_list`. These leave matching authors out of feed and reference results, while ids and
parents you ask for explicitly stay available as context. Gate events still cover the whole chain,
for the shared gate ledger. If the scan meets a membership change to either exclusion list, the
answer is 409 `reset_required`: reload that notification view through its ordinary page route. That
reload removes newly blocked authors and brings back notifications that were excluded before an
unblock. Changes to other lists do not reset the view, and no view is refreshed automatically just
because membership changed.

Example answer:

```json
{
  "at": {
    "generation": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    "sequence": "125",
    "checkpoint": {
      "block": "458",
      "hash": "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
    }
  },
  "until": {
    "generation": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    "sequence": "125",
    "checkpoint": {
      "block": "458",
      "hash": "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
    }
  },
  "gates": true,
  "changes": [
    {
      "sequence": "124",
      "event": {
        "kind": "chain",
        "event": {
          "kind": "reply_gate",
          "id": "9",
          "old": "0x0000000000000000000000000000000000000000",
          "new": "0x0000000000000000000000000000000000000001",
          "position": {
            "block": "457",
            "log": 2
          }
        }
      }
    },
    {
      "sequence": "125",
      "event": {
        "kind": "chain",
        "event": {
          "kind": "author_gate",
          "author": "0x1111111111111111111111111111111111111111",
          "old": "0x0000000000000000000000000000000000000000",
          "new": "0x6666666666666666666666666666666666666666",
          "position": {
            "block": "458",
            "log": 0
          }
        }
      }
    }
  ],
  "records": [],
  "references": [],
  "through": "125",
  "more": false
}
```

| Field | Meaning |
|---|---|
| `at` | Position of the **current** record and reference snapshots in this answer. |
| `until` | The fixed end of this refresh, captured on its first page. |
| `gates` | Whether gate events were requested; repeats the scope's effective setting. |
| `changes` | Matching events in increasing sequence order. Gaps are normal: unrelated entries were looked at but left out. |
| `records` | Current full records of the messages the inspected changes affected, including parents whose counts changed. Apply each snapshot at its `at.sequence`. |
| `references` | Full records with an extra `reference` field, shaped as in `references`. |
| `through` | The last sequence looked at, even when no event matched. Send it as the next request's `after`. |
| `more` | Whether more entries remain before the fixed `until`. |

On the next page, send the **whole `until` object from the first page**, the returned `through` as
`after`, and the same scope and generation. Stop at `more:false`, even when the last page has no
records. Each answer looks through at most 2,000 retained entries and returns at most 50 changes and
100 distinct records, since an entry can change a reply and its parent. The entry that would pass
either limit starts the next page, so every page makes progress. The last 250,000 entries are kept.
An older cursor, a changed generation or an impossible sequence range returns 409 `reset_required`.

`at` may be later than `until`, because snapshots can reflect changes past this scan's end. Never
replace `until` with that newer `at`, and never let an older event or answer overwrite a newer
snapshot. Keep track of the sequence your state reflects: apply events in sequence order and
snapshots at their `at.sequence`. Don't mark a refresh complete until all of its pages have
succeeded.

With `gates:true`, reply-gate and author-gate changes from the whole chain are included, not just
those matching the content scope, so several views kept on screen can safely share one gate ledger.
Within one page's scan, a gate that changed more than once comes back once, as its newest change: a
client applies each gate's latest value and needs no earlier one, so a gate flipped a thousand times
costs it one change rather than a page per flip. A kept change's `old` is the gate just before that
change, not before the previous change on the page, and sequence numbers skip the changes left out.
The page's `through` still passes every change scanned. With `gates:false`, changed record snapshots
can still come back, but gate events are left out.

For a Posted change, the outer `event.kind` is `chain`, and the inner `event` is the complete Posted
object shown in the receipt example below. Unlike a record snapshot, a raw Posted message has no
`author_gate`, `effective_gate`, `commitment` or `replies` fields. When a late body adds references,
the change has this complete shape instead. That happens only for a body that mentions or quotes
someone; a body without references changes no association:

```json
{
  "sequence": "126",
  "event": {
    "kind": "references",
    "id": "9"
  }
}
```

Membership changes advance the inspected sequence but are not returned by this route. Read `state`
or `list` again when membership needs refreshing, and never infer membership from a missing change
entry. Every returned `records` or `references` item is current state, so a deletion or removal can
appear here even though feed, reference and reply pages leave such messages out.

## Transaction services

These are live reads of the configured Board and the registries the host recognizes. They may be
ahead of the index and do not return `at`. They never send a transaction, choose a sponsor or accept
arbitrary contract call data.

| Path | Request | Response |
|---|---|---|
| `POST /api/chains/{id}/admission` | `{parent,replier,gate_data?}`; `gate_data` defaults to `0x`, at most 16 KiB | `{gate}` from Board.checkReply; parent zero returns the zero gate. |
| `POST /api/chains/{id}/post-estimate` | `{author,content_hash,parent,reply_gate?}` | `{gas}` as a decimal string, with a 25% margin, capped at 500,000. `reply_gate` defaults to zero, the account's default; the estimate includes the post's chosen gate and empty gate data. |
| `GET /api/chains/{id}/transaction/{hash}` | None | A pending or mined receipt; the complete forms are shown below. |

Admission follows the Board's rules, which call gates with STATICCALL, and its simulation is capped
at 500,000 gas. A known revert answers 422 `simulation_refused`, with the `revert` bytes, bounded in
size. An estimate over the cap answers 422 `post_exceeds_gas_limit`. An upstream that can't be
reached answers 502 `chain_read_unavailable`. A recognized rate throttle instead answers 503
`chain_read_unavailable` with a numeric `Retry-After`, including when the throttle occurs during
the receipt's header check. Admission, estimates **and receipt reads** can answer
503 `host_busy` when the upstream concurrency limit or the shared RPC budget is used up. For
example:

```json
{"error": "host_busy", "code": "host_busy"}
```

Retry after a pause; neither a busy answer nor a failed read says whether a transaction was sent or
mined. Admission and estimates need a verified chain: they work during backfill, but answer 503
`index_unavailable` while the index is starting or stopped.

Receipt decoding needs no readable index, so it keeps working while an index is starting,
backfilling or stopped. It checks the endpoint's chain id, the receipt's hash and its block header
on the canonical chain. It returns only events from the Board and the configured registries, and
reads at most 4,096 logs. A successful receipt does not prove the requested action happened: clients
must match its event to exactly what they signed. Receipts can be ahead of the index for a while; a
post is ready for an indexed feed only once its message appears there.

### Admission and estimation examples

`POST /api/chains/31337/admission`:

```json
{"parent": "7", "replier": "0x1111111111111111111111111111111111111111", "gate_data": "0x"}
```

If the target is open, or the reply is a protected answer, the answer is:

```json
{"gate": "0x0000000000000000000000000000000000000000"}
```

`gate` is the gate admission depended on; zero means no gate was called. The author of the target's
immediate parent can answer whatever its gate says, but a target that was itself deleted or removed
always refuses. This is a preview, not a reservation: chain state or a gate's logic can change
before the transaction is mined. `gate_data` is opaque bytes written as hex, with a `0x` prefix and
an even number of digits. This host's post estimate uses empty gate data and the requested
`reply_gate`; leaving `reply_gate` out selects the author's account default. For a custom gate this
host doesn't know, the format of its proof must come from that gate's own documentation.

`POST /api/chains/31337/post-estimate`:

```json
{
  "author": "0x1111111111111111111111111111111111111111",
  "content_hash": "0x1163378f19c56cdea3e4b7a722dd73e840f29e2a8350199877e1e48807e6c4a6",
  "parent": "7"
}
```

Example answer:

```json
{"gas": "100000"}
```

### Receipt and event examples

`GET /api/chains/31337/transaction/` followed by the transaction hash returns one of these shapes.
Identical requests made at the same moment share one read of the chain while anyone is still waiting
for it. No answer is kept, so each later request reads again. A pending answer means **no receipt is
available**: the node may not know the transaction, it may not be mined yet, or it may have been
dropped:

```json
{"hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "status": "pending"}
```

A successful post:

```json
{
  "hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "status": "success",
  "block": "100",
  "block_hash": "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
  "events": [
    {
      "kind": "posted",
      "message": {
        "id": "9", "author": "0x1111111111111111111111111111111111111111", "parent": "7",
        "parent_author": "0x2222222222222222222222222222222222222222", "timestamp": 1788750000,
        "content_hash": "0x1163378f19c56cdea3e4b7a722dd73e840f29e2a8350199877e1e48807e6c4a6",
        "transaction": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "position": {"block": "100", "log": 2},
        "gate": "0x0000000000000000000000000000000000000000"
      }
    }
  ]
}
```

A reverted transaction has `status:"reverted"`, the same block fields and `events:[]`. A success may
also have an empty event list: logs that are unrelated or don't decode do not show that the
requested operation happened.

The other events a receipt's `events` array can hold are:

```json
[
  {
    "kind": "reply_gate", "id": "9", "old": "0x0000000000000000000000000000000000000000",
    "new": "0x0000000000000000000000000000000000000001",
    "position": {"block": "456", "log": 0}
  },
  {
    "kind": "author_gate", "author": "0x1111111111111111111111111111111111111111",
    "old": "0x0000000000000000000000000000000000000000",
    "new": "0x6666666666666666666666666666666666666666",
    "position": {"block": "456", "log": 1}
  },
  {
    "kind": "membership",
    "list": {
      "registry": "0x4444444444444444444444444444444444444444",
      "owner": "0x1111111111111111111111111111111111111111",
      "list": "0x5555555555555555555555555555555555555555555555555555555555555555"
    },
    "member": "0x2222222222222222222222222222222222222222", "present": true,
    "position": {"block": "456", "log": 2}
  }
]
```

A membership event uses `present:false` for a removal. An event's position is `{block,log}`, with
the block as a decimal string and the log index as a number. A receipt event has no host sequence;
the index assigns one later, when it accepts the event.

Match a post on its author, content hash and parent, and take its id from the matching Posted event.
For moderation, match the message id or author and the requested new gate. Check the transaction
hash too. A successful receipt is provisional until the index accepts it, and says nothing about
whether the body is available.

## Names

`GET /api/names/{address}` returns `{name,source,avatar}`, each of which may be null. The host shows
a name only when its reverse record and forward resolution agree, checking ENS on Ethereum first and
then Basenames on Base. A name counts only in ENS's normalized spelling (ENSIP-15). The registries
accept any string, so otherwise a capital letter, an invisible character or a label mixing alphabets
could pass for another account's name. Clients need not normalize again. The host never reveals its
private upstream URLs.

With no name service configured, or no verified name, the fields are null; a failed lookup returns
503. A found name is cached for a day, and a negative or partial answer for an hour. Errors are not
cached, but an upstream that fails is paused for 30 seconds rather than asked again on every lookup.
Capacity limits are an exception: a local budget/slot refusal or a recognized RPC throttle answers
503 without caching a missing or partial name, even when a Basename could replace ENS or only the
avatar read failed. A throttle uses the RPC transport's endpoint cooldown and adds no separate
30-second name pause.
A record the host cannot read or verify is that address's answer: null fields, cached as a negative,
with no pause. That covers a name that is not a DNS name, not normalized or not under base.eth, a
record that doesn't decode or is too large, and a resolver that refuses, runs out of gas or keeps
redirecting. It also covers a Basename whose resolver breaks while running: code that halts, a
precompile that refuses the call, or code that asks for too much memory. The host recognizes these
only in the exact wording Base's providers use, and treats any other error as the upstream failing.
The host also counts a timeout waiting for ENS's gateway, after ten seconds, as that name's failure
rather than ENS's: the gateway waits about 30 seconds on a name's own offchain server, so an owner
could otherwise pause ENS at will. That address then has no ENS name for an hour, and a basename
shown in its place is partial. Any other failure reaching the gateway still pauses ENS. Only a
failing upstream is paused, never this host running out of its own call budget.

Up to 24 lookups run at once, which leaves some of the host's chain-call slots for receipts and
upload checks, and up to 96 distinct addresses can wait for them; one that waits three seconds
answers 503 `host_busy`. A lookup has 30 seconds in all. It follows no redirects, and its CCIP-read
destinations are fixed and bounded.

Example answer:

```json
{"name": "alice.eth", "source": "ens", "avatar": "https://example.org/avatar.png"}
```

`source` is `ens`, `basenames` or null, and `avatar` may be null even for a verified name. An
address with no name gets `{"name":null,"source":null,"avatar":null}`. An answer with
`"partial": true` had a read behind it fail: the avatar's provider, its resolver breaking or a
timeout on its offchain records, or ENS while a basename answered, including a timeout on the
address's own ENS records. It is shown, but cached for an hour like a negative answer rather than
for a day, and a client should keep it no longer. The address in the request stays the identity;
names are display labels, not credentials.

For Basenames, the host reads Base's ENS L2 reverse registrar first, and falls back to the legacy
reverse record only when the newer one is empty. A newer record that is set but invalid or
mismatched does not fall back. The Base registry picks the name's current resolver, for both the
forward check and the avatar. Custom resolvers are called only through the configured Base RPC, with
the same limits on gas, answer size and lookup time, and their CCIP URLs are not followed. An empty
answer from a custom resolver means a missing record, not an upstream outage.

Avatar URLs are HTTPS; an IPFS reference becomes an HTTPS gateway URL. NFT avatar URIs are not
resolved. A name does not make its avatar safe: clients limit download sizes, restrict media types
and reject SVG.

### Transfer recipient names

`GET /api/names/resolve/{name}` returns `{"name":"alice.eth","address":"0x..."}`. Only complete
names ending in `.eth`, including `.base.eth`, are accepted. The host normalizes the name with
ENSIP-15 and resolves its Ethereum address record. A smart-contract wallet deployed at a different
address on the transfer chain would not control the resolved address there. ENS names use the
configured Ethereum endpoint and the Universal Resolver, with the fixed CCIP gateway above. Names
ending in `.base.eth` use the configured Base endpoint, looking up the name's current resolver in
the Basenames registry before reading its address record. URLs chosen by a resolver are never
contacted. No reverse name or avatar lookup is needed, and the chain a transfer happens on is
independent of the chain its recipient's name resolves on.

Missing, zero or unreadable address records, a resolver that refuses or breaks, and a timeout on
offchain records, return `address: null`. Invalid names return 400 `invalid_name`; an unavailable
upstream or an unconfigured name service returns 503 `name_unavailable`, as do lookups refused at
their line in the upstream budget. The host supplies these destinations, so they carry the same
trust in the host and its upstreams as other application reads. Clients must show the normalized
name and the resolved address for review and submit that reviewed address, never quietly resolving
again while the wallet asks for approval.

This route shares the per-visitor name budget and the 24 active name lookup slots. Its own queue
holds at most 96 distinct names, with the same three-second wait for a slot and the same 30-second
lookup timeout. Matching requests share one lookup, and a cancelled HTTP request keeps its visitor
slot until the work finishes. A separate cache holds up to 2,048 recipient answers for 30 seconds,
including missing records; errors are not cached. The day-long display-name cache is never used for
recipients.

## Rendering content

Fetch and verify a record as described in
[STORE_API.md](STORE_API.md#envelope-bytes-and-safe-interpretation) before reading it.

Render `text/plain` on the client as escaped text with its line breaks, flat `*bold*` and
`_italics_` spans, and clickable URLs and domains ([spec §6][spec-text]). The host returns the
original bytes, not formatted HTML. Link metadata comes through the host. Thumbnails from outside
sites load automatically for your own posts and the authors you follow; other thumbnails wait for a
press. These are the reference forms ([spec §7][spec-7]):

| Form | Interpretation |
|---|---|
| `record:0x<64 lowercase hex digits>` | Another whole record, by hash. When fetching an image or attachment, name the message it came from as the anchor. |
| `post://<chainId>/<messageId>?commit=0x<64 lowercase hex digits>` | A pinned message. Both ids are decimal. Look it up on that chain's host, passing `commit` to `message/{id}`, and recompute the commitment too. A mismatch must never show a replacement. |
| `account://0x<40 hex digits>` | A mention of an address, whatever its current display name. |

Keep a quote's commitment from the preview through the stored draft and any retry; never replace it
with one fetched later. A malformed pin never falls back to an unpinned quote. Quotes inside quotes
stay links, so cards never nest. Only the first eight mentions or post references count towards
notifications, including repeated and malformed ones.

Show at most four image and quote previews per body, or the host's `max_per_message` if that is
smaller (four without an attachment policy). Each occurrence shown takes a slot. The host's
attachment admission separately counts distinct valid `record:` hashes, in order. Don't fetch
references past the preview limit automatically. Inline pictures can be PNG, JPEG, GIF, WebP or
AVIF. Keep other content inert; the reference client never opens SVG. A client may limit how much of
a body it interprets ([spec §6][spec-text]); none of those choices change how these HTTP requests
are encoded.

Before showing attribution or content, read the record from the index, recompute its immutable
commitment, and compare it with any quote pin. Fetch the content by hash and verify the downloaded
bytes. A mismatch means the content is unavailable; it never permits showing another post or bytes
not anchored on chain. Bodies can safely be cached by hash; counts and gates, which change, come
from snapshots and `changes`.

## Serving the app

With `[gateway]` and `--web-dir`, the host serves the built web app at `/`. Any path that is not an
API route and matches no file gets the app's `index.html`, so the app's own routes work. Files under
`/assets/` are named by content hash and are sent with a year of `immutable` caching, and a missing
one is a plain `404`, never the app page under that name. Every other file (`index.html`,
`embed.html`, `embed.js`, icons) is sent `no-cache`: a browser may keep it but checks with the
server before reusing it, so a newly loaded page refers to the current build. Deployments must keep
older hashed assets separately for tabs that are already open. A `304` carries the same header.

### Social share pages

With `--web-dir`, `GET` and `HEAD` on `/p/{chainId}/{postId}` and `/u/{chainId}/{address}`, and on
that profile path followed by `/replies`, `/keys`, `/keyring` or `/keyring/holders`, serve the app
with Open Graph and Twitter metadata rendered on the server. These routes are public and need no
wallet. Post excerpts are checked against the indexed content hash before they are rendered. Profile
cards use the verified display name when there is one, or else the wallet address, with the site's
whale card image, or the golden key image on the keys tab. An address need not have posted to have a
profile. Unknown chains return 404, and malformed identifiers 400. A post whose body this host has
taken down answers 410 with a card saying its content was removed by this host, and no excerpt. A
card that cannot be built in time serves the app with its generic card; only an unreadable
`index.html` answers 503, with `Retry-After`.

Canonical URLs use the origin from the deployed HTML, never the request's Host header. Answers use
`Cache-Control: no-store`, though social platforms may still cache their own previews. Clicking a
share link opens the post or profile.

### Preview availability

Rich post and profile previews share eight build slots. Each client IP may hold at most two, with a
burst of four new builds and a steady rate of 30 a minute. Forwarded client addresses are used only
from loopback peers; a public listener uses the actual peer address. A local reverse proxy must
overwrite or append the real client address in `X-Forwarded-For`. Requests without connection
details share one anonymous bucket.

Successful previews are cached in memory for up to 60 seconds, at most 256 entries and 256 KiB per
HTML answer. Identical builds running at once share the work, and cache hits skip the build limits.
A cached post card is tied to the complete indexed record and checked again before it is sent, so
deletions, takedowns and accepted reorganizations invalidate it, as they do a card still being
built. Changing the app's entry document also invalidates cached asset links. The client IP table
holds at most 1024 entries, and inactive ones expire after five minutes. Both tables live in the
process and reset on restart.

When building previews is busy, rate-limited or temporarily unavailable, valid post and profile
links serve the app's entry document with generic metadata and HTTP 200, so visitors can still open
the page. Missing, deleted and mismatched posts keep their normal status codes. About pages use no
preview build slots. Answers keep `Cache-Control: no-store` for outside caches.

## Account keyring

### Public purchases and transfers

`GET /api/chains/{chain}/keyring/activity?limit=20` returns all accepted key purchases
(including buying one's own keys) and transfers, newest first. Sales and activations without
purchases are excluded. This is a local index read with no RPC calls:

```text
{ market, checkpoint: { block, hash }, items, next }
```

Each item has `position: { block, log }`, `transaction`, `timestamp` and `event`. Blocks are
decimal strings, log indexes and amounts are u32 numbers, and timestamps are Unix seconds.
A purchase's event is `{ kind: "trade", creator, holder, buy: true, amount, supply }`;
a transfer's is `{ kind: "transfer", creator, from, to, amount }`.

The limit is 1–100 (default 20). Pass both `before_block` and `before_log` from `next` to
continue; the cursor is exclusive. Invalid queries return 400 `invalid_trade_query`.
`next: null` means the history is exhausted. Each request examines at most 2,000 events,
so a page can be empty and still have a continuation. Events involving any host-blocked
participant are omitted; clients apply their own mutes. No history is trimmed from this
public activity index. The existing keyring readiness rules apply: unavailable indexes return
503 `keyring_syncing`, and unconfigured chains return 404 `keyring_not_configured`.

The web app interleaves these events with conversations in Everyone on that chain, using
block and log position. Purchases and transfers link to the creator's keys page. Other
streams stay unchanged. Quiet polls cue Refresh without inserting rows while the reader
is scrolling. A failed key read leaves posts usable and offers a retry.

### Creator trade notifications

`GET /api/chains/{chain}/keyring/{account}/trades?limit=20` returns the account's key inbox as
`{ market, active, checkpoint, items, next, truncated }`. Each item has a `kind`. A `trade` item is
someone else buying or selling this account's keys, with `trader` and a boolean `buy`. A `received`
item is keys someone moved to this account, with `creator`, whose keys they are, and `from`, the
sender, which is never the account itself. Both kinds have `amount` (a positive u32 number),
`timestamp` (Unix seconds), `transaction` (a 32-byte hash) and `position: { block, log }`, where the
block is a decimal string and the log index a u32 number. Items come newest first by position.
`next` is a position or null. To get the items before it, pass both
`before_block=<decimal>&before_log=<number>`. The limit is 1–100 (default 20); invalid limits,
partial cursors and malformed query values return 400.

The index keeps the latest 100 items per account, both kinds together. The account's own trades are
left out, and so are transfers of its keys between other holders. `truncated` means older items were
dropped, not that another page is available. An account that has not activated keys has no `trade`
items but can still have `received` ones. The account's configured blocklist hides blocked traders
and senders before paging; local mutes are up to the client. A read examines at most 100 kept events
and makes no RPC calls. If the key index is unavailable the answer is 503 `keyring_syncing`; if the
blocklist filter cannot read the Board index, it is 503 `index_unavailable`. On a failure, clients
should keep the trade rows they show and go on showing post notifications. Polls may cue a refresh
but must not replace rows on screen.

### Holdings

Each optional `[[keyrings]]` entry in the host's configuration turns on an index of account-key
holdings for one served chain, so a host can index Base and Base Sepolia separately. The index
follows `Activated`, `Trade` and `KeysTransferred` events. A transfer updates both holders without
changing the supply, and gifted keys count for feeds exactly like bought ones.

`/api/status` lists these indexes as
`keyrings: [{ chain, market, start_block, state, reason, checkpoint, minimum_confirmations }]`, an
empty list when none are configured. `state` is `starting`, `syncing`, `ready` or `stopped`. A
journal that fails to open still publishes its configuration and the reason it stopped. The index
waits for at least twelve confirmations. After a temporary RPC failure it can serve the last
verified snapshot for up to sixty seconds from when it last caught up, and then answers unavailable.
Every answer carries its checkpoint, and clients must still compare it with a reply's block before
using it to fold the reply.

`GET /api/chains/{chain}/keyring/{holder}` returns this shape, with the checkpoint block as a
decimal string, as in the Board index:

```text
{ market, checkpoint: { block, hash }, active: boolean, holders: [address, ...],
  holding: [address, ...], truncated: { holders: boolean, holding: boolean } }
```

`holders` lists wallets with a positive balance of this account's keys; `holding` lists accounts
whose keys this wallet owns. Neither includes the requested account itself. `active` is true once
the account has activated its keys, even with no purchase. Feed clients use either list with the
ordinary following-feed query.

An unconfigured chain returns 404 `keyring_not_configured`. Startup, catching up, an RPC failure or
a stopped index return 503 `keyring_syncing`, never an empty keyring. Each list returns at most 256
accounts on its own, in ascending address order, leaving out the requested account before applying
the limit. Its `truncated` flag is true only when more accounts exist. An oversized list still
returns 200 and does not hide the other direction. A query examines at most 257 accounts per
direction after leaving out the account itself, so a read's cost stays bounded.

Known limitation: keyring feeds cannot page through membership, so accounts past that fixed subset
are left out. Clients show a notice that the feed is partial for that direction, and apply
membership or truncation changes they notice only when the reader refreshes. This keeps the feed
available, but it does not stop spam holders from filling the returned subset. Transfers also let
someone gift keys from many creator addresses that sort early, crowding real holdings out of the
"keys I hold" feed; gifts stay ordinary holdings by design. The management pages below make every
holding reachable, but they don't change which accounts a feed selects. The route is public, since
key ownership is public chain data.

### Management balances

`GET /api/chains/{chain}/keyring/{holder}/balances?view=holding&limit=64&after=<address>` returns
one side of the accepted index, without any chain RPC calls:

```json
{
  "market": "0x…",
  "checkpoint": { "block": "100", "hash": "0x…" },
  "view": "holding",
  "active": true,
  "own_balance": 3,
  "total": 65,
  "entries": [{ "address": "0x…", "balance": 2 }],
  "next": "0x…"
}
```

`view` is `holding` (the default) or `holders`. `limit` defaults to 64 and must be 1–256. `after` is
an optional address cursor; the page starts after it. Entries are in ascending address order and
leave out the requested account. `own_balance` gives that account's holding of its own keys
separately, including zero. `total` counts the other accounts on the requested side. Balances are
JSON integers that fit a uint32. `next` is the last address returned when more entries exist, and
null otherwise. The example shortens addresses and entries; a cursor that is not null comes with a
full page. Invalid parameters return 400, and configuration and readiness errors are the same as for
`GET /api/chains/{chain}/keyring/{holder}`.

The cursor seeks straight into the ordered index. A request visits at most `limit + 2` addresses,
counting the account itself and one lookahead, and does one indexed balance lookup per holding.
Totals come from stored list lengths, not full scans. Each answer is consistent as of its
checkpoint. Paging is live, not a kept snapshot: new entries before the cursor appear when you go
back to the first page, and a cursor still works if its entry has been sold or transferred.

In the app, the `/u/{chain}/{address}/keyring` page shows holdings and its `/holders` path shows
holders. Settings and the keyring feed link to the connected account's pages. The page sorts only
the current 64 accounts by quantity and shows the account's own keys separately. It uses the
existing name and avatar cache and does not fetch market prices per row. A visible page polls every
30 seconds, comparing the balances, totals and paging details on screen. A change of checkpoint
alone does not cue a refresh. Polls never replace the rows on screen, and a failed refresh keeps the
last good page. Quantity changes on other pages are read when that page is opened. Buy, sell and
transfer links open the creator's existing market.

### Indexed key holdings for reply display

`POST /api/chains/{id}/key-holdings` takes `{gate, pairs: [{creator, holder}]}` with 1 to 50 pairs,
and returns `{market, checkpoint, held: [boolean]}` in request order. These are point lookups in the
accepted keyring index, not the truncated feed lists, and no request triggers RPC. Invalid counts
return 400; an unverified gate or an unavailable keyring index returns 503. Clients then leave
replies visible and retry on an explicit refresh, never falling back to admission simulations. The
Board and keyring indexes advance separately, so before using an answer to fold a reply, a client
requires its checkpoint block to be at least the reply's block. An older, missing or malformed
checkpoint means unknown, and the reply stays visible until an explicit refresh. Every reply needs
this check, even when several share the same creator and holder.

The host publishes a configured gate only after a background check reads its getters. Every gate
except the closed one must answer `BOARD()` with the configured Board. Key gates must also answer
`KEYS()` with the indexed market. Blocklist and combined gates must answer `REGISTRY()` and
`LIST_ID()` with the configured blocking setup. A plain blocklist gate must have no `KEYS()`, and a
plain key gate no `REGISTRY()`, so a combined gate cannot pass as either. The closed gate must
answer `canReply` with false and have none of `BOARD()`, `KEYS()` and `REGISTRY()`, which every
other gate has. The check does not compare contract code: ruling out a contract that answers the
same getters under other rules is up to the operator. The verified `blocking` gate stays unpublished
until its check passes, while the separate `blocklist` registry and list stay available for display.
Clients offer the key-holder reply option only when their activation market matches the host's
keyring market on that chain. Missing or mismatched deployments stay disabled and are retried once a
minute, without stopping the core Board. All getters are read at one confirmed block. Cards keep
their display verdict until a refresh.

### Keyring health and read limits

`status.keyrings[]` also has `state` (`starting`, `syncing`, `ready` or `stopped`), `reason`,
`checkpoint` and `minimum_confirmations` (12), besides its identity fields `chain`, `market` and
`start_block`. Only a ready index serves holdings. Live read routes may answer 429 with
`Retry-After`; indexed feeds and keyring pages don't count against that budget.

## Link previews

When `status.gateway.link_previews` is true,
`GET /api/link-preview?chain=<chain-id>&id=<message-id>&link=<encoded-original-link-text>` returns
`{ "preview": { "title": "...", "description": "...", "image": "https://..." } }`. `link` is the
link text exactly as written in the post, including any fragment. Before fetching anything not in
its cache, the host looks the message up in its own index and reads its stored `text/plain` body.
The link text must appear in that body, and the host works out the destination from it, adding HTTPS
to a bare domain. It never uses chain RPCs or downloads from peers for this check. Unknown posts and
chains, deleted or removed posts, posts by a blocked author and posts whose body is taken down
return 404; an unavailable index or a missing body returns 503; a failed store read returns 500; and
a link that is not in the body returns 400 `link_not_in_post`. Previews need both the gateway and
the store role; without a local store, the status reports them as off. Cache hits are keyed by URL
and cost no visitor allowance. They skip the body check, but not the check of the post that asks, so
a post that may not ask gets no cached preview either, while the same link's preview still shows for
others.

### Draft previews

Before publishing, the composer may ask for a preview with
`GET /api/link-preview/draft?link=<encoded-original-link-text>`. It sends only the link, never draft
text or picture bytes, and skips the post and body check. It uses the same URL checks and fetcher,
but a draft fetch and a published fetch never share work. Uncached draft requests have their own
visitor budget: a burst of four, one more every fifteen seconds, and two in flight. A refused
visitor gets 429 with `Retry-After: 15`. Draft requests may hold at most two of the host's eight
shared fetch slots, until the work finishes even if readers disconnect. A host-wide allowance
permits a burst of 20 distinct draft fetches, then one every five seconds; when it runs out, the
answer is 503 with `Retry-After: 5`. A full draft lane answers 503 with `Retry-After: 2`, and that
refusal is not cached. Cache hits use neither allowance.

Draft results are not kept on the server and are sent with `Cache-Control: no-store`. A draft
request that reuses a published preview keeps that entry's remaining public cache lifetime;
otherwise it fetches again within these limits. Draft visitor budgets have their own table,
hard-limited to 4096 entries, so slow refills cannot fill the table used for posting and other
reads. The client must honor `Retry-After`, including upstream backoff. This route is on whenever
link previews are; it grants no permission to publish and writes nothing that lasts.

### Requests from browsers

Both preview routes require the header `x-blohard-preview: 1` (header names are case-insensitive).
They allow no cross-origin requests (CORS), refuse cross-site Fetch Metadata, and accept an `Origin`
only for HTTPS on a configured public name. A failed check returns 403 `preview_origin_forbidden`.
These checks come before cache reads and visitor admission. Same-origin browser requests, and other
clients that send no Origin or Fetch Metadata, are accepted with the required header. This guards
against requests made by other sites in a browser; it is not authentication.

### The answer

`preview` is null for a page without useful metadata, or after a permanent refusal. Its `image` can
be null when no acceptable image was found. Fields are plain text, never HTML. The original URL
stays the card's destination and domain; redirects and `og:url` must not replace it. Titles are at
most 256 UTF-8 bytes, descriptions 512 and URLs 2048. The route makes no chain reads and writes
nothing that lasts.

### What gets fetched

Link text must be a single URL, without whitespace, control characters or embedded markup. Only HTTP
and HTTPS pages on standard ports qualify. Credentials, non-public addresses, and the operator's
configured hostnames and discovered addresses are refused at every hop, including in the
connection's own DNS lookup. Fetching is IPv4 only: IPv6 literals are refused and IPv6 DNS answers
ignored. At most three redirects are followed.

The host checks the decompressed HTML at doubling sizes starting at 32 KiB, and stops when the head
ends. A head still open at 512 KiB is read further only if no preview was found yet, up to a hard
cap of 1 MiB, so a larger page can still give its head metadata. The ten-second deadline covers the
destination's DNS lookup, reading the head and checking image addresses. No document tree is built:
scanning stops at the end of the head, and an unfinished token longer than 16 KiB (plus one 256-byte
input chunk) refuses the preview. Image URLs are checked to point at public HTTPS destinations, but
image bytes are never fetched, stored or proxied, so this check cannot control a browser's later DNS
lookup or an image server's redirects.

DEX Screener pair links (`dexscreener.com/<chain>/<pair>`, also `www.dexscreener.com`)
use its public pair API instead of fetching the webpage. The response must match the requested
chain and pair. Cards show the token name and symbol, trading pair and exchange, and prefer the
banner over the icon. Prices and market figures are omitted because previews are cached. The
original link remains the card's destination. Other URLs still use HTML metadata. Successful
published cards use the normal one-day preview lifetime, rather than the API response's cache
headers, so token names and banners can lag changes by up to a day.

API requests use the same outbound address checks, shared slots and ten-second deadline, with the
two-destination-slot limit applied to `api.dexscreener.com`. Responses must be JSON and fit within
64 KiB after decompression; redirects are refused. Image addresses pass the normal HTTPS and
operator-address checks. The slots limit concurrency, not requests per minute: traffic across
many distinct pairs can still hit the provider's rate limit. API throttles and server errors
retain the normal retry cooldown for the requested URL, not the API host as a whole.

### Limits and caching

Each visitor has a preview allowance of its own, separate from uploads, names and chain reads: a
burst of 32, refilled at two a second, with two requests in flight. A visitor's permits stay with
work that continues after it disconnects, including parsing, until the work finishes. Identical
published URLs share one fetch. Eight fetches may run at once, with at most 32 more distinct URLs
waiting up to three seconds. Each destination hostname may have two fetches in flight, checked
before a shared slot is taken and again at each redirect. A destination refusal returns 503 with
`Retry-After: 5`, not cached. The destination table holds at most 128 active hostnames and never
evicts a permit in use.

Each indexed post, keyed by chain and message id, may start at most two uncached attempts in any
five-minute window, across all visitors and destinations. Identical URLs in flight share one
attempt, and cache hits spend none. The post check and the first destination and shared-slot
admission come before this allowance, and refusals there spend nothing; once admitted, a failure
still spends an attempt. A post that has used its attempts returns 503 with its remaining
`Retry-After`, not cached. The post budget holds at most 4096 entries, evicts only entries that have
fully expired, and refuses new entries while full. Drafts keep their separate host-wide budget. This
limits churning through URLs and subdomains with one post, not attacks spread over many posts. A
reader can spend a post's allowance on its other links, delaying the preview of its first link until
the allowance returns, and doing so repeatedly can keep that preview unavailable. Cached previews
are unaffected. Counting per link instead would reopen the attack with many links.

Published fetches use a cache of at most 1024 entries, holding under 6 MiB of text. Only published
fetches fill that cache, so draft results can't be found through it or push published entries out. A
successful published preview lives a day. A page without metadata, a permanent HTTP refusal,
unsupported content or a policy refusal gives a null preview, cached for an hour on the published
route. Network failures, upstream 429s and 5xx answers are temporary, including compressed bodies
cut off midway. After a timeout the host waits five minutes; after another failure, at least a
minute or the upstream's longer `Retry-After` (in seconds or as an HTTP date). Waits are capped at
an hour, like every negative answer; only a successful preview lives a day. A full local queue or
unavailable discovery of the operator's addresses is never cached against a URL. `Cache-Control`
gives the remaining lifetime. Temporary failures and local refusals use `no-store`, and 503 answers
include the remaining `Retry-After`. Invalid URLs return 400.

### Configuration

The optional `[gateway.link_previews]` configuration requires `public_names`: every DNS name that
resolves to the operator's machine itself or to anything on its network, not only the public one.
The fetcher refuses those names and the addresses they resolve to, and always refuses private
addresses. Behind a CDN the public name resolves to the CDN, so the origin's own names must be
listed too, including any public address that routes back into the operator's network; an unlisted
one is the gap. Before the first outside fetch, all of them must resolve to public IPv4 addresses. A
background task starts this discovery at startup, refreshes it every four minutes and retries
failures after fifteen seconds; requests never wait on it. A snapshot older than five minutes blocks
new fetches until discovery succeeds again. Failures keep every known address but don't extend the
snapshot's life. The status then reports `link_previews_ready: false`, with one warning when
discovery becomes unavailable and an info log when it recovers. Old addresses expire only during a
successful refresh, after a week without being seen. Configured hostnames stay blocked between
refreshes, but a public IP that changes can leave a gap until the next discovery. The reverse proxy
may deliberately trust LAN access through the public name; that trust is not what keeps the preview
fetcher safe.

## Host policy

`GET /api/host-policy` returns `null` when no host policy is configured, or else the same
`host_policy` object that `/api/status` publishes. It has `admin`, `admin_chain`, `origin`, a
decimal-string `revision`, the address lists `curated`, `blocked` and `bots` (accounts the admin
labels as bots), `declared_bots` (accounts whose own latest statement says they are bots), and a
decimal-string `declarations` counter. Answers are `no-store`. The lists are public and apply to the
whole host, and each holds at most 4,096 nonzero addresses. Declarations never change `revision`;
`declarations` rises with each accepted statement. So a client orders the admin lists by `revision`
and the declared bots by `declarations`. Every answer below carries all of them.

`POST /api/host-policy` edits one list. The JSON body, at most 16 KiB, has `revision` (a decimal
string), `expires` (Unix seconds), `list` (`curated`, `blocked` or `bots`), `action` (`add` or
`remove`), `addresses` (1–100 nonzero addresses) and a hex `signature`. The configured admin signs
this exact UTF-8 message with EIP-191 personal signing, with the addresses in lowercase, in request
order, and no trailing newline:

```text
Host policy update
Host: <configured origin>
Revision: <revision>
Expires: <expires>
List: <list>
Action: <action>
Addresses:
<address>
<address>
```

Signatures from ordinary accounts (EOAs) are recovered locally; a contract admin is checked with
ERC-1271 on `admin_chain`. The expiry must be between now and five minutes ahead, and is checked
again just before writing. The origin and the stored revision stop an edit from being replayed on
another host or applied twice. A successful edit replaces the file atomically, advances its revision
and returns the new public object. A reader disconnecting does not cancel the write.

The errors include 400 `invalid_policy_edit`, `invalid_signature`, `invalid_policy` or
`policy_too_large` (the file would pass its 1 MiB limit); 403 `not_host_admin`; 404
`host_policy_disabled`; 409 `policy_changed` or `policy_revision_exhausted`; 503 `policy_busy`,
`policy_write_pending` or `policy_write_failed_restart_required`; and 500 `store_failed`. A write
still pending after ten seconds may yet complete, so read the policy before making another edit. The
normal visitor read budget applies to both methods. Administration from another origin in a browser
is not enabled.

### Bot declarations

`POST /api/bot-declaration` records an account's own statement that it is, or is no longer, a bot.
It costs no gas. The JSON body has `account`, `bot` (`true` or `false`), `issued` (Unix seconds) and
a hex `signature`, made with EIP-191 personal signing over this exact UTF-8 message, with the
account in lowercase and no trailing newline:

```text
Bot declaration
Host: <configured origin>
Account: <account>
Bot: yes
Issued: <issued>
```

`Bot: no` withdraws the statement. Signatures from ordinary accounts are recovered locally; contract
accounts are checked with ERC-1271 on `admin_chain`, since the label applies on every chain.
`issued` must be within five minutes of the host's clock and later than the account's previous
statement, so an older statement cannot be replayed. Only an account with at least one message
indexed on a chain this host serves may declare. The host keeps at most 4,096 accounts, declared and
withdrawn; when full, it first drops withdrawals older than five minutes. A success returns the same
public object as `GET /api/host-policy`.

The errors include 400 `invalid_declaration` or `invalid_signature`; 403 `wrong_signer` or
`not_an_author`; 404 `host_policy_disabled`; 409 `stale_declaration` or `declarations_full`; 503
`index_unavailable`, `declarations_busy`, `declarations_write_pending` or
`declarations_write_failed_restart_required`; and 500 `store_failed`. The normal visitor read budget
applies.

### Takedowns

The admin can take content down by hash. The host then neither serves nor accepts those exact bytes,
under any post: `GET /api/content/{hash}` and uploads answer 410 `taken_down`
([STORE_API.md](STORE_API.md#get-apicontenthash)), nothing is fetched from peers for them, held
uploads of them are dropped, share cards drop the post's excerpt and link previews are refused to
the post. The check needs only the hash, so it holds while an index is starting or stopped. Every
upload is checked again as the very last step before its bytes are written or held, and every
download after its bytes are read, so a takedown takes effect for every request that has not yet
reached that point. A write already under way when a takedown lands still completes, but those bytes
are never served. It matches exact bytes only: a changed copy has a different hash. Bytes already
stored stay on disk; the host just stops handing them out. Copies cached elsewhere before the
takedown, and copies on other hosts, are beyond its reach.

The list is never published: it is not in the status or the public policy object, and only the admin
reads it. Anyone holding a hash can still learn that it was taken down, by asking for the content.
It lives beside the host policy file, in a file named like `host-policy.takedowns.json`, which must
exist: a host with lists does not start without a readable takedown list, so taken-down content is
never served because the file went missing. A host with none starts it as
`{"revision":0,"hashes":[]}`. It holds at most 4,096 hashes, and a full list refuses new takedowns
rather than dropping any.

`POST /api/host-policy/takedowns` changes the list. The JSON body has `revision` (a decimal string,
the list's own revision), `expires` (Unix seconds), `action` (`add` or `remove`), `hashes` (1–100
nonzero content hashes) and a hex `signature`. The admin signs this exact UTF-8 message, with the
hashes in lowercase, in request order, and no trailing newline:

```text
Host takedown update
Host: <configured origin>
Revision: <revision>
Expires: <expires>
Action: <action>
Hashes:
<hash>
<hash>
```

`POST /api/host-policy/takedowns/read` returns the list to the admin. The JSON body has `expires`
and a hex `signature` over:

```text
Host takedown list
Host: <configured origin>
Expires: <expires>
```

Signatures are checked as for list edits, and the expiry must be between now and five minutes ahead.
Both answer the list as `{"revision": "<decimal>", "hashes": ["0x…"]}`, with `no-store`. An edit is
saved before it takes effect, and a reader disconnecting does not cancel it. The errors include 400
`invalid_takedown_edit`, `invalid_takedown_read` or `invalid_signature`; 403 `not_host_admin`; 404
`host_policy_disabled`; 409 `takedowns_changed`, `takedowns_full` or `takedowns_revision_exhausted`;
503 `takedowns_busy`, `takedowns_write_pending` or `takedowns_write_failed_restart_required`; and
500 `store_failed`. The normal visitor read budget applies.

### Host blocks

A host block refuses reply admission checks, paid-post estimates and uploads with 403
`host_blocked`. The relayer uses the same code for new sponsorship and faucet requests. Outcomes of
existing transactions stay readable. Raw indexed records and content fetched by hash stay public;
blocks don't rewrite ancestry or erase bytes. The app hides blocked authors everywhere posts appear,
and share pages leave out their post metadata.

[spec-7]: https://github.com/blohard/protocol/blob/main/SPEC.md#7-references-and-attachments
[spec-text]: https://github.com/blohard/protocol/blob/main/SPEC.md#message-text
