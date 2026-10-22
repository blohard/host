# Host storage

This is an inventory of the host's state: what each structure holds, who changes it and what
survives a restart. [INDEXING.md](INDEXING.md) explains the design of the index, and
[STORE_API.md](STORE_API.md) and [GATEWAY_API.md](GATEWAY_API.md) define what clients can see.

The host keeps three kinds of state:

| Kind | What it is for | After a restart |
| --- | --- | --- |
| A journal per indexed chain | A durable record of the chain events the index accepted, plus what it learned from bodies and quotes | Replayed to rebuild the whole in-memory model |
| The body and attachment stores, shared by all chains | The records themselves, which never change, addressed by their hash | Opened at their last durable checkpoint |
| Temporary memory | Uploads waiting for confirmation, shared requests, retry schedules and name caches | Starts empty. Clients retry, and background work resumes from the durable facts |

The gateway role owns the indexes and names. The store role owns the content stores, the holding
area for unconfirmed uploads, and copying from peers. A host may play either role or both.

The chain stays the authority for posts and moderation. The journal is the host's own durable record
of what it accepted, not another authority, and the host derives its feeds and counts from that
record. The content stores are different: they hold bytes the chain doesn't, so if they are lost,
the content must come back from a client or a peer.

## Files on disk

The storage library, [commonware](https://github.com/commonwarexyz/monorepo), owns the files under
`--data-dir`. The names below are the host's **partition names**. Each partition is a set of files
the library manages, and their individual file names are not part of this description. Keep each
partition's files together.

| Partition | Keys and values | Owner |
| --- | --- | --- |
| `index-<chain>-<board>` | Record number → the journal's identity in record zero, then the index's updates | The chain's one index writer |
| `blohard-cdn-freezer-keys` | The Freezer's key index for body hashes | The body store |
| `blohard-cdn-freezer-values` | The Freezer's value journal, holding complete body records | The body store |
| `blohard-cdn-freezer-table` | The Freezer's on-disk hash table, which locates keys and values | The body store |
| `blohard-cdn-freezer-commits` | One key, `U64::new(0, 0)` → the Freezer's latest checkpoint | The body store |
| `blohard-cdn-attachments-freezer-{keys,values,table,commits}` | The same four structures, for attachments | The attachment store, when enabled |
| `keyring-<chain>-<market>` | Record number → the journal's identity in record zero, then accepted ranges of market blocks | The keyring writer, when enabled |

A Freezer is commonware's store for values that are written once and never change. The content
stores' partitions keep the program's earlier name, `blohard-cdn`, so existing stores still open.

Bodies and attachments are shared by every configured chain. Each store maps the keccak-256 hash of
a complete record to the record's bytes, including the envelope header, not just the text or image.
The store computes the hash itself, and storing a hash it already has does nothing. The upload and
peer paths check that a record is anchored on chain before it reaches the store; the store itself
doesn't check authors.

There are no separate durable tables for messages, gates, counts, list members or references. Those
live in memory and are rebuilt from the index journal. The host also keeps no durable cache of log
windows, block headers or message reads.

Sources: [store.rs](src/store.rs), [index/journal.rs](src/index/journal.rs),
[server.rs](src/server.rs).

## The index journal

Record zero holds the format version (1), the deployment's identity and the cursor generation. The
identity is the chain id, the Board's address, the Board's start block, the index's start block and
the configured registry addresses. The generation is a random value chosen when the journal is
created. It changes only when the journal is replaced, and that makes old refresh cursors invalid. A
new journal writes record zero when it opens, and an existing journal keeps its generation across
restarts.

The reference parser and the public `version/1 text/plain` envelope are at version 1 too. No other
format can be read and nothing is migrated: an incompatible change clears the body stores and the
index journals. Ordinary restarts keep the version 1 journals and replay them.

Every later record is one of these updates:

| Update | What it stores | Written by |
| --- | --- | --- |
| `Chain` | The range's first block, its last `(block, hash)` checkpoint, and the posts, reply gate changes, author gate changes and list membership changes in it, in order | The chain follower |
| `Analyze` | A body's hash, the parser version, and the mentions and quotes found in it | The body worker |
| `Bind` | The ids of messages now linked to their body's stored analysis | The body worker |
| `Resolve` | A message and the pinned quote in it, with the quoted message's deployment, its accepted position and the complete quoted message | The quote worker |

A range of blocks with no relevant events moves the served checkpoint forward in memory without a
journal record, until `CHECKPOINT_BLOCKS` (300) blocks have passed since the last journaled
checkpoint. The next `Chain` record starts just after the last journaled block, so it also covers
the empty ranges applied in memory since, and replay never skips a block. A clean stop journals the
in-memory checkpoint. A crash loses at most 300 blocks of empty progress, which the follower simply
reads again. So the checkpoint a host serves can move back by up to that much after a crash, but
never after a clean restart.

The record number, the block number and the change sequence clients see are three different things.
A body discovered late can advance the change sequence without moving the chain checkpoint.

Each writer takes commands through a queue of eight. For each update it checks it, appends it, syncs
the journal, then applies it to memory under that chain's write lock and acknowledges it. No network
or disk work happens while the memory lock is held. Once an update is queued, it belongs to the
writer: if the caller goes away, only the acknowledgement is lost, never the append or the sync.

A record can be at most 128 MiB, and a chain range at most 131,072 events. These limits bound single
writes, not the total history. The journal is split into sections of 1,024 records, with a 64 KiB
write buffer, a 64 KiB replay buffer and a page cache of 64 pages of 4 KiB. Nothing prunes, compacts
or snapshots the journal yet, so its size on disk and the time to replay it at startup grow with the
accepted history, including moderation and reference updates.

## The in-memory model

An `Index` holds one `View` per chain behind a read/write lock: a `Model` and the chain's current
state. Only the writer changes the model. Queries take a read lock and see all its structures at one
consistent moment; they keep no copies of their own of gates or counts.

In the table below, ids are a chain's message ids, addresses are 20 bytes and hashes are 32 bytes.
The exact types are in [index/types.rs](src/index/types.rs).

| `Model` field | Key → value | Used for |
| --- | --- | --- |
| `deployment` | The deployment's identity | Checking replay and the chain, and identifying cursors |
| `at` | The generation, the change sequence and the accepted `(block, hash)` checkpoint, if any | Consistent query positions and refresh |
| `messages` | Id → `Message` | Reading records, ancestry, snapshots and searches during recovery |
| `authors` | Author → their message ids, in order | Author feeds and bounded searches during recovery |
| `parent_authors` | The author of the parent → the ids of replies to them, in order | Replies to an account's messages |
| `children` | Parent id → the ids of its direct replies, in order. Key zero holds the top-level posts | Threads and top-level feeds |
| `author_gates` | Author → their current default gate. No entry means none | Each message's effective reply gate, and the author's state |
| `counts` | Parent id → the number of direct replies the host counts | Reply counts |
| `lists` | `(registry, owner, list hash)` → the list's members, in order | List membership and list pages |
| `changes` | A queue of `(sequence, event)` | Refresh. Only the latest 250,000 changes are kept, at 176 bytes each, about 44 MB |
| `references` | The structures listed in the next section | Mentions, pinned quotes and pending body work |

A `Message` holds its `id`, `author`, `parent`, `parent_author`, `timestamp`, `content_hash`,
transaction hash, `(block, log index)` position and its own current `gate`. It holds no body bytes.
The quote commitment, which never changes, is computed from the author, parent, timestamp and
content hash, so it isn't stored separately.

Deleting a message doesn't erase it or take it out of these lists. Queries apply the visibility
rules instead. Counts leave out deleted and removed direct replies. Each post, deletion or removal
updates its parent's count in constant time, and replay rebuilds the same counts. There is no count
of all descendants. List membership covers every configured registry, not just the host's curated
list.

Apart from the change queue, these structures have no total size limit and nothing in them expires.
They grow with the indexed messages, accounts, lists and references. Dropping an old change from
memory doesn't remove its journal record. A cursor older than the kept changes needs a fresh query.

Sources: [index/model.rs](src/index/model.rs), [index/query.rs](src/index/query.rs).

## References and pending work

These all live in the posting chain's `Model.references`. Replay rebuilds them, pending work
included, so there is no separate durable job queue.

| Field | Key → value, or what the set holds | Meaning |
| --- | --- | --- |
| `analyses` | Content hash → `Analysis { hash, parser, references }` | What one body says, shared by every post with that body on this chain |
| `pending_bodies` | Content hashes | Bodies not analyzed yet |
| `ready_bodies` | Content hashes | Bodies analyzed, with posts still waiting to be linked to the analysis |
| `unbound` | Content hash → the ids of posts waiting for that body's analysis, in order | Removing an id here is also what makes linking the same post twice do nothing |
| `pending_quotes` | `(post id, Quote)` | Quotes whose target hasn't been checked yet |
| `outcomes` | `(post id, Quote)` → the commitment found, and the target's author if it matched | A match stores the author. No author means the quote was rejected, not that it is pending |
| `accounts` | An account → the ids of posts that mention it, in order | Explicit mentions, and quotes that match the account's posts |
| `quotes` | `Quote` → the ids of posts that quote it, in order | Quotes that match one pinned message |

A `Quote` is `(target chain, target id, commitment)`. Version 1 of the parser looks at no more than
eight references in a body, and malformed or repeated ones count toward the eight. A body the parser
can't read still gets a finished analysis, with no references.

A journaled quote check carries the quoted record it used, so replaying one chain never needs
another chain's journal. Body bytes are shared across chains, but each chain records its own
analyses and links. Nothing coordinates writes across the chains' journals.

The background sweep handles at most 32 items per batch. It keeps three private cursors in memory,
`body_after`, `binding_after` and `quote_after`, to take turns fairly through the work. They are not
progress checkpoints, and losing them is harmless. The sweep checks local bytes before asking peers,
and failed peer fetches use the shared backoff described below. A missing body or an unavailable
quote target stays pending with no time limit, so although each pass does bounded work, the set of
pending work has no bound.

Work that just became possible doesn't wait its turn behind that pending set. When an upload or a
peer copy stores a body, even bytes the store already had, or a held body is promoted, every chain's
index that still has work for it notes it as an arrival. Each index keeps at most 1,024 arrivals in
memory, oldest first and each hash once. A pass takes up to 24 of them first: it reads their bytes
from the local store alone, analyses them and binds their posts, one from each arrival in turn. An
arrival with posts still unbound goes back to the end of the queue, keeping its priority until they
are all bound; one whose bytes can't be read leaves it. The round-robin keeps at least 8 of each 32,
and only its own work moves its cursors. Every post bound in a pass has its quotes tried in that
same pass, in updates of at most 32. So new work waits for the next pass: three seconds when the
sweep is idle, longer while a pass waits on slow peer fetches, each limited to 30 seconds. Arrivals
past the limit or lost on restart, and quotes whose target is indexed later, still wait for the
round-robin.

Sources: [index/references.rs](src/index/references.rs), [index/bodies.rs](src/index/bodies.rs).

## How the content stores stay durable

Each `ContentStore` keeps its Freezer and its checkpoint together behind one async read/write lock.
It also keeps a `dirty` flag, meaning bytes have been accepted since the last finished commit. Reads
share the lock; writes and commits take it alone.

Each write runs in its own task, so a caller that stops waiting can't interrupt a write halfway. If
a write or a commit fails, the store becomes unavailable rather than carrying on in an uncertain
state.

A commit first syncs the Freezer, then saves the checkpoint it returns in the store's commits
partition. At startup the host reads that checkpoint before opening the Freezer, and discards
anything written after it. **Without its checkpoint, a Freezer opens empty.** So a content store's
four partitions must be kept together. Copying a running store's files one by one is not a
consistent backup: take a coordinated snapshot, or stop the host first.

Commits run every three seconds on their own timer, and at a clean shutdown. A slow commit can take
longer than that, so what a crash can lose is everything since the last finished checkpoint, which
may be more than three seconds' worth. A successful upload deliberately doesn't wait for its own
commit.

A body can be at most 1 MiB, including its envelope. Attachments have their own configured limit, at
most 16 MiB. Each enabled store has its own key page cache, 64 MiB by default, and its own 1 MiB
write buffers for keys and values. The on-disk table grows with the stored hashes; the host keeps no
full map from hashes to file positions in memory. Neither store evicts content or caps its total
size on disk. Deleting or removing a message on chain doesn't delete its bytes from either store.

## Holding area and caches

Everything here starts empty after a restart. Losing it affects availability, speed or when retries
happen, never the chain facts the journals replay.

| Structure | Key → value | Limits and lifetime |
| --- | --- | --- |
| `Holding.entries` | `(chain id, content hash)` → the complete record, its kind, the body it belongs to, the signer who uploaded it, its transaction hash or message id if given, when it arrived, its receipt retry state, the last accepted block checked and its backoff | By default 64 MiB and 4,096 records in all, 16 records and 12 MiB per signer per chain, kept for 30 minutes |
| `Holding.bytes` | The total bytes held | Updated under the same lock as the entries |
| `Peers.progress` | Content hash → the next peer to ask, the earliest next attempt and the current delay | 10,000 entries, kept for an hour. Failed fetches back off from 30 seconds to 30 minutes |
| `Peers.flights` | Content hash → a fetch in progress, which other requests can join | Eight fetches at once, five seconds per peer and 30 seconds in all |
| `Peers.vouches` | `(chain, message id, content hash)` → whether that message anchors that hash | 10,000 entries, kept for ten seconds. Used by store-only hosts |
| Index arrivals | Hashes of bodies just stored that the index still has work for | At most 1,024 per chain, oldest first and each once. Lost on restart, when the round-robin finds that work instead |
| `Sweep.missing` | A body or attachment hash → the message that vouches for it, and which kind it is | At most 100,000 per chain. An entry goes once the record is stored or this host refuses it. Four are copied per pass, in turn, skipping those backing off |
| `Peers.background` | Two turns for background copying | Shared by every chain's sweep, so readers keep six of the eight fetch slots |
| The sweep's walk cursor | The last message id visited by the walk that runs once per process | Lost on restart, which starts the walk over the store again. It notes analyzed bodies the store has lost, and the attachments of the bodies it has |
| `Names.cache` | Address → a name, its source and an avatar link, or no name | 100,000 entries. A name is kept for a day, no name for an hour, and failures aren't kept |
| `Names.flights` | Address → a lookup in progress, which other requests can join | 24 lookups at once, at most 96 waiting, 30 seconds in all |

The holding area is shared by every chain and never serves unconfirmed bytes. Uploading the same
record again neither extends its life nor counts against the quotas again. The watcher moves
accepted bodies, and the attachments they allow, into the content stores. It drops entries that
expire, whose transactions reverted, or that don't match.

Receipt checks wait ten seconds at first, then back off up to one minute; that schedule is kept with
the held body. On a store-only host, a held body is also read from the Board at most once per
accepted head. If it isn't accepted yet, it isn't read again until the head moves, and a failed read
backs that body off from six seconds to a minute, counted from the failure. Moving a body into the
store is not a commit. A restart loses the holding area, so clients must upload again.

Adding an attachment and the final removal of its body from the holding area take the same lock. So
a late attachment either keeps the body there for another pass, or is refused and can be retried
after the body has moved. Nothing in that handoff does I/O, and an interrupted pass leaves
unfinished entries for the next one. A malformed receipt answer keeps the held bytes: only a valid
receipt can show that a transaction reverted or posted nothing. Bodies that are too large are
refused before they are held, rather than left waiting for a write that can't succeed.

`acceptance::Acceptance` tells promotion and upload admission which commitments are accepted. An
indexed host requires its index to be ready and never goes around it when it fails. A store-only
host shares one lookup of the confirmed head per chain per pass. A provisional upload still reads
`latest` when acceptance hasn't caught up. Peer hints on a store-only host may also use `latest`,
but fetched bytes go through the same upload checks before they can be served.

The name and peer caches use `expiring::Expiring`, a cache of fixed size whose entries expire a set
time after they were added. When it is full, it evicts single entries using the CLOCK algorithm, an
approximation of least recently used, rather than clearing everything. Reading an entry doesn't
extend its life. Eviction or a restart can forget a peer's cooldown, so backoff is a bounded cache
of retry state, not a promise never to retry sooner.

Peer and name fetches keep running when the request that started them disconnects, so other requests
can join them. `flights::Flights` holds the shared key and the capacity until the work finishes, and
cleans up even if it panics. Callers update the cache or the backoff before releasing the key.
Results in flight are not a permanent cache. Name entries hold avatar links, not downloaded images.
Content fetched from a peer goes through the same anchoring checks into the right store. A body
uploaded locally can be used right away, even while fetching it from peers is cooling down.

Sources: [holding.rs](src/holding.rs), [watcher.rs](src/watcher.rs),
[acceptance.rs](src/acceptance.rs), [peers.rs](src/peers.rs), [names.rs](src/names.rs),
[expiring.rs](src/expiring.rs) and [flights.rs](src/flights.rs).

Some other state in memory controls work rather than storing facts:

- The configured chains, registries and peers, and the map from chains to their `Index`, are built
  from the configuration at startup. They aren't stored anywhere else.
- An upload's buffer is charged to its pool's memory as it arrives: 32 MiB for uploads no larger
  than the body limit, and 64 MiB for larger ones. Once its bytes are in, it takes one of its pool's
  slots, 16 or 32. Once buffered work starts, its task keeps both even if the caller goes away.
- `Upstream` keeps one token balance and refill time for every chain and name lookup, plus 28
  permits for visitors' RPC calls and four for indexing. The reserve for indexing is a rule about
  the token balance. A restart refills the balance; it isn't a billing record. See the
  [upstream budget](STORE_API.md#shared-upstream-budget).
- Each follower remembers a dense block its provider refused, and how many times in a row. Its
  durable progress is still the model's checkpoint. The watcher's map of chain heads lasts for one
  pass.
- Response buffers, decoded batches of logs and receipts, and each query's selections last only for
  that one operation. Their size limits don't limit the history the index keeps.

## Restarts and consistency

| What happens | Result and recovery |
| --- | --- |
| The caller goes away before an index command is queued | Nothing changes hands, and the caller may retry |
| The caller goes away after queuing it | The writer finishes on its own, and replay includes any finished journal write |
| A crash after the journal sync but before the update reaches memory | Replay applies the durable update. There is no separate memory checkpoint to repair |
| A crash with empty progress only in memory | Replay resumes at the last journaled checkpoint, at most 300 blocks back, and the follower reads those blocks again. A refresh pinned to the lost checkpoint is refused until the index passes it again |
| A journal write or sync fails | The chain stops serving indexed state. A restart reopens and replays the journal, or fails clearly if it needs repair |
| Applying an update breaks one of the model's rules | That chain stops before a partly updated model can be served, and the same replay fails at startup for that index |
| A body write was acknowledged, but no checkpoint covers it yet | Those bytes can be lost. Anyone holding the same record can upload it again |
| A body is durable, but its analysis or link isn't journaled yet | Replayed pending work finds it in the local store and finishes |
| An analysis is durable, but the body's bytes were lost before their checkpoint | Mentions and quotes can stay available while the content is missing. An upload or a peer restores the bytes |
| The holding area or request caches are lost | Clients retry, and the work is redone from chain facts or new requests |

Body commits and attachment commits are separate, and neither is atomic with an index update. The
design doesn't need them to be: extracting references can be repeated safely, and the index doesn't
rely on being told about uploads. An index can't recreate missing bytes, and a finished analysis
doesn't prove a local copy is still durable.

When a journal opens, it is replayed into a `Starting` view. The follower then checks the endpoint's
chain id, moves to `Backfilling`, checks the accepted history and catches up before becoming
`Ready`. An ordinary failure to open or store can stop one chain while the others keep serving. A
proven contradiction in a chain's history is broader: it stops every index, because quote checks can
depend on other chains. The operator then rebuilds the affected history, as described in
[INDEXING.md](INDEXING.md#53-replay-and-rebuild-are-different). The host can't roll back history
while running.

For backups and rebuilds:

- Keep each content store's four partitions together, checkpoint included. Bodies and attachments
  are separate sets.
- Keep each chain's whole journal, to keep its generation and to restart without rescanning history.
  The in-memory tables need no backup.
- Replacing a journal creates a new generation, so clients' old cursors stop working. If a chain's
  history was replaced, follow the coordinated rebuild instead of keeping old quote checks that span
  chains.
- The content stores can outlive an index rebuild, which they speed up. They don't decide which
  posts belong to the rebuilt chain.

The growth to watch is the index journals' disk space and replay time, the memory used by the model
and reference tables, and the content stores' disk space. The holding area, caches and work batches
have their own fixed limits, which are not a limit on the host's total storage.

## The optional keyring journal

A configured account-keys market has its own commonware journal, in the partition
`keyring-<chain>-<market>`, next to the Board's index. Record zero is its identity: the format (1),
the configured chain, the market and its deployment block, written when the journal opens. Every
later record is one accepted range of blocks: its first block, its last block and that block's hash,
and the market's `Activated`, `Trade` and `KeysTransferred` events in log order, each with its
block, log index, transaction hash and block time. Each record starts at the block after the
previous record's last block.

A single follower reads the market's events from the configured deployment block, in bounded ranges,
using the shared upstream budget and the chain's confirmation depth. It checks that logs are in
order, that none repeat, the block hashes and that integers are encoded canonically. Before
submitting a range, it checks the range's end and the saved checkpoint again.

The writer then checks the range against the in-memory model, plus a scratch copy of only the
supplies and balances it touches: activations, trades before activation, changes of supply and
balances that never go below zero. Transfers move two balances and leave the supply unchanged. It
appends and syncs the record with no lock held, and only then applies it to memory under the write
lock, so readers only ever see whole, durable ranges. A gift of keys counts for membership exactly
like a purchase.

An empty range moves the checkpoint forward in memory without a record, until `CHECKPOINT_BLOCKS`
(300) blocks have passed since the last journaled block. The next record starts just after that
block, covering the empty ranges applied in memory since. A clean stop first ends the follower's
polling, then journals the in-memory checkpoint. The writer takes commands in order, so that record
always comes after any range a poll already submitted. A crash loses at most 300 blocks of empty
progress, which the follower reads again; ranges with events are never lost.

The public activity feed also keeps activations, purchases and transfers in chain order in
memory, including self-purchases. An activation and its immediately following self-purchase
in the same transaction form one card at the purchase's position. Activations with no purchase
have an amount of zero. Its memory use grows with that history. Replay rebuilds it from the
existing range journal; this adds no journal fields or format change.

In memory the keyring holds the activated markets and their supplies, positive balances grouped by
creator, the reverse map from holders to markets, each creator's latest 100 trades by others for
notifications, and the checkpoint. Replaying the journal rebuilds all of it. The model and its state
share one lock, so a reader checks readiness under the same lock. Transfers don't create trade
notifications. Blocked accounts are filtered out when reading, without destroying history.

Replay stops, and the host runs without a keyring, if the identity names another market, format or
deployment block, if a record doesn't decode or contradicts the history before it, or if the journal
reports corruption inside records that were synced. An unfinished tail record that was never synced
is dropped. While running, the keyring stops if the endpoint reports another chain id, if history
below the checkpoint was replaced, if a fetched range contradicts itself, or if an append or sync
fails. Nothing from the offending range is served, and the Board's index keeps serving. To rebuild
it, stop the host, move only that market's `keyring-*` partitions aside and restart; the host
rebuilds from the deployment block. Provider errors, malformed answers and blocks that change during
a fetch can be retried. A retryable error halves the range of blocks read, down to one block, and
each successful range doubles it again, up to 2,000 blocks. A block the provider refuses as logs
however narrow the range is read from its receipts, as the Board's index does, under the same 128
MiB bound and with every receipt checked against the block's header. A block that cannot be read
that way stops the keyring with a different remedy: receipts over the bound, or an endpoint without
`eth_getBlockReceipts` on the sixteenth refusal in a row, leave the history taken so far intact, so
the operator changes the endpoint or the limit and restarts, keeping the journal. Transport failures
neither count toward those sixteen nor clear the count; a range taken in does.

The journal keeps every market event. Its size follows the market's activity on chain, plus one
empty checkpoint record per 300 idle blocks. Nothing prunes or snapshots it yet.
