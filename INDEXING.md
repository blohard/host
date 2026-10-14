# Host indexing

This describes how the gateway indexes the chain. [GATEWAY_API.md](GATEWAY_API.md) defines what
clients see, and [STORAGE.md](STORAGE.md) lists the state the host keeps and what survives a crash
or restart.

In short, each chain has a commonware journal and in-memory indexes that can be rebuilt from it.
Body bytes are stored once and shared by every chain, while what the host learns from a body, and
the quotes it checks, are recorded in the journal of the chain where the post was made.

Every index holds its chain's full history. Hosts that import only recent history may come later;
they would not change anything described here.

## 1. Summary

The host does the indexing work every reader would otherwise repeat, so browsers don't rebuild
feeds, reply trees, counts, gates and references by scanning the chain themselves. The work is
divided like this:

- **The chain records and enforces.** It stays the source of message metadata and of who may post
  what.
- **The host indexes and serves pages.** It reads chain events and checked body references once,
  records them in a journal, and keeps lookup indexes in memory.
- **The app presents and signs.** Drafts, follows, mutes, grouping conversations, navigation and the
  rule that nothing moves on screen unasked all stay in the browser.

There is no SQL database and no index kept in order on disk. The content store keeps body bytes. One
commonware journal per chain keeps the facts needed to rebuild that chain's index. If memory is
lost, the host replays its journal; it doesn't have to read the chain again or parse every body
again.

The costs, accepted on purpose, are memory that grows with the indexed history and a startup time
that grows with the journal. Both should be measured before a host takes on more traffic.

## 2. What the index assumes about the chain

The index accepts a block once it is the configured number of confirmations behind the chain's head,
and assumes accepted blocks stay in the chain during normal operation. That is a choice made for
responsiveness, not a claim that the block is final. On a rollup like Base, the sequencer's
confirmation and the finality that Ethereum provides later are different stages; see the
[OP Stack documentation on finality](https://docs.optimism.io/op-stack/transactions/transaction-finality).

Only accepted blocks enter the index. There is one served state, with no separate unconfirmed tail,
no promotion step and no provisional counts. The rules for holding and serving uploaded content
until it is confirmed still apply. A depth of zero suits throwaway local chains, and is not a
general recommendation.

A quote pins the quoted message's fields that never change, as the protocol spec describes (§7). The
author's app takes the pin from its preview, and the pin is part of the signed body. So quoting
doesn't need to wait for finality: if the quoted message were replaced, readers would see the
mismatch instead of the wrong message. The index checks quotes at the same accepted depth as every
other fact, and requires the pin to match (§5.2). There is no separate wait for finality.

The index doesn't recover from reorganizations by itself yet. When it detects that accepted history
contradicts the chain, it stops all indexed serving on the host. The operator then archives and
replaces all the host's index journals (§5.3), keeping the content. There are no RPC caches to
clear. This accepts some downtime, and rebuilding chains that weren't affected, instead of rolling
back or repairing quotes that span chains.

Other boundaries:

- For now, formats can change without a migration: an incompatible change resets the stores and
  journals, and no reader for older formats is kept.
- The app has no gateway without an index to fall back on, and browsers never scan the chain.
- Every gateway the app uses has an index. Hosts that only store content are still valid.
- Checking bodies by hash, keeping bodies on a best-effort basis, the relayer and wallet signing are
  outside the index. The host only adds hooks for analyzing bodies as they arrive.
- Full-text search, ranking, coordination between hosts and proof that a feed is complete are out of
  scope.
- The rules for grouping conversations and for showing deleted context are not changed by how the
  index stores things. The decisions about what is shown are in §11.

## 3. Identity and ordering

Message ids belong to one chain and one Board deployment. The journal also records the configured
start block and the format and parser versions, and the checkpoint check on every poll catches a
journal whose history isn't this chain's. A chain id alone never safely identifies an existing
index, because chain ids can be reused.

Three values do different jobs:

- **Position:** `(block, logIndex)`, which orders facts within one chain. Message ids increase in
  posting order within a deployment, so they can order lists too.
- **Sequence:** a counter for one chain's index on one host, which also counts references found
  later. The journal fixes the order of the updates, so replay produces the same sequence.
- **Generation:** a random value created with the chain's journal. An ordinary restart keeps it.
  Replacing the journal, as in a rebuild, gives the chain a new generation, which makes old cursors
  invalid.

Pages of history use positions. Refresh uses sequences. A new reference to an old post keeps the
post's old position and gets a new sequence. Sequences can't be compared between chains or hosts, so
a client that merges chains keeps one cursor per chain. Positions and hashes can be compared between
hosts serving the same deployment.

## 4. What is kept on disk and in memory

### 4.1 What is durable

Each chain has one journal, one writer, one checkpoint and one set of in-memory indexes. The chain
followers and the body workers send facts to the right writer through bounded queues. One chain's
write failure or backfill doesn't stop another chain's ingestion. No update writes to two journals:
checking a quote of another chain's post reads that post's durable metadata and records the result
only in the quoting chain's journal.

The journal is commonware's unauthenticated journal of numbered records of varying length. One
bounded update is one journal record. Commonware handles framing, durable writes and dropping an
incomplete record at the end after a crash. The index supplies the versioned record format and
checks its rules.

| Durable fact | Contents |
|---|---|
| Identity | The format version, this chain's generation and the configured deployment |
| Accepted chain data | Normalized `Posted` and gate change events, with their positions and hashes, and messages' timestamps and transaction hashes |
| Body analysis | Keyed by content hash and parser version: whether the body is text, and the references found in it, up to a limit. No body bytes |
| Reference links | Links from a post to what it references, recorded so that repeating one changes nothing, with the commitment the reference requires. For a match, the quoted post's deployment, the position it was read at and its author; otherwise, the mismatch with the accepted metadata |
| Progress | Which update this is, the chain's change sequence it covers and any new checkpoint, including its block hash |

The journal keeps enough metadata that replay needs neither block headers nor bodies. A range of
blocks with nothing relevant can advance the checkpoint without a visible change. An update about a
body or a quote leaves the checkpoint where it is.

Each journal is enough to replay its own chain's index, quote checks included, without opening
another chain's journal. Gates, counts and posting lists have no checkpoints of their own. For now
the journal keeps every fact; snapshots and compaction can come later if measurements call for them.
Dropping old changes from memory doesn't remove facts from the journal.

### 4.2 What is rebuilt in memory

| Structure | Contents or lookup |
|---|---|
| Messages | Metadata that never changes, by deployment and id |
| Posting lists | All messages, top-level posts, by author, by parent, by the parent's author, by mentioned account, by quoted post and by quoted author |
| Gates | Each message's and each author's current gate, with the positions of the events that set them |
| Counts | Direct replies per parent, not counting deleted (`TOMBSTONE`) or removed (`REMOVED`) ones |
| Recovery lookup | Author and content hash → the matching ids |
| Body links | Hash → the messages with that body, its analysis and the quoted posts not yet checked |
| Changes | Recent changes by sequence, up to a limit, with what refresh needs to filter them |
| Progress | This deployment's checkpoint, its published sequence and its generation |

These are ordinary Rust maps and ordered collections. Message metadata stays in memory too, so
serving a page needs no disk reads. Body bytes stay in the content store, not in these structures.

A posting list can seek before or after an id and read a bounded slice. A following feed merges its
authors' lists in order, and combined filters intersect or filter candidate lists. No query builds a
whole history to answer one page. Lists that receive old ids late, references especially, need
ordered insertion, and a tree is the simple choice.

Live ingestion and replay derive gates and counts with the same function. Applying a fact twice
changes nothing, in the lists, the counters and the sequence as well as in the message map. A post
created already deleted, with `TOMBSTONE` as its gate, is classified correctly when it is inserted,
before any later gate event exists.

### 4.3 Publishing an update

1. The writer checks the input against the current state, enforces its limits on events, bytes and
   fanout, and prepares the updates and the range of sequence numbers they will take.
2. It appends the whole update and waits until the journal says it is durable. Syncs happen per
   update, not per post or per upload.
3. It applies the update under exclusive access, and publishes the new sequence and checkpoint
   together. Queries see the state before the update or after it, never halfway.

The writer outlives individual HTTP requests, so a cancelled request can't drop its storage handle.
No disk or network work runs under the memory write lock. Queries copy bounded results and their
`at` marker under shared access, then release the lock before serializing or sending anything.

If a write fails, that chain stops serving indexed reads, rather than serve an update that may not
be durable. A restart may recover a complete update that was written but not yet published, and
applying it is safe. A crash after the sync but before the update reaches memory also recovers the
complete update. A checkpoint can't survive without the facts it covers.

If applying an update breaks one of the model's rules, that chain stops before the write lock is
released, and no reader can use the partly updated model. The journal stays intact for diagnosis,
and replay fails clearly if it meets the same defect, rather than skip the update or serve part of
the history.

## 5. Ingestion and recovery

### 5.1 State transitions

Ordinary ingestion and storage failures affect one chain at a time. A contradiction in accepted
history needs the host-wide stop and the operator rebuild in §5.3.

| State | Event | Result | Test that covers it |
|---|---|---|---|
| Absent | Start | Open an empty journal and write its identity without reaching the chain; report starting | An empty index reports starting |
| Starting | The chain id checks out | Take updates (`Backfilling`) | A slow chain is retried, never marked stopped |
| Starting | The endpoint reports another chain id | Stop that chain before it is established; it accepts no chain updates | A wrong chain can't serve indexed facts; a receipt request checks the endpoint on its own |
| Starting | A journal exists | Replay its complete updates into fresh memory, then check its checkpoint against block headers read directly from the chain | Replay matches the last complete update recovered |
| Starting | A journal can't be opened or replayed | Keep that chain stopped, keep its files, and start the other services | Other chains and downloads of stored content stay available |
| Backfilling | A range succeeds | Commit whole blocks and the checkpoint; publish the update in memory | Repeating a range changes nothing twice |
| Backfilling | It reaches the sampled accepted head | Ready | Queries become available, with progress |
| Ready | New accepted blocks | Commit and publish them in chain order | Gate changes in the same block keep their log order |
| Ready | A new body analysis or quote check | Commit the links and reference changes | An old post appears at a new sequence |
| Backfilling or Ready | An RPC failure, on a range or on one block | Keep the checkpoint and retry with backoff. A failure never counts as proof of a dense block or of replaced history | Failed reads leave no gaps; a block that failed is indexed on the next poll |
| Backfilling or Ready | The provider won't return a block's logs | Halve the range down to that block, then read its Board and registry events from its receipts | A block over the provider's log limit, or over the host's own bound on log responses, indexes the same events |
| Backfilling or Ready | A block has more events than the index takes, or receipts over the response bound | Stop that chain, naming the block; other chains continue | Three events against a capacity of two stop with the block named; receipts over the bound stop |
| Backfilling or Ready | A block's logs are refused, on an endpoint without `eth_getBlockReceipts` | Retry for several minutes, since a refusal may be a rate limit; stop naming the block only if it persists | The chain waits out fifteen refusals and stops on the sixteenth |
| Backfilling or Ready | The last block of a range is replaced while its logs are being read | Retry the range on the next poll: the last block's header is read before the logs and compared after | An empty last block replaced by one holding a post isn't committed without that post |
| Backfilling or Ready | The provider keeps answering a range that fails decoding or checks | Retry with backoff, since a reorg or a lagging replica can look the same; stop, naming the range's start and the reason, if it persists | A post out of order stops the chain on the sixteenth rejection in a row, not before; a transport failure in between neither counts nor resets the count |
| Starting or serving | Accepted history disagrees with the chain's block hashes | Stop all indexed serving and report that an operator rebuild is needed | The mismatch is found without trusting a cached header; no chain keeps serving |
| Any | A journal write fails, or replay finds corruption | Stop and report the error | No partial update is served |
| Serving | Applying an update in memory breaks a rule | Stop that chain's writer and reads, and report the error without poisoning its lock | The partly applied update is unavailable; other chains continue |
| Any | Restart | Replay, check, catch up | Replay itself needs no bodies or headers |

Backfill reads bounded ranges straight from the private RPC endpoint; there is no persistent cache
of log windows. Before advancing, the follower checks the previous accepted checkpoint's hash
against the node, after a restart too. A lagging endpoint is retried, but a contradicting hash that
is returned successfully is never quietly accepted. Asking a provider for history it doesn't have is
not proof of a reorg.

Whole blocks are the unit of progress: updates are bounded in size, and backfill ranges are split at
block boundaries. Gas limits a block's events only through the cost of the log itself. A contract
that toggles a gate or a list membership in a loop pays about 100 gas per repeated storage write, so
one cheap transaction can put more events into a block than a provider will return in one log query.
Such a block is read with `eth_getBlockReceipts`. Ordinary log responses are bounded at 8 MiB, and
the receipts fallback allows 128 MiB. An update holds at most 131,072 relevant events, and a journal
record at most 128 MiB. If a single block still exceeds what the index can take, that chain stops
with the block named, rather than leave out its events or retry forever. Timestamps are fetched once
per relevant block and stored with the message facts.

### 5.2 Bodies and references

Each chain that uses a body records its analysis, by content hash and parser version. Version 1
message bodies are `text/plain`. References are read from the original checked text, which is never
formatted or turned into HTML on the host; formatting happens in the app. Body bytes stay shared.
Two chains may each journal the same small analysis, which keeps their replays independent. Workers
may reuse a checked analysis, but no journal depends on another to recover it.

The parser reads at most eight mentions and quotes, in text order, and then removes duplicate links.
Repeated references count toward the eight. It applies the same rules as the app to addresses,
number ranges and content types. The result is stored even when a body has no references, so a
restart doesn't parse it again.

Hosts and apps share the eight-reference rule, so posts written by any app meet the same limit.
References beyond the eighth are still valid text, but they produce no indexed notifications. This
is not a Board rule, and not a reason to refuse a post or its body.

A body can arrive before its `Posted` event or long after it. One analysis serves every message with
that hash, and each message gets its own links. When a body arrives, the host wakes the work, but
that is not the only way the work happens: backfill, restart and periodic bounded sweeps check
unresolved hashes against the bodies available locally, so a lost wakeup can't leave a durable body
unindexed for good. Temporary failures and missing bytes are retried, never cached as an empty
analysis.

Readers and the body sweep share the timing of retries from peers, keyed by hash and byte limit: 30
seconds after a failed attempt, doubling up to 30 minutes. Misses and errors follow the same
schedule, counted from when the attempt finished, even if the reader left. Every sweep and reader
still checks local storage first, so a late local upload is available without waiting for peers. A
successful fetch from a peer resets the delay. The retry timing and the position of the next peer to
try share an in-memory cache of at most 10,000 entries that expire after an hour; a restart or an
eviction starts a fresh schedule.

Body commits stay batched and best-effort, and the journal and the body store don't share
transactions. A durable analysis can outlive lost body bytes: the reference stays known while
downloading the body can still fail. If a body survives but its analysis doesn't, the host analyzes
it again.

The protocol spec (§7) defines `post://<chain>/<id>?commit=0x…` and the exact hash encoding. The
parser keeps the commitment, and references with different pins are different inputs, even when
their chain and id agree. A malformed pin is never dropped to read the rest as an unpinned
reference. Quote links require pins; unpinned references don't create them. Notifications are
deduplicated only after each reference is checked. The Board sees only the hash of the quoting body,
which already commits to the pin, so enforcing what the pin means is up to readers and indexers.

Every quote, whether of the same chain or another one, is checked against the quoted post's durable
metadata in that chain's ready index, at that chain's configured accepted depth. The quoting chain's
bounded work compares it with the required pin:

- A match journals the link, with the quoted post's deployment, the position it was read at and its
  author. The position names the quoted chain's checkpoint and its block hash.
- A mismatch journals a rejection without a link, so the check isn't retried forever.
- A missing target or a failed read leaves the reference unresolved, to be retried.

Ingesting the quoted chain wakes the same bounded work, so the quoting chain's changes appear even
without a new block or body there. The host doesn't guess at targets it doesn't serve, and doesn't
look up a separate finalized head. It copies the quoted post's metadata under that index's read lock
and releases the lock before writing to the quoting chain's journal, so no operation holds locks on
two chains' indexes.

A quoted chain that is still catching up delays only the quote's link, not the quoting chain's feed
or a plain mention in the same body. Links and rejections make the same assumption about accepted
history as every other indexed fact. A detected replacement stops serving and rebuilds every index,
including these outcomes (§5.3). Pins detect mismatches, but don't repair old links by themselves,
and under this recovery policy no index of dependencies or check of target generations is needed.

The list of quoted authors answers an account's notifications without walking all that author's
posts. Each matching post is listed once, and when the same post both mentions and quotes the
reader, the mention wins.

A host can rebuild chain facts from the chain, but it can't rebuild missing body text from a hash.
How many references the index covers depends on which bodies are available, the parser version and
which quoted chains the host serves, and it is reported separately from chain progress. A gateway
needs a source of bodies before it can offer references, and must not report an empty index as
complete coverage. Hosts that serve the app play both roles.

### 5.3 Replay and rebuild are different

An ordinary restart replays each existing journal and keeps its generation. The host checks each
chain's checkpoint before turning on its indexed serving, then lets each chain catch up on its own.
If a check fails on another chain later, all indexes stop, including ready ones that may have served
quotes across chains. There is no startup barrier for the whole host. Replay itself needs no headers
or bodies. Checking continuity needs headers read directly from the chain, and unresolved bodies and
quotes resume their bounded work (§5.2).

After a detected reorg, or when a chain is intentionally reset, the operator rebuilds:

1. Stop the host, which ends ingestion, queries and background work.
2. Archive every `index-<chain>-<board>` journal partition in the host's data directory, with its
   metadata. Leave the body and attachment stores alone.
3. Restart with the intended configuration. Each empty journal gets a new generation and backfills
   from its configured indexing start, and the available bodies are analyzed again. Clients' old
   cursors need a fresh view.

There are no chain caches to clear or reuse. Even unchanged chains are rebuilt, because their
journals can hold quote links that span chains. An ordinary restart replays the existing journals
instead, and doesn't do this reset.

Separate journals per chain isolate ordinary writes, failures and backfills, but they don't promise
that one chain can recover from a reorg alone. There is no automatic reorg recovery; use the full
rebuild above. The relayer is separate, and stopping the host also stops its content routes.

## 6. Queries

The routes live under `/api/chains/{chain_id}/`. Each result includes
`at: { generation, sequence, checkpoint: { block, hash } | null }` from that chain's published state
in memory. The block is the indexed checkpoint, not the endpoint's newest head. Fields taken from
other chains name their deployment and the position they were read at. There is no host-wide
sequence and no snapshot across chains, so merged views carry one cursor per chain. The identity and
progress in the status let clients compare hosts at the same position on the chain.

Pages of history use opaque cursors that bind the deployment, the generation, the filters, the
direction and the last position examined. A walk through ordinary chain history captures an upper
bound first, so new posts don't move its boundaries. Pages use the current gates at their reported
`at`; they keep no snapshots of past gates. References inserted later behind a cursor arrive through
`changes`.

Change pages move through a bounded, captured range of sequences. They return the next sequence to
scan even when the filter matches nothing. Cursors that are too old, or from another generation, get
an explicit answer that a reset is required. The host keeps a configured maximum number of changes;
when a cursor falls off the end, the client needs a fresh view, rather than silently skipping
changes. A failed request moves nothing forward.

| Route | Inputs | Result |
|---|---|---|
| `feed` | Any of author, parent author and kind clauses, a cursor and a limit | Candidate records with their gates and reply counts, newest first |
| `message/{id}` | Up to 32 ancestors, and an optional commitment to check | The record, and its context nearest first |
| `replies` | A parent, a cursor and a limit | Direct replies, oldest first, plus the parent's record and count |
| `state` | Up to 50 message, account and membership entries | Current snapshots, with counts and gates |
| `list` | A configured registry, owner and list id, and an address cursor | The list's current members, in address order |
| `notifications` | An account, accounts to leave out, a cursor and a limit | Direct replies and references together, without duplicates, newest first, with the reason for a reference |
| `references` | An account, a cursor and a limit | Matching records, with whether each is a mention or a quote |
| `lookup` | An author and content hash, optionally a parent or lowest block, and a cursor | Matching records, deleted ones included |
| `changes` | A generation and sequence, a captured upper bound and a bounded scope | Events, the records and counts they affect, and references, with how far the page reached and whether there is more |

A request returns at most 50 messages in all, not per parent. Authors, ids, bytes and the candidate
work are bounded too. A selective filter may need to look at many more candidates than it returns,
so the host answers with a continuation and says the page is incomplete, rather than promise
constant work for any filter. The app may ask for more candidates until its display rule has enough
rows, within a budget per look. One page from the host is not necessarily twenty conversations.

The app groups conversations and computes any exact count of new visible rows. If a bounded quiet
check can't establish the number, it shows a general cue that there is new activity, rather than
call the host's event count new rows. Polling still never moves content. Refresh results name the
parents and authors affected as well as the changed ids, so deleting a reply updates its parent's
count, and an author's gate change reaches every card by that author.

When the app recovering a post knows the transaction hash, it uses the receipt first. A receipt
tells reverted and missing transactions apart and checks the `Posted` event. An indexed lookup
replaces only the search by author. A miss in the index proves neither that a post was refused nor
that its transaction was never sent. A message just posted waits until the index covers its receipt
block before its absence counts as an ordinary not-found.

The app never applies an older response over newer knowledge it is showing. An action that has
landed gives a minimum block for the state it affects until the host catches up, so a stale page
can't undo it.

The browser uses Ethereum only through its wallet, to sign and submit. The host owns admission
checks for posting, post estimates and decoding receipts, as narrow endpoints separate from the
in-memory queries. Indexed reads never run gates. The configured registries are indexed, including
the host's member list and the shared blocking registry, so browsers never replay a registry.

## 7. Formats

The public envelope, the reference parser and the journal format are all at version 1. For now,
there is no compatibility path or migration of old data: an incompatible change resets the body
stores and the index journals, and an ordinary restart replays the existing journals (§5.3). The
app's tests use snapshots, pages and changes, and the host's deterministic tests cover ingestion,
replay and storage failures.

## 8. Trust

The gateway is trusted to answer for the chain, and to answer completely. Checking a hash proves
that bytes match a given commitment, not the gateway's claim that the commitment belongs to a
particular author or post.

Checking `getMessage` through the same host is a consistency check, not an independent one. No
automatic random spot checks are required. An independent RPC endpoint can check claims about
records that never change, when wanted, but it still can't prove a feed or a list of references
complete.

Compare hosts by deployment and block hash. State derived from the chain should agree at the same
position. References agree only if the hosts have the same body analyses, parser version and quoted
metadata. Local sequence numbers need not agree.

## 9. Operations and limits

Each chain in `/api/status` reports its own `index` object. When a chain leaves backfilling, the
host logs one `index ready` line with its message count, checkpoint block and sequence, so the time
it took can be read from the log instead of by polling:

```text
index: {
  at: { generation, sequence, checkpoint: { block, hash } | null },
  state: starting | backfilling | ready | stopped,
  messages, pending_bodies, pending_quotes
}
```

Indexed queries answer 503 until the startup check and the backfill finish. An ordinary backfill or
storage failure can leave other chains available. A journal that fails to open or replay at startup
also leaves other services available, and that chain stays stopped until it is repaired and the host
restarts. A chain stopped at startup has no usable snapshot and never accepts writes. A mistake in
the local configuration is still a startup error.

The follower checks each endpoint's chain id before setting up its journal: transport failures are
retried while starting, and a wrong id stops that chain. Admission checks and estimates need the
chain to be backfilling or ready. Receipt recovery checks the endpoint directly and doesn't depend
on the index. A contradiction in history marks every chain stopped, so recorded quote links across
chains can't outlive the history they assumed. The status itself answers 503 when any index is
stopped. The counts of pending bodies and quotes report unfinished reference work separately from
the accepted checkpoint. Process memory and other timings are for the operator to measure; the
status API doesn't promise them.

Ingestion queues, update sizes, analysis workers, the work per response and the kept change history
all have bounds. Memory for records and posting lists grows with history: the index doesn't claim
constant memory, and doesn't quietly drop old messages. Planning capacity is part of running a host,
so measure the process's memory and the index sizes. Limits per update stop an oversized range
without advancing its checkpoint; there is no configured cap on total memory.

There is no snapshot format, no posting lists on disk, no compression and no automatic journal
compaction. Each should be added only for a measured bottleneck. The deployment configuration and
the journal's format identity must stay the same across restarts; an incompatible change needs an
explicit rebuild.

## 10. Measurements

A cold home page takes one request to the host, and a thread with twenty replies takes two, with no
upstream calls for history that is already indexed. Ingestion is timed separately. Notifications use
indexed references, and recovering a pending post uses receipts or a bounded indexed lookup.

On the test deployment on 2026-09-07, a fresh index of 3,020 messages over about 10,600 blocks on
two chains became ready in 1.6 seconds, using 3,045 chain calls, about one per block with events.
Its peak resident memory was 1.1 GB, which was page cache. Idle ingestion at the default poll of one
second cost about eight chain calls a second for two chains, one of them producing a block a second.
A restart brought both indexes back to ready from the journal in under a second.

Once caught up, indexed reads need no chain calls in the foreground for their indexed fields.
Background ingestion, body fetching and wallet operations still make calls. Count requests after the
app's real grouping and paging; one feed request doesn't always produce twenty visible rows.

## 11. Decisions

One commonware journal and writer per chain, in-memory indexes that can be rebuilt, body bytes
shared by every chain, and reference facts kept with the posting chain. No SQLite, no ordered index
on disk, no unconfirmed tail, no extra wait for quote finality and no compatibility path. Snapshots
and faster reorg recovery wait until measurements show a need.

- **Counts and deleted context.** Counts include only direct replies that aren't deleted or removed.
  Reply pages, feeds and references leave out deleted and removed messages. Replies below a deleted
  or removed reply don't move up to take its place. A reply that a feed selects on its own still
  gets its real ancestors as context. Grouping and personal choices stay in the browser.
- **Membership.** The host indexes events from the configured member and blocking registries and
  serves point lookups and explicit list pages. A registry that isn't configured is refused; there
  is no general registry RPC to fall back on.
- **References on a gateway without a store.** The index reads bodies from its local content store.
  A gateway without the store role keeps bodies pending and doesn't claim full reference coverage. A
  separate service that supplies bodies may come later. Hosts serving the app play both roles, and
  browsers never scan as a fallback.
