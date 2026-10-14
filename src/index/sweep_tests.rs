//! The body sweep against a real index: work that just became possible is
//! done in the pass that finds it, not after a backlog that may never clear.
use super::{Copied, Host, Index, bodies::Sweep, model::*, references::*, types::*};
use crate::chain::{Address, Checkpoint, Hash, Hex, Position};
use crate::keccak::Digest;
use commonware_runtime::{Runner as _, Supervisor as _, deterministic};
use std::{collections::HashMap, sync::Mutex};

const CHAIN: u64 = 42;

fn hash(n: u8) -> Hash {
    Hex([n; 32])
}
fn deployment() -> Deployment {
    Deployment {
        chain: CHAIN,
        board: Hex([42; 20]),
        start_block: 1,
        index_start_block: 1,
        registries: Vec::new(),
    }
}
fn body(text: &str) -> (Hash, Vec<u8>) {
    let bytes = format!("version/1 text/plain\n{text}").into_bytes();
    (Hex(crate::keccak::hash(&[&bytes]).0), bytes)
}
fn message(id: u64, block: u64, content_hash: Hash) -> Message {
    Message {
        id,
        author: Hex([7; 20]),
        parent: 0,
        parent_author: Address::ZERO,
        timestamp: 100,
        content_hash,
        transaction: hash(3),
        position: Position {
            block,
            log: id as u32,
        },
        gate: Address::ZERO,
    }
}
/// A quote of `target`, as a body writes it.
fn quoting(target: &Message) -> String {
    format!(
        "post://{CHAIN}/{}?commit={} ",
        target.id,
        target.commitment()
    )
}

/// A host whose store holds `bodies` and that has no peers. With
/// `copy_fails`, asking through the copy path, which may wait on peers,
/// fails instead.
struct Local {
    verifier: crate::verifier::Verifier,
    indexes: HashMap<u64, Index>,
    bodies: Mutex<HashMap<Hash, Vec<u8>>>,
    copy_fails: bool,
}
impl Local {
    async fn open(ctx: &deterministic::Context) -> Self {
        let index = Index::open(ctx.child("index"), deployment(), hash(2))
            .await
            .unwrap();
        index.establish().unwrap();
        Self {
            verifier: crate::verifier::Verifier::AllowAll,
            indexes: HashMap::from([(CHAIN, index)]),
            bodies: Mutex::new(HashMap::new()),
            copy_fails: false,
        }
    }
    fn index(&self) -> &Index {
        &self.indexes[&CHAIN]
    }
    fn store(&self, (hash, bytes): (Hash, Vec<u8>)) {
        self.bodies.lock().unwrap().insert(hash, bytes);
    }
    /// Index `messages`, all in `block`.
    async fn post(&self, block: u64, messages: Vec<Message>) {
        let events = messages
            .into_iter()
            .map(|message| Event::Posted { message })
            .collect();
        self.index()
            .update(Update::Chain(Batch {
                from: block,
                through: Checkpoint {
                    block,
                    hash: hash(4),
                },
                events,
            }))
            .await
            .unwrap();
    }
    /// `body` comes into the store, and the index hears of it.
    fn arrive(&self, body: &(Hash, Vec<u8>)) {
        self.store(body.clone());
        self.index().body_arrived(body.0);
    }
    /// Analyse `bodies`, as earlier passes would have.
    async fn analyse(&self, bodies: &[(Hash, Vec<u8>)]) {
        for chunk in bodies.chunks(WORK_BATCH) {
            let analyses = chunk.iter().map(|(h, b)| analyse(*h, b).unwrap()).collect();
            self.index()
                .update(Update::Analyze(analyses))
                .await
                .unwrap();
        }
    }
    /// Analyse `body` and bind every post that carries it, as earlier
    /// passes would have.
    async fn analyse_and_bind(&self, body: (Hash, Vec<u8>), ids: impl IntoIterator<Item = u64>) {
        self.analyse(std::slice::from_ref(&body)).await;
        let ids = ids.into_iter().collect::<Vec<_>>();
        for chunk in ids.chunks(WORK_BATCH) {
            self.index()
                .update(Update::Bind(chunk.to_vec()))
                .await
                .unwrap();
        }
    }
    fn references<T>(&self, read: impl FnOnce(&References) -> T) -> T {
        read(&self.index().view.read().unwrap().model.references)
    }
}
impl Host for Local {
    fn verifier(&self) -> &crate::verifier::Verifier {
        &self.verifier
    }
    fn indexes(&self) -> Option<&HashMap<u64, Index>> {
        Some(&self.indexes)
    }
    fn has_bodies(&self) -> bool {
        true
    }
    fn has_peers(&self) -> bool {
        false
    }
    fn attachment_limit(&self) -> Option<usize> {
        None
    }
    async fn stored_body(&self, digest: Digest) -> anyhow::Result<Option<bytes::Bytes>> {
        Ok(self
            .bodies
            .lock()
            .unwrap()
            .get(&Hex(digest.0))
            .cloned()
            .map(Into::into))
    }
    fn peer_waiting(&self, _: &Digest, _: crate::store::Kind) -> bool {
        false
    }
    fn blocked(&self, _: &[u8; 20]) -> bool {
        false
    }
    async fn copy(
        &self,
        _: u64,
        _: u64,
        digest: Digest,
        _: crate::store::Kind,
    ) -> anyhow::Result<Copied> {
        anyhow::ensure!(!self.copy_fails, "peer unreachable");
        Ok(match self.bodies.lock().unwrap().get(&Hex(digest.0)) {
            Some(bytes) => Copied::Stored(Some(bytes.clone().into())),
            None => Copied::Missing,
        })
    }
}

/// 128 posts quote eight posts that never exist: 1,024 quotes that will
/// never resolve, all ahead of a new one in the round-robin. The new post's
/// quote of a post this host has is resolved in the pass that binds it.
#[test]
fn a_new_quote_is_resolved_in_the_pass_that_binds_it() {
    deterministic::Runner::default().start(|ctx| async move {
        let host = Local::open(&ctx).await;
        let target = message(1, 1, hash(10));
        let never = (0..8)
            .map(|n| format!("post://{CHAIN}/{}?commit={} ", 1_000_000 + n, hash(8)))
            .collect::<String>();
        let never = body(&never);
        let mut first = vec![target.clone()];
        first.extend((2..=129).map(|id| message(id, 1, never.0)));
        host.post(1, first).await;
        host.analyse_and_bind(never, 2..=129).await;
        assert_eq!(host.references(|r| r.pending_quotes.len()), 1024);
        host.index().mark_ready();
        let new = body(&quoting(&target));
        host.store(new.clone());
        host.post(2, vec![message(130, 2, new.0)]).await;
        Sweep::default()
            .run(&host, CHAIN, host.index())
            .await
            .unwrap();
        let key = QuoteKey {
            source: 130,
            quote: Quote {
                chain: CHAIN,
                id: 1,
                commit: target.commitment(),
            },
        };
        host.references(|r| {
            assert!(!r.pending_quotes.contains(&key));
            assert_eq!(r.outcomes[&key].author, Some(target.author));
        });
    });
}

/// Thirty-two posts sharing one body, each quoting eight posts, are bound
/// in one pass: their 256 quotes are all resolved in that pass, in updates
/// of at most 32.
#[test]
fn quotes_of_a_full_binding_batch_resolve_together() {
    deterministic::Runner::default().start(|ctx| async move {
        let host = Local::open(&ctx).await;
        let targets = (1..=8)
            .map(|id| message(id, 1, hash(10)))
            .collect::<Vec<_>>();
        let shared = body(&targets.iter().map(quoting).collect::<String>());
        let mut first = targets.clone();
        first.extend((9..=40).map(|id| message(id, 1, shared.0)));
        host.post(1, first).await;
        host.index().mark_ready();
        host.store(shared);
        Sweep::default()
            .run(&host, CHAIN, host.index())
            .await
            .unwrap();
        host.references(|r| {
            assert!(
                r.pending_quotes.is_empty(),
                "{} pending",
                r.pending_quotes.len()
            );
            assert_eq!(r.outcomes.len(), 256);
        });
    });
}

/// A post that mentions account 9 and is otherwise numbered `n`.
fn mentioning(n: usize) -> (Hash, Vec<u8>) {
    body(&format!("post {n} for account://{}", Hex([9u8; 20])))
}
/// Bodies `mentioning` some number, the first `count` whose hash `fits`:
/// a way to place them in the round-robin's order.
fn bodies_where(count: usize, from: usize, fits: impl Fn(&Hash) -> bool) -> Vec<(Hash, Vec<u8>)> {
    (from..)
        .map(mentioning)
        .filter(|(hash, _)| fits(hash))
        .take(count)
        .collect()
}
/// Hashes no body will ever have, ahead of every real one in the round-robin.
fn never(n: u32) -> Hash {
    let mut bytes = [0; 32];
    bytes[28..].copy_from_slice(&n.to_be_bytes());
    Hex(bytes)
}
fn mentions_of(host: &Local, id: u64) -> bool {
    host.references(|r| {
        r.accounts
            .get(&Hex([9; 20]))
            .is_some_and(|ids| ids.contains(&id))
    })
}

/// A thousand posts whose bodies never come are ahead of a new post in the
/// round-robin. Its body arrives, and the next pass analyses it and binds
/// the post, so its mention is recorded.
#[test]
fn an_arriving_body_is_taken_up_ahead_of_a_backlog() {
    deterministic::Runner::default().start(|ctx| async move {
        let host = Local::open(&ctx).await;
        let mut posts = (1..=1000)
            .map(|id| message(id, 1, never(id as u32)))
            .collect::<Vec<_>>();
        let new = mentioning(0);
        posts.push(message(1001, 1, new.0));
        host.post(1, posts).await;
        host.arrive(&new);
        Sweep::default()
            .run(&host, CHAIN, host.index())
            .await
            .unwrap();
        assert!(mentions_of(&host, 1001));
    });
}

/// A hundred analysed bodies wait to be bound, all ahead in the round-robin.
/// A new post's body arrives, and so does the body of a post reusing one
/// already analysed and bound: the next pass binds both posts.
#[test]
fn posts_of_arrivals_are_bound_ahead_of_a_binding_backlog() {
    deterministic::Runner::default().start(|ctx| async move {
        let host = Local::open(&ctx).await;
        let late = |hash: &Hash| hash.0[0] >= 0xf0;
        let backlog = bodies_where(100, 0, |hash| !late(hash));
        let [new, reused] = <[_; 2]>::try_from(bodies_where(2, 0, late)).unwrap();
        let mut posts = backlog
            .iter()
            .enumerate()
            .map(|(n, (hash, _))| message(n as u64 + 1, 1, *hash))
            .collect::<Vec<_>>();
        posts.push(message(101, 1, reused.0));
        host.post(1, posts).await;
        host.analyse(&backlog).await;
        host.analyse_and_bind(reused.clone(), [101]).await;
        host.post(2, vec![message(102, 2, new.0), message(103, 2, reused.0)])
            .await;
        host.store(new.clone());
        host.store(reused.clone());
        host.index().body_arrived(new.0);
        host.index().body_arrived(reused.0);
        Sweep::default()
            .run(&host, CHAIN, host.index())
            .await
            .unwrap();
        assert!(mentions_of(&host, 102), "the new post");
        assert!(mentions_of(&host, 103), "the post reusing a body");
    });
}

/// Forty posts share one body. When it arrives, the first pass binds as
/// many as one batch holds and the next pass the rest.
#[test]
fn an_arrival_shared_by_many_posts_binds_them_all() {
    deterministic::Runner::default().start(|ctx| async move {
        let host = Local::open(&ctx).await;
        let shared = mentioning(0);
        host.post(1, (1..=40).map(|id| message(id, 1, shared.0)).collect())
            .await;
        host.arrive(&shared);
        let mut sweep = Sweep::default();
        sweep.run(&host, CHAIN, host.index()).await.unwrap();
        assert_eq!((1..=40).filter(|&id| mentions_of(&host, id)).count(), 32);
        sweep.run(&host, CHAIN, host.index()).await.unwrap();
        assert!((1..=40).all(|id| mentions_of(&host, id)));
    });
}

/// The same body arriving again takes no second place and does not move the
/// first. Nothing is noted for a body with no work left, and no more than
/// 1,024 wait at once.
#[test]
fn arrivals_wait_once_each_in_order_and_bounded() {
    deterministic::Runner::default().start(|ctx| async move {
        let host = Local::open(&ctx).await;
        let done = mentioning(0);
        host.post(1, vec![message(1, 1, done.0)]).await;
        host.analyse_and_bind(done.clone(), [1]).await;
        let posts = (2..=1101)
            .map(|id| message(id, 2, never(id as u32)))
            .collect();
        host.post(2, posts).await;
        let index = host.index();
        for hash in [never(5), never(6), never(5), never(5)] {
            index.body_arrived(hash);
        }
        index.body_arrived(done.0);
        assert_eq!(index.arrivals_for_test(), [never(5), never(6)]);
        for id in 2..=1101 {
            index.body_arrived(never(id));
        }
        let waiting = index.arrivals_for_test();
        assert_eq!(waiting.len(), 1024);
        assert_eq!(waiting[..2], [never(5), never(6)]);
    });
}

/// More bodies arrive every pass than arrivals may take, yet the round-robin
/// still moves: a stored body that was never noted as an arrival, behind a
/// hundred that never come, is analysed within the passes its share allows.
#[test]
fn the_round_robin_moves_while_bodies_keep_arriving() {
    deterministic::Runner::default().start(|ctx| async move {
        let host = Local::open(&ctx).await;
        let unnoted = bodies_where(1, 0, |hash| (0x10..0xe0).contains(&hash.0[0])).remove(0);
        let mut posts = (1..=100)
            .map(|id| message(id, 1, never(id as u32)))
            .collect::<Vec<_>>();
        posts.push(message(101, 1, unnoted.0));
        host.post(1, posts).await;
        host.store(unnoted.clone());
        let mut arriving = bodies_where(30 * 14, 0, |hash| hash.0[0] >= 0xf0).into_iter();
        let mut sweep = Sweep::default();
        let mut id = 101;
        for (pass, block) in (2..16).enumerate() {
            let batch = arriving.by_ref().take(30).collect::<Vec<_>>();
            let posts = batch.iter().map(|(hash, _)| {
                id += 1;
                message(id, block, *hash)
            });
            host.post(block, posts.collect()).await;
            for body in batch {
                host.arrive(&body);
            }
            sweep.run(&host, CHAIN, host.index()).await.unwrap();
            if mentions_of(&host, 101) {
                assert!(pass < 14);
                return;
            }
        }
        panic!("the round-robin never reached the unnoted body");
    });
}

/// The same bytes can back posts on more than one chain: an arrival reaches
/// every index still waiting on them, and no other.
#[test]
fn an_arrival_reaches_every_chain_waiting_on_it() {
    deterministic::Runner::default().start(|ctx| async move {
        let open = |chain: u64, label: &'static str| {
            let ctx = ctx.child(label);
            async move {
                let index = Index::open(
                    ctx,
                    Deployment {
                        chain,
                        ..deployment()
                    },
                    hash(2),
                )
                .await
                .unwrap();
                index.establish().unwrap();
                index
            }
        };
        let shared = mentioning(0);
        let indexes = HashMap::from([
            (42, open(42, "a").await),
            (43, open(43, "b").await),
            (44, open(44, "c").await),
        ]);
        for chain in [42, 43] {
            indexes[&chain]
                .update(Update::Chain(Batch {
                    from: 1,
                    through: Checkpoint {
                        block: 1,
                        hash: hash(4),
                    },
                    events: vec![Event::Posted {
                        message: message(1, 1, shared.0),
                    }],
                }))
                .await
                .unwrap();
        }
        super::bodies::arrived(&indexes, shared.0);
        assert_eq!(indexes[&42].arrivals_for_test(), [shared.0]);
        assert_eq!(indexes[&43].arrivals_for_test(), [shared.0]);
        assert!(indexes[&44].arrivals_for_test().is_empty());
    });
}

/// An arrival is read from the store alone, never through the copy path,
/// which may wait on peers and here fails.
#[test]
fn an_arrival_is_read_from_the_store_alone() {
    deterministic::Runner::default().start(|ctx| async move {
        let mut host = Local::open(&ctx).await;
        host.copy_fails = true;
        let new = mentioning(0);
        host.post(1, vec![message(1, 1, new.0)]).await;
        host.arrive(&new);
        Sweep::default()
            .run(&host, CHAIN, host.index())
            .await
            .unwrap();
        assert!(mentions_of(&host, 1));
    });
}

/// A hundred analysed bodies wait to be bound. Forty new posts share one
/// arriving body, and one more has its own. Arrivals keep their priority
/// until all their posts are bound: two passes bind all forty-one.
#[test]
fn arrivals_keep_priority_until_all_their_posts_are_bound() {
    deterministic::Runner::default().start(|ctx| async move {
        let host = Local::open(&ctx).await;
        let backlog = bodies_where(100, 0, |hash| hash.0[0] < 0xf0);
        let arrivals = bodies_where(2, 0, |hash| hash.0[0] >= 0xf0);
        let mut posts = backlog
            .iter()
            .enumerate()
            .map(|(n, (hash, _))| message(n as u64 + 1, 1, *hash))
            .collect::<Vec<_>>();
        posts.extend((101..=140).map(|id| message(id, 1, arrivals[0].0)));
        posts.push(message(141, 1, arrivals[1].0));
        host.post(1, posts).await;
        host.analyse(&backlog).await;
        for arrival in arrivals {
            host.arrive(&arrival);
        }
        let mut sweep = Sweep::default();
        sweep.run(&host, CHAIN, host.index()).await.unwrap();
        assert!(
            mentions_of(&host, 141),
            "the single post is not left behind the forty"
        );
        sweep.run(&host, CHAIN, host.index()).await.unwrap();
        assert_eq!((101..=141).filter(|&id| mentions_of(&host, id)).count(), 41);
        assert!(
            host.index().arrivals_for_test().is_empty(),
            "finished arrivals leave"
        );
    });
}

/// Two arriving bodies with forty posts each share the arrivals' part of a
/// pass, taking posts from each in turn.
#[test]
fn arrivals_share_each_pass_in_turn() {
    deterministic::Runner::default().start(|ctx| async move {
        let host = Local::open(&ctx).await;
        let [first, second] = <[_; 2]>::try_from(bodies_where(2, 0, |_| true)).unwrap();
        let mut posts = (1..=40)
            .map(|id| message(id, 1, first.0))
            .collect::<Vec<_>>();
        posts.extend((41..=80).map(|id| message(id, 1, second.0)));
        host.post(1, posts).await;
        for body in [&first, &second] {
            host.arrive(body);
        }
        Sweep::default()
            .run(&host, CHAIN, host.index())
            .await
            .unwrap();
        let bound =
            |ids: std::ops::RangeInclusive<u64>| ids.filter(|&id| mentions_of(&host, id)).count();
        assert!(
            bound(1..=40) >= 12 && bound(41..=80) >= 12,
            "{} and {}",
            bound(1..=40),
            bound(41..=80)
        );
    });
}

/// While a pass works on an arrival, it still counts as waiting: the same
/// body arriving again meanwhile takes no second place, and the arrival
/// goes back to the end of the queue if its posts are not all bound.
#[test]
fn an_arrival_being_worked_on_still_counts_as_waiting() {
    deterministic::Runner::default().start(|ctx| async move {
        let host = Local::open(&ctx).await;
        let shared = mentioning(0);
        let other = mentioning(1);
        let mut posts = (1..=40)
            .map(|id| message(id, 1, shared.0))
            .collect::<Vec<_>>();
        posts.push(message(41, 1, other.0));
        host.post(1, posts).await;
        host.analyse_and_bind(shared.clone(), []).await;
        let index = host.index();
        index.body_arrived(shared.0);
        let taken = index.take_arrivals(24);
        assert_eq!(taken, [shared.0]);
        index.body_arrived(shared.0);
        index.body_arrived(other.0);
        assert_eq!(index.arrivals_for_test(), [other.0], "no second place");
        index.return_arrivals(taken);
        assert_eq!(
            index.arrivals_for_test(),
            [other.0, shared.0],
            "back of the queue"
        );
        // Taken or not, every waiting arrival counts toward the limit.
        let taken = index.take_arrivals(1);
        let posts = (42..=1141)
            .map(|id| message(id, 2, never(id as u32)))
            .collect();
        host.post(2, posts).await;
        for id in 42..=1141 {
            index.body_arrived(never(id));
        }
        assert_eq!(index.arrivals_for_test().len(), 1023);
        index.return_arrivals(taken);
    });
}

/// A finished arrival has left the queue: a later post reusing its body
/// arrives anew instead of being passed over as already waiting.
#[test]
fn a_finished_arrival_arrives_anew_for_a_later_post() {
    deterministic::Runner::default().start(|ctx| async move {
        let host = Local::open(&ctx).await;
        let shared = mentioning(0);
        host.post(1, vec![message(1, 1, shared.0)]).await;
        host.arrive(&shared);
        let index = host.index();
        let taken = index.take_arrivals(24);
        assert_eq!(taken, [shared.0]);
        host.analyse_and_bind(shared.clone(), [1]).await;
        index.return_arrivals(taken);
        assert!(index.arrivals_for_test().is_empty(), "finished");
        host.post(2, vec![message(2, 2, shared.0)]).await;
        index.body_arrived(shared.0);
        assert_eq!(index.arrivals_for_test(), [shared.0]);
    });
}

/// An arrival whose body cannot be read leaves the queue: the round-robin
/// tries it again in its turn.
#[test]
fn an_arrival_that_cannot_be_analysed_leaves_the_queue() {
    deterministic::Runner::default().start(|ctx| async move {
        let host = Local::open(&ctx).await;
        let lost = mentioning(0);
        host.post(1, vec![message(1, 1, lost.0)]).await;
        host.index().body_arrived(lost.0);
        Sweep::default()
            .run(&host, CHAIN, host.index())
            .await
            .unwrap();
        assert!(host.index().arrivals_for_test().is_empty());
        assert!(host.references(|r| r.pending_bodies.contains(&lost.0)));
    });
}
