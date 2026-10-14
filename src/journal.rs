//! Journal configuration and durability helpers shared by both indexes.
//! Each index owns its records, writer, replay and failure handling.

use anyhow::Result;
use commonware_runtime::buffer::paged::CacheRef;
use commonware_storage::{
    Context,
    journal::contiguous::variable::{Config, Journal},
};
use commonware_utils::{NZU16, NZU64, NZUsize};

/// Empty ranges advance the served checkpoint in RAM and reach the journal
/// once this many blocks have passed since the last journaled one. A crash
/// loses at most this many blocks of empty progress, which the follower
/// reads again.
pub(crate) const CHECKPOINT_BLOCKS: u64 = 300;
/// Read-ahead for replaying either journal at startup.
pub(crate) const REPLAY_BUFFER: usize = 64 << 10;

/// Settings for bounded JSON records in the Board and keyring journals.
pub(crate) fn journal_config<E: Context>(
    context: &E,
    partition: String,
    max_record: usize,
) -> Config<<Vec<u8> as commonware_codec::Read>::Cfg> {
    Config {
        partition,
        items_per_section: NZU64!(1024),
        compression: None,
        codec_config: ((0..=max_record).into(), ()),
        page_cache: CacheRef::from_pooler(context, NZU16!(4096), NZUsize!(64)),
        write_buffer: NZUsize!(64 << 10),
        replay_buffer: NZUsize!(REPLAY_BUFFER),
    }
}

/// Append one record and sync it. The journal is consumed: a write that
/// fails takes it along, so the writer has nothing left to write with and
/// must stop.
pub(crate) async fn append_and_sync<E: Context>(
    journal: Journal<E, Vec<u8>>,
    bytes: Vec<u8>,
) -> Result<Journal<E, Vec<u8>>> {
    let (journal, _) = journal.append(&bytes).await?;
    journal.sync().await.map_err(anyhow::Error::from)
}
