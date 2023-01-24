// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! CLI introspection tools for persist

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt;
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use anyhow::anyhow;
use bytes::BufMut;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::trace::Description;
use mz_persist_types::codec_impls::TodoSchema;
use prost::Message;

use mz_build_info::BuildInfo;
use mz_ore::cast::CastFrom;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_persist::indexed::encoding::BlobTraceBatchPart;
use mz_persist_types::{Codec, Codec64};
use mz_proto::RustType;
use serde_json::json;

use crate::cli::admin::{make_blob, make_consensus};
use crate::fetch::EncodedPart;
use crate::internal::paths::{
    BlobKey, BlobKeyPrefix, PartialBatchKey, PartialBlobKey, PartialRollupKey,
};
use crate::internal::state::{ProtoStateDiff, ProtoStateRollup};
use crate::{Metrics, PersistConfig, ShardId, StateVersions};

const READ_ALL_BUILD_INFO: BuildInfo = BuildInfo {
    version: "10000000.0.0+test",
    sha: "0000000000000000000000000000000000000000",
    time: "",
};

/// Commands for read-only inspection of persist state
#[derive(Debug, clap::Args)]
pub struct InspectArgs {
    #[clap(subcommand)]
    command: Command,
}

/// Individual subcommands of inspect
#[derive(Debug, clap::Subcommand)]
pub(crate) enum Command {
    /// Prints latest consensus state as JSON
    State(StateArgs),

    /// Prints latest consensus rollup state as JSON
    StateRollup(StateRollupArgs),

    /// Prints consensus rollup state of all known rollups as JSON
    StateRollups(StateArgs),

    /// Prints the count and size of blobs in an environment
    BlobCount(BlobCountArgs),

    /// Prints blob batch part contents
    BlobBatchPart(BlobBatchPartArgs),

    /// Prints the unreferenced blobs across all shards
    UnreferencedBlobs(StateArgs),

    /// Prints information about blob usage for a shard
    BlobUsage(StateArgs),

    /// Prints each consensus state change as JSON. Output includes the full consensus state
    /// before and after each state transitions:
    ///
    /// ```text
    /// {
    ///     "previous": previous_consensus_state,
    ///     "new": new_consensus_state,
    /// }
    /// ```
    ///
    /// This is most helpfully consumed using a JSON diff tool like `jd`. A useful incantation
    /// to show only the changed fields between state transitions:
    ///
    /// ```text
    /// persistcli inspect state-diff --shard-id <shard> --consensus-uri <consensus_uri> |
    ///     while read diff; do
    ///         echo $diff | jq '.new' > temp_new
    ///         echo $diff | jq '.previous' > temp_previous
    ///         echo $diff | jq '.new.seqno'
    ///         jd -color -set temp_previous temp_new
    ///     done
    /// ```
    ///
    #[clap(verbatim_doc_comment)]
    StateDiff(StateArgs),
}

/// Runs the given read-only inspect command.
pub async fn run(command: InspectArgs) -> Result<(), anyhow::Error> {
    match command.command {
        Command::State(args) => {
            let state = fetch_latest_state(&args).await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&state).expect("unserializable state")
            );
        }
        Command::StateRollup(args) => {
            let state_rollup = fetch_state_rollup(&args).await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&state_rollup).expect("unserializable state")
            );
        }
        Command::StateRollups(args) => {
            let state_rollups = fetch_state_rollups(&args).await?;
            println!(
                "{}",
                serde_json::to_string_pretty(&state_rollups).expect("unserializable state")
            );
        }
        Command::StateDiff(args) => {
            let states = fetch_state_diffs(&args).await?;
            for window in states.windows(2) {
                println!(
                    "{}",
                    json!({
                        "previous": window[0],
                        "new": window[1]
                    })
                );
            }
        }
        Command::BlobCount(args) => {
            let blob_counts = blob_counts(&args.blob_uri).await?;
            println!("{}", json!(blob_counts));
        }
        Command::BlobBatchPart(args) => {
            let shard_id = ShardId::from_str(&args.shard_id).expect("invalid shard id");
            let updates = blob_batch_part(&args.blob_uri, shard_id, args.key, args.limit).await?;
            println!("{}", json!(updates));
        }
        Command::UnreferencedBlobs(args) => {
            let unreferenced_blobs = unreferenced_blobs(&args).await?;
            println!("{}", json!(unreferenced_blobs));
        }
        Command::BlobUsage(args) => {
            let () = blob_usage(&args).await?;
        }
    }

    Ok(())
}

/// Arguments for viewing the state rollup of a shard
#[derive(Debug, Clone, clap::Parser)]
pub struct StateRollupArgs {
    #[clap(flatten)]
    pub(crate) state: StateArgs,

    /// Inspect the state rollup with the given ID, if available.
    #[clap(long)]
    pub(crate) rollup_key: Option<String>,
}

/// Arguments for viewing the current state of a given shard
#[derive(Debug, Clone, clap::Parser)]
pub struct StateArgs {
    /// Shard to view
    #[clap(long)]
    pub(crate) shard_id: String,

    /// Consensus to use.
    ///
    /// When connecting to a deployed environment's consensus table, the Postgres/CRDB connection
    /// string must contain the database name and `options=--search_path=consensus`.
    ///
    /// When connecting to Cockroach Cloud, use the following format:
    ///
    /// ```text
    /// postgresql://<user>:$COCKROACH_PW@<hostname>:<port>/environment_<environment-id>
    ///   ?sslmode=verify-full
    ///   &sslrootcert=/path/to/cockroach-cloud/certs/cluster-ca.crt
    ///   &options=--search_path=consensus
    /// ```
    ///
    #[clap(long, verbatim_doc_comment, env = "CONSENSUS_URI")]
    pub(crate) consensus_uri: String,

    /// Blob to use
    ///
    /// When connecting to a deployed environment's blob, the necessary connection glue must be in
    /// place. e.g. for S3, sign into SSO, set AWS_PROFILE and AWS_REGION appropriately, with a blob
    /// URI scoped to the environment's bucket prefix.
    #[clap(long, env = "BLOB_URI")]
    pub(crate) blob_uri: String,
}

/// Fetches the current state of a given shard
pub async fn fetch_latest_state(args: &StateArgs) -> Result<impl serde::Serialize, anyhow::Error> {
    let shard_id = args.shard_id();
    let state_versions = args.open().await?;
    let versions = state_versions
        .fetch_recent_live_diffs::<u64>(&shard_id)
        .await;

    let state = match state_versions
        .fetch_current_state::<K, V, u64, D>(&shard_id, versions.0.clone())
        .await
    {
        Ok(s) => s.into_proto(),
        Err(codec) => {
            {
                let mut kvtd = KVTD_CODECS.lock().expect("lockable");
                *kvtd = codec.actual;
            }
            state_versions
                .fetch_current_state::<K, V, u64, D>(&shard_id, versions.0)
                .await
                .expect("codecs match")
                .into_proto()
        }
    };

    Ok(state)
}

/// Fetches a state rollup of a given shard. If the seqno is not provided, choose the latest;
/// if the rollup id is not provided, discover it by inspecting state.
pub async fn fetch_state_rollup(
    args: &StateRollupArgs,
) -> Result<impl serde::Serialize, anyhow::Error> {
    let shard_id = args.state.shard_id();
    let state_versions = args.state.open().await?;

    let rollup_key = if let Some(rollup_key) = &args.rollup_key {
        PartialRollupKey(rollup_key.to_owned())
    } else {
        let latest_state = state_versions.consensus.head(&shard_id.to_string()).await?;
        let diff_buf = latest_state.ok_or_else(|| anyhow!("unknown shard"))?;
        let diff = ProtoStateDiff::decode(diff_buf.data).expect("invalid encoded diff");
        PartialRollupKey(diff.latest_rollup_key)
    };
    let rollup_buf = state_versions
        .blob
        .get(&rollup_key.complete(&shard_id))
        .await?
        .expect("fetching the specified state rollup");
    let proto = ProtoStateRollup::decode(rollup_buf.as_slice()).expect("invalid encoded state");
    Ok(proto)
}

/// Fetches the state from all known rollups of a given shard
pub async fn fetch_state_rollups(args: &StateArgs) -> Result<impl serde::Serialize, anyhow::Error> {
    let shard_id = args.shard_id();
    let state_versions = args.open().await?;

    let mut rollup_keys = HashSet::new();
    let mut state_iter = match state_versions
        .fetch_all_live_states::<K, V, u64, D>(&shard_id)
        .await
    {
        Ok(state_iter) => state_iter,
        Err(codec) => {
            {
                let mut kvtd = KVTD_CODECS.lock().expect("lockable");
                *kvtd = codec.actual;
            }
            state_versions
                .fetch_all_live_states::<K, V, u64, D>(&shard_id)
                .await?
        }
    };

    while let Some(v) = state_iter.next() {
        for key in v.collections.rollups.values() {
            rollup_keys.insert(key.clone());
        }
    }

    if rollup_keys.is_empty() {
        return Err(anyhow!("unknown shard"));
    }

    let mut rollup_states = HashMap::with_capacity(rollup_keys.len());
    for key in rollup_keys {
        let rollup_buf = state_versions
            .blob
            .get(&key.complete(&shard_id))
            .await
            .unwrap();
        if let Some(rollup_buf) = rollup_buf {
            let proto =
                ProtoStateRollup::decode(rollup_buf.as_slice()).expect("invalid encoded state");
            rollup_states.insert(key.to_string(), proto);
        }
    }

    Ok(rollup_states)
}

/// Fetches each state in a shard
pub async fn fetch_state_diffs(
    args: &StateArgs,
) -> Result<Vec<impl serde::Serialize>, anyhow::Error> {
    let shard_id = args.shard_id();
    let state_versions = args.open().await?;

    let mut live_states = vec![];
    let mut state_iter = match state_versions
        .fetch_all_live_states::<K, V, u64, D>(&shard_id)
        .await
    {
        Ok(state_iter) => state_iter,
        Err(codec) => {
            {
                let mut kvtd = KVTD_CODECS.lock().expect("lockable");
                *kvtd = codec.actual;
            }
            state_versions
                .fetch_all_live_states::<K, V, u64, D>(&shard_id)
                .await?
        }
    };

    while let Some(v) = state_iter.next() {
        live_states.push(v.into_proto());
    }

    Ok(live_states)
}

/// Arguments for viewing contents of a batch part
#[derive(Debug, Clone, clap::Parser)]
pub struct BlobBatchPartArgs {
    /// Shard to view
    #[clap(long)]
    shard_id: String,

    /// Blob key (without shard)
    #[clap(long)]
    key: String,

    /// Blob to use
    ///
    /// When connecting to a deployed environment's blob, the necessary connection glue must be in
    /// place. e.g. for S3, sign into SSO, set AWS_PROFILE and AWS_REGION appropriately, with a blob
    /// URI scoped to the environment's bucket prefix.
    #[clap(long)]
    blob_uri: String,

    /// Number of updates to output. Default is unbounded.
    #[clap(long, default_value = "18446744073709551615")]
    limit: usize,
}

#[derive(Debug, serde::Serialize)]
struct BatchPartOutput {
    desc: Description<u64>,
    updates: Vec<BatchPartUpdate>,
}

#[derive(Debug, serde::Serialize)]
struct BatchPartUpdate {
    k: String,
    v: String,
    t: u64,
    d: i64,
}

#[derive(PartialOrd, Ord, PartialEq, Eq)]
struct PrettyBytes<'a>(&'a [u8]);

impl fmt::Debug for PrettyBytes<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match std::str::from_utf8(self.0) {
            Ok(x) => fmt::Debug::fmt(x, f),
            Err(_) => fmt::Debug::fmt(self.0, f),
        }
    }
}

/// Fetches the updates in a blob batch part
pub async fn blob_batch_part(
    blob_uri: &str,
    shard_id: ShardId,
    partial_key: String,
    limit: usize,
) -> Result<impl serde::Serialize, anyhow::Error> {
    let cfg = PersistConfig::new(&READ_ALL_BUILD_INFO, SYSTEM_TIME.clone());
    let metrics = Arc::new(Metrics::new(&cfg, &MetricsRegistry::new()));
    let blob = make_blob(blob_uri, NO_COMMIT, metrics).await?;

    let key = PartialBatchKey(partial_key).complete(&shard_id);
    let part = blob
        .get(&*key)
        .await
        .expect("blob exists")
        .expect("part exists");
    let part = BlobTraceBatchPart::<u64>::decode(&part).expect("decodable");
    let desc = part.desc.clone();

    let mut encoded_part = EncodedPart::new(&*key, part.desc.clone(), part);
    let mut out = BatchPartOutput {
        desc,
        updates: Vec::new(),
    };
    while let Some((k, v, t, d)) = encoded_part.next() {
        if out.updates.len() > limit {
            break;
        }
        out.updates.push(BatchPartUpdate {
            k: format!("{:?}", PrettyBytes(k)),
            v: format!("{:?}", PrettyBytes(v)),
            t,
            d: i64::from_le_bytes(d),
        })
    }

    Ok(out)
}

/// Arguments for viewing the blobs of a given shard
#[derive(Debug, Clone, clap::Parser)]
pub struct BlobCountArgs {
    /// Blob to use
    ///
    /// When connecting to a deployed environment's blob, the necessary connection glue must be in
    /// place. e.g. for S3, sign into SSO, set AWS_PROFILE and AWS_REGION appropriately, with a blob
    /// URI scoped to the environment's bucket prefix.
    #[clap(long)]
    blob_uri: String,
}

#[derive(Debug, Default, serde::Serialize)]
struct BlobCounts {
    batch_part_count: usize,
    batch_part_bytes: usize,
    rollup_count: usize,
    rollup_bytes: usize,
}

/// Fetches the blob count for given path
pub async fn blob_counts(blob_uri: &str) -> Result<impl serde::Serialize, anyhow::Error> {
    let cfg = PersistConfig::new(&READ_ALL_BUILD_INFO, SYSTEM_TIME.clone());
    let metrics = Arc::new(Metrics::new(&cfg, &MetricsRegistry::new()));
    let blob = make_blob(blob_uri, NO_COMMIT, metrics).await?;

    let mut blob_counts = BTreeMap::new();
    let () = blob
        .list_keys_and_metadata(&BlobKeyPrefix::All.to_string(), &mut |metadata| {
            match BlobKey::parse_ids(metadata.key) {
                Ok((shard, PartialBlobKey::Batch(_, _))) => {
                    let blob_count = blob_counts.entry(shard).or_insert_with(BlobCounts::default);
                    blob_count.batch_part_count += 1;
                    blob_count.batch_part_bytes += usize::cast_from(metadata.size_in_bytes);
                }
                Ok((shard, PartialBlobKey::Rollup(_, _))) => {
                    let blob_count = blob_counts.entry(shard).or_insert_with(BlobCounts::default);
                    blob_count.rollup_count += 1;
                    blob_count.rollup_bytes += usize::cast_from(metadata.size_in_bytes);
                }
                Err(err) => {
                    eprintln!("error parsing blob: {}", err);
                }
            }
        })
        .await?;

    Ok(blob_counts)
}

#[derive(Debug, Default, serde::Serialize)]
struct UnreferencedBlobs {
    batch_parts: BTreeSet<PartialBatchKey>,
    rollups: BTreeSet<PartialRollupKey>,
}

/// Fetches the unreferenced blobs for given environment
pub async fn unreferenced_blobs(args: &StateArgs) -> Result<impl serde::Serialize, anyhow::Error> {
    let shard_id = args.shard_id();
    let state_versions = args.open().await?;

    let mut all_parts = vec![];
    let mut all_rollups = vec![];
    let () = state_versions
        .blob
        .list_keys_and_metadata(
            &BlobKeyPrefix::Shard(&shard_id).to_string(),
            &mut |metadata| match BlobKey::parse_ids(metadata.key) {
                Ok((_, PartialBlobKey::Batch(writer, part))) => {
                    all_parts.push((PartialBatchKey::new(&writer, &part), writer.clone()));
                }
                Ok((_, PartialBlobKey::Rollup(seqno, rollup))) => {
                    all_rollups.push(PartialRollupKey::new(seqno, &rollup));
                }
                Err(_) => {}
            },
        )
        .await?;

    let mut state_iter = match state_versions
        .fetch_all_live_states::<K, V, u64, D>(&shard_id)
        .await
    {
        Ok(state_iter) => state_iter,
        Err(codec) => {
            {
                let mut kvtd = KVTD_CODECS.lock().expect("lockable");
                *kvtd = codec.actual;
            }
            state_versions
                .fetch_all_live_states::<K, V, u64, D>(&shard_id)
                .await?
        }
    };

    let mut known_parts = HashSet::new();
    let mut known_rollups = HashSet::new();
    let mut known_writers = HashSet::new();
    while let Some(v) = state_iter.next() {
        for writer_id in v.collections.writers.keys() {
            known_writers.insert(writer_id.clone());
        }
        for batch in v.collections.trace.batches() {
            for batch_part in &batch.parts {
                known_parts.insert(batch_part.key.clone());
            }
        }
        for rollup in v.collections.rollups.values() {
            known_rollups.insert(rollup.clone());
        }
    }

    let mut unreferenced_blobs = UnreferencedBlobs::default();
    for (part, writer) in all_parts {
        if !known_writers.contains(&writer) && !known_parts.contains(&part) {
            unreferenced_blobs.batch_parts.insert(part);
        }
    }
    for rollup in all_rollups {
        if !known_rollups.contains(&rollup) {
            unreferenced_blobs.rollups.insert(rollup);
        }
    }

    Ok(unreferenced_blobs)
}

/// Returns information about blob usage for a shard
pub async fn blob_usage(args: &StateArgs) -> Result<(), anyhow::Error> {
    let shard_id = args.shard_id();
    let state_versions = args.open().await?;

    let mut s3_contents_before = HashMap::new();
    let () = state_versions
        .blob
        .list_keys_and_metadata(&BlobKeyPrefix::Shard(&shard_id).to_string(), &mut |b| {
            s3_contents_before.insert(b.key.to_owned(), usize::cast_from(b.size_in_bytes));
        })
        .await?;

    let mut state_iter = match state_versions
        .fetch_all_live_states::<K, V, u64, D>(&shard_id)
        .await
    {
        Ok(state_iter) => state_iter,
        Err(codec) => {
            {
                let mut kvtd = KVTD_CODECS.lock().expect("lockable");
                *kvtd = codec.actual;
            }
            state_versions
                .fetch_all_live_states::<K, V, u64, D>(&shard_id)
                .await?
        }
    };

    let mut referenced_parts = HashMap::new();
    let mut referenced_rollups = HashSet::new();
    while let Some(state) = state_iter.next() {
        state.collections.trace.map_batches(|b| {
            for part in b.parts.iter() {
                referenced_parts.insert(
                    part.key.complete(&shard_id).to_string(),
                    part.encoded_size_bytes,
                );
            }
        });
        for rollup_key in state.collections.rollups.values() {
            referenced_rollups.insert(rollup_key.complete(&shard_id).to_string());
        }
    }

    let mut current_parts = HashMap::new();
    let mut current_rollups = HashSet::new();
    state_iter.state().collections.trace.map_batches(|b| {
        for part in b.parts.iter() {
            current_parts.insert(
                part.key.complete(&shard_id).to_string(),
                part.encoded_size_bytes,
            );
        }
    });
    for rollup_key in state_iter.state().collections.rollups.values() {
        current_rollups.insert(rollup_key.complete(&shard_id).to_string());
    }

    // There's a bit of a race condition between fetching s3 and state, but
    // probably fine for most cases?
    let (mut referenced_by_state_count, mut referenced_by_state_bytes) = (0, 0);
    let (mut current_writer_pending_count, mut current_writer_pending_bytes) = (0, 0);
    let (mut leaked_count, mut leaked_bytes) = (0, 0);
    for (key, bytes) in s3_contents_before.iter() {
        let referenced_by_state =
            referenced_parts.contains_key(key) || referenced_rollups.contains(key);
        if referenced_by_state {
            referenced_by_state_count += 1;
            referenced_by_state_bytes += bytes;
        } else {
            let current_writer_pending =
                match BlobKey::parse_ids(key).expect("key should be in known format") {
                    (_, PartialBlobKey::Batch(writer_id, _)) => state_iter
                        .state()
                        .collections
                        .writers
                        .contains_key(&writer_id),
                    (_, PartialBlobKey::Rollup(_, _)) => false,
                };
            if current_writer_pending {
                current_writer_pending_count += 1;
                current_writer_pending_bytes += bytes;
            } else {
                leaked_count += 1;
                leaked_bytes += bytes;
            }
        }
    }
    let (mut gc_able_parts_count, mut gc_able_parts_bytes) = (0, 0);
    let (current_parts_count, current_parts_bytes) =
        (current_parts.len(), current_parts.values().sum::<usize>());
    for (key, bytes) in referenced_parts.iter() {
        if !current_parts.contains_key(key) {
            gc_able_parts_count += 1;
            gc_able_parts_bytes += bytes;
        }
    }
    let (mut gc_able_rollups_count, mut gc_able_rollups_bytes) = (0, 0);
    let (mut current_rollups_count, mut current_rollups_bytes) = (0, 0);
    for key in referenced_rollups.iter() {
        let Some(bytes) = s3_contents_before.get(key) else {
            println!("unknown size due to race condition: {}", key);
            continue;
        };
        if current_rollups.contains(key) {
            current_rollups_count += 1;
            current_rollups_bytes += *bytes;
        } else {
            gc_able_rollups_count += 1;
            gc_able_rollups_bytes += *bytes;
        }
    }

    println!(
        "total s3 contents:        {} ({} blobs)",
        human_bytes(s3_contents_before.values().sum::<usize>()),
        s3_contents_before.len(),
    );
    println!(
        "  leaked:                 {} ({} blobs)",
        human_bytes(leaked_bytes),
        leaked_count,
    );
    println!(
        "  current writer pending: {} ({} blobs)",
        human_bytes(current_writer_pending_bytes),
        current_writer_pending_count,
    );
    println!(
        "  referenced:             {} ({} blobs)",
        human_bytes(referenced_by_state_bytes),
        referenced_by_state_count,
    );
    println!(
        "    gc-able:              {} ({} blobs)",
        human_bytes(gc_able_parts_bytes + gc_able_rollups_bytes),
        gc_able_parts_count + gc_able_rollups_count,
    );
    println!(
        "      gc-able parts:      {} ({} blobs)",
        human_bytes(gc_able_parts_bytes),
        gc_able_parts_count,
    );
    println!(
        "      gc-able rollups:    {} ({} blobs)",
        human_bytes(gc_able_rollups_bytes),
        gc_able_rollups_count,
    );
    println!(
        "    current:              {} ({} blobs)",
        human_bytes(current_parts_bytes + current_rollups_bytes),
        current_parts_count + current_rollups_count,
    );
    println!(
        "      current parts:      {} ({} blobs)",
        human_bytes(current_parts_bytes),
        current_parts_count,
    );
    println!(
        "      current rollups:    {} ({} blobs)",
        human_bytes(current_rollups_bytes),
        current_rollups_count,
    );

    Ok(())
}

fn human_bytes(bytes: usize) -> String {
    if bytes < 10_240 {
        return format!("{}B", bytes);
    }
    #[allow(clippy::as_conversions)]
    let mut bytes = bytes as f64 / 1_024f64;
    if bytes < 10_240f64 {
        return format!("{:.1}KiB", bytes);
    }
    bytes = bytes / 1_024f64;
    if bytes < 10_240f64 {
        return format!("{:.1}MiB", bytes);
    }
    bytes = bytes / 1_024f64;
    if bytes < 10_240f64 {
        return format!("{:.1}GiB", bytes);
    }
    bytes = bytes / 1_024f64;
    format!("{:.1}TiB", bytes)
}

// All `inspect` command are read-only.
const NO_COMMIT: bool = false;

impl StateArgs {
    fn shard_id(&self) -> ShardId {
        ShardId::from_str(&self.shard_id).expect("invalid shard id")
    }

    async fn open(&self) -> Result<StateVersions, anyhow::Error> {
        let cfg = PersistConfig::new(&READ_ALL_BUILD_INFO, SYSTEM_TIME.clone());
        let metrics = Arc::new(Metrics::new(&cfg, &MetricsRegistry::new()));
        let consensus =
            make_consensus(&cfg, &self.consensus_uri, NO_COMMIT, Arc::clone(&metrics)).await?;
        let blob = make_blob(&self.blob_uri, NO_COMMIT, Arc::clone(&metrics)).await?;
        Ok(StateVersions::new(cfg, consensus, blob, metrics))
    }
}

/// The following is a very terrible hack that no one should draw inspiration from. Currently State
/// is generic over <K, V, T, D>, with KVD being represented as phantom data for type safety and to
/// detect persisted codec mismatches. However, reading persisted States does not require actually
/// decoding KVD, so we only need their codec _names_ to match, not the full types. For the purposes
/// of `persistcli inspect`, which only wants to read the persistent data, we create new types that
/// return static Codec names, and rebind the names if/when we get a CodecMismatch, so we can convince
/// the type system and our safety checks that we really can read the data.

#[derive(Debug)]
pub(crate) struct K;
#[derive(Debug)]
pub(crate) struct V;
#[derive(Debug)]
struct T;
#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Ord)]
struct D(i64);

pub(crate) static KVTD_CODECS: Mutex<(String, String, String, String)> =
    Mutex::new((String::new(), String::new(), String::new(), String::new()));

impl Codec for K {
    type Schema = TodoSchema<K>;

    fn codec_name() -> String {
        KVTD_CODECS.lock().expect("lockable").0.clone()
    }

    fn encode<B>(&self, _buf: &mut B)
    where
        B: BufMut,
    {
    }

    fn decode(_buf: &[u8]) -> Result<Self, String> {
        Ok(Self)
    }
}

impl Codec for V {
    type Schema = TodoSchema<V>;

    fn codec_name() -> String {
        KVTD_CODECS.lock().expect("lockable").1.clone()
    }

    fn encode<B>(&self, _buf: &mut B)
    where
        B: BufMut,
    {
    }

    fn decode(_buf: &[u8]) -> Result<Self, String> {
        Ok(Self)
    }
}

impl Codec for T {
    type Schema = TodoSchema<T>;

    fn codec_name() -> String {
        KVTD_CODECS.lock().expect("lockable").2.clone()
    }

    fn encode<B>(&self, _buf: &mut B)
    where
        B: BufMut,
    {
    }

    fn decode(_buf: &[u8]) -> Result<Self, String> {
        Ok(Self)
    }
}

impl Codec64 for D {
    fn codec_name() -> String {
        KVTD_CODECS.lock().expect("lockable").3.clone()
    }

    fn encode(&self) -> [u8; 8] {
        [0; 8]
    }

    fn decode(_buf: [u8; 8]) -> Self {
        Self(0)
    }
}

impl Semigroup for D {
    fn plus_equals(&mut self, _rhs: &Self) {}

    fn is_zero(&self) -> bool {
        false
    }
}
