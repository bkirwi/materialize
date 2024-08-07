// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Benchmarks of internal code not exposed through the public API.

#![allow(missing_docs)]

use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use crate::cfg::PersistConfig;
use crate::fetch::FetchBatchFilter;
use differential_dataflow::trace::Description;
use mz_ore::metrics::MetricsRegistry;
use mz_persist::indexed::encoding::BlobTraceUpdates;
use mz_persist::mem::{MemBlob, MemBlobConfig};
use mz_persist::workload::DataGenerator;
use mz_persist_types::ShardId;
use timely::progress::Antichain;
use tracing::info;

use crate::internal::state::HollowBatch;
use crate::internal::trace::Trace;
use crate::iter::{ConsolidationPart, Consolidator};
use crate::metrics::Metrics;

pub fn trace_push_batch_one_iter(num_batches: usize) {
    let mut trace = Trace::<usize>::default();
    let mut start = Instant::now();
    for ts in 0..num_batches {
        if ts % 1000 == 0 {
            info!("{} {:?}", ts, start.elapsed());
            start = Instant::now();
        }
        // A single non-empty batch followed by a large number of empty batches
        // and no compaction. This is a particularly problematic workload for
        // our fork of Spine which came up during deserialization of State in
        // #17214.
        //
        // Other, much better handled, workloads include all empty or all
        // non-empty.
        let len = if ts == 0 { 1 } else { 0 };
        let _ = trace.push_batch(HollowBatch::new(
            Description::new(
                Antichain::from_elem(ts),
                Antichain::from_elem(ts + 1),
                Antichain::from_elem(0),
            ),
            vec![],
            len,
            vec![],
        ));
    }
    black_box(trace);
}

pub async fn consolidate(data: &DataGenerator) -> anyhow::Result<()> {
    let shard_id = ShardId::new();
    let blob = MemBlob::open(MemBlobConfig::new(false));
    let metrics = Metrics::new(&PersistConfig::new_for_tests(), &MetricsRegistry::new());
    let read_metrics = metrics.read.compaction.clone();
    let shard_metrics = metrics.shards.shard(&shard_id, "benchmarking");
    let mut consolidator: Consolidator<u64, i64> = Consolidator::new(
        "bench".to_string(),
        ShardId::new(),
        Arc::new(blob),
        Arc::new(metrics),
        shard_metrics,
        read_metrics,
        FetchBatchFilter::Snapshot {
            as_of: Antichain::from_elem(0u64),
        },
        0,
        false,
    );
    for batch in data.batches() {
        let part = ConsolidationPart::from_updates(BlobTraceUpdates::Row(batch), true);
        consolidator.push_run([(part, 100)].into());
    }

    while let Some(out) = consolidator.next_chunk(10000).await? {
        black_box(out);
    }

    Ok(())
}
