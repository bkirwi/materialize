// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A source that reads from an a persist shard.

use std::any::Any;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use mz_persist_client::operators::shard_source::shard_source;
use mz_persist_client::stats::PartStats;
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::columnar::Data;
use mz_persist_types::stats::{ColumnStats, DynStats};
use mz_repr::stats::PersistSourceDataStats;
use timely::communication::Push;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::channels::Bundle;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::{Capability, OkErr};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use timely::scheduling::Activator;

use mz_expr::{ColumnSpecs, Interpreter, MfpPlan, ResultSpec, UnmaterializableFunc};
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::fetch::FetchedPart;
use mz_repr::{
    Datum, DatumToPersist, DatumToPersistFn, DatumVec, Diff, GlobalId, RelationDesc, Row, RowArena,
    Timestamp,
};
use tracing::error;

use crate::controller::CollectionMetadata;
use crate::types::errors::DataflowError;
use crate::types::sources::{fake_relation_desc, SourceData, SOURCE_DATA_ERROR};

pub use mz_persist_client::operators::shard_source::FlowControl;
use mz_timely_util::buffer::ConsolidateBuffer;

/// Creates a new source that reads from a persist shard, distributing the work
/// of reading data to all timely workers.
///
/// All times emitted will have been [advanced by] the given `as_of` frontier.
/// All updates at times greater or equal to `until` will be suppressed.
/// The `map_filter_project` argument, if supplied, may be partially applied,
/// and any un-applied part of the argument will be left behind in the argument.
///
/// Users of this function have the ability to apply flow control to the output
/// to limit the in-flight data (measured in bytes) it can emit. The flow control
/// input is a timely stream that communicates the frontier at which the data
/// emitted from by this source have been dropped.
///
/// **Note:** Because this function is reading batches from `persist`, it is working
/// at batch granularity. In practice, the source will be overshooting the target
/// flow control upper by an amount that is related to the size of batches.
///
/// If no flow control is desired an empty stream whose frontier immediately advances
/// to the empty antichain can be used. An easy easy of creating such stream is by
/// using [`timely::dataflow::operators::generic::operator::empty`].
///
/// [advanced by]: differential_dataflow::lattice::Lattice::advance_by
pub fn persist_source<G, YFn>(
    scope: &G,
    source_id: GlobalId,
    persist_clients: Arc<PersistClientCache>,
    metadata: CollectionMetadata,
    as_of: Option<Antichain<Timestamp>>,
    until: Antichain<Timestamp>,
    map_filter_project: Option<&mut MfpPlan>,
    flow_control: Option<FlowControl<G>>,
    yield_fn: YFn,
) -> (
    Stream<G, (Row, Timestamp, Diff)>,
    Stream<G, (DataflowError, Timestamp, Diff)>,
    Rc<dyn Any>,
)
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
    YFn: Fn(Instant, usize) -> bool + 'static,
{
    let (stream, token) = persist_source_core(
        scope,
        source_id,
        persist_clients,
        metadata,
        as_of,
        until,
        map_filter_project,
        flow_control,
        yield_fn,
    );
    let (ok_stream, err_stream) = stream.ok_err(|(d, t, r)| match d {
        Ok(row) => Ok((row, t, r)),
        Err(err) => Err((err, t, r)),
    });
    (ok_stream, err_stream, token)
}

/// Creates a new source that reads from a persist shard, distributing the work
/// of reading data to all timely workers.
///
/// All times emitted will have been [advanced by] the given `as_of` frontier.
///
/// [advanced by]: differential_dataflow::lattice::Lattice::advance_by
#[allow(clippy::needless_borrow)]
pub fn persist_source_core<G, YFn>(
    scope: &G,
    source_id: GlobalId,
    persist_clients: Arc<PersistClientCache>,
    metadata: CollectionMetadata,
    as_of: Option<Antichain<Timestamp>>,
    until: Antichain<Timestamp>,
    map_filter_project: Option<&mut MfpPlan>,
    flow_control: Option<FlowControl<G>>,
    yield_fn: YFn,
) -> (
    Stream<G, (Result<Row, DataflowError>, Timestamp, Diff)>,
    Rc<dyn Any>,
)
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
    YFn: Fn(Instant, usize) -> bool + 'static,
{
    let name = source_id.to_string();
    let desc = metadata.relation_desc.clone();
    let fake_desc = fake_relation_desc(&desc);
    let filter_plan = map_filter_project.as_ref().map(|p| (*p).clone());
    let time_range = if let Some(lower) = as_of.as_ref().and_then(|a| a.as_option().copied()) {
        // If we have a lower bound, we can provide a bound on mz_now to our filter pushdown.
        // Range is inclusive, so it's safe to use the maximum timestamp as the upper bound when
        // `until ` is the empty antichain.
        // TODO: continually narrow this as the frontier progresses.
        let upper = until.as_option().copied().unwrap_or(Timestamp::MAX);
        ResultSpec::value_between(Datum::MzTimestamp(lower), Datum::MzTimestamp(upper))
    } else {
        ResultSpec::anything()
    };
    let (fetched, token) = shard_source(
        &mut scope.clone(),
        &name,
        persist_clients,
        metadata.persist_location,
        metadata.data_shard,
        as_of,
        until.clone(),
        flow_control,
        Arc::new(metadata.relation_desc),
        Arc::new(UnitSchema),
        move |stats| {
            if let Some(plan) = &filter_plan {
                let arena = RowArena::new();
                let mut ranges = ColumnSpecs::new(desc.typ().clone(), &arena);
                // TODO: even better if we can use the lower bound of the part itself!
                ranges.push_unmaterializable(UnmaterializableFunc::MzNow, time_range);
                let stats = PersistSourceDataStatsImpl {
                    desc: &fake_desc,
                    stats,
                };
                let total_count = stats.len();
                for (id, _) in desc.iter().enumerate() {
                    let min = stats.col_min(id, &arena);
                    let max = stats.col_max(id, &arena);
                    let nulls = stats.col_null_count(id);
                    if let (Some(total_count), Some(min), Some(max), Some(nulls)) =
                        (total_count, min, max, nulls)
                    {
                        let value_range = if nulls == total_count {
                            ResultSpec::nothing()
                        } else {
                            ResultSpec::value_between(min, max)
                        };
                        let null_range = if nulls == 0 {
                            ResultSpec::nothing()
                        } else {
                            ResultSpec::null()
                        };
                        let range = value_range.union(null_range);
                        if nulls > 0 {
                            assert!(range.may_contain(Datum::Null));
                        }
                        ranges.push_column(id, range);
                    }
                }
                let result = ranges.eval_mfp_plan(plan).range;
                result.may_contain(Datum::True) || result.may_fail()
            } else {
                true
            }
        },
    );
    let rows = decode_and_mfp(&fetched, &name, until, map_filter_project, yield_fn);
    (rows, token)
}

pub fn decode_and_mfp<G, YFn>(
    fetched: &Stream<G, FetchedPart<SourceData, (), Timestamp, Diff>>,
    name: &str,
    until: Antichain<Timestamp>,
    mut map_filter_project: Option<&mut MfpPlan>,
    yield_fn: YFn,
) -> Stream<G, (Result<Row, DataflowError>, Timestamp, Diff)>
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
    YFn: Fn(Instant, usize) -> bool + 'static,
{
    let scope = fetched.scope();
    let mut builder = OperatorBuilder::new(
        format!("persist_source::decode_and_mfp({})", name),
        scope.clone(),
    );
    let operator_info = builder.operator_info();

    let mut fetched_input = builder.new_input(fetched, Pipeline);
    let (mut updates_output, updates_stream) = builder.new_output();

    // Re-used state for processing and building rows.
    let mut datum_vec = mz_repr::DatumVec::new();
    let mut row_builder = Row::default();

    // Extract the MFP if it exists; leave behind an identity MFP in that case.
    let map_filter_project = map_filter_project.as_mut().map(|mfp| mfp.take());

    builder.build(move |_caps| {
        // Acquire an activator to reschedule the operator when it has unfinished work.
        let activations = scope.activations();
        let activator = Activator::new(&operator_info.address[..], activations);
        // Maintain a list of work to do
        let mut pending_work = std::collections::VecDeque::new();
        let mut buffer = Default::default();

        move |_frontier| {
            fetched_input.for_each(|time, data| {
                data.swap(&mut buffer);
                let capability = time.retain();
                for fetched_part in buffer.drain(..) {
                    pending_work.push_back(PendingWork {
                        capability: capability.clone(),
                        fetched_part,
                    })
                }
            });

            let mut work = 0;
            let start_time = Instant::now();
            let mut output = updates_output.activate();
            let mut handle = ConsolidateBuffer::new(&mut output, 0);
            while !pending_work.is_empty() && !yield_fn(start_time, work) {
                let done = pending_work.front_mut().unwrap().do_work(
                    &mut work,
                    start_time,
                    &yield_fn,
                    &until,
                    map_filter_project.as_ref(),
                    &mut datum_vec,
                    &mut row_builder,
                    &mut handle,
                );
                if done {
                    pending_work.pop_front();
                }
            }
            if !pending_work.is_empty() {
                activator.activate();
            }
        }
    });

    updates_stream
}

/// Pending work to read from fetched parts
struct PendingWork {
    /// The time at which the work should happen.
    capability: Capability<Timestamp>,
    /// Pending fetched part.
    fetched_part: FetchedPart<SourceData, (), Timestamp, Diff>,
}

impl PendingWork {
    /// Perform work, reading from the fetched part, decoding, and sending outputs, while checking
    /// `yield_fn` whether more fuel is available.
    fn do_work<P, YFn>(
        &mut self,
        work: &mut usize,
        start_time: Instant,
        yield_fn: YFn,
        until: &Antichain<Timestamp>,
        map_filter_project: Option<&MfpPlan>,
        datum_vec: &mut DatumVec,
        row_builder: &mut Row,
        output: &mut ConsolidateBuffer<Timestamp, Result<Row, DataflowError>, Diff, P>,
    ) -> bool
    where
        P: Push<Bundle<Timestamp, (Result<Row, DataflowError>, Timestamp, Diff)>>,
        YFn: Fn(Instant, usize) -> bool,
    {
        while let Some(((key, val), time, diff)) = self.fetched_part.next() {
            if until.less_equal(&time) {
                continue;
            }
            match (key, val) {
                (Ok(SourceData(Ok(row))), Ok(())) => {
                    if let Some(mfp) = map_filter_project {
                        let arena = mz_repr::RowArena::new();
                        let mut datums_local = datum_vec.borrow_with(&row);
                        for result in mfp.evaluate(
                            &mut datums_local,
                            &arena,
                            time,
                            diff,
                            |time| !until.less_equal(time),
                            row_builder,
                        ) {
                            match result {
                                Ok((row, time, diff)) => {
                                    // Additional `until` filtering due to temporal filters.
                                    if !until.less_equal(&time) {
                                        output.give_at(&self.capability, (Ok(row), time, diff));
                                        *work += 1;
                                    }
                                }
                                Err((err, time, diff)) => {
                                    // Additional `until` filtering due to temporal filters.
                                    if !until.less_equal(&time) {
                                        output.give_at(&self.capability, (Err(err), time, diff));
                                        *work += 1;
                                    }
                                }
                            }
                        }
                    } else {
                        output.give_at(&self.capability, (Ok(row), time, diff));
                        *work += 1;
                    }
                }
                (Ok(SourceData(Err(err))), Ok(())) => {
                    output.give_at(&self.capability, (Err(err), time, diff));
                    *work += 1;
                }
                // TODO(petrosagg): error handling
                (Err(_), Ok(_)) | (Ok(_), Err(_)) | (Err(_), Err(_)) => {
                    panic!("decoding failed")
                }
            }
            if yield_fn(start_time, *work) {
                return false;
            }
        }
        true
    }
}

#[derive(Debug)]
struct PersistSourceDataStatsImpl<'a> {
    desc: &'a RelationDesc,
    stats: &'a PartStats,
}

fn downcast_stats<'a, T: Data>(stats: &'a dyn DynStats) -> Option<&'a T::Stats> {
    match stats.as_any().downcast_ref::<T::Stats>() {
        Some(x) => Some(x),
        None => {
            error!(
                "unexpected stats type for {}: {}",
                std::any::type_name::<T>(),
                stats.type_name()
            );
            None
        }
    }
}

impl PersistSourceDataStats for PersistSourceDataStatsImpl<'_> {
    fn len(&self) -> Option<usize> {
        Some(self.stats.key.len)
    }

    fn err_count(&self) -> Option<usize> {
        // Counter-intuitive: We can easily calculate the number of errors that
        // were None from the column stats, but not how many were Some. So, what
        // we do is count the number of Nones, which is the number of Oks, and
        // then subtract that from the total.
        let num_results = self.stats.key.len;
        let num_oks = self
            .stats
            .key
            .col::<Option<Vec<u8>>>(SOURCE_DATA_ERROR)
            .expect("stats type should match column")
            .map(|x| x.none);
        num_oks.map(|num_oks| num_results - num_oks)
    }

    fn col_min<'a>(&'a self, idx: usize, arena: &'a RowArena) -> Option<Datum<'a>> {
        struct ColMin<'a>(&'a dyn DynStats, &'a RowArena);
        impl<'a> DatumToPersistFn<Option<Datum<'a>>> for ColMin<'a> {
            fn call<T: DatumToPersist>(self) -> Option<Datum<'a>> {
                let ColMin(stats, arena) = self;
                downcast_stats::<T::Data>(stats)?
                    .lower()
                    .map(|val| arena.make_datum(|packer| T::decode(val, packer)))
            }
        }

        let name = self.desc.get_name(idx);
        let typ = &self.desc.typ().column_types[idx];
        let stats = self.stats.key.cols.get(name.as_str())?;
        typ.to_persist(ColMin(stats.as_ref(), arena))
    }

    fn col_max<'a>(&'a self, idx: usize, arena: &'a RowArena) -> Option<Datum<'a>> {
        struct ColMax<'a>(&'a dyn DynStats, &'a RowArena);
        impl<'a> DatumToPersistFn<Option<Datum<'a>>> for ColMax<'a> {
            fn call<T: DatumToPersist>(self) -> Option<Datum<'a>> {
                let ColMax(stats, arena) = self;
                downcast_stats::<T::Data>(stats)?
                    .upper()
                    .map(|val| arena.make_datum(|packer| T::decode(val, packer)))
            }
        }

        let name = self.desc.get_name(idx);
        let typ = &self.desc.typ().column_types[idx];
        let stats = self.stats.key.cols.get(name.as_str())?;
        typ.to_persist(ColMax(stats.as_ref(), arena))
    }

    fn col_null_count(&self, idx: usize) -> Option<usize> {
        struct ColNullCount<'a>(&'a dyn DynStats);
        impl<'a> DatumToPersistFn<Option<usize>> for ColNullCount<'a> {
            fn call<T: DatumToPersist>(self) -> Option<usize> {
                let ColNullCount(stats) = self;
                let stats = downcast_stats::<T::Data>(stats).expect("WIP");
                Some(stats.none_count())
            }
        }

        let name = self.desc.get_name(idx);
        let typ = &self.desc.typ().column_types[idx];
        let stats = self.stats.key.cols.get(name.as_str());
        typ.to_persist(ColNullCount(stats?.as_ref()))
    }

    fn row_min(&self, _row: &mut Row) -> Option<usize> {
        None
    }

    fn row_max(&self, _row: &mut Row) -> Option<usize> {
        None
    }
}

#[cfg(test)]
mod tests {
    use mz_persist_client::stats::PartStats;
    use mz_persist_types::codec_impls::UnitSchema;
    use mz_persist_types::columnar::{PartEncoder, Schema};
    use mz_persist_types::part::PartBuilder;
    use mz_persist_types::stats::StructStats;
    use mz_repr::stats::PersistSourceDataStats;
    use mz_repr::{
        Datum, DatumToPersist, DatumToPersistFn, RelationDesc, Row, RowArena, ScalarType,
    };
    use proptest::prelude::*;

    use crate::source::persist_source::PersistSourceDataStatsImpl;
    use crate::types::sources::{fake_relation_desc, SourceData};

    proptest! {
       #[test]
       #[cfg_attr(miri, ignore)] // too slow
        fn scalar_type_columnar_roundtrip(scalar_type in any::<ScalarType>() ) {
            use mz_persist_types::parquet::validate_roundtrip;
            let mut rows = Vec::new();
            for datum in scalar_type.interesting_datums() {
               rows.push(SourceData(Ok(Row::pack(std::iter::once(datum)))));
            }

            // Non-nullable version of the column.
            let schema = RelationDesc::empty().with_column("col", scalar_type.clone().nullable(false));
            for row in rows.iter() {
                assert_eq!(validate_roundtrip(&schema, row), Ok(()));
            }

            // Nullable version of the column.
            let schema = RelationDesc::empty().with_column("col", scalar_type.nullable(true));
            rows.push(SourceData(Ok(Row::pack(std::iter::once(Datum::Null)))));
            for row in rows.iter() {
                assert_eq!(validate_roundtrip(&schema, row), Ok(()));
            }
        }
    }

    /// A helper for writing tests that validate stats collection.
    pub fn validate_stats<T, S, F>(schema: &S, val: &T, validate: F) -> Result<(), String>
    where
        T: Default + PartialEq + std::fmt::Debug,
        S: Schema<T>,
        F: FnOnce(&PartStats) -> Result<(), String>,
    {
        let mut part = PartBuilder::new(schema, &UnitSchema);
        {
            let part_mut = part.get_mut();
            schema.encoder(part_mut.key)?.encode(val);
            part_mut.ts.push(1);
            part_mut.diff.push(1);
        }
        let part = part.finish()?;

        let mut key = StructStats {
            len: part.len(),
            cols: Default::default(),
        };
        let mut key_cols = part.key_ref();
        for (name, _typ, stats_fn) in schema.columns() {
            let col_stats = key_cols.stats(&name, stats_fn)?;
            key.cols.insert(name, col_stats);
        }
        key_cols.finish()?;
        let stats = PartStats { key };

        validate(&stats)
    }

    struct VerifyStatsSome<'a>(PersistSourceDataStatsImpl<'a>, &'a RowArena, Datum<'a>);
    impl<'a> DatumToPersistFn<()> for VerifyStatsSome<'a> {
        fn call<T: DatumToPersist>(self) -> () {
            let VerifyStatsSome(stats, arena, datum) = self;
            if let Some(lower) = stats.col_min(0, arena) {
                assert!(lower <= datum, "{} vs {} stats={:?}", lower, datum, stats);
            }
            if let Some(upper) = stats.col_max(0, arena) {
                assert!(upper >= datum, "{} vs {}", upper, datum);
            }
            assert_eq!(stats.col_null_count(0), Some(0));
        }
    }

    struct VerifyStatsNone<'a>(PersistSourceDataStatsImpl<'a>);
    impl<'a> DatumToPersistFn<()> for VerifyStatsNone<'a> {
        fn call<T: DatumToPersist>(self) -> () {
            let VerifyStatsNone(stats) = self;
            // assert!(stats.lower().is_none());
            // assert!(stats.upper().is_none());
            assert_eq!(stats.col_null_count(0), Some(1));
        }
    }

    proptest! {
       #[test]
       #[cfg_attr(miri, ignore)] // too slow
        fn scalar_type_stats_roundtrip(scalar_type in any::<ScalarType>() ) {
            mz_ore::test::init_logging();
            let arena = RowArena::default();

            // Non-nullable version of the column.
            let column_type = scalar_type.clone().nullable(false);
            let schema = RelationDesc::empty().with_column("col", column_type.clone());
            let fake_schema = fake_relation_desc(&schema);
            for datum in scalar_type.interesting_datums() {
                let row = SourceData(Ok(Row::pack(std::iter::once(datum))));
                let res = validate_stats(&schema, &row, |stats| {
                    let stats = PersistSourceDataStatsImpl{stats,desc: &fake_schema };
                    column_type.to_persist(VerifyStatsSome(stats, &arena, datum));
                    Ok(())
                });
                assert_eq!(res, Ok(()));
             }

            // Nullable version of the column.
            let column_type = scalar_type.clone().nullable(true);
            let schema = RelationDesc::empty().with_column("col", column_type.clone());
            let fake_schema = fake_relation_desc(&schema);
            for datum in scalar_type.interesting_datums() {
                let row = SourceData(Ok(Row::pack(std::iter::once(datum))));
                let res = validate_stats(&schema, &row, |stats| {
                    let stats = PersistSourceDataStatsImpl{stats,desc: &fake_schema };
                    column_type.to_persist(VerifyStatsSome(stats, &arena, datum));
                    Ok(())
                });
                assert_eq!(res, Ok(()));
            }
            let row = SourceData(Ok(Row::pack(std::iter::once(Datum::Null))));
            let res = validate_stats(&schema, &row, |stats| {
                let stats = PersistSourceDataStatsImpl{stats,desc: &fake_schema };
                column_type.to_persist(VerifyStatsNone(stats));
                Ok(())
            });
            assert_eq!(res, Ok(()));
        }
    }
}
