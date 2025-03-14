// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A mechanism to ensure that a sequence of writes and reads proceed correctly through timestamps.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use crate::coord::id_bundle::CollectionIdBundle;
use crate::coord::read_policy::ReadHolds;
use crate::coord::Coordinator;
use crate::AdapterError;
use chrono::{DateTime, Utc};
use futures::Future;
use itertools::Itertools;
use mz_adapter_types::connection::ConnectionId;
use mz_catalog::memory::objects::{CatalogItem, ContinualTask, MaterializedView, View};
use mz_compute_types::ComputeInstanceId;
use mz_expr::CollectionPlan;
use mz_ore::instrument;
use mz_ore::now::{to_datetime, EpochMillis, NowFn};
use mz_repr::{CatalogItemId, GlobalId, Timestamp};
use mz_sql::names::{ResolvedDatabaseSpecifier, SchemaSpecifier};
use mz_timestamp_oracle::batching_oracle::BatchingTimestampOracle;
use mz_timestamp_oracle::postgres_oracle::{
    PostgresTimestampOracle, PostgresTimestampOracleConfig,
};
use mz_timestamp_oracle::{self, TimestampOracle, WriteTimestamp};
use timely::progress::Timestamp as TimelyTimestamp;
use tracing::{error, info, Instrument};

/// An enum describing whether or not a query belongs to a timeline and whether the query can be
/// affected by the timestamp at which it executes.
#[derive(Clone, Copy, Debug, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub enum TimelineContext {
    /// Can belong to any timeline. The answer will depend on a timestamp chosen from some
    /// timeline.
    TimestampDependent,
    /// The answer does not depend on a chosen timestamp.
    TimestampIndependent,
}

impl TimelineContext {
    pub fn timestamp_dependent(&self) -> bool {
        match self {
            TimelineContext::TimestampDependent => true,
            TimelineContext::TimestampIndependent => false,
        }
    }
}

/// Global state for a single timeline.
///
/// For each timeline we maintain a timestamp oracle, which is responsible for
/// providing read (and sometimes write) timestamps, and a set of read holds which
/// guarantee that those read timestamps are valid.
pub(crate) struct TimelineState<T: TimelyTimestamp> {
    pub(crate) oracle: Arc<dyn TimestampOracle<T> + Send + Sync>,
    pub(crate) read_holds: ReadHolds<T>,
}

impl<T: TimelyTimestamp> fmt::Debug for TimelineState<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TimelineState")
            .field("read_holds", &self.read_holds)
            .finish()
    }
}

impl Coordinator {
    pub(crate) fn now(&self) -> EpochMillis {
        (self.catalog().config().now)()
    }

    pub(crate) fn now_datetime(&self) -> DateTime<Utc> {
        to_datetime(self.now())
    }

    pub(crate) fn get_timestamp_oracle(&self) -> Arc<dyn TimestampOracle<Timestamp> + Send + Sync> {
        let oracle = &self.global_timeline.oracle;
        Arc::clone(oracle)
    }

    /// Returns a [`TimestampOracle`] used for reads and writes from/to a local input.
    pub(crate) fn get_local_timestamp_oracle(
        &self,
    ) -> Arc<dyn TimestampOracle<Timestamp> + Send + Sync> {
        self.get_timestamp_oracle()
    }

    /// Assign a timestamp for a read from a local input. Reads following writes
    /// must be at a time >= the write's timestamp; we choose "equal to" for
    /// simplicity's sake and to open as few new timestamps as possible.
    pub(crate) async fn get_local_read_ts(&self) -> Timestamp {
        self.get_local_timestamp_oracle().read_ts().await
    }

    /// Assign a timestamp for a write to a local input and increase the local ts.
    /// Writes following reads must ensure that they are assigned a strictly larger
    /// timestamp to ensure they are not visible to any real-time earlier reads.
    #[instrument(name = "coord::get_local_write_ts")]
    pub(crate) async fn get_local_write_ts(&mut self) -> WriteTimestamp {
        self.global_timeline.oracle.write_ts().await
    }

    /// Peek the current timestamp used for operations on local inputs. Used to determine how much
    /// to block group commits by.
    pub(crate) async fn peek_local_write_ts(&self) -> Timestamp {
        self.get_local_timestamp_oracle().peek_write_ts().await
    }

    /// Marks a write at `timestamp` as completed, using a [`TimestampOracle`].
    pub(crate) fn apply_local_write(
        &self,
        timestamp: Timestamp,
    ) -> impl Future<Output = ()> + Send + 'static {
        let now = self.now().into();

        let upper_bound = upper_bound(&now);
        if timestamp > upper_bound {
            error!(
                %now,
                "Setting local read timestamp to {timestamp}, which is more than \
                the desired upper bound {upper_bound}."
            );
        }

        let oracle = self.get_local_timestamp_oracle();

        async move {
            oracle
                .apply_write(timestamp)
                .instrument(tracing::debug_span!("apply_local_write_static", ?timestamp))
                .await
        }
    }

    /// Assign a timestamp for a write to the catalog. This timestamp should have the following
    /// properties:
    ///
    ///   - Monotonically increasing.
    ///   - Greater than or equal to the current catalog upper.
    ///   - Greater than the largest write timestamp used in the
    ///     [epoch millisecond timeline](Timeline::EpochMilliseconds).
    ///
    /// In general this is fully satisfied by the getting the current write timestamp in the
    /// [epoch millisecond timeline](Timeline::EpochMilliseconds) from the timestamp oracle,
    /// however, in read-only mode we cannot modify the timestamp oracle.
    pub(crate) async fn get_catalog_write_ts(&mut self) -> Timestamp {
        if self.read_only_controllers {
            let (write_ts, upper) =
                futures::future::join(self.peek_local_write_ts(), self.catalog().current_upper())
                    .await;
            std::cmp::max(write_ts, upper)
        } else {
            self.get_local_write_ts().await.timestamp
        }
    }

    /// Ensures that a global timeline state exists for `timeline`, with an initial time
    /// of `initially`.
    #[instrument]
    pub(crate) async fn ensure_timeline_state_with_initial_time<'a>(
        initially: Timestamp,
        now: NowFn,
        pg_oracle_config: Option<PostgresTimestampOracleConfig>,
        read_only: bool,
    ) -> TimelineState<Timestamp> {
        info!("opening a new CRDB/postgres TimestampOracle for timeline",);

        let now_fn = now;

        let pg_oracle_config = pg_oracle_config.expect(
                        "missing --timestamp-oracle-url even though the crdb-backed timestamp oracle was configured");

        let batching_metrics = Arc::clone(&pg_oracle_config.metrics);

        let pg_oracle: Arc<dyn TimestampOracle<mz_repr::Timestamp> + Send + Sync> = Arc::new(
            PostgresTimestampOracle::open(
                pg_oracle_config,
                "M".to_string(),
                initially,
                now_fn,
                read_only,
            )
            .await,
        );

        let batching_oracle = BatchingTimestampOracle::new(batching_metrics, pg_oracle);

        let oracle: Arc<dyn TimestampOracle<mz_repr::Timestamp> + Send + Sync> =
            Arc::new(batching_oracle);

        TimelineState {
            oracle,
            read_holds: ReadHolds::new(),
        }
    }

    /// Return an error if the ids are from incompatible [`TimelineContext`]s. This should
    /// be used to prevent users from doing things that are either meaningless
    /// (joining data from timelines that have similar numbers with different
    /// meanings like two separate debezium topics) or will never complete (joining
    /// cdcv2 and realtime data).
    pub(crate) fn validate_timeline_context<I>(
        &self,
        ids: I,
    ) -> Result<TimelineContext, AdapterError>
    where
        I: IntoIterator<Item = GlobalId>,
    {
        let items_ids = ids
            .into_iter()
            .filter_map(|gid| self.catalog().try_resolve_item_id(&gid));
        let timeline_contexts: Vec<_> = self.get_timeline_contexts(items_ids).into_iter().collect();

        // A single or group of objects may contain multiple compatible timeline
        // contexts. For example `SELECT *, 1, mz_now() FROM t` will contain all
        // types of contexts. We choose the strongest context level to return back.
        if timeline_contexts
            .iter()
            .contains(&TimelineContext::TimestampDependent)
        {
            Ok(TimelineContext::TimestampDependent)
        } else {
            Ok(TimelineContext::TimestampIndependent)
        }
    }

    // /// Return the [`TimelineContext`] belonging to a [`CatalogItemId`], if one exists.
    // pub(crate) fn get_timeline_context(&self, id: CatalogItemId) -> TimelineContext {
    //     let entry = self.catalog().get_entry(&id);
    //     self.validate_timeline_context(entry.global_ids())
    //         .expect("impossible for a single object to belong to incompatible timeline contexts")
    // }
    //
    // /// Return the [`TimelineContext`] belonging to a [`GlobalId`], if one exists.
    // pub(crate) fn get_timeline_context_for_global_id(&self, id: GlobalId) -> TimelineContext {
    //     self.validate_timeline_context(vec![id])
    //         .expect("impossible for a single object to belong to incompatible timeline contexts")
    // }

    /// Return the [`TimelineContext`]s belonging to a list of [`CatalogItemId`]s, if any exist.
    fn get_timeline_contexts<I>(&self, ids: I) -> BTreeSet<TimelineContext>
    where
        I: IntoIterator<Item = CatalogItemId>,
    {
        let mut seen: BTreeSet<CatalogItemId> = BTreeSet::new();
        let mut timelines: BTreeSet<TimelineContext> = BTreeSet::new();

        // Recurse through IDs to find all sources and tables, adding new ones to
        // the set until we reach the bottom.
        let mut ids: Vec<_> = ids.into_iter().collect();
        while let Some(id) = ids.pop() {
            // Protect against possible infinite recursion. Not sure if it's possible, but
            // a cheap prevention for the future.
            if !seen.insert(id) {
                continue;
            }
            if let Some(entry) = self.catalog().try_get_entry(&id) {
                match entry.item() {
                    CatalogItem::Source(_) => {
                        timelines.insert(TimelineContext::TimestampDependent);
                    }
                    CatalogItem::Index(index) => {
                        let on_id = self.catalog().resolve_item_id(&index.on);
                        ids.push(on_id);
                    }
                    CatalogItem::View(View { optimized_expr, .. }) => {
                        // If the definition contains a temporal function, the timeline must
                        // be timestamp dependent.
                        if optimized_expr.contains_temporal() {
                            timelines.insert(TimelineContext::TimestampDependent);
                        } else {
                            timelines.insert(TimelineContext::TimestampIndependent);
                        }
                        let item_ids = optimized_expr
                            .depends_on()
                            .into_iter()
                            .map(|gid| self.catalog().resolve_item_id(&gid));
                        ids.extend(item_ids);
                    }
                    CatalogItem::MaterializedView(MaterializedView { optimized_expr, .. }) => {
                        // In some cases the timestamp selected may not affect the answer to a
                        // query, but it may affect our ability to query the materialized view.
                        // Materialized views must durably materialize the result of a query, even
                        // for constant queries. If we choose a timestamp larger than the upper,
                        // which represents the current progress of the view, then the query will
                        // need to block and wait for the materialized view to advance.
                        timelines.insert(TimelineContext::TimestampDependent);
                        let item_ids = optimized_expr
                            .depends_on()
                            .into_iter()
                            .map(|gid| self.catalog().resolve_item_id(&gid));
                        ids.extend(item_ids);
                    }
                    CatalogItem::ContinualTask(ContinualTask { raw_expr, .. }) => {
                        // See comment in MaterializedView
                        timelines.insert(TimelineContext::TimestampDependent);
                        let item_ids = raw_expr
                            .depends_on()
                            .into_iter()
                            .map(|gid| self.catalog().resolve_item_id(&gid));
                        ids.extend(item_ids);
                    }
                    CatalogItem::Table(_) => {
                        timelines.insert(TimelineContext::TimestampDependent);
                    }
                    CatalogItem::Log(_) => {
                        timelines.insert(TimelineContext::TimestampDependent);
                    }
                    CatalogItem::Sink(_)
                    | CatalogItem::Type(_)
                    | CatalogItem::Func(_)
                    | CatalogItem::Secret(_)
                    | CatalogItem::Connection(_) => {}
                }
            }
        }

        timelines
    }

    // /// Returns an iterator that partitions an id bundle by the [`TimelineContext`] that each id
    // /// belongs to.
    // pub fn partition_ids_by_timeline_context(
    //     &self,
    //     id_bundle: &CollectionIdBundle,
    // ) -> impl Iterator<Item = (TimelineContext, CollectionIdBundle)> {
    //     let mut res: BTreeMap<TimelineContext, CollectionIdBundle> = BTreeMap::new();
    //
    //     for gid in &id_bundle.storage_ids {
    //         let timeline_context = self.get_timeline_context_for_global_id(*gid);
    //         res.entry(timeline_context)
    //             .or_default()
    //             .storage_ids
    //             .insert(*gid);
    //     }
    //
    //     for (compute_instance, ids) in &id_bundle.compute_ids {
    //         for gid in ids {
    //             let timeline_context = self.get_timeline_context_for_global_id(*gid);
    //             res.entry(timeline_context)
    //                 .or_default()
    //                 .compute_ids
    //                 .entry(*compute_instance)
    //                 .or_default()
    //                 .insert(*gid);
    //         }
    //     }
    //
    //     res.into_iter()
    // }

    /// Return the set of ids in a timedomain and verify timeline correctness.
    ///
    /// When a user starts a transaction, we need to prevent compaction of anything
    /// they might read from. We use a heuristic of "anything in the same database
    /// schemas with the same timeline as whatever the first query is".
    pub(crate) fn timedomain_for<'a, I>(
        &self,
        uses_ids: I,
        timeline_context: &TimelineContext,
        conn_id: &ConnectionId,
        compute_instance: ComputeInstanceId,
    ) -> Result<CollectionIdBundle, AdapterError>
    where
        I: IntoIterator<Item = &'a GlobalId>,
    {
        // Gather all the used schemas.
        let mut schemas = BTreeSet::new();
        for id in uses_ids {
            let entry = self.catalog().get_entry_by_global_id(id);
            let name = entry.name();
            schemas.insert((name.qualifiers.database_spec, name.qualifiers.schema_spec));
        }

        let pg_catalog_schema = (
            ResolvedDatabaseSpecifier::Ambient,
            SchemaSpecifier::Id(self.catalog().get_pg_catalog_schema_id()),
        );
        let system_schemas: Vec<_> = self
            .catalog()
            .system_schema_ids()
            .map(|id| (ResolvedDatabaseSpecifier::Ambient, SchemaSpecifier::Id(id)))
            .collect();

        if system_schemas.iter().any(|s| schemas.contains(s)) {
            // If any of the system schemas is specified, add the rest of the
            // system schemas.
            schemas.extend(system_schemas);
        } else if !schemas.is_empty() {
            // Always include the pg_catalog schema, if schemas is non-empty. The pg_catalog schemas is
            // sometimes used by applications in followup queries.
            schemas.insert(pg_catalog_schema);
        }

        // Gather the IDs of all items in all used schemas.
        let mut collection_ids: BTreeSet<GlobalId> = BTreeSet::new();
        for (db, schema) in schemas {
            let schema = self.catalog().get_schema(&db, &schema, conn_id);
            // Note: We include just the latest `GlobalId` instead of all `GlobalId`s associated
            // with an object, because older versions will already get included, if there are
            // objects the depend on them.
            let global_ids = schema
                .items
                .values()
                .map(|item_id| self.catalog().get_entry(item_id).latest_global_id());
            collection_ids.extend(global_ids);
        }

        // Gather the dependencies of those items.
        let mut id_bundle: CollectionIdBundle = self
            .index_oracle(compute_instance)
            .sufficient_collections(collection_ids);

        // Filter out ids from different timelines.
        for ids in [
            &mut id_bundle.storage_ids,
            &mut id_bundle.compute_ids.entry(compute_instance).or_default(),
        ] {
            ids.retain(|gid| {
                let id_timeline_context = self
                    .validate_timeline_context(vec![*gid])
                    .expect("single id should never fail");
                id_timeline_context == *timeline_context
            });
        }

        Ok(id_bundle)
    }

    #[instrument(level = "debug")]
    pub(crate) async fn advance_timelines(&mut self) {
        let read_ts = self.global_timeline.oracle.read_ts().await;
        self.global_timeline.read_holds.downgrade(read_ts);
    }
}

/// Convenience function for calculating the current upper bound that we want to
/// prevent the global timestamp from exceeding.
fn upper_bound(now: &mz_repr::Timestamp) -> mz_repr::Timestamp {
    const TIMESTAMP_INTERVAL_MS: u64 = 5000;
    const TIMESTAMP_INTERVAL_UPPER_BOUND: u64 = 2;

    now.saturating_add(TIMESTAMP_INTERVAL_MS * TIMESTAMP_INTERVAL_UPPER_BOUND)
}
