// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_repr::GlobalId;
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::notice::{
    Action, ActionKind, OptimizerNotice, OptimizerNoticeApi, OptimizerNoticeKind,
    RawOptimizerNotice,
};

use crate::catalog::{Catalog, CatalogState};

impl Catalog {
    /// Transform the [`DataflowMetainfo`] by rendering an [`OptimizerNotice`]
    /// for each [`RawOptimizerNotice`].
    ///
    /// Delegates to [`CatalogState::render_notices`].
    pub fn render_notices(
        &self,
        df_meta: DataflowMetainfo<RawOptimizerNotice>,
        item_id: Option<GlobalId>,
    ) -> DataflowMetainfo<OptimizerNotice> {
        self.state.render_notices(df_meta, item_id)
    }
}

impl CatalogState {
    /// Transform the [`DataflowMetainfo`] by rendering an [`OptimizerNotice`]
    /// for each [`RawOptimizerNotice`].
    pub fn render_notices(
        &self,
        df_meta: DataflowMetainfo<RawOptimizerNotice>,
        item_id: Option<GlobalId>,
    ) -> DataflowMetainfo<OptimizerNotice> {
        let optimizer_notices = df_meta
            .optimizer_notices
            .into_iter()
            .map(|notice| OptimizerNotice {
                kind: OptimizerNoticeKind::from(&notice),
                item_id,
                dependencies: notice.dependencies(),
                message: notice.message(self, false).to_string(),
                hint: notice.hint(self, false).to_string(),
                action: match notice.action_kind(self) {
                    ActionKind::SqlStatements => {
                        Action::SqlStatements(notice.action(self, false).to_string())
                    }
                    ActionKind::PlainText => {
                        Action::PlainText(notice.action(self, false).to_string())
                    }
                    ActionKind::None => {
                        Action::None // No concrete action.
                    }
                },
                message_redacted: notice.message(self, true).to_string(),
                hint_redacted: notice.hint(self, true).to_string(),
                action_redacted: match notice.action_kind(self) {
                    ActionKind::SqlStatements => {
                        Action::SqlStatements(notice.action(self, true).to_string())
                    }
                    ActionKind::PlainText => {
                        Action::PlainText(notice.action(self, true).to_string())
                    }
                    ActionKind::None => {
                        Action::None // No concrete action.
                    }
                },
                created_at: (self.config().now)(),
            })
            .collect();

        DataflowMetainfo {
            optimizer_notices,
            index_usage_types: df_meta.index_usage_types,
        }
    }
}
