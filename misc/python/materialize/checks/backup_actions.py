# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

from typing import TYPE_CHECKING

from materialize.checks.actions import Action
from materialize.checks.executors import Executor
from materialize.mzcompose.services.minio import MINIO_BLOB_URI

if TYPE_CHECKING:
    pass


class Backup(Action):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()
        c.run("mc", "mb", "persist/crdb-backup")
        c.exec(
            "cockroach",
            "cockroach",
            "sql",
            "--insecure",
            "-e",
            """
           CREATE EXTERNAL CONNECTION backup_bucket AS 's3://persist/crdb-backup?AWS_ENDPOINT=http://minio:9000/&AWS_REGION=minio&AWS_ACCESS_KEY_ID=minioadmin&AWS_SECRET_ACCESS_KEY=minioadmin';
           BACKUP INTO 'external://backup_bucket';
        """,
        )

    def join(self, e: Executor) -> None:
        # Action is blocking
        pass


class Restore(Action):
    def execute(self, e: Executor) -> None:
        c = e.mzcompose_composition()
        c.exec(
            "cockroach",
            "cockroach",
            "sql",
            "--insecure",
            "-e",
            """
            DROP DATABASE defaultdb;
            RESTORE DATABASE defaultdb FROM LATEST IN 'external://backup_bucket';
            SELECT shard, min(sequence_number), max(sequence_number)
            FROM consensus.consensus GROUP BY 1 ORDER BY 2 DESC, 3 DESC, 1 ASC LIMIT 32;
        """,
        )
        c.run(
            "persistcli",
            "admin",
            "--commit",
            "restore-blob",
            f"--blob-uri={MINIO_BLOB_URI}",
            "--consensus-uri=postgres://root@cockroach:26257?options=--search_path=consensus",
        )

    def join(self, e: Executor) -> None:
        # Action is blocking
        pass
