# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Specify the behaviour of the status history tables
$ set-regex match="\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d(\.\d\d\d)?" replacement="<TIMESTAMP>"

# History starts out empty when there are no sinks
> select * from mz_internal.mz_sink_status_history

$ kafka-create-topic topic=status-history

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}');

> CREATE SOURCE kafka_source
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-status-history-${testdrive.seed}')
  FORMAT TEXT

$ set-from-sql var=source_id
SELECT id FROM mz_sources WHERE name = 'kafka_source'

> select * from mz_internal.mz_source_status_history where source_id = '${source_id}' order by occurred_at;
"<TIMESTAMP> UTC" ${source_id} starting <null> <null>

$ kafka-ingest format=bytes topic=status-history
1
2
3
4

> select * from kafka_source order by 1;
1
2
3
4

> select * from mz_internal.mz_source_status_history where source_id = '${source_id}' order by occurred_at;
"<TIMESTAMP> UTC" ${source_id} starting <null> <null>
"<TIMESTAMP> UTC" ${source_id} running <null> <null>
