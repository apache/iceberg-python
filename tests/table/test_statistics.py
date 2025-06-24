# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from pyiceberg.table.statistics import PartitionStatisticsFile


def test_partition_statistics_file() -> None:
    partition_statistics_file_json = (
        """{"snapshot-id":123,"statistics-path":"s3://bucket/statistics.parquet","file-size-in-bytes":345}"""
    )
    partition_statistics_file = PartitionStatisticsFile.model_validate_json(partition_statistics_file_json)

    assert partition_statistics_file == PartitionStatisticsFile(
        snapshot_id=123, statistics_path="s3://bucket/statistics.parquet", file_size_in_bytes=345
    )

    assert partition_statistics_file.model_dump_json() == partition_statistics_file_json
