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
from pyiceberg.table.statistics import BlobMetadata, PartitionStatisticsFile, StatisticsFile


def test_partition_statistics_file() -> None:
    partition_statistics_file_json = (
        """{"snapshot-id":123,"statistics-path":"s3://bucket/statistics.parquet","file-size-in-bytes":345}"""
    )
    partition_statistics_file = PartitionStatisticsFile.model_validate_json(partition_statistics_file_json)

    assert partition_statistics_file == PartitionStatisticsFile(
        snapshot_id=123, statistics_path="s3://bucket/statistics.parquet", file_size_in_bytes=345
    )

    assert partition_statistics_file.model_dump_json() == partition_statistics_file_json


def test_statistics_file() -> None:
    statistics_file_json = """{"snapshot-id":123,"statistics-path":"s3://bucket/statistics.parquet","file-size-in-bytes":345,"file-footer-size-in-bytes":456,"blob-metadata":[{"type":"apache-datasketches-theta-v1","snapshot-id":567,"sequence-number":22,"fields":[1,2,3],"properties":{"foo":"bar"}}]}"""
    statistics_file = StatisticsFile.model_validate_json(statistics_file_json)

    assert statistics_file == StatisticsFile(
        snapshot_id=123,
        statistics_path="s3://bucket/statistics.parquet",
        file_size_in_bytes=345,
        file_footer_size_in_bytes=456,
        key_metadata=None,
        blob_metadata=[
            BlobMetadata(
                type="apache-datasketches-theta-v1",
                snapshot_id=567,
                sequence_number=22,
                fields=[1, 2, 3],
                properties={"foo": "bar"},
            )
        ],
    )

    assert statistics_file.model_dump_json() == statistics_file_json
