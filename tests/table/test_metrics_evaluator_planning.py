# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

import pytest

import pyiceberg.table as table_module
from pyiceberg.expressions import BooleanExpression, EqualTo
from pyiceberg.manifest import DataFile, FileFormat
from pyiceberg.schema import Schema
from pyiceberg.table import ManifestGroupPlanner, Table
from pyiceberg.typedef import Record


def _data_file(file_number: int) -> DataFile:
    return DataFile.from_args(
        file_path=f"s3://bucket/data-{file_number}.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(file_number),
        record_count=100,
        file_size_in_bytes=1,
    )


def test_build_metrics_evaluator_prepares_one_instance(table_v2: Table, monkeypatch: pytest.MonkeyPatch) -> None:
    class CountingMetricsEvaluator:
        def __init__(
            self,
            schema: Schema,
            expr: BooleanExpression,
            case_sensitive: bool = True,
            include_empty_files: bool = False,
        ) -> None:
            self.calls: list[DataFile] = []
            instances.append(self)

        def eval(self, data_file: DataFile) -> bool:
            self.calls.append(data_file)
            return True

    instances: list[CountingMetricsEvaluator] = []
    monkeypatch.setattr(table_module, "_InclusiveMetricsEvaluator", CountingMetricsEvaluator)
    planner = ManifestGroupPlanner(table_metadata=table_v2.metadata, io=table_v2.io, row_filter=EqualTo("x", 10))
    first_file = _data_file(1)
    second_file = _data_file(2)

    metrics_evaluator = planner._build_metrics_evaluator()
    assert len(instances) == 1
    assert metrics_evaluator(first_file)
    assert metrics_evaluator(second_file)
    assert instances[0].calls == [first_file, second_file]
