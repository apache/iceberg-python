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

from collections.abc import Callable

import pytest

import pyiceberg.table as table_module
from pyiceberg.expressions import BooleanExpression, EqualTo, GreaterThan
from pyiceberg.manifest import DataFile, FileFormat
from pyiceberg.schema import Schema
from pyiceberg.table import ManifestGroupPlanner, Table
from pyiceberg.typedef import Record, StructProtocol


def _data_file(file_number: int, partition_value: int) -> DataFile:
    return DataFile.from_args(
        file_path=f"s3://bucket/data-{file_number}.parquet",
        file_format=FileFormat.PARQUET,
        partition=Record(partition_value),
        record_count=100,
        file_size_in_bytes=1,
    )


def test_partition_evaluator_prepares_once_per_spec(table_v2: Table, monkeypatch: pytest.MonkeyPatch) -> None:
    evaluator_calls: list[list[int]] = []

    def counting_expression_evaluator(
        schema: Schema, unbound: BooleanExpression, case_sensitive: bool
    ) -> Callable[[StructProtocol], bool]:
        calls: list[int] = []
        evaluator_calls.append(calls)

        def evaluate(struct: StructProtocol) -> bool:
            value = struct[0]
            calls.append(value)
            return value > 5

        return evaluate

    monkeypatch.setattr(table_module, "expression_evaluator", counting_expression_evaluator)
    planner = ManifestGroupPlanner(table_metadata=table_v2.metadata, io=table_v2.io, row_filter=GreaterThan("x", 5))
    partition_evaluator = planner._build_partition_evaluator(0)

    assert len(evaluator_calls) == 1
    assert not partition_evaluator(_data_file(1, 1))
    assert partition_evaluator(_data_file(2, 10))
    assert evaluator_calls == [[1, 10]]


def test_metrics_evaluator_prepares_once_per_plan(table_v2: Table, monkeypatch: pytest.MonkeyPatch) -> None:
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
    first_file = _data_file(1, 1)
    second_file = _data_file(2, 2)

    metrics_evaluator = planner._build_metrics_evaluator()
    assert len(instances) == 1
    assert metrics_evaluator(first_file)
    assert metrics_evaluator(second_file)
    assert instances[0].calls == [first_file, second_file]
