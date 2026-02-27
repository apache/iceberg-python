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
# pylint:disable=redefined-outer-name

import pyarrow as pa
import pytest

from pyiceberg.catalog import Catalog
from pyiceberg.expressions import EqualTo
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import TableProperties
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import LongType, NestedField
from utils import _create_table

_SCHEMA = Schema(
    NestedField(field_id=1, name="id", field_type=LongType(), required=False),
    NestedField(field_id=2, name="partition_col", field_type=LongType(), required=False),
)

_PARTITION_SPEC = PartitionSpec(PartitionField(source_id=2, field_id=1001, transform=IdentityTransform(), name="partition_col"))

_DATA_P1 = pa.table({"id": [1, 2, 3], "partition_col": [1, 1, 1]})
_DATA_P2 = pa.table({"id": [4, 5, 6], "partition_col": [2, 2, 2]})
_DATA_P1_NEW = pa.table({"id": [10, 11, 12], "partition_col": [1, 1, 1]})


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_overwrite_with_merging_bounds_manifest_count(session_catalog: Catalog, format_version: int) -> None:
    """After a partial overwrite with manifest merging enabled, the manifest count should
    not exceed the count before the overwrite, even when many manifests were accumulated."""
    identifier = f"default.test_overwrite_merges_manifests_v{format_version}"

    # Build up 6 manifests via fast append (merging disabled)
    tbl = _create_table(
        session_catalog,
        identifier,
        {
            "format-version": str(format_version),
            TableProperties.MANIFEST_MERGE_ENABLED: "false",
        },
        partition_spec=_PARTITION_SPEC,
        schema=_SCHEMA,
    )

    for _ in range(3):
        tbl.append(_DATA_P1)
        tbl.append(_DATA_P2)

    assert len(tbl.inspect.manifests()) == 6

    # Enable merging before the overwrite
    with tbl.transaction() as tx:
        tx.set_properties(
            {
                TableProperties.MANIFEST_MERGE_ENABLED: "true",
                TableProperties.MANIFEST_MIN_MERGE_COUNT: "2",
            }
        )

    # Overwrite partition_col=1 only: the 3 partition_col=2 manifests are preserved
    # and merged together with the new manifest by _MergeAppendFiles._process_manifests
    tbl.overwrite(_DATA_P1_NEW, EqualTo("partition_col", 1))

    assert len(tbl.inspect.manifests()) < 6

    # Data correctness: partition_col=2 has 3 × 3 = 9 rows; partition_col=1 replaced with 3 new rows
    result = tbl.scan().to_arrow()
    assert len(result) == 12
    assert sorted(result.column("id").to_pylist()) == [4, 4, 4, 5, 5, 5, 6, 6, 6, 10, 11, 12]


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_overwrite_without_merging_increases_manifest_count(session_catalog: Catalog, format_version: int) -> None:
    """Control test: without manifest merging, a partial overwrite grows the manifest count."""
    identifier = f"default.test_overwrite_no_merge_manifests_v{format_version}"

    tbl = _create_table(
        session_catalog,
        identifier,
        {
            "format-version": str(format_version),
            TableProperties.MANIFEST_MERGE_ENABLED: "false",
        },
        partition_spec=_PARTITION_SPEC,
        schema=_SCHEMA,
    )

    for _ in range(3):
        tbl.append(_DATA_P1)
    for _ in range(3):
        tbl.append(_DATA_P2)

    assert len(tbl.inspect.manifests()) == 6

    # Overwrite without merging: new manifest is added on top of the existing ones
    tbl.overwrite(_DATA_P1_NEW, EqualTo("partition_col", 1))

    assert len(tbl.inspect.manifests()) == 4

    # Data correctness is identical regardless of merging strategy
    result = tbl.scan().to_arrow()
    assert len(result) == 12
    assert sorted(result.column("id").to_pylist()) == [4, 4, 4, 5, 5, 5, 6, 6, 6, 10, 11, 12]


@pytest.mark.integration
@pytest.mark.parametrize("format_version", [1, 2])
def test_fast_append_does_not_merge_manifests(session_catalog: Catalog, format_version: int) -> None:
    """Fast append bypasses _MergingSnapshotProducer, so manifests grow with each append
    even when manifest merging properties are set to trigger early."""
    identifier = f"default.test_fast_append_no_merge_v{format_version}"

    tbl = _create_table(
        session_catalog,
        identifier,
        {
            "format-version": str(format_version),
            TableProperties.MANIFEST_MERGE_ENABLED: "false",
            TableProperties.MANIFEST_MIN_MERGE_COUNT: "2",
        },
        schema=_SCHEMA,
    )

    for expected_count in range(1, 6):
        tbl.append(_DATA_P1)
        assert len(tbl.inspect.manifests()) == expected_count
