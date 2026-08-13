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

import pyarrow as pa

from pyiceberg.catalog import Catalog
from pyiceberg.expressions import EqualTo, Reference
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import BucketTransform
from pyiceberg.types import IntegerType, NestedField, StringType


def test_delete_data_file_manifest_pruning_bucket_transform_succeeds(catalog: Catalog) -> None:
    """delete_data_file should work for non-identity specs.

    Manifest-pruning predicates are built against the partition struct (using the
    partition field name, e.g. the bucket id) rather than the source column, so this
    works regardless of the partition transform.
    """
    catalog.create_namespace_if_not_exists("default")
    identifier = "default.bucket_delete"

    schema = Schema(
        NestedField(1, "tenant_id", StringType(), required=True),
        NestedField(2, "value", IntegerType(), required=True),
    )
    spec = PartitionSpec(
        PartitionField(
            source_id=1,
            field_id=1000,
            transform=BucketTransform(8),
            name="tenant_id_bucket",
        ),
        spec_id=0,
    )
    table = catalog.create_table(
        identifier=identifier,
        schema=schema,
        partition_spec=spec,
        properties={"format-version": "2"},
    )

    table.append(
        pa.Table.from_pylist(
            [
                {"tenant_id": "tenant-a", "value": 1},
                {"tenant_id": "tenant-b", "value": 2},
            ],
            schema=pa.schema(
                [
                    pa.field("tenant_id", pa.string(), nullable=False),
                    pa.field("value", pa.int32(), nullable=False),
                ]
            ),
        )
    )

    before = table.scan().to_arrow()
    before_paths = {task.file.file_path for task in table.scan().plan_files()}
    existing_file = next(iter(table.scan().plan_files())).file

    with table.transaction() as txn:
        with txn.update_snapshot().overwrite() as overwrite:
            overwrite.delete_data_file(existing_file)

    after = table.scan().to_arrow()
    after_paths = {task.file.file_path for task in table.scan().plan_files()}

    assert existing_file.file_path not in after_paths
    assert before_paths - after_paths == {existing_file.file_path}
    assert len(after_paths) == len(before_paths) - 1
    assert after.num_rows < before.num_rows


def test_delete_data_file_manifest_pruning_predicate_uses_partition_field(catalog: Catalog) -> None:
    """The manifest-pruning predicate must reference the partition field, not the source column.

    `_OverwriteFiles` deletes by exact `DataFile` identity regardless of this predicate, so an
    end-to-end delete would still succeed even if pruning silently degraded back to a
    non-discriminating fallback. This test guards the pruning predicate itself.
    """
    catalog.create_namespace_if_not_exists("default")
    identifier = "default.bucket_delete_pruning_predicate"

    schema = Schema(
        NestedField(1, "tenant_id", StringType(), required=True),
        NestedField(2, "value", IntegerType(), required=True),
    )
    spec = PartitionSpec(
        PartitionField(
            source_id=1,
            field_id=1000,
            transform=BucketTransform(8),
            name="tenant_id_bucket",
        ),
        spec_id=0,
    )
    table = catalog.create_table(
        identifier=identifier,
        schema=schema,
        partition_spec=spec,
        properties={"format-version": "2"},
    )
    table.append(
        pa.Table.from_pylist(
            [{"tenant_id": "tenant-a", "value": 1}],
            schema=pa.schema(
                [
                    pa.field("tenant_id", pa.string(), nullable=False),
                    pa.field("value", pa.int32(), nullable=False),
                ]
            ),
        )
    )
    existing_file = next(iter(table.scan().plan_files())).file
    expected_bucket_id = BucketTransform(8).transform(StringType())("tenant-a")

    with table.transaction() as txn:
        with txn.update_snapshot().overwrite() as overwrite:
            overwrite.delete_data_file(existing_file)
            overwrite._build_delete_files_partition_predicate()
            predicate = overwrite._delete_files_partition_filters[existing_file.spec_id]

    assert predicate == EqualTo(Reference("tenant_id_bucket"), expected_bucket_id)


def test_delete_data_file_manifest_pruning_bucket_on_same_result_type_succeeds(catalog: Catalog) -> None:
    """delete_data_file must not silently skip a manifest when the bucket id happens to share the source column's type.

    Pre-fix, the buggy predicate compared the source column (an int) against the bucket id
    (also an int), so binding succeeded instead of raising. The manifest's min/max stats for
    that column then incorrectly ruled out the manifest containing the target file, so the
    whole manifest was skipped and the file was silently never deleted - no exception, no
    error, just a delete that quietly did nothing. A string source column can't hit this path
    since it would fail to bind (see the other tests here), so this needs a same-result-type
    source to catch a regression back to the source-column domain.
    """
    catalog.create_namespace_if_not_exists("default")
    identifier = "default.bucket_delete_same_result_type"

    schema = Schema(NestedField(1, "value", IntegerType(), required=True))
    spec = PartitionSpec(
        PartitionField(
            source_id=1,
            field_id=1000,
            transform=BucketTransform(8),
            name="value_bucket",
        ),
        spec_id=0,
    )
    table = catalog.create_table(
        identifier=identifier,
        schema=schema,
        partition_spec=spec,
    )
    table.append(
        pa.Table.from_pylist(
            [{"value": 42}],
            schema=pa.schema([pa.field("value", pa.int32(), nullable=False)]),
        )
    )

    before_paths = {task.file.file_path for task in table.scan().plan_files()}
    existing_file = next(iter(table.scan().plan_files())).file

    with table.transaction() as txn:
        with txn.update_snapshot().overwrite() as overwrite:
            overwrite.delete_data_file(existing_file)

    after_paths = {task.file.file_path for task in table.scan().plan_files()}

    assert before_paths - after_paths == {existing_file.file_path}
