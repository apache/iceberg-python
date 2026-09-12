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

from tempfile import TemporaryDirectory

import pytest
from fastavro import reader

from conftest import record_to_fastavro
from pyiceberg.avro.codecs import AvroCompressionCodec
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import (
    data_file_with_partition,
    manifest_entry_schema_with_data_file,
    write_manifest,
)
from pyiceberg.table import Table


@pytest.fixture()
def catalog() -> Catalog:
    return load_catalog(
        "local",
        **{
            "type": "rest",
            "uri": "http://localhost:8181",
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "admin",
            "s3.secret-access-key": "password",
        },
    )


@pytest.fixture()
def table_test_all_types(catalog: Catalog) -> Table:
    return catalog.load_table("default.test_all_types")


@pytest.mark.integration
@pytest.mark.parametrize("compression", ["null", "deflate"])
def test_write_sample_manifest(table_test_all_types: Table, compression: AvroCompressionCodec) -> None:
    test_snapshot = table_test_all_types.current_snapshot()
    if test_snapshot is None:
        raise ValueError("Table has no current snapshot, check the docker environment")
    io = table_test_all_types.io
    test_manifest_file = test_snapshot.manifests(io)[0]
    test_manifest_entries = test_manifest_file.fetch_manifest_entry(io)
    entry = test_manifest_entries[0]
    test_schema = table_test_all_types.schema()
    test_spec = table_test_all_types.spec()
    data_file_v3_type = data_file_with_partition(
        partition_type=test_spec.partition_type(test_schema),
        format_version=3,
    )
    data_file_v2_type = data_file_with_partition(
        partition_type=test_spec.partition_type(test_schema),
        format_version=2,
    )
    entry_v3_type = manifest_entry_schema_with_data_file(format_version=3, data_file=data_file_v3_type).as_struct()
    entry_v2_type = manifest_entry_schema_with_data_file(format_version=2, data_file=data_file_v2_type).as_struct()

    with TemporaryDirectory() as tmpdir:
        tmp_avro_file = tmpdir + "/test_write_manifest.avro"
        output = PyArrowFileIO().new_output(tmp_avro_file)
        with write_manifest(
            format_version=2,
            spec=test_spec,
            schema=test_schema,
            output_file=output,
            snapshot_id=test_snapshot.snapshot_id,
            avro_compression=compression,
        ) as manifest_writer:
            # For simplicity, try one entry first
            manifest_writer.add_entry(test_manifest_entries[0])

        with open(tmp_avro_file, "rb") as fo:
            r = reader(fo=fo)
            it = iter(r)
            fa_entry = next(it)

            assert fa_entry == record_to_fastavro(entry, record_struct=entry_v3_type, file_struct=entry_v2_type)
