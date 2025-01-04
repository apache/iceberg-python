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

import os
from functools import wraps

from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.table import StaticTable


def test_read_biglake_table() -> None:
    # There's a downloaded version of BigLake Iceberg table in ../../kevinliu_blmt/
    current_dir = os.path.dirname(os.path.abspath(__file__))
    warehouse_location = os.path.abspath(os.path.join(current_dir, "../../kevinliu_blmt"))
    metadata_json_file_path = warehouse_location + "/metadata/v1727125782.metadata.json"
    tbl = StaticTable.from_metadata(metadata_json_file_path)
    assert tbl
    # This is a V1 table
    assert tbl.format_version == 1
    # And metadata currently refers to the original GCS bucket (gs://kevinliu_blmt)
    assert "gs://" in tbl.metadata.location
    assert "gs://" in tbl.metadata.current_snapshot().manifest_list

    # Let's redirect GCS bucket to local file path by overriding PyArrowFileIO's parse_location function
    GCS_BUCKET = "gs://kevinliu_blmt"
    LOCAL_WAREHOUSE = f"file://{warehouse_location}"

    def gcs_location_override(f):
        @wraps(f)
        def wrapper(location: str):
            if location.startswith(GCS_BUCKET):
                location = location.replace(GCS_BUCKET, LOCAL_WAREHOUSE)
                print(f"Redirected location: {location}")
            return f(location)

        return wrapper

    PyArrowFileIO.parse_location = staticmethod(gcs_location_override(PyArrowFileIO.parse_location))

    # Now we can try to read the table
    tbl.scan().to_pandas()
