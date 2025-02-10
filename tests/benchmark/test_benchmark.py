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
import statistics
import timeit
import urllib

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pyiceberg.transforms import DayTransform


@pytest.fixture(scope="session")
def taxi_dataset(tmp_path_factory: pytest.TempPathFactory) -> pa.Table:
    """Reads the Taxi dataset to disk"""
    taxi_dataset = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2022-01.parquet"
    taxi_dataset_dest = tmp_path_factory.mktemp("taxi_dataset") / "yellow_tripdata_2022-01.parquet"
    urllib.request.urlretrieve(taxi_dataset, taxi_dataset_dest)

    return pq.read_table(taxi_dataset_dest)


@pytest.mark.benchmark
def test_partitioned_write(tmp_path_factory: pytest.TempPathFactory, taxi_dataset: pa.Table) -> None:
    """Tests writing to a partitioned table with something that would be close a production-like situation"""
    from pyiceberg.catalog.sql import SqlCatalog

    warehouse_path = str(tmp_path_factory.mktemp("warehouse"))
    catalog = SqlCatalog(
        "default",
        uri=f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
        warehouse=f"file://{warehouse_path}",
    )

    catalog.create_namespace("default")

    tbl = catalog.create_table("default.taxi_partitioned", schema=taxi_dataset.schema)

    with tbl.update_spec() as spec:
        spec.add_field("tpep_pickup_datetime", DayTransform())

    # Profiling can sometimes be handy as well
    # with cProfile.Profile() as pr:
    #     tbl.append(taxi_dataset)
    #
    # pr.print_stats(sort=True)

    runs = []
    for run in range(5):
        start_time = timeit.default_timer()
        tbl.append(taxi_dataset)
        elapsed = timeit.default_timer() - start_time

        print(f"Run {run} took: {elapsed}")
        runs.append(elapsed)

    print(f"Average runtime of {round(statistics.mean(runs), 2)} seconds")
