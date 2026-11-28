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

from sqlalchemy import Engine, create_engine
from sqlalchemy.pool import StaticPool

from pyiceberg.catalog import URI
from pyiceberg.catalog.sql import SqlCatalog


class InMemoryCatalog(SqlCatalog):
    """
    An in-memory catalog implementation that uses SqlCatalog with SQLite in-memory database.

    This is useful for test, demo, and playground but not in production as it does not support concurrent access.
    """

    def __init__(
        self, name: str, warehouse: str = "file:///tmp/iceberg/warehouse", engine: Engine | None = None, **kwargs: str
    ) -> None:
        self._warehouse_location = warehouse
        if engine is None and URI not in kwargs:
            echo_str = str(kwargs.get("echo", DEFAULT_ECHO_VALUE)).lower()
            engine = create_engine(
                "sqlite://",
                echo=strtobool(echo_str) if echo_str != "debug" else "debug",
                pool_pre_ping=strtobool(kwargs.get("pool_pre_ping", DEFAULT_POOL_PRE_PING_VALUE)),
                poolclass=StaticPool,
                connect_args={"check_same_thread": False},
            )
        super().__init__(name=name, engine=engine, warehouse=warehouse, **kwargs)
