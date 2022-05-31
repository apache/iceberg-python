#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

from __future__ import annotations

from abc import ABC
from typing import Union

from iceberg.catalog.base import Identifier


class Table(ABC):
    """Placeholder for Table managed by the Catalog that points to the current Table Metadata.

    To be implemented by https://github.com/apache/iceberg/issues/3227
    """

    identifier: Union[str, Identifier]


class PartitionSpec:
    """Placeholder for Partition Specification

    To be implemented by https://github.com/apache/iceberg/issues/4631
    """
