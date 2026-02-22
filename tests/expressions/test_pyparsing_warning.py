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
import importlib

import pyparsing as pp

import pyiceberg.expressions.parser as parser
import pyiceberg.table as table


def test_table_import_has_no_ungrouped_named_tokens_warning() -> None:
    diagnostic = pp.Diagnostics.warn_ungrouped_named_tokens_in_collection
    was_enabled = pp.__diag__.warn_ungrouped_named_tokens_in_collection

    pp.enable_diag(diagnostic)
    try:
        importlib.reload(parser)
        importlib.reload(table)
    finally:
        if not was_enabled:
            pp.disable_diag(diagnostic)
