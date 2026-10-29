<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Encrypted manifest list test data

`manifest-list-v3-encrypted.avro` is an empty manifest list encrypted by iceberg-rust, copied from its
`crates/iceberg/testdata/manifests_lists/`. It was written by iceberg-rust's `EncryptionManager`, with a memory KMS
holding master key `master-1` = bytes `0x00` to `0x0f`. Its keys and snapshot are in the
`example_table_metadata_v3_encrypted` fixture in `tests/conftest.py`.
