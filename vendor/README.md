<!--
 - Licensed to the Apache Software Foundation (ASF) under one or more
 - contributor license agreements.  See the NOTICE file distributed with
 - this work for additional information regarding copyright ownership.
 - The ASF licenses this file to You under the Apache License, Version 2.0
 - (the "License"); you may not use this file except in compliance with
 - the License.  You may obtain a copy of the License at
 -
 -   http://www.apache.org/licenses/LICENSE-2.0
 -
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and
 - limitations under the License.
 -->
# Vendor packages

Some packages we want to maintain in the repository itself, because there is no good 3rd party alternative.

## Quick Setup

Generate all vendor packages:

```bash
make all
```

Generate individual packages:

```bash
make fb303           # FB303 Thrift client only
make hive-metastore  # Hive Metastore Thrift definitions only
```

## Packages

### FB303 Thrift client

fb303 is a base Thrift service and a common set of functionality for querying stats, options, and other information from a service.

**Generate with Make:**
```bash
make fb303
```

# Hive Metastore Thrift definition

The thrift definition require the fb303 service as a dependency

**Generate with Make:**
```bash
make hive-metastore
```

## Requirements

- Apache Thrift compiler (`thrift`)
- `curl` for downloading Thrift definitions