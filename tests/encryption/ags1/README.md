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

# AGS1 cross-client test fixtures

These files were written by Java's `AesGcmOutputStream` (Apache Iceberg 1.11.0), not
by PyIceberg. They exist so PyIceberg's AGS1 support is checked against another
implementation's bytes rather than only against its own round trip. All four also
decrypt with iceberg-rust 0.10.1, through its `EncryptedInputFile`.

## Fixtures

| File | Encrypted size | Plaintext | Pins |
| --- | --- | --- | --- |
| `empty.ags1` | 36 B | 0 B | Java writes an 8 byte header **plus one empty block** for an empty file, not a bare header |
| `partial-block.ags1` | 136 B | 100 B | Header, nonce/tag layout, and a single short block |
| `partial-block-no-aad.ags1` | 136 B | 100 B | The same stream with a null AAD prefix, so the block index alone is the AAD |
| `aligned-multi-block.ags1` | 2097216 B | 2 MiB | Two full blocks: the little-endian block index in each block's AAD, and that a block-aligned write appends **no** trailing empty block |

`empty.ags1` is worth calling out. The spec says the last block has a non-zero
length, which makes a bare 8 byte header the natural encoding of an empty file, but
Java writes 36 bytes and its `AesGcmInputFile` rejects anything shorter, as does
iceberg-rust. PyIceberg matches them: `MIN_STREAM_LENGTH` is 36, so a bare header is
rejected rather than read as an empty stream.

Block-aligned and partial *single* block variants are deliberately not checked in.
The 1 MiB block size is hard-coded, so each would add another 1 MiB of
incompressible ciphertext without covering a case the four files above miss.

## Parameters

Every fixture uses:

- **Key**: 16 bytes, `0x00` through `0x0f`
- **AAD prefix**: ASCII `pyiceberg-ags1`, except `partial-block-no-aad.ags1`, which has none
- **Plaintext**: byte `i` is `i % 251`. The period is prime and therefore coprime with the
  1 MiB block size, so the pattern shifts phase at every block boundary and a
  misordered or misindexed block is detectable from the plaintext alone

## Regenerating

Each block uses a fresh random nonce, so regenerating produces different bytes.
The file lengths, and the plaintext each file decrypts to, are deterministic. Tests
decrypt these fixtures rather than comparing them byte for byte, so a regeneration
is safe as long as the parameters above are unchanged.

From this directory, with a JDK 17 or later:

<!-- markdown-link-check-disable-next-line -->
```bash
V=1.11.0
for a in iceberg-core iceberg-api iceberg-bundled-guava; do
  curl -sfLO "https://repo1.maven.org/maven2/org/apache/iceberg/$a/$V/$a-$V.jar"
done
java -cp "iceberg-api-$V.jar:iceberg-bundled-guava-$V.jar:iceberg-core-$V.jar" \
  GenerateAgs1Fixtures.java .
rm iceberg-*-$V.jar
```
