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
import math
from typing import TYPE_CHECKING

from pyroaring import BitMap, FrozenBitMap

from pyiceberg.table.puffin import PuffinFile

if TYPE_CHECKING:
    import pyarrow as pa

EMPTY_BITMAP = FrozenBitMap()
MAX_JAVA_SIGNED = int(math.pow(2, 31)) - 1
PROPERTY_REFERENCED_DATA_FILE = "referenced-data-file"


class DeletionVector:
    _deletion_vectors: dict[str, list[BitMap]]

    def __init__(self, deletion_vectors: dict[str, list[BitMap]]) -> None:
        self._deletion_vectors = deletion_vectors

    @classmethod
    def from_puffin_file(cls, puffin_file: PuffinFile) -> "DeletionVector":
        return cls(
            {
                blob.properties[PROPERTY_REFERENCED_DATA_FILE]: cls._deserialize_bitmap(puffin_file.get_blob_payload(blob))
                for blob in puffin_file.footer.blobs
            }
        )

    @staticmethod
    def _deserialize_bitmap(pl: bytes) -> list[BitMap]:
        number_of_bitmaps = int.from_bytes(pl[0:8], byteorder="little")
        pl = pl[8:]

        bitmaps = []
        last_key = -1
        for _ in range(number_of_bitmaps):
            key = int.from_bytes(pl[0:4], byteorder="little")
            if key < 0:
                raise ValueError(f"Invalid unsigned key: {key}")
            if key <= last_key:
                raise ValueError("Keys must be sorted in ascending order")
            if key > MAX_JAVA_SIGNED:
                raise ValueError(f"Key {key} is too large, max {MAX_JAVA_SIGNED} to maintain compatibility with Java impl")
            pl = pl[4:]

            while last_key < key - 1:
                bitmaps.append(EMPTY_BITMAP)
                last_key += 1

            bm = BitMap().deserialize(pl)
            # TODO: Optimize this
            pl = pl[len(bm.serialize()) :]
            bitmaps.append(bm)

            last_key = key

        return bitmaps

    @staticmethod
    def _bitmaps_to_chunked_array(bitmaps: list[BitMap]) -> "pa.ChunkedArray":
        import pyarrow as pa

        return pa.chunked_array([(key_pos << 32) + pos for pos in bitmap] for key_pos, bitmap in enumerate(bitmaps))

    def to_vector(self) -> dict[str, "pa.ChunkedArray"]:
        return {path: self._bitmaps_to_chunked_array(bitmaps) for path, bitmaps in self._deletion_vectors.items()}
