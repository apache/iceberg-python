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
import io
import math
import zlib
from collections.abc import Iterable
from typing import TYPE_CHECKING

from pyroaring import BitMap, FrozenBitMap

from pyiceberg.table.puffin import PuffinBlob, PuffinBlobMetadata, PuffinFile

if TYPE_CHECKING:
    import pyarrow as pa

EMPTY_BITMAP = FrozenBitMap()
MAX_JAVA_SIGNED = int(math.pow(2, 31)) - 1
PROPERTY_REFERENCED_DATA_FILE = "referenced-data-file"
DELETION_VECTOR_MAGIC = b"\xd1\xd3\x39\x64"
# Reserved field id of the row position (_pos) metadata column, referenced by
# deletion-vector-v1 blob metadata (Java: MetadataColumns.ROW_POSITION)
ROW_POSITION_FIELD_ID = 2147483645


class DeletionVector:
    referenced_data_file: str
    _bitmaps: list[BitMap]

    def __init__(self, referenced_data_file: str, bitmaps: list[BitMap]) -> None:
        self.referenced_data_file = referenced_data_file
        self._bitmaps = bitmaps

    @classmethod
    def from_positions(cls, referenced_data_file: str, positions: Iterable[int]) -> "DeletionVector":
        bitmaps_by_key: dict[int, BitMap] = {}
        for position in positions:
            if position < 0:
                raise ValueError(f"Invalid position: {position}, positions must be non-negative")
            bitmaps_by_key.setdefault(position >> 32, BitMap()).add(position & 0xFFFFFFFF)

        if not bitmaps_by_key:
            raise ValueError("Deletion vector must contain at least one position")

        # Materialize a list indexed by key, padding gaps with the empty bitmap (mirrors _deserialize_bitmap)
        bitmaps: list[BitMap] = [bitmaps_by_key.get(key, EMPTY_BITMAP) for key in range(max(bitmaps_by_key) + 1)]
        return cls(referenced_data_file, bitmaps)

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
    def _serialize_bitmap(bitmaps: list[BitMap]) -> bytes:
        # Counterpart of _deserialize_bitmap: number of bitmaps (8 bytes, little-endian), then for each
        # non-empty bitmap in ascending key order its key (4 bytes, little-endian) and serialized payload.
        non_empty = [(key, bitmap) for key, bitmap in enumerate(bitmaps) if len(bitmap) > 0]

        with io.BytesIO() as out:
            out.write(len(non_empty).to_bytes(8, "little"))
            for key, bitmap in non_empty:
                if key > MAX_JAVA_SIGNED:
                    raise ValueError(f"Key {key} is too large, max {MAX_JAVA_SIGNED} to maintain compatibility with Java impl")
                out.write(key.to_bytes(4, "little"))
                out.write(bitmap.serialize())
            return out.getvalue()

    @staticmethod
    def _bitmaps_to_chunked_array(bitmaps: list[BitMap]) -> "pa.ChunkedArray":
        import pyarrow as pa

        return pa.chunked_array([(key_pos << 32) + pos for pos in bitmap] for key_pos, bitmap in enumerate(bitmaps))

    def to_vector(self) -> "pa.ChunkedArray":
        return self._bitmaps_to_chunked_array(self._bitmaps)

    def to_blob(self) -> PuffinBlob:
        vector_payload = self._serialize_bitmap(self._bitmaps)

        # deletion-vector-v1 blob layout: combined length of magic and vector (4 bytes, big-endian),
        # the DV magic bytes, the serialized vector, and a CRC-32 checksum of magic + vector (4 bytes, big-endian)
        blob_content = DELETION_VECTOR_MAGIC + vector_payload
        payload = len(blob_content).to_bytes(4, "big") + blob_content + zlib.crc32(blob_content).to_bytes(4, "big")

        cardinality = sum(len(bitmap) for bitmap in self._bitmaps)
        metadata = PuffinBlobMetadata(
            type="deletion-vector-v1",
            fields=[ROW_POSITION_FIELD_ID],
            # -1 means the snapshot id and sequence number are inherited at commit time
            snapshot_id=-1,
            sequence_number=-1,
            # offset and length are placeholders; PuffinWriter fills them in when assembling the file
            offset=0,
            length=0,
            properties={PROPERTY_REFERENCED_DATA_FILE: self.referenced_data_file, "cardinality": str(cardinality)},
            compression_codec=None,
        )
        return PuffinBlob(metadata=metadata, payload=payload)


def _extract_vector_payload(blob_payload: bytes) -> bytes:
    """Strip deletion-vector-v1 blob framing: length(4 big-endian) + DV magic(4) ... CRC(4 big-endian)."""
    length_prefix = int.from_bytes(blob_payload[0:4], "big")
    return blob_payload[8 : 4 + length_prefix]


def deletion_vectors_from_puffin_file(puffin_file: PuffinFile) -> list[DeletionVector]:
    return [
        DeletionVector(
            referenced_data_file=blob.properties[PROPERTY_REFERENCED_DATA_FILE],
            bitmaps=DeletionVector._deserialize_bitmap(_extract_vector_payload(puffin_file.get_blob_payload(blob))),
        )
        for blob in puffin_file.footer.blobs
    ]
