from typing import Iterator, List, Optional, Tuple

from pyroaring import BitMap as BitMap


class RoaringPositionBitmap:
    _BITMAP_COUNT_SIZE_BYTES: int = 8
    _BITMAP_KEY_SIZE_BYTES: int = 4
    MAX_POSITION = ((2**31 - 2) << 32) | ((-(2**31)) & 0xFFFFFFFF)

    def __init__(self, bitmaps: Optional[List[BitMap]] = None):
        if bitmaps is not None:
            self._bitmaps = bitmaps
        else:
            self._bitmaps = []

    def set(self, pos: int) -> None:
        self.validate_position(pos)
        key = self.key(pos)
        pos32 = self.pos32_bits(pos)
        self.allocate_bitmaps_if_needed(key + 1)
        self._bitmaps[key].add(pos32)

    def set_range(self, pos_start_inclusive: int, pos_end_exclusive: int) -> None:
        for pos in range(pos_start_inclusive, pos_end_exclusive):
            self.set(pos)

    def set_all(self, that: "RoaringPositionBitmap") -> None:
        self.allocate_bitmaps_if_needed(len(that._bitmaps))
        for key in range(len(that._bitmaps)):
            self._bitmaps[key] |= that._bitmaps[key]

    def contains(self, pos: int) -> bool:
        self.validate_position(pos)
        key = self.key(pos)
        pos32 = self.pos32_bits(pos)
        return key < len(self._bitmaps) and pos32 in self._bitmaps[key]

    def is_empty(self) -> bool:
        return self.cardinality() == 0

    def cardinality(self) -> int:
        return sum(len(bm) for bm in self._bitmaps)

    def run_length_encode(self) -> bool:
        changed = False
        for bm in self._bitmaps:
            changed |= bm.run_optimize()
        return changed

    def allocated_bitmap_count(self) -> int:
        return len(self._bitmaps)

    def allocate_bitmaps_if_needed(self, required_length: int) -> None:
        if len(self._bitmaps) < required_length:
            if len(self._bitmaps) == 0 and required_length == 1:
                self._bitmaps = [BitMap()]
            else:
                self._bitmaps = self._bitmaps + [BitMap() for _ in range(required_length - len(self._bitmaps))]

    def serialized_size_in_bytes(self) -> int:
        size = self._BITMAP_COUNT_SIZE_BYTES
        for bm in self._bitmaps:
            size += self._BITMAP_KEY_SIZE_BYTES + len(bm.serialize())
        return size

    def serialize(self, result: bytearray) -> None:
        """Serialize the bitmap using a portable serialization format."""
        # Write the number of bitmaps (8 bytes, little-endian)
        result += len(self._bitmaps).to_bytes(8, byteorder="little")
        for key, bitmap in enumerate(self._bitmaps):
            result += key.to_bytes(4, byteorder="little")
            result += bitmap.serialize()

    @classmethod
    def deserialize(cls, buffer: bytes) -> "RoaringPositionBitmap":
        """Deserializes a bitmap from a buffer, assuming the portable serialization format."""
        offset = 0
        bitmap_count, offset = cls.read_bitmap_count(buffer, offset)
        bitmaps: List[BitMap] = []
        last_key = -1

        for _ in range(bitmap_count):
            key, offset = cls.read_key(buffer, last_key, offset)
            # Fill gaps
            while last_key < key - 1:
                bitmaps.append(BitMap())
                last_key += 1
            bitmap, offset = cls.read_bitmap(buffer, offset)
            bitmaps.append(bitmap)
            last_key = key
        return cls(bitmaps)

    @staticmethod
    def key(pos: int) -> int:
        return (pos >> 32) & 0xFFFFFFFF

    @staticmethod
    def pos32_bits(pos: int) -> int:
        return pos & 0xFFFFFFFF

    @staticmethod
    def to_position(key: int, pos32_bits: int) -> int:
        return (key << 32) | (pos32_bits & 0xFFFFFFFF)

    @staticmethod
    def validate_position(pos: int) -> None:
        if not (0 <= pos <= RoaringPositionBitmap.MAX_POSITION):
            raise ValueError(f"Bitmap supports positions that are >= 0 and <= {RoaringPositionBitmap.MAX_POSITION}: {pos}")

    @staticmethod
    def read_bitmap_count(buffer: bytes, offset: int) -> Tuple[int, int]:
        bitmap_count = int.from_bytes(buffer[offset : offset + 8], byteorder="little")
        if not (0 <= bitmap_count <= 2**31 - 1):
            raise ValueError(f"Invalid bitmap count: {bitmap_count}")
        return bitmap_count, offset + 8

    @staticmethod
    def read_key(buffer: bytes, last_key: int, offset: int) -> Tuple[int, int]:
        key = int.from_bytes(buffer[offset : offset + 4], byteorder="little")
        if key < 0:
            raise ValueError(f"Invalid unsigned key: {key}")
        if key > (2**31 - 2):
            raise ValueError(f"Key is too large: {key}")
        if key <= last_key:
            raise ValueError("Keys must be sorted in ascending order")
        return key, offset + 4

    @staticmethod
    def read_bitmap(buffer: bytes, offset: int) -> Tuple[BitMap, int]:
        bitmap = BitMap.deserialize(buffer[offset:])
        return bitmap, offset + len(bitmap.serialize())

    def __iter__(self) -> Iterator[int]:
        """Return an iterator over all set positions in the bitmap."""
        for key, bitmap in enumerate(self._bitmaps):
            for pos32 in bitmap:
                yield self.to_position(key, pos32)
