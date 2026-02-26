import random
from os import path
from typing import Set, Tuple

import pytest

from pyiceberg.table.deletions import RoaringPositionBitmap

CONTAINER_SIZE = 0xFFFF
CONTAINER_OFFSET = CONTAINER_SIZE + 1
BITMAP_OFFSET = 0xFFFFFFFF + 1
SUPPORTED_OFFICIAL_EXAMPLE_FILES = frozenset(
    {
        "64map32bitvals.bin",
        "64mapempty.bin",
        "64mapspreadvals.bin",
    }
)
BITMAP_SIZE = 0xFFFFFFFF
VALIDATION_LOOKUP_COUNT = 20_000

SEED = random.getrandbits(64)
VALIDATION_SEED = random.getrandbits(64)


@pytest.fixture
def empty_bitmap() -> RoaringPositionBitmap:
    return RoaringPositionBitmap()


def position(bitmap_index: int, container_index: int, value: int) -> int:
    return bitmap_index * BITMAP_OFFSET + container_index * CONTAINER_OFFSET + value


def round_trip_serialize(bitmap: RoaringPositionBitmap) -> RoaringPositionBitmap:
    result = bytearray()
    bitmap.serialize(result)
    return RoaringPositionBitmap.deserialize(bytes(result))


def _open_file(file: str) -> bytes:
    cur_dir = path.dirname(path.realpath(__file__))
    with open(f"{cur_dir}/bitmaps/{file}", "rb") as f:
        return f.read()


def test_add() -> None:
    bitmap = RoaringPositionBitmap()
    bitmap.set(10)
    assert bitmap.contains(10)
    bitmap.set(0)
    assert bitmap.contains(0)
    bitmap.set(10)
    assert bitmap.contains(10)


def test_add_positions_requiring_multiple_bitmaps() -> None:
    bitmap = RoaringPositionBitmap()
    pos1 = (0 << 32) | 10
    pos2 = (1 << 32) | 20
    pos3 = (2 << 32) | 30
    pos4 = (100 << 32) | 40

    bitmap.set(pos1)
    bitmap.set(pos2)
    bitmap.set(pos3)
    bitmap.set(pos4)

    assert bitmap.contains(pos1)
    assert bitmap.contains(pos2)
    assert bitmap.contains(pos3)
    assert bitmap.contains(pos4)
    assert bitmap.cardinality() == 4
    assert bitmap.serialized_size_in_bytes() > 4
    assert bitmap.allocated_bitmap_count() == 101


def test_add_range() -> None:
    bitmap = RoaringPositionBitmap()
    posStartInclusive = 10
    posEndExclusive = 20
    bitmap.set_range(posStartInclusive, posEndExclusive)
    for pos in range(posStartInclusive, posEndExclusive):
        assert bitmap.contains(pos)
    assert not bitmap.contains(9)
    assert not bitmap.contains(20)
    assert bitmap.cardinality() == 10


def test_add_range_across_keys() -> None:
    bitmap = RoaringPositionBitmap()
    posStartInclusive = (1 << 32) - 5
    posEndExclusive = (1 << 32) + 5
    bitmap.set_range(posStartInclusive, posEndExclusive)
    for pos in range(posStartInclusive, posEndExclusive):
        assert bitmap.contains(pos)
    assert not bitmap.contains(0)
    assert not bitmap.contains(posEndExclusive)
    assert bitmap.cardinality() == 10


def test_add_empty_range() -> None:
    bitmap = RoaringPositionBitmap()
    bitmap.set_range(10, 10)
    assert bitmap.is_empty()


def test_add_all() -> None:
    bitmap1 = RoaringPositionBitmap()
    bitmap1.set(10)
    bitmap1.set(20)

    bitmap2 = RoaringPositionBitmap()
    bitmap2.set(30)
    bitmap2.set(40)
    bitmap2.set(2 << 32)

    bitmap1.set_all(bitmap2)

    assert bitmap1.contains(10)
    assert bitmap1.contains(20)
    assert bitmap1.contains(30)
    assert bitmap1.contains(40)
    assert bitmap1.contains(2 << 32)
    assert bitmap1.cardinality() == 5

    assert not bitmap2.contains(10)
    assert not bitmap2.contains(20)
    assert bitmap2.cardinality() == 3


def test_add_all_with_empty_bitmap() -> None:
    bitmap1 = RoaringPositionBitmap()
    bitmap1.set(10)
    bitmap1.set(20)
    empty_bitmap = RoaringPositionBitmap()
    bitmap1.set_all(empty_bitmap)
    assert bitmap1.contains(10)
    assert bitmap1.contains(20)
    assert bitmap1.cardinality() == 2

    assert not empty_bitmap.contains(10)
    assert not empty_bitmap.contains(20)
    assert empty_bitmap.cardinality() == 0
    assert empty_bitmap.is_empty()


def test_add_all_with_overlapping_bitmap() -> None:
    bitmap1 = RoaringPositionBitmap()
    bitmap1.set(10)
    bitmap1.set(20)
    bitmap1.set(30)
    bitmap2 = RoaringPositionBitmap()
    bitmap2.set(20)
    bitmap2.set(40)
    bitmap1.set_all(bitmap2)
    assert bitmap1.contains(10)
    assert bitmap1.contains(20)
    assert bitmap1.contains(30)
    assert bitmap1.contains(40)
    assert bitmap1.cardinality() == 4

    assert not bitmap2.contains(10)
    assert bitmap2.contains(20)
    assert not bitmap2.contains(30)
    assert bitmap2.contains(40)
    assert bitmap2.cardinality() == 2


def test_add_all_sparse_bitmaps() -> None:
    bitmap1 = RoaringPositionBitmap()
    bitmap1.set((0 << 32) | 100)
    bitmap1.set((1 << 32) | 200)
    bitmap2 = RoaringPositionBitmap()
    bitmap2.set((2 << 32) | 300)
    bitmap2.set((3 << 32) | 400)

    bitmap1.set_all(bitmap2)

    assert bitmap1.contains((0 << 32) | 100)
    assert bitmap1.contains((1 << 32) | 200)
    assert bitmap1.contains((2 << 32) | 300)
    assert bitmap1.contains((3 << 32) | 400)
    assert bitmap1.cardinality() == 4


def test_cardinality() -> None:
    bitmap = RoaringPositionBitmap()
    assert bitmap.cardinality() == 0
    bitmap.set(10)
    bitmap.set(20)
    bitmap.set(30)
    assert bitmap.cardinality() == 3
    bitmap.set(10)
    assert bitmap.cardinality() == 3


def test_cardinality_sparse_bitmaps() -> None:
    bitmap = RoaringPositionBitmap()
    bitmap.set((0 << 32) | 100)
    bitmap.set((0 << 32) | 101)
    bitmap.set((0 << 32) | 105)
    bitmap.set((1 << 32) | 200)
    bitmap.set((100 << 32) | 300)
    assert bitmap.cardinality() == 5


def test_serialize_deserialize_all_container_bitmap() -> None:
    bitmap = RoaringPositionBitmap()

    # bitmap 0, container 0 (array)
    bitmap.set(position(0, 0, 5))
    bitmap.set(position(0, 0, 7))
    # bitmap 0, container 1 (array that can be compressed)
    bitmap.set_range(position(0, 1, 1), position(0, 1, 1000))
    # bitmap 0, container 2 (bitset)
    bitmap.set_range(position(0, 2, 1), position(0, 2, CONTAINER_OFFSET - 1))
    # bitmap 1, container 0 (array)
    bitmap.set(position(1, 0, 10))
    bitmap.set(position(1, 0, 20))
    # bitmap 1, container 1 (array that can be compressed)
    bitmap.set_range(position(1, 1, 10), position(1, 1, 500))
    # bitmap 1, container 2 (bitset)
    bitmap.set_range(position(1, 2, 1), position(1, 2, CONTAINER_OFFSET - 1))

    # RLE encode if available
    try:
        assert bitmap.run_length_encode()
    except AttributeError:
        pass

    # Serialize/deserialize
    bitmap_copy = round_trip_serialize(bitmap)

    assert bitmap_copy.cardinality() == bitmap.cardinality()
    for pos in bitmap_copy:
        assert bitmap.contains(pos)
    for pos in bitmap:
        assert bitmap_copy.contains(pos)


def test_deserialize_supportedRoaring_examples() -> None:
    for file in SUPPORTED_OFFICIAL_EXAMPLE_FILES:
        if file == "64mapempty.bin":
            assert RoaringPositionBitmap.deserialize(_open_file(file)).is_empty()
        else:
            assert not RoaringPositionBitmap.deserialize(_open_file(file)).is_empty()


def test_deserialize_unsupported_roaring_example() -> None:
    with pytest.raises(ValueError, match="Key is too large: 4022190063"):
        RoaringPositionBitmap.deserialize(_open_file("64maphighvals.bin"))


def test_unsupported_positions() -> None:
    bitmap = RoaringPositionBitmap()
    max_position = RoaringPositionBitmap.MAX_POSITION
    message = f"Bitmap supports positions that are >= 0 and <= {max_position}"

    with pytest.raises(ValueError, match=message):
        bitmap.set(-1)
    with pytest.raises(ValueError, match=message):
        bitmap.contains(-1)
    with pytest.raises(ValueError, match=message):
        bitmap.set(max_position + 1)
    with pytest.raises(ValueError, match=message):
        bitmap.contains(max_position + 1)


def test_random_sparse_bitmap() -> None:
    bitmap, positions = generate_sparse_bitmap(0, 5 << 32, 100000)
    assert_equal(bitmap, positions)
    assert_random_positions(bitmap, positions)


def test_random_dense_bitmap() -> None:
    bitmap, positions = generate_dense_bitmap(7)
    assert_equal(bitmap, positions)
    assert_random_positions(bitmap, positions)


def test_random_mixed_bitmap() -> None:
    bitmap, positions = generate_sparse_bitmap(0, 5 << 32, 100000)

    bitmap_1, positions_1 = generate_dense_bitmap(9)
    bitmap.set_all(bitmap_1)
    positions.update(positions_1)

    bitmap_2, positions_2 = generate_sparse_bitmap(0, 3 << 32, 25000)
    bitmap.set_all(bitmap_2)
    positions.update(positions_2)

    bitmap_3, positions_3 = generate_dense_bitmap(3)
    bitmap.set_all(bitmap_3)
    positions.update(positions_3)

    bitmap_4, positions_4 = generate_sparse_bitmap(0, 1 << 32, 5000)
    bitmap.set_all(bitmap_4)
    positions.update(positions_4)

    assert_equal(bitmap, positions)
    assert_random_positions(bitmap, positions)


def next_long(rng: random.Random, min_inclusive: int, max_exclusive: int) -> int:
    return rng.randint(min_inclusive, max_exclusive - 1)


def generate_sparse_bitmap(min_inclusive: int, max_exclusive: int, size: int) -> Tuple[RoaringPositionBitmap, Set[int]]:
    rng = random.Random(SEED)
    bitmap = RoaringPositionBitmap()
    positions: Set[int] = set()
    while len(positions) < size:
        position = next_long(rng, min_inclusive, max_exclusive)
        positions.add(position)
        bitmap.set(position)
    return bitmap, positions


def generate_dense_bitmap(required_bitmap_count: int) -> Tuple[RoaringPositionBitmap, Set[int]]:
    rng = random.Random(SEED)
    bitmap = RoaringPositionBitmap()
    positions: Set[int] = set()
    current_position = 0
    while bitmap.allocated_bitmap_count() <= required_bitmap_count:
        max_run_position = current_position + next_long(rng, 1000, 2 * CONTAINER_SIZE)
        for position in range(current_position, max_run_position + 1):
            bitmap.set(position)
            positions.add(position)
        shift = next_long(rng, int(0.1 * BITMAP_SIZE), int(0.25 * BITMAP_SIZE))
        current_position = max_run_position + shift
    return bitmap, positions


def assert_equal(bitmap: RoaringPositionBitmap, positions: set[int]) -> None:
    assert_equal_content(bitmap, positions)

    bitmap_copy1 = round_trip_serialize(bitmap)
    assert_equal_content(bitmap_copy1, positions)

    bitmap.run_length_encode()
    bitmap_copy2 = round_trip_serialize(bitmap)
    assert_equal_content(bitmap_copy2, positions)


def assert_equal_content(bitmaps: RoaringPositionBitmap, positions: set[int]) -> None:
    assert bitmaps.cardinality() == len(positions)
    for pos in positions:
        assert bitmaps.contains(pos)
    for bitmap in bitmaps:
        assert bitmap in positions


def assert_random_positions(bitmap: RoaringPositionBitmap, positions: set[int]) -> None:
    rng = random.Random(VALIDATION_SEED)
    for _ in range(VALIDATION_LOOKUP_COUNT):
        position = next_long(rng, 0, RoaringPositionBitmap.MAX_POSITION)
        assert bitmap.contains(position) == (position in positions)
