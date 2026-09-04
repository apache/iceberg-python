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
"""Tests for the RemoveOrphanFiles maintenance action."""

from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timedelta, timezone
from functools import partial
from pathlib import Path

import pyarrow as pa
import pytest

from pyiceberg.catalog import Catalog
from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.exceptions import ValidationException
from pyiceberg.io import FileIO
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.maintenance.orphan_files import (
    _DEFAULT_EQUAL_SCHEMES,
    PrefixMismatchMode,
    _find_orphans,
    _flatten_mapping,
)
from pyiceberg.table.statistics import StatisticsFile
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import LongType, NestedField, StringType


def _make_table(tmp_path: Path, name: str = "default.t", properties: dict[str, str] | None = None) -> Table:
    catalog: Catalog = InMemoryCatalog("test", warehouse=tmp_path.absolute().as_posix())
    catalog.create_namespace("default")
    schema = Schema(
        NestedField(1, "c1", LongType(), required=False),
        NestedField(2, "c2", StringType(), required=False),
        NestedField(3, "c3", StringType(), required=False),
    )
    table = catalog.create_table(name, schema=schema, properties=properties or {})
    return table


def _make_partitioned_table(tmp_path: Path) -> Table:
    """A table partitioned on a field whose name starts with an underscore, as in the Java tests."""
    catalog: Catalog = InMemoryCatalog("test", warehouse=tmp_path.absolute().as_posix())
    catalog.create_namespace("default")
    schema = Schema(
        NestedField(1, "c1", LongType(), required=False),
        NestedField(2, "_c2", StringType(), required=False),
        NestedField(3, "c3", StringType(), required=False),
    )
    spec = PartitionSpec(PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="_c2"))
    return catalog.create_table("default.partitioned", schema=schema, partition_spec=spec)


def _append(table: Table, rows: list[dict[str, object]]) -> None:
    table.append(pa.Table.from_pylist(rows, schema=table.schema().as_arrow()))


def _backdate(path: str | Path, hours: int) -> None:
    past = time.time() - hours * 3600
    os.utime(path, (past, past))


def _list_data_files(table: Table) -> list[str]:
    """List the files under <table>/data in the form that list_prefix reports them."""
    out = []
    for root, _dirs, files in os.walk(f"{table.metadata.location}/data"):
        for f in files:
            if not f.startswith(".") and not f.startswith("_"):
                out.append(_path(Path(root) / f))
    return out


def _path(path: Path) -> str:
    """A local path in the form that list_prefix reports it, with forward slashes on any platform."""
    return path.as_posix()


def _wait_until_after_now() -> None:
    """Sleep until the wall clock advances past the moment of the call."""
    target = time.time()
    while time.time() <= target:
        time.sleep(0.001)


def _execute_paths_test(
    valid_files: list[str],
    actual_files: list[str],
    expected_orphans: list[str],
    equal_schemes: dict[str, str] | None = None,
    equal_authorities: dict[str, str] | None = None,
    mode: PrefixMismatchMode = PrefixMismatchMode.IGNORE,
) -> None:
    """Drive ``_find_orphans`` directly so path-normalization tests stay one-liners."""
    schemes = dict(_DEFAULT_EQUAL_SCHEMES)
    schemes.update(_flatten_mapping(equal_schemes or {}))
    authorities = _flatten_mapping(equal_authorities or {})

    candidates = [(p, 0) for p in actual_files]
    orphans, conflicts = _find_orphans(candidates, set(valid_files), schemes, authorities, mode)

    if mode == PrefixMismatchMode.ERROR and conflicts:
        raise ValidationException(f"Unable to determine whether certain files are orphan: {sorted(conflicts)}")

    assert [p for p, _ in orphans] == expected_orphans


def test_dry_run(tmp_path: Path) -> None:
    """Default cutoff finds nothing; explicit cutoff finds the orphan; execute deletes it."""
    table = _make_table(tmp_path)
    _append(table, [{"c1": 1, "c2": "AAAAAAAAAA", "c3": "AAAA"}])
    _append(table, [{"c1": 1, "c2": "AAAAAAAAAA", "c3": "AAAA"}])

    valid_files = set(_list_data_files(table))
    assert len(valid_files) == 2

    orphan = Path(table.metadata.location) / "data" / "orphan.parquet"
    orphan.write_bytes(b"junk")

    all_files = set(_list_data_files(table))
    invalid = sorted(all_files - valid_files)
    assert invalid == [_path(orphan)]

    _wait_until_after_now()

    result1 = table.maintenance.remove_orphan_files().delete_with(lambda _: None).execute()
    assert result1.orphan_file_locations == []

    cutoff = datetime.now(tz=timezone.utc)
    result2 = table.maintenance.remove_orphan_files().older_than(cutoff).delete_with(lambda _: None).execute()
    assert sorted(result2.orphan_file_locations) == invalid
    assert orphan.exists()

    result3 = table.maintenance.remove_orphan_files().older_than(cutoff).execute()
    assert sorted(result3.deleted_files) == invalid
    assert not orphan.exists()


def test_all_valid_files_are_kept(tmp_path: Path) -> None:
    """Files referenced by any snapshot survive the action even when older than the cutoff."""
    table = _make_table(tmp_path)
    _append(table, [{"c1": 1, "c2": "AAAAAAAAAA", "c3": "AAAA"}])
    _append(table, [{"c1": 2, "c2": "AAAAAAAAAA", "c3": "AAAA"}])
    _append(table, [{"c1": 3, "c2": "AAAAAAAAAA", "c3": "AAAA"}])

    for root, _dirs, files in os.walk(table.metadata.location):
        for f in files:
            _backdate(Path(root) / f, hours=24 * 7)

    result = table.maintenance.remove_orphan_files().execute()
    assert result.orphan_file_locations == []
    assert result.deleted_files == []


def test_orphaned_files_removed_in_parallel(tmp_path: Path) -> None:
    """Deletion fans out across the shared executor."""
    table = _make_table(tmp_path)
    _append(table, [{"c1": 1, "c2": "AAAAAAAAAA", "c3": "AAAA"}])

    for i in range(4):
        orphan = Path(table.metadata.location) / "data" / f"orphan-{i}.parquet"
        orphan.write_bytes(b"junk")
        _backdate(orphan, hours=24 * 4)

    seen_threads: set[str] = set()
    deleted: list[str] = []
    lock = threading.Lock()

    def record_and_delete(path: str) -> None:
        # Hold each worker briefly so all four pick up a task before any returns.
        time.sleep(0.05)
        with lock:
            seen_threads.add(threading.current_thread().name)
            deleted.append(path)

    result = table.maintenance.remove_orphan_files().delete_with(record_and_delete).execute()

    assert len(deleted) == 4
    assert len(result.deleted_files) == 4
    assert len(seen_threads) > 1, f"expected parallel deletes, only saw threads: {seen_threads}"


def test_metadata_folder_is_intact(tmp_path: Path) -> None:
    """When the scan is restricted to a data path, files under metadata/ are never touched."""
    table = _make_table(tmp_path, properties={"write.data.path": f"{tmp_path.as_posix()}/data-redirect"})
    _append(table, [{"c1": 1, "c2": "AAAAAAAAAA", "c3": "AAAA"}])

    orphan = Path(f"{tmp_path}/data-redirect") / "stray.parquet"
    orphan.parent.mkdir(parents=True, exist_ok=True)
    orphan.write_bytes(b"junk")
    _backdate(orphan, hours=24 * 4)

    metadata_files_before = sorted(p.name for p in Path(table.metadata.location, "metadata").iterdir())

    result = table.maintenance.remove_orphan_files().location(f"{tmp_path.as_posix()}/data-redirect").execute()

    assert len(result.deleted_files) == 1
    assert result.deleted_files[0].endswith("stray.parquet")
    metadata_files_after = sorted(p.name for p in Path(table.metadata.location, "metadata").iterdir())
    assert metadata_files_before == metadata_files_after


def test_older_than_timestamp(tmp_path: Path) -> None:
    """Only files modified strictly before the cutoff are deleted; fresher files are kept."""
    table = _make_table(tmp_path)
    _append(table, [{"c1": 1, "c2": "AAAAAAAAAA", "c3": "AAAA"}])

    old1 = Path(table.metadata.location) / "data" / "old1.parquet"
    old2 = Path(table.metadata.location) / "data" / "old2.parquet"
    old1.write_bytes(b"junk")
    old2.write_bytes(b"junk")
    _backdate(old1, hours=2)
    _backdate(old2, hours=2)

    cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=1)

    fresh = Path(table.metadata.location) / "data" / "fresh.parquet"
    fresh.write_bytes(b"junk")

    result = table.maintenance.remove_orphan_files().older_than(cutoff).execute()

    deleted = sorted(p.split("/")[-1] for p in result.deleted_files)
    assert deleted == ["old1.parquet", "old2.parquet"]
    assert fresh.exists()


def test_remove_unreachable_metadata_version_files(tmp_path: Path) -> None:
    """A metadata.json file not tracked by metadata-log is treated as orphan."""
    table = _make_table(tmp_path)
    _append(table, [{"c1": 1, "c2": "AAAAAAAAAA", "c3": "AAAA"}])

    stray = Path(table.metadata.location) / "metadata" / "v0.unreferenced.metadata.json"
    stray.write_bytes(b"{}")
    _backdate(stray, hours=24 * 4)

    result = table.maintenance.remove_orphan_files().execute()

    assert any(p.endswith("v0.unreferenced.metadata.json") for p in result.deleted_files)
    assert not stray.exists()


def test_garbage_collection_disabled(tmp_path: Path) -> None:
    """The action refuses to run when the table's gc.enabled property is false."""
    table = _make_table(tmp_path)
    _append(table, [{"c1": 1, "c2": "AAAAAAAAAA", "c3": "AAAA"}])

    table.metadata = table.metadata.model_copy(update={"properties": {**table.metadata.properties, "gc.enabled": "false"}})

    with pytest.raises(ValidationException, match="gc.enabled is false"):
        table.maintenance.remove_orphan_files().execute()


def test_compare_to_file_list(tmp_path: Path) -> None:
    """The action consumes an explicit (path, last_modified) list and still respects location()."""
    table = _make_table(tmp_path)
    _append(table, [{"c1": 1, "c2": "AAAAAAAAAA", "c3": "AAAA"}])
    _append(table, [{"c1": 1, "c2": "AAAAAAAAAA", "c3": "AAAA"}])

    valid_files = set(_list_data_files(table))
    orphan = Path(table.metadata.location) / "data" / "orphan.parquet"
    orphan.write_bytes(b"junk")
    all_files = set(_list_data_files(table))
    invalid = sorted(all_files - valid_files)
    assert invalid == [_path(orphan)]

    now = datetime.now(tz=timezone.utc)
    file_list = [(p, now) for p in all_files]

    result1 = table.maintenance.remove_orphan_files().compare_to_file_list(file_list).delete_with(lambda _: None).execute()
    assert result1.orphan_file_locations == []

    cutoff = datetime.now(tz=timezone.utc) + timedelta(seconds=5)
    result2 = (
        table.maintenance.remove_orphan_files()
        .compare_to_file_list(file_list)
        .older_than(cutoff)
        .delete_with(lambda _: None)
        .execute()
    )
    assert sorted(result2.orphan_file_locations) == invalid
    assert orphan.exists()

    result3 = table.maintenance.remove_orphan_files().compare_to_file_list(file_list).older_than(cutoff).execute()
    assert sorted(result3.deleted_files) == invalid
    assert not orphan.exists()

    outside = [("/tmp/mock1", datetime.fromtimestamp(0, tz=timezone.utc))]
    result4 = (
        table.maintenance.remove_orphan_files()
        .location(table.metadata.location)
        .compare_to_file_list(outside)
        .delete_with(lambda _: None)
        .execute()
    )
    assert result4.orphan_file_locations == []


def test_remove_orphan_files_with_statistic_files(tmp_path: Path) -> None:
    """Statistics files registered on the table are protected; once unregistered they become orphan."""
    table = _make_table(tmp_path)
    _append(table, [{"c1": 1, "c2": "AAAAAAAAAA", "c3": "AAAA"}])

    current_snapshot = table.metadata.current_snapshot()
    assert current_snapshot is not None
    snapshot_id = current_snapshot.snapshot_id
    stats_path = Path(table.metadata.location) / "data" / "some-stats-file.puffin"
    stats_path.parent.mkdir(parents=True, exist_ok=True)
    stats_path.write_bytes(b"PFA1stub")
    _backdate(stats_path, hours=24 * 4)

    stats_file = StatisticsFile(
        snapshot_id=snapshot_id,
        statistics_path=_path(stats_path),
        file_size_in_bytes=stats_path.stat().st_size,
        file_footer_size_in_bytes=4,
        blob_metadata=[],
    )
    table.metadata = table.metadata.model_copy(update={"statistics": [stats_file]})

    result1 = table.maintenance.remove_orphan_files().execute()
    assert all(not p.endswith("some-stats-file.puffin") for p in result1.deleted_files)
    assert stats_path.exists()

    table.metadata = table.metadata.model_copy(update={"statistics": []})

    result2 = table.maintenance.remove_orphan_files().execute()
    assert any(p.endswith("some-stats-file.puffin") for p in result2.deleted_files)
    assert not stats_path.exists()


def test_paths_with_extra_slashes() -> None:
    """Runs of slashes inside a URI are collapsed during normalization."""
    _execute_paths_test(
        valid_files=["file:///dir1/dir2/file1"],
        actual_files=["file:///dir1/////dir2///file1"],
        expected_orphans=[],
    )


def test_paths_with_valid_file_having_no_authority() -> None:
    """A referenced file with no authority matches an authority-bearing candidate without conflict."""
    _execute_paths_test(
        valid_files=["hdfs:///dir1/dir2/file1"],
        actual_files=["hdfs://servicename/dir1/dir2/file1"],
        expected_orphans=[],
        mode=PrefixMismatchMode.ERROR,
    )


def test_paths_with_actual_file_having_no_authority() -> None:
    """A candidate file with no authority is not flagged when the referenced version has one."""
    _execute_paths_test(
        valid_files=["hdfs://servicename/dir1/dir2/file1"],
        actual_files=["hdfs:///dir1/dir2/file1"],
        expected_orphans=[],
    )


def test_paths_with_equal_schemes() -> None:
    """ERROR mode raises on a scheme conflict; declaring the schemes equal silences it."""
    valid = ["scheme1://bucket1/dir1/dir2/file1"]
    actual = ["scheme2://bucket1/dir1/dir2/file1"]

    with pytest.raises(ValidationException, match="scheme1.*scheme2|scheme2.*scheme1"):
        _execute_paths_test(valid, actual, [], mode=PrefixMismatchMode.ERROR)

    _execute_paths_test(
        valid,
        actual,
        [],
        equal_schemes={"scheme1,scheme2": "scheme"},
        mode=PrefixMismatchMode.ERROR,
    )


def test_paths_with_equal_authorities() -> None:
    """ERROR mode raises on an authority conflict; declaring the authorities equal silences it."""
    valid = ["hdfs://servicename1/dir1/dir2/file1"]
    actual = ["hdfs://servicename2/dir1/dir2/file1"]

    with pytest.raises(ValidationException, match="servicename1.*servicename2|servicename2.*servicename1"):
        _execute_paths_test(valid, actual, [], mode=PrefixMismatchMode.ERROR)

    _execute_paths_test(
        valid,
        actual,
        [],
        equal_authorities={"servicename1,servicename2": "servicename"},
        mode=PrefixMismatchMode.ERROR,
    )


def test_remove_orphan_file_action_with_delete_mode() -> None:
    """DELETE mode treats prefix-mismatched candidates as orphan rather than recording a conflict."""
    _execute_paths_test(
        valid_files=["hdfs://servicename1/dir1/dir2/file1"],
        actual_files=["hdfs://servicename2/dir1/dir2/file1"],
        expected_orphans=["hdfs://servicename2/dir1/dir2/file1"],
        mode=PrefixMismatchMode.DELETE,
    )


def test_hidden_paths_are_ignored(tmp_path: Path) -> None:
    """Files under a path component starting with '_' or '.' are never considered orphan."""
    table = _make_table(tmp_path)
    _append(table, [{"c1": 1, "c2": "AAAAAAAAAA", "c3": "AAAA"}])

    data_dir = Path(table.metadata.location) / "data"
    hidden = [data_dir / "_SUCCESS", data_dir / ".staging" / "part.parquet", data_dir / "_temporary" / "part.parquet"]
    visible = data_dir / "stray.parquet"
    for path in [*hidden, visible]:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"junk")
        _backdate(path, hours=24 * 4)

    result = table.maintenance.remove_orphan_files().execute()

    assert result.deleted_files == [_path(visible)]
    assert all(path.exists() for path in hidden)


def test_hidden_partition_paths_are_not_ignored(tmp_path: Path) -> None:
    """A partition directory named after a '_'-prefixed field is scanned like any other."""
    table = _make_partitioned_table(tmp_path)
    _append(table, [{"c1": 1, "_c2": "AAAAAAAAAA", "c3": "AAAA"}])

    data_dir = Path(table.metadata.location) / "data"
    in_partition = data_dir / "_c2=AAAAAAAAAA" / "stray.parquet"
    in_hidden_lookalike = data_dir / "_c2" / "file.txt"
    for path in (in_partition, in_hidden_lookalike):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"junk")
        _backdate(path, hours=24 * 4)

    result = table.maintenance.remove_orphan_files().execute()

    assert result.deleted_files == [_path(in_partition)]
    assert in_hidden_lookalike.exists()


def test_file_io_without_list_prefix(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A FileIO that cannot list storage fails with a clear error."""
    table = _make_table(tmp_path)
    _append(table, [{"c1": 1, "c2": "AAAAAAAAAA", "c3": "AAAA"}])
    monkeypatch.setattr(table.io, "list_prefix", partial(FileIO.list_prefix, table.io))

    with pytest.raises(NotImplementedError, match="does not support list_prefix"):
        table.maintenance.remove_orphan_files().execute()
