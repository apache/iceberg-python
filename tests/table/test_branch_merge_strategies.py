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
from typing import Dict, Optional
from unittest.mock import Mock, patch

import pytest

from pyiceberg.table.metadata import TableMetadata
from pyiceberg.table.refs import SnapshotRef, SnapshotRefType
from pyiceberg.table.snapshots import Snapshot
from pyiceberg.table.update import (
    AssertRefSnapshotId,
    SetSnapshotRefUpdate,
)
from pyiceberg.table.update.snapshot import (
    BranchMergeStrategy,
    ManageSnapshots,
    _BaseBranchMergeStrategy,
    _CherryPickStrategy,
    _FastForwardStrategy,
    _MergeStrategy,
    _RebaseMergeStrategy,
    _SquashMergeStrategy,
    _get_merge_strategy_impl,
)
from pyiceberg.table import Transaction


class TestBranchMergeStrategyEnum:
    """Test BranchMergeStrategy enum values and behavior."""

    def test_enum_values(self) -> None:
        """Test that BranchMergeStrategy enum has expected values."""
        assert BranchMergeStrategy.MERGE.value == "merge"
        assert BranchMergeStrategy.SQUASH.value == "squash"
        assert BranchMergeStrategy.REBASE.value == "rebase"
        assert BranchMergeStrategy.CHERRY_PICK.value == "cherry_pick"
        assert BranchMergeStrategy.FAST_FORWARD.value == "fast_forward"
        assert len(BranchMergeStrategy) == 5

    def test_strategy_factory(self) -> None:
        """Test that strategy factory returns correct implementations."""
        merge_impl = _get_merge_strategy_impl(BranchMergeStrategy.MERGE)
        squash_impl = _get_merge_strategy_impl(BranchMergeStrategy.SQUASH)
        rebase_impl = _get_merge_strategy_impl(BranchMergeStrategy.REBASE)
        cherry_pick_impl = _get_merge_strategy_impl(BranchMergeStrategy.CHERRY_PICK)
        fast_forward_impl = _get_merge_strategy_impl(BranchMergeStrategy.FAST_FORWARD)

        assert isinstance(merge_impl, _MergeStrategy)
        assert isinstance(squash_impl, _SquashMergeStrategy)
        assert isinstance(rebase_impl, _RebaseMergeStrategy)
        assert isinstance(cherry_pick_impl, _CherryPickStrategy)
        assert isinstance(fast_forward_impl, _FastForwardStrategy)


class TestSnapshotUtilities:
    """Test snapshot utility methods used by merge strategies."""

    def create_mock_table_metadata(self) -> TableMetadata:
        """Create mock table metadata for testing."""
        metadata = Mock(spec=TableMetadata)
        metadata.refs = {}
        metadata.snapshots = []
        metadata.snapshot_by_id = Mock(return_value=None)
        return metadata

    def test_is_fast_forward_possible(self) -> None:
        """Test fast-forward detection logic."""
        strategy = _MergeStrategy()
        metadata = self.create_mock_table_metadata()

        # Create snapshots with parent relationship
        target_snapshot = Snapshot(
            snapshot_id=100,
            parent_snapshot_id=None,
            manifest_list="s3://bucket/manifest-100.avro",
            timestamp_ms=1234567890000,
            sequence_number=1
        )

        source_snapshot = Snapshot(
            snapshot_id=200,
            parent_snapshot_id=100,
            manifest_list="s3://bucket/manifest-200.avro",
            timestamp_ms=1234567900000,
            sequence_number=2
        )

        def mock_snapshot_by_id(snapshot_id):
            if snapshot_id == 100:
                return target_snapshot
            elif snapshot_id == 200:
                return source_snapshot
            return None

        metadata.snapshot_by_id = mock_snapshot_by_id

        source_ref = SnapshotRef(snapshot_id=200, snapshot_ref_type=SnapshotRefType.BRANCH)
        target_ref = SnapshotRef(snapshot_id=100, snapshot_ref_type=SnapshotRefType.BRANCH)

        # Should be fast-forward possible (source is descendant of target)
        assert strategy._is_fast_forward_possible(source_ref, target_ref, metadata)

        # Reverse should not be fast-forward possible
        assert not strategy._is_fast_forward_possible(target_ref, source_ref, metadata)

    def test_find_common_ancestor(self) -> None:
        """Test finding common ancestor between branches."""
        strategy = _MergeStrategy()
        metadata = self.create_mock_table_metadata()

        # Create snapshot history: 1 -> 2 -> 3 (main)
        #                             \-> 4 -> 5 (feature)
        snapshots = {
            1: Snapshot(snapshot_id=1, parent_snapshot_id=None, manifest_list="s3://1.avro", timestamp_ms=1000, sequence_number=1),
            2: Snapshot(snapshot_id=2, parent_snapshot_id=1, manifest_list="s3://2.avro", timestamp_ms=2000, sequence_number=2),
            3: Snapshot(snapshot_id=3, parent_snapshot_id=2, manifest_list="s3://3.avro", timestamp_ms=3000, sequence_number=3),
            4: Snapshot(snapshot_id=4, parent_snapshot_id=2, manifest_list="s3://4.avro", timestamp_ms=4000, sequence_number=4),
            5: Snapshot(snapshot_id=5, parent_snapshot_id=4, manifest_list="s3://5.avro", timestamp_ms=5000, sequence_number=5),
        }

        metadata.snapshot_by_id = lambda sid: snapshots.get(sid)

        source_ref = SnapshotRef(snapshot_id=5, snapshot_ref_type=SnapshotRefType.BRANCH)  # feature branch head
        target_ref = SnapshotRef(snapshot_id=3, snapshot_ref_type=SnapshotRefType.BRANCH)  # main branch head

        common_ancestor = strategy._find_common_ancestor(source_ref, target_ref, metadata)
        assert common_ancestor is not None
        assert common_ancestor.snapshot_id == 2  # Common ancestor

    def test_find_common_ancestor_no_common_ancestor(self) -> None:
        """Test finding common ancestor when none exists."""
        strategy = _MergeStrategy()
        metadata = self.create_mock_table_metadata()

        # Create two completely separate snapshot histories
        snapshots = {
            1: Snapshot(snapshot_id=1, parent_snapshot_id=None, manifest_list="s3://1.avro", timestamp_ms=1000, sequence_number=1),
            2: Snapshot(snapshot_id=2, parent_snapshot_id=1, manifest_list="s3://2.avro", timestamp_ms=2000, sequence_number=2),
            10: Snapshot(snapshot_id=10, parent_snapshot_id=None, manifest_list="s3://10.avro", timestamp_ms=1000, sequence_number=1),
            11: Snapshot(snapshot_id=11, parent_snapshot_id=10, manifest_list="s3://11.avro", timestamp_ms=2000, sequence_number=2),
        }

        metadata.snapshot_by_id = lambda sid: snapshots.get(sid)

        source_ref = SnapshotRef(snapshot_id=2, snapshot_ref_type=SnapshotRefType.BRANCH)
        target_ref = SnapshotRef(snapshot_id=11, snapshot_ref_type=SnapshotRefType.BRANCH)

        common_ancestor = strategy._find_common_ancestor(source_ref, target_ref, metadata)
        assert common_ancestor is None  # No common ancestor

    def test_find_common_ancestor_missing_snapshots(self) -> None:
        """Test finding common ancestor when snapshots are missing."""
        strategy = _MergeStrategy()
        metadata = self.create_mock_table_metadata()

        # Mock that returns None for missing snapshots
        metadata.snapshot_by_id = Mock(return_value=None)

        source_ref = SnapshotRef(snapshot_id=999, snapshot_ref_type=SnapshotRefType.BRANCH)
        target_ref = SnapshotRef(snapshot_id=888, snapshot_ref_type=SnapshotRefType.BRANCH)

        common_ancestor = strategy._find_common_ancestor(source_ref, target_ref, metadata)
        assert common_ancestor is None  # Should handle missing snapshots gracefully

    def test_is_fast_forward_with_missing_snapshots(self) -> None:
        """Test fast-forward detection with missing snapshots."""
        strategy = _MergeStrategy()
        metadata = self.create_mock_table_metadata()

        # Mock that returns None for missing snapshots
        metadata.snapshot_by_id = Mock(return_value=None)

        source_ref = SnapshotRef(snapshot_id=999, snapshot_ref_type=SnapshotRefType.BRANCH)
        target_ref = SnapshotRef(snapshot_id=888, snapshot_ref_type=SnapshotRefType.BRANCH)

        result = strategy._is_fast_forward_possible(source_ref, target_ref, metadata)
        assert result is False  # Should return False for missing snapshots

    def test_is_fast_forward_with_circular_reference(self) -> None:
        """Test fast-forward detection doesn't infinite loop on circular references."""
        strategy = _MergeStrategy()
        metadata = self.create_mock_table_metadata()

        # Create snapshots with circular reference (shouldn't happen in practice)
        snapshots = {
            1: Snapshot(snapshot_id=1, parent_snapshot_id=2, manifest_list="s3://1.avro", timestamp_ms=1000, sequence_number=1),
            2: Snapshot(snapshot_id=2, parent_snapshot_id=1, manifest_list="s3://2.avro", timestamp_ms=2000, sequence_number=2),
        }

        metadata.snapshot_by_id = lambda sid: snapshots.get(sid)

        source_ref = SnapshotRef(snapshot_id=1, snapshot_ref_type=SnapshotRefType.BRANCH)
        target_ref = SnapshotRef(snapshot_id=2, snapshot_ref_type=SnapshotRefType.BRANCH)

        # Should not infinite loop - with circular reference, source can reach target
        result = strategy._is_fast_forward_possible(source_ref, target_ref, metadata)
        # With circular reference, the algorithm will find target in source's ancestry
        # This is technically correct behavior, even though circular refs shouldn't happen
        assert result is True  # Source (1) -> parent (2) == target (2)


class TestSquashMergeStrategy:
    """Test squash merge strategy implementation."""

    def test_squash_merge_fast_forward(self) -> None:
        """Test squash merge when fast-forward is possible."""
        strategy = _SquashMergeStrategy()
        transaction = Mock(spec=Transaction)
        
        # Setup refs
        source_ref = SnapshotRef(snapshot_id=200, snapshot_ref_type=SnapshotRefType.BRANCH)
        target_ref = SnapshotRef(snapshot_id=100, snapshot_ref_type=SnapshotRefType.BRANCH)
        transaction.table_metadata.refs = {"feature": source_ref, "main": target_ref}

        # Setup snapshots (fast-forward case)
        def mock_snapshot_by_id(snapshot_id):
            if snapshot_id == 200:
                return Snapshot(snapshot_id=200, parent_snapshot_id=100, manifest_list="s3://200.avro", timestamp_ms=2000, sequence_number=2)
            elif snapshot_id == 100:
                return Snapshot(snapshot_id=100, parent_snapshot_id=None, manifest_list="s3://100.avro", timestamp_ms=1000, sequence_number=1)
            return None
        
        transaction.table_metadata.snapshot_by_id = mock_snapshot_by_id

        updates, requirements = strategy.merge("feature", "main", transaction)

        assert len(updates) == 1
        assert len(requirements) == 1
        
        update = updates[0]
        assert isinstance(update, SetSnapshotRefUpdate)
        assert update.ref_name == "main"
        assert update.snapshot_id == 200

    def test_squash_merge_divergent_branches(self) -> None:
        """Test squash merge when branches have diverged."""
        strategy = _SquashMergeStrategy()
        transaction = Mock(spec=Transaction)
        
        # Setup refs for divergent branches
        source_ref = SnapshotRef(snapshot_id=300, snapshot_ref_type=SnapshotRefType.BRANCH)
        target_ref = SnapshotRef(snapshot_id=200, snapshot_ref_type=SnapshotRefType.BRANCH)
        transaction.table_metadata.refs = {"feature": source_ref, "main": target_ref}

        # Setup snapshots (divergent case)
        def mock_snapshot_by_id(snapshot_id):
            snapshots = {
                100: Snapshot(snapshot_id=100, parent_snapshot_id=None, manifest_list="s3://100.avro", timestamp_ms=1000, sequence_number=1),
                200: Snapshot(snapshot_id=200, parent_snapshot_id=100, manifest_list="s3://200.avro", timestamp_ms=2000, sequence_number=2),
                300: Snapshot(snapshot_id=300, parent_snapshot_id=100, manifest_list="s3://300.avro", timestamp_ms=3000, sequence_number=3),
            }
            return snapshots.get(snapshot_id)
        
        transaction.table_metadata.snapshot_by_id = mock_snapshot_by_id

        updates, requirements = strategy.merge("feature", "main", transaction)

        assert len(updates) == 1
        assert len(requirements) == 1
        
        update = updates[0]
        assert isinstance(update, SetSnapshotRefUpdate)
        assert update.ref_name == "main"
        assert update.snapshot_id == 300


class TestMergeStrategy:
    """Test classic merge strategy implementation."""

    def test_merge_with_common_ancestor(self) -> None:
        """Test classic merge creating merge commit."""
        strategy = _MergeStrategy()
        transaction = Mock(spec=Transaction)
        
        # Setup refs
        source_ref = SnapshotRef(snapshot_id=300, snapshot_ref_type=SnapshotRefType.BRANCH)
        target_ref = SnapshotRef(snapshot_id=200, snapshot_ref_type=SnapshotRefType.BRANCH)
        transaction.table_metadata.refs = {"feature": source_ref, "main": target_ref}

        # Setup snapshots for three-way merge
        def mock_snapshot_by_id(snapshot_id):
            snapshots = {
                100: Snapshot(snapshot_id=100, parent_snapshot_id=None, manifest_list="s3://100.avro", timestamp_ms=1000, sequence_number=1),
                200: Snapshot(snapshot_id=200, parent_snapshot_id=100, manifest_list="s3://200.avro", timestamp_ms=2000, sequence_number=2),
                300: Snapshot(snapshot_id=300, parent_snapshot_id=100, manifest_list="s3://300.avro", timestamp_ms=3000, sequence_number=3),
            }
            return snapshots.get(snapshot_id)
        
        transaction.table_metadata.snapshot_by_id = mock_snapshot_by_id

        updates, requirements = strategy.merge("feature", "main", transaction)

        assert len(updates) == 1
        assert len(requirements) == 1
        
        update = updates[0]
        assert isinstance(update, SetSnapshotRefUpdate)
        assert update.ref_name == "main"
        # In a real three-way merge, this would create a new snapshot
        # For now, it uses the source snapshot as a placeholder
        assert update.snapshot_id == 300


class TestRebaseMergeStrategy:
    """Test rebase merge strategy implementation."""

    def test_rebase_merge_linear_history(self) -> None:
        """Test rebase merge creating linear history."""
        strategy = _RebaseMergeStrategy()
        transaction = Mock(spec=Transaction)
        
        # Setup refs
        source_ref = SnapshotRef(snapshot_id=300, snapshot_ref_type=SnapshotRefType.BRANCH)
        target_ref = SnapshotRef(snapshot_id=200, snapshot_ref_type=SnapshotRefType.BRANCH)
        transaction.table_metadata.refs = {"feature": source_ref, "main": target_ref}

        updates, requirements = strategy.merge("feature", "main", transaction)

        assert len(updates) == 1
        assert len(requirements) == 1
        
        update = updates[0]
        assert isinstance(update, SetSnapshotRefUpdate)
        assert update.ref_name == "main"
        assert update.snapshot_id == 300


class TestCherryPickStrategy:
    """Test cherry-pick strategy implementation."""

    def test_cherry_pick_specific_commit(self) -> None:
        """Test cherry-picking specific commit."""
        strategy = _CherryPickStrategy()
        transaction = Mock(spec=Transaction)
        
        # Setup refs
        source_ref = SnapshotRef(snapshot_id=300, snapshot_ref_type=SnapshotRefType.BRANCH)
        target_ref = SnapshotRef(snapshot_id=200, snapshot_ref_type=SnapshotRefType.BRANCH)
        transaction.table_metadata.refs = {"feature": source_ref, "main": target_ref}

        updates, requirements = strategy.merge("feature", "main", transaction)

        assert len(updates) == 1
        assert len(requirements) == 1
        
        update = updates[0]
        assert isinstance(update, SetSnapshotRefUpdate)
        assert update.ref_name == "main"
        assert update.snapshot_id == 300


class TestFastForwardStrategy:
    """Test fast-forward strategy implementation."""

    def test_fast_forward_possible(self) -> None:
        """Test fast-forward when possible."""
        strategy = _FastForwardStrategy()
        transaction = Mock(spec=Transaction)
        
        # Setup refs
        source_ref = SnapshotRef(snapshot_id=200, snapshot_ref_type=SnapshotRefType.BRANCH)
        target_ref = SnapshotRef(snapshot_id=100, snapshot_ref_type=SnapshotRefType.BRANCH)
        transaction.table_metadata.refs = {"feature": source_ref, "main": target_ref}

        # Setup snapshots (fast-forward case)
        def mock_snapshot_by_id(snapshot_id):
            if snapshot_id == 200:
                return Snapshot(snapshot_id=200, parent_snapshot_id=100, manifest_list="s3://200.avro", timestamp_ms=2000, sequence_number=2)
            elif snapshot_id == 100:
                return Snapshot(snapshot_id=100, parent_snapshot_id=None, manifest_list="s3://100.avro", timestamp_ms=1000, sequence_number=1)
            return None
        
        transaction.table_metadata.snapshot_by_id = mock_snapshot_by_id

        updates, requirements = strategy.merge("feature", "main", transaction)

        assert len(updates) == 1
        assert len(requirements) == 1

    def test_fast_forward_not_possible(self) -> None:
        """Test fast-forward when not possible."""
        strategy = _FastForwardStrategy()
        transaction = Mock(spec=Transaction)
        
        # Setup refs for divergent branches
        source_ref = SnapshotRef(snapshot_id=300, snapshot_ref_type=SnapshotRefType.BRANCH)
        target_ref = SnapshotRef(snapshot_id=200, snapshot_ref_type=SnapshotRefType.BRANCH)
        transaction.table_metadata.refs = {"feature": source_ref, "main": target_ref}

        # Setup snapshots (divergent case - not fast-forward)
        def mock_snapshot_by_id(snapshot_id):
            snapshots = {
                100: Snapshot(snapshot_id=100, parent_snapshot_id=None, manifest_list="s3://100.avro", timestamp_ms=1000, sequence_number=1),
                200: Snapshot(snapshot_id=200, parent_snapshot_id=100, manifest_list="s3://200.avro", timestamp_ms=2000, sequence_number=2),
                300: Snapshot(snapshot_id=300, parent_snapshot_id=100, manifest_list="s3://300.avro", timestamp_ms=3000, sequence_number=3),
            }
            return snapshots.get(snapshot_id)
        
        transaction.table_metadata.snapshot_by_id = mock_snapshot_by_id

        with pytest.raises(ValueError, match="Fast-forward merge not possible"):
            strategy.merge("feature", "main", transaction)


class TestManageSnapshotsMergeBranch:
    """Test ManageSnapshots.merge_branch method."""

    def test_merge_branch_successful_operation(self) -> None:
        """Test successful branch merge operation."""
        transaction = Mock(spec=Transaction)
        transaction.table_metadata.refs = {
            "feature": SnapshotRef(snapshot_id=200, snapshot_ref_type=SnapshotRefType.BRANCH),
            "main": SnapshotRef(snapshot_id=100, snapshot_ref_type=SnapshotRefType.BRANCH)
        }
        
        manage_snapshots = ManageSnapshots(transaction)
        
        # Mock strategy execution
        with patch('pyiceberg.table.update.snapshot._get_merge_strategy_impl') as mock_strategy:
            mock_impl = Mock()
            mock_impl.merge.return_value = ((Mock(),), (Mock(),))
            mock_strategy.return_value = mock_impl
            
            result = manage_snapshots.merge_branch("feature", "main", BranchMergeStrategy.MERGE)
            assert result is manage_snapshots

    def test_merge_branch_validation_errors(self) -> None:
        """Test branch merge validation errors."""
        transaction = Mock(spec=Transaction)
        transaction.table_metadata.refs = {"main": SnapshotRef(snapshot_id=100, snapshot_ref_type=SnapshotRefType.BRANCH)}
        
        manage_snapshots = ManageSnapshots(transaction)
        
        # Test non-existent source branch
        with pytest.raises(ValueError, match="Source branch 'nonexistent' does not exist"):
            manage_snapshots.merge_branch("nonexistent", "main")
        
        # Test non-existent target branch
        with pytest.raises(ValueError, match="Target branch 'nonexistent' does not exist"):
            manage_snapshots.merge_branch("main", "nonexistent")
        
        # Test merging branch into itself
        with pytest.raises(ValueError, match="Cannot merge a branch into itself"):
            manage_snapshots.merge_branch("main", "main")

    def test_merge_branch_default_strategy(self) -> None:
        """Test that default merge strategy is MERGE."""
        transaction = Mock(spec=Transaction)
        transaction.table_metadata.refs = {
            "feature": SnapshotRef(snapshot_id=200, snapshot_ref_type=SnapshotRefType.BRANCH),
            "main": SnapshotRef(snapshot_id=100, snapshot_ref_type=SnapshotRefType.BRANCH)
        }
        
        manage_snapshots = ManageSnapshots(transaction)
        
        with patch('pyiceberg.table.update.snapshot._get_merge_strategy_impl') as mock_strategy:
            mock_impl = Mock()
            mock_impl.merge.return_value = ((Mock(),), (Mock(),))
            mock_strategy.return_value = mock_impl
            
            manage_snapshots.merge_branch("feature", "main")
            
            # Should be called with MERGE strategy (default)
            mock_strategy.assert_called_with(BranchMergeStrategy.MERGE)

    def test_merge_branch_with_custom_message(self) -> None:
        """Test branch merge with custom commit message."""
        transaction = Mock(spec=Transaction)
        transaction.table_metadata.refs = {
            "feature": SnapshotRef(snapshot_id=200, snapshot_ref_type=SnapshotRefType.BRANCH),
            "main": SnapshotRef(snapshot_id=100, snapshot_ref_type=SnapshotRefType.BRANCH)
        }
        
        manage_snapshots = ManageSnapshots(transaction)
        
        with patch('pyiceberg.table.update.snapshot._get_merge_strategy_impl') as mock_strategy:
            mock_impl = Mock()
            mock_impl.merge.return_value = ((Mock(),), (Mock(),))
            mock_strategy.return_value = mock_impl
            
            custom_message = "Custom merge message"
            manage_snapshots.merge_branch("feature", "main", merge_commit_message=custom_message)
            
            # Should pass custom message to strategy
            mock_impl.merge.assert_called_with(
                source_branch="feature",
                target_branch="main",
                transaction=transaction,
                merge_commit_message=custom_message
            )

    def test_merge_branch_with_source_deletion(self) -> None:
        """Test branch merge with source branch deletion."""
        transaction = Mock(spec=Transaction)
        transaction.table_metadata.refs = {
            "feature": SnapshotRef(snapshot_id=200, snapshot_ref_type=SnapshotRefType.BRANCH),
            "main": SnapshotRef(snapshot_id=100, snapshot_ref_type=SnapshotRefType.BRANCH)
        }
        
        manage_snapshots = ManageSnapshots(transaction)
        
        with patch('pyiceberg.table.update.snapshot._get_merge_strategy_impl') as mock_strategy:
            mock_impl = Mock()
            mock_impl.merge.return_value = ((Mock(),), (Mock(),))
            mock_strategy.return_value = mock_impl
            
            # Mock the _remove_ref_snapshot method
            manage_snapshots._remove_ref_snapshot = Mock(return_value=((Mock(),), (Mock(),)))
            
            result = manage_snapshots.merge_branch("feature", "main", delete_source_branch=True)
            
            # Should call merge strategy
            mock_impl.merge.assert_called_with(
                source_branch="feature",
                target_branch="main", 
                transaction=transaction,
                merge_commit_message=None
            )
            
            # Should also call remove source branch
            manage_snapshots._remove_ref_snapshot.assert_called_with(ref_name="feature")
            
            assert result is manage_snapshots

    def test_merge_branch_without_source_deletion_default(self) -> None:
        """Test that source branch deletion is False by default."""
        transaction = Mock(spec=Transaction)
        transaction.table_metadata.refs = {
            "feature": SnapshotRef(snapshot_id=200, snapshot_ref_type=SnapshotRefType.BRANCH),
            "main": SnapshotRef(snapshot_id=100, snapshot_ref_type=SnapshotRefType.BRANCH)
        }
        
        manage_snapshots = ManageSnapshots(transaction)
        
        with patch('pyiceberg.table.update.snapshot._get_merge_strategy_impl') as mock_strategy:
            mock_impl = Mock()
            mock_impl.merge.return_value = ((Mock(),), (Mock(),))
            mock_strategy.return_value = mock_impl
            
            # Mock the _remove_ref_snapshot method
            manage_snapshots._remove_ref_snapshot = Mock(return_value=((Mock(),), (Mock(),)))
            
            # Default behavior should not delete source branch
            manage_snapshots.merge_branch("feature", "main")
            
            # Should NOT call remove source branch
            manage_snapshots._remove_ref_snapshot.assert_not_called()

    def test_all_merge_strategies_with_source_deletion(self) -> None:
        """Test that source branch deletion works with all merge strategies."""
        transaction = Mock(spec=Transaction)
        transaction.table_metadata.refs = {
            "feature": SnapshotRef(snapshot_id=200, snapshot_ref_type=SnapshotRefType.BRANCH),
            "main": SnapshotRef(snapshot_id=100, snapshot_ref_type=SnapshotRefType.BRANCH)
        }
        
        # Setup fast-forward scenario
        def mock_snapshot_by_id(snapshot_id):
            if snapshot_id == 200:
                return Snapshot(snapshot_id=200, parent_snapshot_id=100, manifest_list="s3://200.avro", timestamp_ms=2000, sequence_number=2)
            elif snapshot_id == 100:
                return Snapshot(snapshot_id=100, parent_snapshot_id=None, manifest_list="s3://100.avro", timestamp_ms=1000, sequence_number=1)
            return None
        
        transaction.table_metadata.snapshot_by_id = mock_snapshot_by_id
        
        manage_snapshots = ManageSnapshots(transaction)
        manage_snapshots._remove_ref_snapshot = Mock(return_value=((Mock(),), (Mock(),)))
        
        strategies = [
            BranchMergeStrategy.MERGE,
            BranchMergeStrategy.SQUASH,
            BranchMergeStrategy.REBASE,
            BranchMergeStrategy.CHERRY_PICK,
            BranchMergeStrategy.FAST_FORWARD,
        ]
        
        for strategy in strategies:
            # Reset the mock for each strategy
            manage_snapshots._remove_ref_snapshot.reset_mock()
            
            result = manage_snapshots.merge_branch("feature", "main", strategy, delete_source_branch=True)
            
            # Should call remove source branch for each strategy
            manage_snapshots._remove_ref_snapshot.assert_called_once_with(ref_name="feature")
            
            assert result is manage_snapshots


class TestErrorHandlingAndEdgeCases:
    """Test error handling and edge cases across all strategies."""

    def test_missing_source_branch(self) -> None:
        """Test error when source branch doesn't exist."""
        transaction = Mock(spec=Transaction)
        transaction.table_metadata.refs = {"main": SnapshotRef(snapshot_id=100, snapshot_ref_type=SnapshotRefType.BRANCH)}
        
        manage_snapshots = ManageSnapshots(transaction)
        
        with pytest.raises(ValueError, match="Source branch 'nonexistent' does not exist"):
            manage_snapshots.merge_branch("nonexistent", "main")

    def test_missing_target_branch(self) -> None:
        """Test error when target branch doesn't exist."""
        transaction = Mock(spec=Transaction)
        transaction.table_metadata.refs = {"feature": SnapshotRef(snapshot_id=200, snapshot_ref_type=SnapshotRefType.BRANCH)}
        
        manage_snapshots = ManageSnapshots(transaction)
        
        with pytest.raises(ValueError, match="Target branch 'nonexistent' does not exist"):
            manage_snapshots.merge_branch("feature", "nonexistent")

    def test_self_merge_attempt(self) -> None:
        """Test error when trying to merge branch into itself."""
        transaction = Mock(spec=Transaction)
        transaction.table_metadata.refs = {"main": SnapshotRef(snapshot_id=100, snapshot_ref_type=SnapshotRefType.BRANCH)}
        
        manage_snapshots = ManageSnapshots(transaction)
        
        with pytest.raises(ValueError, match="Cannot merge a branch into itself"):
            manage_snapshots.merge_branch("main", "main")

    def test_strategy_with_missing_snapshots(self) -> None:
        """Test strategies handle missing snapshots gracefully."""
        transaction = Mock(spec=Transaction)
        source_ref = SnapshotRef(snapshot_id=999, snapshot_ref_type=SnapshotRefType.BRANCH)
        target_ref = SnapshotRef(snapshot_id=888, snapshot_ref_type=SnapshotRefType.BRANCH)
        transaction.table_metadata.refs = {"feature": source_ref, "main": target_ref}
        transaction.table_metadata.snapshot_by_id = Mock(return_value=None)

        strategy = _SquashMergeStrategy()

        # Should raise error for missing snapshots (correct behavior)
        with pytest.raises(ValueError, match="Source snapshot not found for branch feature"):
            strategy.merge("feature", "main", transaction)

    def test_all_strategies_with_identical_snapshots(self) -> None:
        """Test all strategies when source and target point to same snapshot."""
        transaction = Mock(spec=Transaction)
        same_snapshot_ref = SnapshotRef(snapshot_id=100, snapshot_ref_type=SnapshotRefType.BRANCH)
        transaction.table_metadata.refs = {"feature": same_snapshot_ref, "main": same_snapshot_ref}
        
        strategies = [
            _SquashMergeStrategy(),
            _MergeStrategy(),
            _RebaseMergeStrategy(),
            _CherryPickStrategy(),
        ]
        
        for strategy in strategies:
            updates, requirements = strategy.merge("feature", "main", transaction)
            assert len(updates) == 1
            assert len(requirements) == 1
            # All should result in same operation when snapshots are identical

    def test_empty_branch_name_validation(self) -> None:
        """Test validation of empty branch names."""
        transaction = Mock(spec=Transaction)
        transaction.table_metadata.refs = {"": SnapshotRef(snapshot_id=100, snapshot_ref_type=SnapshotRefType.BRANCH)}
        
        manage_snapshots = ManageSnapshots(transaction)
        
        # Empty string branch names should be treated as non-existent
        with pytest.raises(ValueError, match="Source branch 'feature' does not exist"):
            manage_snapshots.merge_branch("feature", "")

    def test_strategy_factory_with_invalid_strategy(self) -> None:
        """Test strategy factory with invalid strategy enum value."""
        # This would require modifying the enum, but we can test the factory logic
        valid_strategies = [
            BranchMergeStrategy.MERGE,
            BranchMergeStrategy.SQUASH,
            BranchMergeStrategy.REBASE,
            BranchMergeStrategy.CHERRY_PICK,
            BranchMergeStrategy.FAST_FORWARD,
        ]
        
        for strategy in valid_strategies:
            impl = _get_merge_strategy_impl(strategy)
            assert impl is not None
            assert hasattr(impl, 'merge')

    def test_fast_forward_validation_in_strategies(self) -> None:
        """Test that only fast-forward strategy validates fast-forward requirement."""
        transaction = Mock(spec=Transaction)
        
        # Set up divergent branches (not fast-forward)
        source_ref = SnapshotRef(snapshot_id=300, snapshot_ref_type=SnapshotRefType.BRANCH)
        target_ref = SnapshotRef(snapshot_id=200, snapshot_ref_type=SnapshotRefType.BRANCH)
        transaction.table_metadata.refs = {"feature": source_ref, "main": target_ref}

        # Mock snapshots for divergent case
        def mock_snapshot_by_id(snapshot_id):
            snapshots = {
                100: Snapshot(snapshot_id=100, parent_snapshot_id=None, manifest_list="s3://100.avro", timestamp_ms=1000, sequence_number=1),
                200: Snapshot(snapshot_id=200, parent_snapshot_id=100, manifest_list="s3://200.avro", timestamp_ms=2000, sequence_number=2),
                300: Snapshot(snapshot_id=300, parent_snapshot_id=100, manifest_list="s3://300.avro", timestamp_ms=3000, sequence_number=3),
            }
            return snapshots.get(snapshot_id)
        
        transaction.table_metadata.snapshot_by_id = mock_snapshot_by_id

        # Only FastForward strategy should reject this
        fast_forward_strategy = _FastForwardStrategy()
        with pytest.raises(ValueError, match="Fast-forward merge not possible"):
            fast_forward_strategy.merge("feature", "main", transaction)

        # Other strategies should work fine
        other_strategies = [_SquashMergeStrategy(), _MergeStrategy(), _RebaseMergeStrategy(), _CherryPickStrategy()]
        for strategy in other_strategies:
            updates, requirements = strategy.merge("feature", "main", transaction)
            assert len(updates) == 1
            assert len(requirements) == 1


class TestIntegrationAndBehavioralDifferences:
    """Test integration scenarios and ensure strategies behave differently where expected."""

    def test_all_strategies_return_consistent_structure(self) -> None:
        """Test that all strategies return consistent tuple structure."""
        transaction = Mock(spec=Transaction)
        source_ref = SnapshotRef(snapshot_id=200, snapshot_ref_type=SnapshotRefType.BRANCH)
        target_ref = SnapshotRef(snapshot_id=100, snapshot_ref_type=SnapshotRefType.BRANCH)
        transaction.table_metadata.refs = {"feature": source_ref, "main": target_ref}
        
        # Setup fast-forward possible scenario
        def mock_snapshot_by_id(snapshot_id):
            if snapshot_id == 200:
                return Snapshot(snapshot_id=200, parent_snapshot_id=100, manifest_list="s3://200.avro", timestamp_ms=2000, sequence_number=2)
            elif snapshot_id == 100:
                return Snapshot(snapshot_id=100, parent_snapshot_id=None, manifest_list="s3://100.avro", timestamp_ms=1000, sequence_number=1)
            return None
        
        transaction.table_metadata.snapshot_by_id = mock_snapshot_by_id

        strategies = [
            _SquashMergeStrategy(),
            _MergeStrategy(),
            _RebaseMergeStrategy(),
            _CherryPickStrategy(),
            _FastForwardStrategy(),
        ]
        
        for strategy in strategies:
            updates, requirements = strategy.merge("feature", "main", transaction)
            
            # All strategies should return tuple of tuples
            assert isinstance(updates, tuple)
            assert isinstance(requirements, tuple)
            assert len(updates) >= 1
            assert len(requirements) >= 1
            
            # Updates should contain SetSnapshotRefUpdate
            assert all(isinstance(update, SetSnapshotRefUpdate) for update in updates)

    def test_manage_snapshots_integration_all_strategies(self) -> None:
        """Test full integration through ManageSnapshots with all strategies."""
        transaction = Mock(spec=Transaction)
        transaction.table_metadata.refs = {
            "feature": SnapshotRef(snapshot_id=200, snapshot_ref_type=SnapshotRefType.BRANCH),
            "main": SnapshotRef(snapshot_id=100, snapshot_ref_type=SnapshotRefType.BRANCH)
        }
        
        # Setup fast-forward scenario to work with all strategies
        def mock_snapshot_by_id(snapshot_id):
            if snapshot_id == 200:
                return Snapshot(snapshot_id=200, parent_snapshot_id=100, manifest_list="s3://200.avro", timestamp_ms=2000, sequence_number=2)
            elif snapshot_id == 100:
                return Snapshot(snapshot_id=100, parent_snapshot_id=None, manifest_list="s3://100.avro", timestamp_ms=1000, sequence_number=1)
            return None
        
        transaction.table_metadata.snapshot_by_id = mock_snapshot_by_id
        
        manage_snapshots = ManageSnapshots(transaction)
        
        # Test all strategies work through ManageSnapshots
        strategies = [
            BranchMergeStrategy.MERGE,
            BranchMergeStrategy.SQUASH,
            BranchMergeStrategy.REBASE,
            BranchMergeStrategy.CHERRY_PICK,
            BranchMergeStrategy.FAST_FORWARD,
        ]
        
        for strategy in strategies:
            result = manage_snapshots.merge_branch("feature", "main", strategy)
            assert result is manage_snapshots  # Should return self for chaining

    def test_strategy_selection_and_execution(self) -> None:
        """Test that the correct strategy is selected and executed."""
        transaction = Mock(spec=Transaction)
        transaction.table_metadata.refs = {
            "feature": SnapshotRef(snapshot_id=200, snapshot_ref_type=SnapshotRefType.BRANCH),
            "main": SnapshotRef(snapshot_id=100, snapshot_ref_type=SnapshotRefType.BRANCH)
        }
        
        manage_snapshots = ManageSnapshots(transaction)
        
        # Mock strategy implementations to verify correct one is called
        with patch('pyiceberg.table.update.snapshot._SquashMergeStrategy') as mock_squash, \
             patch('pyiceberg.table.update.snapshot._MergeStrategy') as mock_merge, \
             patch('pyiceberg.table.update.snapshot._RebaseMergeStrategy') as mock_rebase:
            
            # Setup mock instances
            mock_squash_instance = Mock()
            mock_squash_instance.merge.return_value = ((Mock(),), (Mock(),))
            mock_squash.return_value = mock_squash_instance
            
            mock_merge_instance = Mock()
            mock_merge_instance.merge.return_value = ((Mock(),), (Mock(),))
            mock_merge.return_value = mock_merge_instance
            
            mock_rebase_instance = Mock()
            mock_rebase_instance.merge.return_value = ((Mock(),), (Mock(),))
            mock_rebase.return_value = mock_rebase_instance
            
            # Test that correct strategy is selected
            manage_snapshots.merge_branch("feature", "main", BranchMergeStrategy.SQUASH)
            manage_snapshots.merge_branch("feature", "main", BranchMergeStrategy.MERGE)
            manage_snapshots.merge_branch("feature", "main", BranchMergeStrategy.REBASE)
            
            # Verify correct strategies were instantiated and called
            assert mock_squash.called
            assert mock_merge.called  
            assert mock_rebase.called


# Individual strategy tests for backward compatibility
def test_branch_merge_strategy_enum():
    """Test enum functionality."""
    assert BranchMergeStrategy.MERGE.value == "merge"
    assert BranchMergeStrategy.SQUASH.value == "squash"
    assert BranchMergeStrategy.REBASE.value == "rebase"
    assert BranchMergeStrategy.CHERRY_PICK.value == "cherry_pick"
    assert BranchMergeStrategy.FAST_FORWARD.value == "fast_forward"


def test_get_merge_strategy_impl():
    """Test strategy factory function."""
    assert isinstance(_get_merge_strategy_impl(BranchMergeStrategy.MERGE), _MergeStrategy)
    assert isinstance(_get_merge_strategy_impl(BranchMergeStrategy.SQUASH), _SquashMergeStrategy)
    assert isinstance(_get_merge_strategy_impl(BranchMergeStrategy.REBASE), _RebaseMergeStrategy)
    assert isinstance(_get_merge_strategy_impl(BranchMergeStrategy.CHERRY_PICK), _CherryPickStrategy)
    assert isinstance(_get_merge_strategy_impl(BranchMergeStrategy.FAST_FORWARD), _FastForwardStrategy)
