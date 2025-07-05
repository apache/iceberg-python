#!/usr/bin/env python3
"""
Test script to validate the retention strategies implementation in MaintenanceTable.

This script demonstrates the new retention features:
1. retain_last_n_snapshots() - Keep only the last N snapshots
2. expire_snapshots_older_than_with_retention() - Time-based expiration with retention constraints
3. expire_snapshots_with_retention_policy() - Comprehensive retention policy
"""

# Example usage (commented out since we don't have an actual table)
"""
from pyiceberg.table.maintenance import MaintenanceTable
from pyiceberg.table import Table

# Assume we have a table instance
table = Table(...)  # Initialize your table
maintenance = MaintenanceTable(table)

# Example 1: Keep only the last 5 snapshots regardless of age
# This is helpful when regular snapshot creation occurs and users always want 
# to keep the last few for rollback
maintenance.retain_last_n_snapshots(5)

# Example 2: Expire snapshots older than a timestamp but keep at least 3 total
# This acts as a guardrail to prevent aggressive expiration logic from removing too many snapshots
import time
one_week_ago = int((time.time() - 7 * 24 * 60 * 60) * 1000)  # 7 days ago in milliseconds
maintenance.expire_snapshots_older_than_with_retention(
    timestamp_ms=one_week_ago,
    min_snapshots_to_keep=3
)

# Example 3: Combined policy - expire old snapshots but keep last 10 and at least 5 total
# This provides comprehensive control combining both strategies
maintenance.expire_snapshots_with_retention_policy(
    timestamp_ms=one_week_ago,
    retain_last_n=10,
    min_snapshots_to_keep=5
)

# Example 4: Just keep the last 20 snapshots (no time constraint)
expired_ids = maintenance.expire_snapshots_with_retention_policy(retain_last_n=20)
print(f"Expired {len(expired_ids)} snapshots")
"""

def test_validation():
    """Test parameter validation logic"""
    
    # Mock a simple snapshot class for testing
    class MockSnapshot:
        def __init__(self, snapshot_id, timestamp_ms):
            self.snapshot_id = snapshot_id
            self.timestamp_ms = timestamp_ms
    
    # Mock table metadata
    class MockTableMetadata:
        def __init__(self, snapshots):
            self.snapshots = snapshots
            self.refs = {}  # Empty refs for simplicity
            
        def snapshot_by_id(self, snapshot_id):
            for snapshot in self.snapshots:
                if snapshot.snapshot_id == snapshot_id:
                    return snapshot
            return None
    
    # Mock table
    class MockTable:
        def __init__(self, snapshots):
            self.metadata = MockTableMetadata(snapshots)
    
    # Test the retention logic (without actual table operations)
    from pyiceberg.table.maintenance import MaintenanceTable
    
    # Create test snapshots (oldest to newest)
    test_snapshots = [
        MockSnapshot(1, 1000),  # oldest
        MockSnapshot(2, 2000),
        MockSnapshot(3, 3000),
        MockSnapshot(4, 4000),
        MockSnapshot(5, 5000),  # newest
    ]
    
    mock_table = MockTable(test_snapshots)
    
    # Test the helper method directly
    maintenance = MaintenanceTable(mock_table)
    
    print("Testing retention strategies validation...")
    
    # Test 1: retain_last_n should keep the 3 most recent snapshots
    snapshots_to_expire = maintenance._get_snapshots_to_expire_with_retention(
        retain_last_n=3
    )
    print(f"Test 1 - Retain last 3: Should expire snapshots [1, 2], got {snapshots_to_expire}")
    
    # Test 2: min_snapshots_to_keep should prevent expiring too many
    snapshots_to_expire = maintenance._get_snapshots_to_expire_with_retention(
        timestamp_ms=4500,  # Should expire snapshots 1,2,3,4
        min_snapshots_to_keep=3
    )
    print(f"Test 2 - Min keep 3: Should expire snapshots [1, 2], got {snapshots_to_expire}")
    
    # Test 3: Combined constraints
    snapshots_to_expire = maintenance._get_snapshots_to_expire_with_retention(
        timestamp_ms=3500,  # Would expire 1,2,3
        retain_last_n=2,    # Keep last 2 (snapshots 4,5)
        min_snapshots_to_keep=4  # Keep at least 4 total
    )
    print(f"Test 3 - Combined: Should expire snapshot [1], got {snapshots_to_expire}")
    
    print("Validation tests completed!")

if __name__ == "__main__":
    test_validation()
