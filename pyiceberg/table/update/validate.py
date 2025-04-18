class ValidationException(Exception):
    """Raised when validation fails."""



from pyiceberg.table import Table
from pyiceberg.table.snapshots import Snapshot, Operation, ancestors_between
from pyiceberg.manifest import ManifestFile, ManifestContent


def validation_history(
    table: Table,
    starting_snapshot_id: int,
    matching_operations: set[Operation],
    manifest_content: ManifestContent,
    parent: Snapshot,
) -> tuple[list[ManifestFile], set[Snapshot]]:
    """Return newly added manifests and snapshot IDs between the starting snapshot ID and parent snapshot

    Args:
        table: Table to get the history from
        starting_snapshot_id: ID of the starting snapshot
        matching_operations: Operations to match on
        manifest_content: Manifest content type to filter
        parent: Parent snapshot to get the history from

    Raises:
        ValidationException: If no matching snapshot is found or only one snapshot is found

    Returns:
        List of manifest files and set of snapshots matching conditions
    """
    manifests_files: list[ManifestFile] = []
    snapshots: set[Snapshot] = set()

    last_snapshot = None
    for snapshot in ancestors_between(starting_snapshot_id, parent.snapshot_id, table.metadata):
        last_snapshot = snapshot
        if snapshot.operation in matching_operations:
            snapshots.add(snapshot)
            if manifest_content == ManifestContent.DATA:
                manifests_files.extend([manifest for manifest in snapshot.data_manifests(table.io) if manifest.added_snapshot_id == snapshot.snapshot_id])
            else:
                manifests_files.extend([manifest for manifest in snapshot.delete_manifests(table.io) if manifest.added_snapshot_id == snapshot.snapshot_id])

    if last_snapshot is None or last_snapshot.snapshot_id == starting_snapshot_id:
        raise ValidationException("No matching snapshot found.")

    return manifests_files, snapshots

