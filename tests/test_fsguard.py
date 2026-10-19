"""Phase 4 step 2: the network-filesystem guard."""

from __future__ import annotations

from pathlib import Path

import pytest

from webhook_receiver.fsguard import NetworkFilesystemError, check_database_filesystem, mount_fstype

MOUNTS = """\
overlay / overlay rw,relatime 0 0
proc /proc proc rw 0 0
/dev/sda1 /data ext4 rw,relatime 0 0
server:/export /mnt/nfs nfs4 rw 0 0
//host/share /mnt/smb cifs rw 0 0
sshfs#me@host: /mnt/fuse fuse.sshfs rw 0 0
/dev/sdb1 /mnt/with\\040space xfs rw 0 0
"""


@pytest.mark.parametrize(
    ("path", "fstype"),
    [
        ("/data", "ext4"),
        ("/data/sub", "ext4"),
        ("/datax", "overlay"),
        ("/mnt/nfs/db", "nfs4"),
        ("/mnt/smb", "cifs"),
        ("/mnt/fuse/x", "fuse.sshfs"),
        ("/mnt/with space/db", "xfs"),
        ("/tmp", "overlay"),
    ],
)
def test_mount_fstype_picks_longest_mount_point(path: str, fstype: str) -> None:
    assert mount_fstype(path, MOUNTS) == fstype


@pytest.mark.parametrize("directory", ["/mnt/nfs", "/mnt/smb", "/mnt/fuse"])
def test_network_filesystems_are_refused(tmp_path: Path, directory: str) -> None:
    mounts = tmp_path / "mounts"
    mounts.write_text(MOUNTS)
    with pytest.raises(NetworkFilesystemError):
        check_database_filesystem(f"{directory}/events.db", str(mounts))


def test_local_filesystem_is_allowed(tmp_path: Path) -> None:
    mounts = tmp_path / "mounts"
    mounts.write_text(MOUNTS)
    assert check_database_filesystem("/data/events.db", str(mounts)) == "ext4"


def test_missing_proc_mounts_is_best_effort(tmp_path: Path) -> None:
    assert check_database_filesystem("/data/events.db", str(tmp_path / "missing")) is None
