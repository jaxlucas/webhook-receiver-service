"""Refuse to run SQLite on a network filesystem: WAL needs shared memory and reliable POSIX locks."""

from __future__ import annotations

import os

NETWORK_FS_PREFIXES = ("nfs", "cifs", "smb", "fuse.")


class NetworkFilesystemError(RuntimeError):
    pass


def _unescape(field: str) -> str:
    # /proc/mounts escapes spaces and tabs as octal sequences.
    return field.replace("\\040", " ").replace("\\011", "\t").replace("\\134", "\\")


def mount_fstype(path: str, mounts_text: str) -> str | None:
    """The filesystem type of the longest mount point containing ``path``."""
    target = os.path.realpath(path)
    best: tuple[int, str] | None = None
    for line in mounts_text.splitlines():
        parts = line.split()
        if len(parts) < 3:
            continue
        mount_point, fstype = _unescape(parts[1]), parts[2]
        prefix = mount_point.rstrip("/") + "/"
        if (target == mount_point or target.startswith(prefix) or mount_point == "/") and (
            best is None or len(mount_point) > best[0]
        ):
            best = (len(mount_point), fstype)
    return best[1] if best else None


def check_database_filesystem(database_path: str, mounts_path: str = "/proc/mounts") -> str | None:
    """Raise ``NetworkFilesystemError`` when the DB directory is on nfs/cifs/smb/fuse. Best effort."""
    try:
        with open(mounts_path, encoding="utf-8") as handle:
            mounts_text = handle.read()
    except OSError:
        return None
    directory = os.path.dirname(os.path.abspath(database_path))
    fstype = mount_fstype(directory, mounts_text)
    if fstype and fstype.startswith(NETWORK_FS_PREFIXES):
        raise NetworkFilesystemError(
            f"DATABASE_PATH {database_path!r} is on a {fstype!r} filesystem. SQLite WAL with several "
            "replicas only works on a local volume on one host; a network filesystem will corrupt it."
        )
    return fstype
