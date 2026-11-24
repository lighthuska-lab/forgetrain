"""Immutable metadata for bounded trusted local artifacts, never model loading."""

from __future__ import annotations

import hashlib
import os
import re
import sqlite3
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

MAX_BYTES = 64 * 1024 * 1024


def _label(value: Any, name: str) -> str:
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value) > 128
        or "\x00" in value
    ):
        raise ValueError(f"invalid {name}")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError(f"invalid Unicode in {name}") from exc
    return value


def _identifier(value: Any) -> str:
    text = _label(value, "artifact ID")
    if not re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", text):
        raise ValueError("invalid artifact ID")
    return text


class ArtifactRegistry:
    """One immutable artifact namespace under one trusted root per local database."""

    def __init__(self, path: str | Path):
        self.path = Path(path).resolve(strict=True)
        if not self.path.is_file():
            raise ValueError("registry must be a regular file")

    @classmethod
    def create(cls, path: str | Path, *, root: str | Path) -> ArtifactRegistry:
        supplied_root = Path(root)
        if supplied_root.is_symlink():
            raise ValueError("artifact root must not be a symlink")
        base = supplied_root.resolve(strict=True)
        if not base.is_dir():
            raise ValueError("artifact root must be a directory")
        target = Path(path)
        descriptor = os.open(target, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(descriptor)
        registry = cls(target)
        with registry._transaction() as db:
            db.execute("CREATE TABLE registry (version INTEGER, root TEXT NOT NULL)")
            db.execute("INSERT INTO registry VALUES (1, ?)", (str(base),))
            db.execute("""CREATE TABLE artifacts (
                id TEXT PRIMARY KEY, relative_path TEXT NOT NULL, kind TEXT NOT NULL,
                format TEXT NOT NULL, producer_revision TEXT NOT NULL,
                sha256 TEXT NOT NULL, size_bytes INTEGER NOT NULL CHECK(size_bytes>=0),
                registered_at TEXT NOT NULL)""")
            db.execute("""CREATE TABLE registrations (
                seq INTEGER PRIMARY KEY, artifact_id TEXT UNIQUE NOT NULL,
                registered_at TEXT NOT NULL)""")
        return registry

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        db = sqlite3.connect(
            self.path.as_uri() + "?mode=rw", uri=True, timeout=5, isolation_level=None
        )
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA synchronous=FULL")
        try:
            yield db
        finally:
            db.close()

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._connection() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                yield db
                db.execute("COMMIT")
            except BaseException:
                db.execute("ROLLBACK")
                raise

    @staticmethod
    def _root(db: sqlite3.Connection) -> Path:
        row = db.execute("SELECT * FROM registry").fetchone()
        if row is None or row["version"] != 1:
            raise ValueError("unsupported registry schema")
        return Path(row["root"])

    def _observe(self, root: Path, relative_path: str) -> tuple[str, int]:
        if not isinstance(relative_path, str) or len(relative_path) > 4096:
            raise ValueError("invalid artifact path")
        relative = PurePosixPath(relative_path)
        if (
            relative.is_absolute()
            or "\\" in relative_path
            or "\x00" in relative_path
            or any(part in {"", ".", ".."} for part in relative_path.split("/"))
        ):
            raise ValueError("artifact path must be canonical relative POSIX")
        path = root
        for part in relative.parts:
            path = path / part
            if path.is_symlink():
                raise ValueError("artifact symlinks are not supported")
        resolved = path.resolve(strict=True)
        if not resolved.is_relative_to(root):
            raise ValueError("artifact escapes root")
        if resolved.parent == self.path.parent and resolved.name in {
            self.path.name,
            self.path.name + "-journal",
            self.path.name + "-wal",
            self.path.name + "-shm",
        }:
            raise ValueError("registry internals cannot be artifacts")
        flags = (
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
        )
        descriptor = os.open(resolved, flags)
        try:
            before = os.fstat(descriptor)
            if not stat.S_ISREG(before.st_mode):
                raise ValueError("artifact must be a regular file")
            database = self.path.stat()
            if (before.st_dev, before.st_ino) == (database.st_dev, database.st_ino):
                raise ValueError("registry database cannot be an artifact")
            if before.st_size > MAX_BYTES:
                raise ValueError("artifact exceeds 64 MiB")
            digest = hashlib.sha256()
            size = 0
            while block := os.read(descriptor, 1024 * 1024):
                size += len(block)
                if size > MAX_BYTES:
                    raise ValueError("artifact exceeds 64 MiB")
                digest.update(block)
            after = os.fstat(descriptor)
            current = path.stat(follow_symlinks=False)

            def signature(info: os.stat_result) -> tuple[int, int, int, int, int]:
                return (
                    info.st_dev,
                    info.st_ino,
                    info.st_size,
                    info.st_mtime_ns,
                    info.st_ctime_ns,
                )

            if (
                signature(before) != signature(after)
                or signature(after) != signature(current)
                or size != after.st_size
            ):
                raise ValueError("artifact changed during hashing")
            return digest.hexdigest(), size
        finally:
            os.close(descriptor)

    def register(
        self,
        artifact_id: str,
        *,
        relative_path: str,
        kind: str,
        format: str,
        producer_revision: str,
    ) -> dict[str, Any]:
        artifact_id = _identifier(artifact_id)
        if not isinstance(kind, str) or kind not in {"checkpoint", "metrics", "other"}:
            raise ValueError("unsupported artifact kind")
        format = _label(format, "format declaration")
        if not isinstance(producer_revision, str) or not re.fullmatch(
            r"(?:[0-9a-f]{40}|[0-9a-f]{64})", producer_revision
        ):
            raise ValueError("producer revision must be a full lowercase commit digest")
        with self._connection() as db:
            root = self._root(db)
        digest, size = self._observe(root, relative_path)
        values = {
            "id": artifact_id,
            "relative_path": relative_path,
            "kind": kind,
            "format": format,
            "producer_revision": producer_revision,
            "sha256": digest,
            "size_bytes": size,
        }
        with self._transaction() as db:
            self._root(db)
            existing = db.execute(
                "SELECT * FROM artifacts WHERE id=?", (artifact_id,)
            ).fetchone()
            if existing is not None:
                record = dict(existing)
                if any(record[key] != value for key, value in values.items()):
                    raise ValueError(
                        "artifact ID already registered with conflicting provenance"
                    )
                return record
            timestamp = datetime.now(UTC).isoformat()
            db.execute(
                "INSERT INTO artifacts VALUES (?,?,?,?,?,?,?,?)",
                (*values.values(), timestamp),
            )
            self._audit(db, artifact_id, timestamp)
            return dict(values, registered_at=timestamp)

    @staticmethod
    def _audit(db: sqlite3.Connection, artifact_id: str, timestamp: str) -> None:
        db.execute(
            "INSERT INTO registrations(artifact_id,registered_at) VALUES (?,?)",
            (artifact_id, timestamp),
        )

    def get(self, artifact_id: str) -> dict[str, Any]:
        artifact_id = _identifier(artifact_id)
        with self._connection() as db:
            self._root(db)
            row = db.execute(
                "SELECT * FROM artifacts WHERE id=?", (artifact_id,)
            ).fetchone()
            if row is None:
                raise KeyError(artifact_id)
            return dict(row)

    def list_page(self, *, limit: int = 20, after: str | None = None) -> dict[str, Any]:
        """Read at most 100 records plus one lookahead, ordered by immutable ID.

        Cursors are exclusive artifact IDs, not offsets. Separate pages are not
        one global snapshot if new IDs are registered between requests.
        """
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("page limit must be an integer from 1 to 100")
        if after is not None:
            after = _identifier(after)
        with self._connection() as db:
            db.execute("BEGIN")
            self._root(db)
            rows = db.execute(
                "SELECT * FROM artifacts WHERE id > ? ORDER BY id LIMIT ?",
                (after if after is not None else "", limit + 1),
            ).fetchall()
            records = [dict(row) for row in rows[:limit]]
            more = len(rows) > limit
            return {
                "schema_version": 1,
                "records": records,
                "content_files_verified": False,
                "has_more": more,
                "next_after": records[-1]["id"] if more else None,
            }

    def verify(self, artifact_id: str) -> dict[str, Any]:
        record = self.get(artifact_id)
        with self._connection() as db:
            root = self._root(db)
        digest, size = self._observe(root, record["relative_path"])
        return {
            "id": record["id"],
            "matches": digest == record["sha256"] and size == record["size_bytes"],
            "expected_sha256": record["sha256"],
            "observed_sha256": digest,
            "expected_size_bytes": record["size_bytes"],
            "observed_size_bytes": size,
            "checkpoint_validity_verified": False,
        }
