"""File-backed group-write / WAL / checkpoint recovery drill model.

This is an educational persistence model, not a production database engine.
It keeps the on-disk shape deliberately inspectable:

* one length+CRC framed record per atomic group (multi-key put/delete);
* an fsync boundary before a group is acknowledged as durable;
* an atomically published checkpoint (temp file + fsync + rename);
* log segments reclaimed only when every record in them is covered by the
  latest complete checkpoint;
* recovery accepts only a complete checkpoint and a contiguous valid log.
  An incomplete trailing record is discarded; corruption in the middle stops
  recovery and leaves every file untouched ("preserve the scene").
"""
from __future__ import annotations

import json
import os
import re
import struct
import threading
import zlib
from copy import deepcopy
from hashlib import sha256
from pathlib import Path

MAGIC = b"LWD1"
HEADER = struct.Struct(">4sII")  # magic, payload length, crc32
CHECKPOINT_MAGIC = "LSM-DRILL-CKPT-1"
CHECKPOINT_FILE = "checkpoint.json"
CHECKPOINT_TMP = "checkpoint.json.tmp"
SEAL_FILE = "segment.seal"
SEGMENT_RE = re.compile(r"^wal-(\d{6})\.log$")
MAX_OPERATIONS = 2000
MAX_KEY_LENGTH = 64
MAX_VALUE_LENGTH = 200
MAX_GROUP_ID_LENGTH = 64


class DrillError(Exception):
    """Base class for drill-store failures."""


class NotWritable(DrillError):
    """Raised when a crashed or stopped drill is asked to accept writes."""


class GroupConflict(DrillError):
    """Same idempotency key, different group content: whole group rejected."""

    def __init__(self, group_id: str):
        self.group_id = group_id
        super().__init__(f"同组号 {group_id} 已写入不同内容，整组拒绝")


class RecoveryError(DrillError):
    """Recovery stopped; the report carries the reason and preserved state."""

    def __init__(self, report: dict):
        self.report = report
        super().__init__(report.get("error") or "恢复停止")


def canonical(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def operation_digest(operations: list[dict]) -> str:
    return sha256(canonical(operations).encode("utf-8")).hexdigest()


def normalize_operations(operations) -> list[dict]:
    """Validate and normalize a group before any byte is written."""
    if not isinstance(operations, list) or not 1 <= len(operations) <= MAX_OPERATIONS:
        raise ValueError(f"每组操作数必须在1到{MAX_OPERATIONS}之间")
    normalized = []
    for item in operations:
        if not isinstance(item, dict):
            raise ValueError("每条操作必须是对象")
        kind = item.get("op")
        key = item.get("key")
        if kind not in ("put", "delete"):
            raise ValueError("操作必须是 put 或 delete")
        if not isinstance(key, str) or not 1 <= len(key) <= MAX_KEY_LENGTH:
            raise ValueError(f"键必须是1到{MAX_KEY_LENGTH}个字符")
        if kind == "put":
            value = item.get("value")
            if not isinstance(value, str) or len(value) > MAX_VALUE_LENGTH:
                raise ValueError(f"put 的 value 必须是不超过{MAX_VALUE_LENGTH}字的字符串")
            normalized.append({"op": "put", "key": key, "value": value})
        else:
            normalized.append({"op": "delete", "key": key})
    return normalized


def apply_operations(kv: dict, operations: list[dict]) -> None:
    for item in operations:
        if item["op"] == "put":
            kv[item["key"]] = item["value"]
        else:
            kv.pop(item["key"], None)


def encode_record(payload: dict) -> bytes:
    body = canonical(payload).encode("utf-8")
    return HEADER.pack(MAGIC, len(body), zlib.crc32(body) & 0xFFFFFFFF) + body


class DurableStore:
    """One independent drill directory."""

    def __init__(self, root, segment_limit: int = 16):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.segment_limit = max(1, int(segment_limit))
        self._lock = threading.RLock()
        self._checkpoint_lock = threading.RLock()
        self._reset_memory()

    # ------------------------------------------------------------------ state
    def _reset_memory(self):
        self.kv: dict[str, str] = {}
        self.groups: dict[str, dict] = {}
        self.checkpoint_lsn = 0
        self.last_lsn = 0
        self.status = "unloaded"  # unloaded | writable | crashed | stopped
        self._segments: dict[int, dict] = {}
        self._active_seg: int | None = None
        self._torn_segment: int | None = None
        self._next_seg = 1
        self.last_report: dict | None = None

    def _ensure_writable(self):
        if self.status != "writable":
            raise NotWritable("当前演练不在可写状态，请先执行恢复")

    def _segment_path(self, number: int) -> Path:
        return self.root / f"wal-{number:06d}.log"

    def _segment_numbers(self) -> list[int]:
        numbers = []
        for path in self.root.iterdir():
            match = SEGMENT_RE.match(path.name)
            if match:
                numbers.append(int(match.group(1)))
        return sorted(numbers)

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "status": self.status,
                "checkpoint_lsn": self.checkpoint_lsn,
                "last_lsn": self.last_lsn,
                "group_count": len(self.groups),
                "kv": dict(sorted(self.kv.items())),
                "groups": [
                    {"group_id": gid, "lsn": meta["lsn"], "hash": meta["hash"][:16]}
                    for gid, meta in sorted(self.groups.items(), key=lambda item: item[1]["lsn"])
                ],
                "segments": [
                    {
                        "segment": f"wal-{number:06d}.log",
                        "records": info["records"],
                        "max_lsn": info["max_lsn"],
                    }
                    for number, info in sorted(self._segments.items())
                ],
            }

    # ----------------------------------------------------------------- append
    def _fsync_dir(self):
        try:
            fd = os.open(self.root, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _new_segment(self) -> int:
        number = self._next_seg
        while self._segment_path(number).exists():
            number += 1
        path = self._segment_path(number)
        with open(path, "wb") as handle:
            handle.flush()
            os.fsync(handle.fileno())
        self._fsync_dir()
        self._segments[number] = {"records": 0, "max_lsn": 0}
        self._active_seg = number
        self._next_seg = number + 1
        self._write_seal(number, self.last_lsn)
        return number

    def _write_seal(self, highest: int, last_lsn: int) -> None:
        """Record the highest allocated segment and the newest written LSN.

        Losing the newest log segment would otherwise look like a clean prefix.
        The seal distinguishes two cases on recovery: a missing tail segment
        that only ever held nothing acknowledged (safe) versus one that held
        acknowledged records (a real gap).
        """
        tmp = self.root / (SEAL_FILE + ".tmp")
        with open(tmp, "w", encoding="utf-8") as handle:
            handle.write(json.dumps({"highest": highest, "last_lsn": last_lsn}))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, self.root / SEAL_FILE)
        self._fsync_dir()

    def _read_seal(self):
        path = self.root / SEAL_FILE
        if not path.exists():
            return None
        try:
            body = json.loads(path.read_text(encoding="utf-8"))
            highest = int(body["highest"])
            last_lsn = int(body["last_lsn"])
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError):
            raise self._failure(
                "SEAL_INVALID",
                "日志段封口文件损坏，无法确认日志尾部是否完整，恢复停止并保留现场",
            )
        if highest < 1 or last_lsn < 0:
            raise self._failure(
                "SEAL_INVALID",
                "日志段封口文件非法，恢复停止并保留现场",
            )
        return highest, last_lsn

    def _ensure_active(self):
        if self._active_seg is None:
            self._new_segment()
            return
        info = self._segments[self._active_seg]
        if info["records"] >= self.segment_limit:
            self._new_segment()

    def _append_bytes(self, data: bytes, lsn: int | None, fsync: bool):
        """Append one record. ``lsn=None`` means a deliberate torn write."""
        with self._lock:
            if self.status != "writable":
                raise NotWritable("当前演练不在可写状态，请先执行恢复")
            self._ensure_active()
            path = self._segment_path(self._active_seg)
            with open(path, "ab") as handle:
                handle.write(data)
                handle.flush()
                if fsync:
                    os.fsync(handle.fileno())
            if lsn is None:
                # Remember the segment holding an unacknowledged half record so
                # checkpoint reclamation never rotates or deletes it.
                self._torn_segment = self._active_seg
            else:
                info = self._segments[self._active_seg]
                if info["records"] == 0:
                    # First acknowledged record of this segment: raise the seal
                    # so deleting the segment later is detectable.
                    self._write_seal(self._active_seg, lsn)
                info["records"] += 1
                info["max_lsn"] = max(info["max_lsn"], lsn)

    # ------------------------------------------------------------------ write
    def write_group(self, group_id, operations, crash: str = "none") -> dict:
        """Atomically append one multi-key group.

        ``crash`` is a drill injection:
        * ``before_fsync``: only half of the framed record reaches the file and
          the write is never acknowledged; recovery must discard it.
        * ``after_fsync``: the record is durable, then memory is abandoned to
          force a recovery from disk.
        """
        self._ensure_writable()
        if not isinstance(group_id, str) or not 1 <= len(group_id) <= MAX_GROUP_ID_LENGTH:
            raise ValueError(f"组号必须是1到{MAX_GROUP_ID_LENGTH}个字符")
        operations = normalize_operations(operations)
        if crash not in ("none", "before_fsync", "after_fsync"):
            raise ValueError("未知的掉电时机")
        with self._lock:
            digest = operation_digest(operations)
            existing = self.groups.get(group_id)
            if existing:
                if existing["hash"] == digest:
                    return {
                        "outcome": "duplicate",
                        "group_id": group_id,
                        "lsn": existing["lsn"],
                        "applied": False,
                        "durable": True,
                        "message": "同组号与相同内容的重试，未重复写入",
                    }
                raise GroupConflict(group_id)
            lsn = self.last_lsn + 1
            record = encode_record(
                {"group_id": group_id, "lsn": lsn, "hash": digest, "ops": operations}
            )
            if crash == "before_fsync":
                # A torn write: magic+length survive, the payload/CRC does not.
                self._append_bytes(record[: max(1, len(record) // 2)], None, fsync=False)
                self.status = "crashed"
                return {
                    "outcome": "lost_before_fsync",
                    "group_id": group_id,
                    "lsn": lsn,
                    "applied": False,
                    "durable": False,
                    "message": "掉电发生在 fsync 前，整组未生效",
                }
            self._append_bytes(record, lsn, fsync=True)
            apply_operations(self.kv, operations)
            self.groups[group_id] = {"hash": digest, "lsn": lsn}
            self.last_lsn = lsn
            if crash == "after_fsync":
                self.status = "crashed"
                return {
                    "outcome": "durable_after_fsync",
                    "group_id": group_id,
                    "lsn": lsn,
                    "applied": True,
                    "durable": True,
                    "message": "掉电发生在 fsync 后，整组已落盘，等待恢复确认",
                }
            return {
                "outcome": "committed",
                "group_id": group_id,
                "lsn": lsn,
                "applied": True,
                "durable": True,
                "message": "整组已 fsync",
            }

    # ------------------------------------------------------------- checkpoint
    def checkpoint(self, interrupt: bool = False, before_publish=None) -> dict:
        """Publish a checkpoint, optionally interrupting publication.

        The snapshot is taken under the state lock, then the checkpoint file is
        written without holding it. Concurrent group writes therefore continue
        and their LSNs stay above the snapshot LSN; reclamation never removes
        them.
        """
        self._ensure_writable()
        with self._checkpoint_lock:
            with self._lock:
                snapshot_lsn = self.last_lsn
                snapshot_kv = deepcopy(self.kv)
                snapshot_groups = deepcopy(self.groups)
            body = {
                "magic": CHECKPOINT_MAGIC,
                "lsn": snapshot_lsn,
                "kv": snapshot_kv,
                "groups": snapshot_groups,
            }
            body["checksum"] = sha256(canonical(body).encode("utf-8")).hexdigest()
            encoded = json.dumps(body, ensure_ascii=False, sort_keys=True, indent=2).encode("utf-8")
            tmp = self.root / CHECKPOINT_TMP
            if tmp.exists():
                tmp.unlink()
            if interrupt:
                # Power loss while publishing: a partial temp file exists, the
                # canonical checkpoint is untouched, and nothing is reclaimed.
                with open(tmp, "wb") as handle:
                    handle.write(encoded[: max(1, len(encoded) // 2)])
                    handle.flush()
                    os.fsync(handle.fileno())
                self.status = "crashed"
                return {
                    "outcome": "checkpoint_interrupted",
                    "lsn": snapshot_lsn,
                    "canonical_checkpoint_lsn": self.checkpoint_lsn,
                    "temp_file": CHECKPOINT_TMP,
                    "reclaimed_segments": [],
                    "message": "检查点发布中断，旧检查点与全部日志保留",
                }
            with open(tmp, "wb") as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            if before_publish is not None:
                before_publish(self)
            with self._lock:
                if self.status != "writable":
                    # A crash injection won the race: never publish a snapshot
                    # from a run that is no longer accepting writes.
                    tmp.unlink(missing_ok=True)
                    raise NotWritable("检查点发布前演练已掉电，旧检查点保持不变")
                os.replace(tmp, self.root / CHECKPOINT_FILE)
                self.checkpoint_lsn = snapshot_lsn
                reclaimed = self._reclaim(snapshot_lsn)
                active = self._active_seg
            self._fsync_dir()
            return {
                "outcome": "published",
                "lsn": snapshot_lsn,
                "canonical_checkpoint_lsn": snapshot_lsn,
                "temp_file": None,
                "reclaimed_segments": reclaimed,
                "active_segment": active,
                "message": "检查点已原子发布",
            }

    def _reclaim(self, checkpoint_lsn: int) -> list[int]:
        """Delete only closed segments whose every record is checkpointed."""
        reclaimed = []
        if self._active_seg is not None:
            info = self._segments[self._active_seg]
            if (
                info["records"]
                and info["max_lsn"] <= checkpoint_lsn
                and self._active_seg != self._torn_segment
            ):
                # Rotate so the fully-checkpointed active segment becomes a
                # closed segment; the new empty segment is the only survivor.
                self._new_segment()
        for number in sorted(self._segments):
            if number == self._active_seg:
                continue
            info = self._segments[number]
            if number == self._torn_segment:
                continue
            if info["max_lsn"] <= checkpoint_lsn:
                self._segment_path(number).unlink(missing_ok=True)
                del self._segments[number]
                reclaimed.append(number)
        if reclaimed:
            self._fsync_dir()
        return reclaimed

    # ------------------------------------------------------------------- crash
    def simulate_crash(self) -> dict:
        """Abandon the in-memory view. Normal writes were already fsynced."""
        with self._lock:
            self._ensure_writable()
            lsn = self.last_lsn
            self.status = "crashed"
            return {"lsn": lsn, "message": "已模拟掉电，内存状态失效，等待从磁盘恢复"}

    # ---------------------------------------------------------------- recovery
    def recover(self) -> dict:
        """Replay a complete checkpoint plus contiguous valid logs."""
        with self._lock:
            self._reset_memory()
            try:
                report = self._load()
            except RecoveryError as exc:
                self.status = "stopped"
                self.last_report = exc.report
                raise
            self.status = "writable"
            self.last_report = report
            return report

    def ensure_loaded(self) -> dict:
        """Load a writable drill after a backend process restart.

        A store that was deliberately crashed or stopped stays that way: only
        an explicit ``recover()`` puts it back into the writable state.
        """
        with self._lock:
            if self.status in ("writable", "crashed", "stopped"):
                return self.last_report or {}
            self._reset_memory()
            try:
                report = self._load()
            except RecoveryError as exc:
                self.status = "stopped"
                self.last_report = exc.report
                raise
            self.status = "writable"
            self.last_report = report
            return report

    @staticmethod
    def _failure(code: str, message: str, **extra) -> RecoveryError:
        report = {
            "status": "stopped",
            "error_code": code,
            "error": message,
            "preserved": True,
            "kv": None,
            "partial_kv": None,
            "applied_groups": [],
            "duplicates_skipped": [],
            "rejections": [],
            "discarded_tail": None,
            "ignored_incomplete_checkpoint": False,
            "checkpoint_lsn": 0,
            "last_lsn": 0,
            "group_count": 0,
        }
        report.update(extra)
        return RecoveryError(report)

    def _read_checkpoint(self):
        path = self.root / CHECKPOINT_FILE
        if not path.exists():
            return 0, {}, {}
        try:
            body = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(body, dict) or body.get("magic") != CHECKPOINT_MAGIC:
                raise ValueError("检查点标记错误")
            checksum = body.pop("checksum")
            if checksum != sha256(canonical(body).encode("utf-8")).hexdigest():
                raise ValueError("检查点校验和不匹配")
            lsn = body["lsn"]
            kv = body["kv"]
            groups = body["groups"]
            if not isinstance(lsn, int) or lsn < 0:
                raise ValueError("检查点 LSN 非法")
            if not isinstance(kv, dict) or not all(
                isinstance(k, str) and isinstance(v, str) for k, v in kv.items()
            ):
                raise ValueError("检查点键值非法")
            if not isinstance(groups, dict):
                raise ValueError("检查点组索引非法")
            for group_id, meta in groups.items():
                if (
                    not isinstance(group_id, str)
                    or not isinstance(meta, dict)
                    or not isinstance(meta.get("hash"), str)
                    or not isinstance(meta.get("lsn"), int)
                ):
                    raise ValueError("检查点组索引条目非法")
        except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
            raise self._failure(
                "CHECKPOINT_INVALID",
                f"检查点不完整或损坏，拒绝恢复并保留现场：{exc}",
                checkpoint_file=CHECKPOINT_FILE,
            )
        return lsn, kv, groups

    def _read_segment(self, number: int, is_last: bool):
        """Return (records, torn_offset).

        An incomplete final record is a torn tail only in the last segment.
        Bad magic, bad CRC or a short record in a closed segment is corruption.
        """
        path = self._segment_path(number)
        data = path.read_bytes()
        records = []
        position = 0
        while position < len(data):
            if len(data) - position < HEADER.size:
                if is_last:
                    return records, position
                raise self._failure(
                    "LOG_CORRUPT",
                    f"{path.name} 在偏移 {position} 处截断，且不是最后一段，恢复停止并保留现场",
                    segment=path.name,
                    offset=position,
                )
            magic, length, crc = HEADER.unpack_from(data, position)
            if magic != MAGIC:
                raise self._failure(
                    "LOG_CORRUPT",
                    f"{path.name} 在偏移 {position} 处记录标记损坏，恢复停止并保留现场",
                    segment=path.name,
                    offset=position,
                )
            start = position + HEADER.size
            if len(data) - start < length:
                if is_last:
                    return records, position
                raise self._failure(
                    "LOG_CORRUPT",
                    f"{path.name} 在偏移 {position} 处记录不完整，且不是最后一段，恢复停止并保留现场",
                    segment=path.name,
                    offset=position,
                )
            body = data[start : start + length]
            if zlib.crc32(body) & 0xFFFFFFFF != crc:
                raise self._failure(
                    "LOG_CORRUPT",
                    f"{path.name} 在偏移 {position} 处 CRC 校验失败，恢复停止并保留现场",
                    segment=path.name,
                    offset=position,
                )
            try:
                payload = json.loads(body.decode("utf-8"))
                group_id = payload["group_id"]
                lsn = payload["lsn"]
                digest = payload["hash"]
                operations = normalize_operations(payload["ops"])
            except (ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
                raise self._failure(
                    "LOG_CORRUPT",
                    f"{path.name} 在偏移 {position} 处记录内容非法，恢复停止并保留现场：{exc}",
                    segment=path.name,
                    offset=position,
                )
            if not isinstance(group_id, str) or not isinstance(lsn, int) or digest != operation_digest(operations):
                raise self._failure(
                    "LOG_CORRUPT",
                    f"{path.name} 在偏移 {position} 处组号/序号/摘要非法，恢复停止并保留现场",
                    segment=path.name,
                    offset=position,
                )
            records.append(
                {"group_id": group_id, "lsn": lsn, "hash": digest, "ops": operations}
            )
            position = start + length
        return records, None

    def peek(self) -> dict:
        """Read files without repairing them; used by read-only views.

        A torn trailing record is reported but not truncated, so simply opening
        a page never changes the preserved scene. The state lock is held so this
        never observes a half-appended record from a concurrent write.
        """
        if not self.root.exists():
            return {
                "status": "missing",
                "error_code": "DIRECTORY_MISSING",
                "error": "演练目录不存在",
                "preserved": True,
                "kv": None,
                "checkpoint_lsn": 0,
                "last_lsn": 0,
                "group_count": 0,
            }
        scratch = DurableStore(self.root, self.segment_limit)
        with self._lock:
            try:
                return scratch._load(repair_tail=False)
            except RecoveryError as exc:
                return dict(exc.report)

    def _load(self, repair_tail: bool = True) -> dict:
        checkpoint_lsn, kv, groups = self._read_checkpoint()
        self.kv = dict(kv)
        self.groups = deepcopy(groups)
        self.checkpoint_lsn = checkpoint_lsn
        self.last_lsn = checkpoint_lsn

        numbers = self._segment_numbers()
        temp = self.root / CHECKPOINT_TMP
        ignored_incomplete = temp.exists()
        seal = self._read_seal()
        if numbers:
            for left, right in zip(numbers, numbers[1:]):
                if right != left + 1:
                    raise self._failure(
                        "SEGMENT_GAP",
                        f"日志段不连续：{left:06d} 之后是 {right:06d}，恢复停止并保留现场",
                        checkpoint_lsn=checkpoint_lsn,
                        last_lsn=checkpoint_lsn,
                    )
            self._next_seg = numbers[-1] + 1
        else:
            self._next_seg = 1

        pending: list[dict] = []
        torn: dict | None = None
        for index, number in enumerate(numbers):
            records, torn_offset = self._read_segment(number, is_last=index == len(numbers) - 1)
            info = self._segments.setdefault(number, {"records": 0, "max_lsn": 0})
            # Records already covered by the checkpoint do not count toward
            # this segment's records: reclamation works on live-log coverage.
            live = [r for r in records if r["lsn"] > checkpoint_lsn]
            info["records"] = len(live)
            info["max_lsn"] = max((record["lsn"] for record in live), default=0)
            pending.extend(records)
            if torn_offset is not None:
                path = self._segment_path(number)
                torn = {
                    "segment": path.name,
                    "offset": torn_offset,
                    "bytes": path.stat().st_size - torn_offset,
                }
        if numbers:
            self._active_seg = numbers[-1]

        applied = []
        duplicates = []
        rejections = []
        expected = checkpoint_lsn + 1
        for record in pending:
            lsn = record["lsn"]
            if lsn <= checkpoint_lsn:
                # A retained log prefix already represented by the checkpoint.
                continue
            if lsn != expected:
                raise self._failure(
                    "LOG_GAP",
                    f"日志序号不连续：期望 {expected}，实际 {lsn}，恢复停止并保留现场",
                    checkpoint_lsn=checkpoint_lsn,
                    last_lsn=self.last_lsn,
                    partial_kv=dict(sorted(self.kv.items())),
                )
            expected += 1
            self.last_lsn = lsn
            group_id = record["group_id"]
            existing = self.groups.get(group_id)
            if existing:
                if existing["hash"] == record["hash"]:
                    duplicates.append({"group_id": group_id, "lsn": lsn})
                else:
                    rejections.append(
                        {
                            "group_id": group_id,
                            "lsn": lsn,
                            "reason": "IDEMPOTENCY_CONFLICT",
                            "message": "同组号内容不同，重放时整组拒绝",
                        }
                    )
                continue
            apply_operations(self.kv, record["ops"])
            self.groups[group_id] = {"hash": record["hash"], "lsn": lsn}
            applied.append(
                {"group_id": group_id, "lsn": lsn, "operations": len(record["ops"])}
            )

        if seal is not None:
            highest, sealed_lsn = seal
            tail_missing = not numbers or numbers[-1] < highest
            if tail_missing and self.last_lsn < sealed_lsn:
                existing = f"{numbers[-1]:06d}" if numbers else "无"
                raise self._failure(
                    "SEGMENT_GAP",
                    f"日志段尾部缺失：封口记录最高段 {highest:06d}（最后写入 LSN {sealed_lsn}），"
                    f"现存最高 {existing}、可恢复到 LSN {self.last_lsn}，恢复停止并保留现场",
                    checkpoint_lsn=checkpoint_lsn,
                    last_lsn=self.last_lsn,
                )

        report = {
            "status": "recovered",
            "error_code": None,
            "error": None,
            "preserved": False,
            "checkpoint_lsn": checkpoint_lsn,
            "last_lsn": self.last_lsn,
            "kv": dict(sorted(self.kv.items())),
            "partial_kv": None,
            "applied_groups": applied,
            "duplicates_skipped": duplicates,
            "rejections": rejections,
            "discarded_tail": torn,
            "ignored_incomplete_checkpoint": ignored_incomplete,
            "segments": [self._segment_path(number).name for number in numbers],
            "group_count": len(self.groups),
        }
        if torn is not None and repair_tail:
            # Only after every other segment is known to be valid do we remove
            # the explicitly-discardable half record at the tail.
            path = self._segment_path(numbers[-1])
            with open(path, "r+b") as handle:
                handle.truncate(torn["offset"])
                handle.flush()
                os.fsync(handle.fileno())
            report["discarded_tail"] = {**torn, "action": "已丢弃尾部半条记录"}
        elif torn is not None:
            report["discarded_tail"] = {**torn, "action": "待恢复时丢弃"}
        if ignored_incomplete and repair_tail:
            # The canonical checkpoint is valid and the whole log was already
            # accepted, so a temp file from an interrupted publication is inert.
            temp.unlink(missing_ok=True)
            self._fsync_dir()
        return report
