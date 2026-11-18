"""Durability drill orchestration: files under /artifacts/drills/<id>, state in MySQL.

The files are the source of truth for recovery. MySQL only stores the operator
view (status, counters, latest report) so the page can list drills.

One backend process serves the drill API, so one DurableStore instance per
drill is cached and shared: its internal locks make concurrent group writes
serialize at the record level, while checkpoint publication only holds the
state lock for the snapshot and for reclamation, not while writing the file.
"""
from __future__ import annotations

import os
import threading
from pathlib import Path

from fastapi import HTTPException
from sqlalchemy import select

from ..models.dataset import Dataset
from ..models.drill import Drill
from .durable_store import DurableStore, GroupConflict, NotWritable, RecoveryError

DRILL_ROOT = Path(os.getenv('DRILL_ROOT', '/artifacts/drills'))
_registry_lock = threading.Lock()
_stores: dict[int, DurableStore] = {}


def _store_for(ident: int, segment_limit: int = 8) -> DurableStore:
    with _registry_lock:
        store = _stores.get(ident)
        if store is None:
            store = DurableStore(DRILL_ROOT / str(ident), segment_limit=segment_limit)
            _stores[ident] = store
        return store


def _root(ident: int) -> Path:
    return DRILL_ROOT / str(ident)


def _pending_report() -> dict:
    return {
        "status": "pending_recovery",
        "error_code": None,
        "error": None,
        "preserved": False,
        "checkpoint_lsn": 0,
        "last_lsn": 0,
        "kv": None,
        "partial_kv": None,
        "applied_groups": [],
        "duplicates_skipped": [],
        "rejections": [],
        "discarded_tail": None,
        "ignored_incomplete_checkpoint": False,
        "segments": [],
        "group_count": 0,
        "message": "演练目录已创建，尚未执行恢复；写入在恢复完成前不可用。",
    }


def serialize(x: Drill) -> dict:
    return {
        "id": x.id,
        "name": x.name,
        "dataset_id": x.dataset_id,
        "status": x.status,
        "segment_limit": x.segment_limit,
        "checkpoint_lsn": x.checkpoint_lsn,
        "last_lsn": x.last_lsn,
        "group_count": x.group_count,
        "created_at": str(x.created_at),
        "updated_at": str(x.updated_at),
        "last_report": x.last_report,
        "last_action": x.last_action,
    }


def store_peek(ident: int, segment_limit: int = 8) -> dict:
    """Read-only disk view; never truncates or republishes anything."""
    return _store_for(ident, segment_limit).peek()


def listing(db) -> list[dict]:
    return [serialize(x) for x in db.scalars(select(Drill).order_by(Drill.id.desc()))]


def _get(db, ident: int) -> Drill:
    x = db.get(Drill, ident)
    if not x:
        raise HTTPException(404, '持久化演练不存在')
    return x


DRILL_SCRIPT: list[dict] = [
    {
        "group": 'g1',
        "ops": [
            {'op': 'put', 'key': 'station-01', 'value': '18.2'},
            {'op': 'put', 'key': 'station-02', 'value': '19.1'},
        ],
    },
    {
        "group": 'g2',
        "ops": [
            {'op': 'put', 'key': 'station-03', 'value': '22.4'},
            {'op': 'delete', 'key': 'station-01'},
            {'op': 'put', 'key': 'station-04', 'value': '17.0'},
        ],
    },
    {
        "group": 'g3',
        "ops": [
            {'op': 'put', 'key': 'station-02', 'value': '20.7'},
            {'op': 'put', 'key': 'station-05', 'value': '21.6'},
        ],
    },
    {
        "group": 'g4',
        "ops": [
            {'op': 'delete', 'key': 'station-05'},
            {'op': 'put', 'key': 'station-01', 'value': '19.0'},
        ],
    },
]


def script_for(x: Drill, db) -> list[dict]:
    """Use the linked dataset as a deterministic group script when possible.

    Every operation may carry an explicit ``group`` key. Operations with the
    same group are replayed as one atomic multi-key group in the given order,
    so the page can offer a one-click deterministic drill script.
    """
    dataset = db.get(Dataset, x.dataset_id)
    if dataset and dataset.entries and all(isinstance(e, dict) and e.get('group') for e in dataset.entries):
        grouped: dict[str, list[dict]] = {}
        for entry in dataset.entries:
            grouped.setdefault(str(entry['group']), []).append(entry)
        return [{"group": key, "ops": grouped[key]} for key in grouped]
    return DRILL_SCRIPT


def create(db, data) -> dict:
    dataset = db.get(Dataset, data.dataset_id)
    if not dataset:
        raise HTTPException(404, '写入批次不存在')
    if db.scalar(select(Drill).where(Drill.name == data.name)):
        raise HTTPException(409, '演练名称已存在')
    x = Drill(
        name=data.name,
        dataset_id=data.dataset_id,
        status='pending_recovery',
        segment_limit=data.segment_limit,
        last_report=_pending_report(),
        last_action={'action': 'create', 'outcome': 'pending_recovery', 'message': '演练目录已建立'},
    )
    db.add(x)
    db.commit()
    # Materialize the directory so the operator can inspect an empty scene.
    _store_for(x.id, x.segment_limit)
    return serialize(x)


def detail(db, ident: int) -> dict:
    x = _get(db, ident)
    payload = serialize(x)
    payload['script'] = script_for(x, db)
    payload['on_disk'] = _disk_listing(_root(ident))
    with _registry_lock:
        store = _stores.get(ident)
    payload['snapshot'] = store.snapshot() if store is not None else store_peek(ident, x.segment_limit)
    return payload


def _disk_listing(root: Path) -> list[dict]:
    rows = []
    for path in sorted(root.iterdir()) if root.exists() else []:
        if path.is_file():
            rows.append({'file': path.name, 'bytes': path.stat().st_size})
    return rows


def _gate(x: Drill) -> None:
    if x.status == 'pending_recovery':
        raise HTTPException(409, '演练尚未恢复，写入不可用；请先执行恢复')
    if x.status == 'stopped':
        raise HTTPException(409, '恢复已停止且保留现场，先修复磁盘文件再重新恢复')


def _load_store(db, x: Drill) -> DurableStore:
    """Return the shared store, loaded from its files when first used."""
    _gate(x)
    store = _store_for(x.id, x.segment_limit)
    try:
        store.ensure_loaded()
    except RecoveryError as exc:
        report = dict(exc.report)
        report['segments'] = _disk_listing(_root(x.id))
        _save_report(db, x, report)
        raise HTTPException(409, report)
    return store


def _status_for(store: DurableStore) -> str:
    if store.status == 'writable':
        return 'writable'
    if store.status == 'stopped':
        return 'stopped'
    return 'pending_recovery'


def _sync(db, x: Drill, store: DurableStore) -> dict:
    snap = store.snapshot()
    x.status = _status_for(store)
    x.checkpoint_lsn = snap['checkpoint_lsn']
    x.last_lsn = snap['last_lsn']
    x.group_count = len(store.groups)
    db.commit()
    return snap


def _record_action(x: Drill, action: str, outcome: str, message: str, **extra) -> None:
    x.last_action = {'action': action, 'outcome': outcome, 'message': message, **extra}


def _checkpoint_report(snap: dict, message: str) -> dict:
    return {
        **_pending_report(),
        'group_count': snap['group_count'],
        'checkpoint_lsn': snap['checkpoint_lsn'],
        'last_lsn': snap['last_lsn'],
        'segments': snap['segments'],
        'message': message,
    }


def write_group(db, ident: int, data) -> dict:
    x = _get(db, ident)
    store = _load_store(db, x)
    try:
        result = store.write_group(data.group_id, data.operations, data.crash)
    except GroupConflict as exc:
        _record_action(
            x, 'write_group', 'rejected', str(exc), group_id=data.group_id,
            reason='IDEMPOTENCY_CONFLICT',
        )
        db.commit()
        raise HTTPException(409, str(exc))
    except NotWritable as exc:
        x.status = _status_for(store)
        db.commit()
        raise HTTPException(409, str(exc))
    snap = _sync(db, x, store)
    _record_action(x, 'write_group', result['outcome'], result['message'], group_id=result['group_id'], lsn=result['lsn'])
    if result['outcome'] in ('lost_before_fsync', 'durable_after_fsync'):
        x.last_report = _checkpoint_report(snap, result['message'])
        x.status = 'pending_recovery'
        db.commit()
        # The process "lost power": do not present the abandoned memory as
        # readable state. Only a recovery may report keys again.
        return {**result, **snap, 'kv': None, 'groups': [], 'status': x.status}
    db.commit()
    return {**result, **snap, 'status': x.status}


def checkpoint(db, ident: int, interrupt: bool = False) -> dict:
    x = _get(db, ident)
    store = _load_store(db, x)
    try:
        result = store.checkpoint(interrupt=interrupt)
    except NotWritable as exc:
        raise HTTPException(409, str(exc))
    snap = _sync(db, x, store)
    _record_action(x, 'checkpoint', result['outcome'], result['message'], lsn=result['lsn'])
    if interrupt:
        x.last_report = _checkpoint_report(snap, result['message'])
        x.status = 'pending_recovery'
        db.commit()
        return {**result, **snap, 'kv': None, 'groups': [], 'status': x.status}
    db.commit()
    return {**result, **snap, 'status': x.status}


def crash(db, ident: int) -> dict:
    x = _get(db, ident)
    store = _load_store(db, x)
    try:
        result = store.simulate_crash()
    except NotWritable as exc:
        raise HTTPException(409, str(exc))
    snap = _sync(db, x, store)
    _record_action(x, 'crash', 'crashed', result['message'], lsn=result['lsn'])
    x.last_report = _checkpoint_report(snap, result['message'])
    x.status = 'pending_recovery'
    db.commit()
    return {**snap, 'kv': None, 'groups': [], 'status': x.status, 'lsn': result['lsn'], 'message': result['message']}


def _save_report(db, x: Drill, report: dict):
    x.checkpoint_lsn = report.get('checkpoint_lsn', 0)
    x.last_lsn = report.get('last_lsn', 0)
    x.group_count = report.get('group_count', len(report.get('kv') or {}))
    x.last_report = report
    x.status = 'writable' if report.get('status') == 'recovered' else 'stopped'
    db.commit()


def recover(db, ident: int, reason: str = 'manual') -> dict:
    x = _get(db, ident)
    store = _store_for(x.id, x.segment_limit)
    try:
        report = store.recover()
    except RecoveryError as exc:
        report = dict(exc.report)
        report['segments'] = _disk_listing(_root(ident))
        _save_report(db, x, report)
        _record_action(x, 'recover', 'stopped', report.get('error') or '恢复停止', error_code=report.get('error_code'))
        db.commit()
        raise HTTPException(409, report)
    report['segments'] = _disk_listing(_root(ident))
    _save_report(db, x, report)
    _record_action(
        x, 'recover', 'recovered',
        f"恢复完成：检查点 LSN {report['checkpoint_lsn']}，重放 {report['last_lsn']}",
        lsn=report['last_lsn'], reason=reason,
    )
    db.commit()
    # Report values stay authoritative; only the group index is added on top.
    return {**report, 'groups': store.snapshot()['groups'], 'status': x.status}


def status(db, ident: int) -> dict:
    """Read-only view; never loads or repairs files (no side effects on GET)."""
    x = _get(db, ident)
    payload = serialize(x)
    payload['on_disk'] = _disk_listing(_root(ident))
    view = store_peek(ident, x.segment_limit)
    if view.get('status') != 'recovered':
        # A stopped scene is reported as a reason only; never as readable keys.
        view = {**view, 'kv': None}
    payload['disk_view'] = view
    return payload


def export(db, ident: int) -> dict:
    x = _get(db, ident)
    return {'drill': serialize(x), 'script': script_for(x, db)}
