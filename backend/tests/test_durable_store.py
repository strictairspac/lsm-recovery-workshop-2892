"""Durability drill tests: no database or Redis required for the file model."""
import json
import tempfile
import threading
import unittest
from pathlib import Path

from src.services.durable_store import (
    CHECKPOINT_FILE,
    CHECKPOINT_TMP,
    DurableStore,
    GroupConflict,
    NotWritable,
    RecoveryError,
)

G1 = [
    {"op": "put", "key": "station-01", "value": "18.2"},
    {"op": "put", "key": "station-02", "value": "19.1"},
]
G2 = [
    {"op": "put", "key": "station-03", "value": "22.4"},
    {"op": "delete", "key": "station-01"},
    {"op": "put", "key": "station-04", "value": "17.0"},
]
G3 = [
    {"op": "put", "key": "station-02", "value": "20.7"},
    {"op": "put", "key": "station-05", "value": "21.6"},
]


def fresh(limit=2):
    root = Path(tempfile.mkdtemp(prefix="drill-"))
    store = DurableStore(root, segment_limit=limit)
    store.recover()  # empty directory -> writable, empty kv
    return store


class StoreTests(unittest.TestCase):
    def test_group_is_atomic_multi_key_with_overwrite_and_delete(self):
        store = fresh()
        store.write_group("g1", G1)
        store.write_group("g2", G2)
        snap = store.snapshot()
        self.assertEqual(snap["kv"], {"station-02": "19.1", "station-03": "22.4", "station-04": "17.0"})
        self.assertEqual(snap["last_lsn"], 2)

    def test_successful_groups_survive_restart(self):
        store = fresh()
        store.write_group("g1", G1)
        store.write_group("g2", G2)
        restarted = DurableStore(store.root, segment_limit=2)
        report = restarted.recover()
        self.assertEqual(report["status"], "recovered")
        self.assertEqual(report["kv"], {"station-02": "19.1", "station-03": "22.4", "station-04": "17.0"})
        self.assertEqual([row["group_id"] for row in report["applied_groups"]], ["g1", "g2"])

    def test_same_group_same_content_is_not_written_twice(self):
        store = fresh()
        first = store.write_group("g1", G1)
        second = store.write_group("g1", G1)
        self.assertEqual(first["outcome"], "committed")
        self.assertEqual(second["outcome"], "duplicate")
        self.assertFalse(second["applied"])
        self.assertEqual(store.snapshot()["last_lsn"], first["lsn"])
        self.assertEqual(store.snapshot()["group_count"], 1)

    def test_same_group_different_content_is_rejected_whole(self):
        store = fresh()
        store.write_group("g1", G1)
        with self.assertRaises(GroupConflict):
            store.write_group("g1", G2)
        snap = store.snapshot()
        self.assertEqual(snap["kv"], {"station-01": "18.2", "station-02": "19.1"})
        self.assertEqual(snap["last_lsn"], 1)

    def test_crash_before_fsync_loses_whole_group(self):
        store = fresh()
        store.write_group("g1", G1)
        store.write_group("g2", G2, crash="before_fsync")
        self.assertEqual(store.status, "crashed")
        restarted = DurableStore(store.root, segment_limit=2)
        report = restarted.recover()
        self.assertEqual(report["kv"], {"station-01": "18.2", "station-02": "19.1"})
        self.assertEqual([r["group_id"] for r in report["applied_groups"]], ["g1"])
        self.assertIsNotNone(report["discarded_tail"])
        self.assertEqual(report["discarded_tail"]["action"], "已丢弃尾部半条记录")
        self.assertEqual(report["last_lsn"], 1)

    def test_crash_after_fsync_keeps_whole_group(self):
        store = fresh()
        store.write_group("g1", G1)
        store.write_group("g2", G2, crash="after_fsync")
        self.assertEqual(store.status, "crashed")
        restarted = DurableStore(store.root, segment_limit=2)
        report = restarted.recover()
        self.assertEqual(report["kv"], {"station-02": "19.1", "station-03": "22.4", "station-04": "17.0"})
        self.assertEqual([r["group_id"] for r in report["applied_groups"]], ["g1", "g2"])

    def test_crash_without_injection_keeps_all_fsynced_groups(self):
        store = fresh()
        store.write_group("g1", G1)
        store.write_group("g2", G2)
        store.simulate_crash()
        with self.assertRaises(NotWritable):
            store.write_group("g3", G3)
        report = DurableStore(store.root, segment_limit=2).recover()
        self.assertEqual(sorted(report["kv"]), ["station-02", "station-03", "station-04"])

    def test_checkpoint_then_log_reclaim(self):
        store = fresh(limit=1)
        for gid, ops in (("g1", G1), ("g2", G2), ("g3", G3)):
            store.write_group(gid, ops)
        self.assertGreaterEqual(len(store._segment_numbers()), 3)
        result = store.checkpoint()
        self.assertEqual(result["outcome"], "published")
        self.assertEqual(result["reclaimed_segments"], [1, 2, 3])
        self.assertEqual(store._segment_numbers(), [4])
        report = DurableStore(store.root, segment_limit=1).recover()
        self.assertEqual(report["checkpoint_lsn"], 3)
        self.assertEqual(report["kv"], store.snapshot()["kv"])
        self.assertEqual(report["applied_groups"], [])

    def test_checkpoint_ignores_incomplete_temp_file(self):
        store = fresh()
        store.write_group("g1", G1)
        result = store.checkpoint(interrupt=True)
        self.assertEqual(result["outcome"], "checkpoint_interrupted")
        self.assertTrue((store.root / CHECKPOINT_TMP).exists())
        restarted = DurableStore(store.root, segment_limit=2)
        report = restarted.recover()
        self.assertEqual(report["status"], "recovered")
        self.assertTrue(report["ignored_incomplete_checkpoint"])
        self.assertEqual(report["kv"], {"station-01": "18.2", "station-02": "19.1"})
        self.assertEqual([r["group_id"] for r in report["applied_groups"]], ["g1"])
        self.assertFalse((store.root / CHECKPOINT_TMP).exists())

    def test_concurrent_write_during_checkpoint_is_kept(self):
        store = fresh(limit=1)
        store.write_group("g1", G1)
        injected = {}

        def concurrent(inner):
            # Runs after the snapshot is durable in tmp, before publication.
            injected["result"] = inner.write_group("g-concurrent", G2)

        result = store.checkpoint(before_publish=concurrent)
        self.assertEqual(result["outcome"], "published")
        self.assertEqual(result["lsn"], 1)
        self.assertEqual(injected["result"]["lsn"], 2)
        # The checkpointed segment 1 is reclaimed, the concurrent write survives.
        self.assertNotIn(1, store._segment_numbers())
        self.assertIn(2, store._segment_numbers())
        report = DurableStore(store.root, segment_limit=1).recover()
        self.assertEqual(report["checkpoint_lsn"], 1)
        self.assertEqual(report["kv"], {"station-02": "19.1", "station-03": "22.4", "station-04": "17.0"})
        self.assertEqual([r["group_id"] for r in report["applied_groups"]], ["g-concurrent"])

    def test_checkpoint_races_with_parallel_writers(self):
        store = fresh(limit=2)
        failures = []
        stop = threading.Event()

        def writer(index):
            try:
                round_no = 0
                while not stop.is_set():
                    store.write_group(
                        f"race-{index}-{round_no}",
                        [{"op": "put", "key": f"race-{index}-{round_no}", "value": str(round_no)}],
                    )
                    round_no += 1
            except Exception as exc:  # pragma: no cover
                failures.append(exc)

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(4)]
        for thread in threads:
            thread.start()
        for _ in range(8):
            store.checkpoint()
        stop.set()
        for thread in threads:
            thread.join()
        self.assertEqual(failures, [])
        acked = dict(store.snapshot()["kv"])
        report = DurableStore(store.root, segment_limit=2).recover()
        self.assertEqual(report["kv"], acked)
        self.assertEqual(report["status"], "recovered")
        self.assertGreater(report["checkpoint_lsn"], 0)

    def test_parallel_writers_do_not_lose_acknowledged_groups(self):
        store = fresh(limit=2)
        failures = []

        def writer(index):
            try:
                for round_no in range(5):
                    store.write_group(
                        f"g-{index}-{round_no}",
                        [{"op": "put", "key": f"k-{index}-{round_no}", "value": str(round_no)}],
                    )
            except Exception as exc:  # pragma: no cover - surfaced through assertion
                failures.append(exc)

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(failures, [])
        report = DurableStore(store.root, segment_limit=2).recover()
        self.assertEqual(report["last_lsn"], 30)
        self.assertEqual(len(report["applied_groups"]), 30)
        self.assertEqual(store.snapshot()["kv"], report["kv"])

    def test_corrupt_middle_log_stops_and_preserves_files(self):
        store = fresh(limit=1)
        store.write_group("g1", G1)
        store.write_group("g2", G2)
        store.write_group("g3", G3)
        paths = [store.root / f"wal-{n:06d}.log" for n in store._segment_numbers()]
        # Damage the CRC of the record in the first (closed) segment.
        data = bytearray(paths[0].read_bytes())
        data[12] ^= 0xFF
        paths[0].write_bytes(bytes(data))
        before = {path.name: path.read_bytes() for path in paths}
        restarted = DurableStore(store.root, segment_limit=1)
        with self.assertRaises(RecoveryError) as caught:
            restarted.recover()
        report = caught.exception.report
        self.assertEqual(report["status"], "stopped")
        self.assertEqual(report["error_code"], "LOG_CORRUPT")
        self.assertTrue(report["preserved"])
        self.assertIsNone(report["kv"])
        self.assertEqual({path.name: path.read_bytes() for path in paths}, before)
        self.assertEqual(restarted.status, "stopped")

    def test_missing_segment_mid_sequence_stops_recovery(self):
        store = fresh(limit=1)
        store.write_group("g1", G1)
        store.write_group("g2", G2)
        store.write_group("g3", G3)
        middle = store.root / "wal-000002.log"
        middle.unlink()
        with self.assertRaises(RecoveryError) as caught:
            DurableStore(store.root, segment_limit=1).recover()
        self.assertEqual(caught.exception.report["error_code"], "SEGMENT_GAP")
        self.assertFalse(middle.exists())

    def test_missing_last_segment_is_detected_by_seal(self):
        store = fresh(limit=2)
        store.write_group("g1", G1)
        store.write_group("g2", G2)
        store.write_group("g3", G3)
        self.assertEqual(store._segment_numbers(), [1, 2])
        (store.root / "wal-000002.log").unlink()
        with self.assertRaises(RecoveryError) as caught:
            DurableStore(store.root, segment_limit=2).recover()
        self.assertEqual(caught.exception.report["error_code"], "SEGMENT_GAP")
        self.assertIn("尾部缺失", caught.exception.report["error"])
        self.assertTrue((store.root / "wal-000001.log").exists())

    def test_losing_empty_tail_segment_is_safe(self):
        store = fresh(limit=2)
        store.write_group("g1", G1)
        store.checkpoint()
        # A new empty segment is active after reclamation. It holds nothing
        # acknowledged, so losing it is not a gap.
        (store.root / "wal-000002.log").unlink()
        report = DurableStore(store.root, segment_limit=2).recover()
        self.assertEqual(report["status"], "recovered")
        self.assertEqual(report["kv"], {"station-01": "18.2", "station-02": "19.1"})

    def test_losing_tail_segment_with_acknowledged_writes_stops(self):
        store = fresh(limit=1)
        store.write_group("g1", G1)
        store.checkpoint()
        store.write_group("g2", G2)  # segment 2 holds acknowledged data
        self.assertIn(2, store._segment_numbers())
        (store.root / "wal-000002.log").unlink()
        with self.assertRaises(RecoveryError) as caught:
            DurableStore(store.root, segment_limit=1).recover()
        self.assertEqual(caught.exception.report["error_code"], "SEGMENT_GAP")
        self.assertIn("尾部缺失", caught.exception.report["error"])

    def test_corrupt_checkpoint_is_rejected(self):
        store = fresh(limit=1)
        store.write_group("g1", G1)
        store.checkpoint()
        checkpoint = store.root / CHECKPOINT_FILE
        body = json.loads(checkpoint.read_text())
        body["kv"]["station-01"] = "tampered"
        checkpoint.write_text(json.dumps(body))
        with self.assertRaises(RecoveryError) as caught:
            DurableStore(store.root, segment_limit=1).recover()
        self.assertEqual(caught.exception.report["error_code"], "CHECKPOINT_INVALID")
        self.assertIsNone(caught.exception.report["kv"])

    def test_discarded_tail_allows_future_writes(self):
        store = fresh(limit=2)
        store.write_group("g1", G1)
        store.write_group("g2", G2, crash="before_fsync")
        restarted = DurableStore(store.root, segment_limit=2)
        restarted.recover()
        restarted.write_group("g3", G3)
        final = DurableStore(store.root, segment_limit=2)
        report = final.recover()
        self.assertEqual(report["last_lsn"], 2)
        self.assertEqual(
            report["kv"],
            {"station-01": "18.2", "station-02": "20.7", "station-05": "21.6"},
        )

    def test_checkpoint_plus_later_write_survives_crash(self):
        store = fresh(limit=1)
        store.write_group("g1", G1)
        store.checkpoint()
        store.write_group("g2", G2)  # not checkpointed, must be replayed
        store.simulate_crash()
        report = DurableStore(store.root, segment_limit=1).recover()
        self.assertEqual(report["checkpoint_lsn"], 1)
        self.assertEqual(report["last_lsn"], 2)
        self.assertEqual(report["kv"], {"station-02": "19.1", "station-03": "22.4", "station-04": "17.0"})
        self.assertEqual([r["group_id"] for r in report["applied_groups"]], ["g2"])

    def test_idempotency_index_survives_restart(self):
        store = fresh(limit=2)
        store.write_group("g1", G1)
        store.simulate_crash()
        restarted = DurableStore(store.root, segment_limit=2)
        restarted.recover()
        duplicate = restarted.write_group("g1", G1)
        self.assertEqual(duplicate["outcome"], "duplicate")
        self.assertEqual(duplicate["lsn"], 1)
        with self.assertRaises(GroupConflict):
            restarted.write_group("g1", G2)
        final = DurableStore(store.root, segment_limit=2)
        final.recover().get("kv")
        self.assertEqual(final.snapshot()["last_lsn"], 1)

    def test_retry_after_discarded_tail_does_not_duplicate(self):
        store = fresh(limit=2)
        store.write_group("g1", G1)
        store.write_group("g2", G2, crash="before_fsync")
        restarted = DurableStore(store.root, segment_limit=2)
        restarted.recover()
        first = restarted.write_group("g2", G2)
        second = restarted.write_group("g2", G2)
        self.assertEqual(first["outcome"], "committed")
        self.assertEqual(second["outcome"], "duplicate")
        self.assertEqual(second["lsn"], first["lsn"])

    def test_invalid_group_is_rejected_before_any_write(self):
        store = fresh()
        with self.assertRaises(ValueError):
            store.write_group("bad", [{"op": "put", "key": "k"}])
        with self.assertRaises(ValueError):
            store.write_group("bad", [{"op": "nope", "key": "k"}])
        snap = store.snapshot()
        self.assertEqual(snap["last_lsn"], 0)
        self.assertEqual(snap["kv"], {})


if __name__ == "__main__":
    unittest.main()
