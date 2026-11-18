"""Service-level drill tests against SQLite metadata + temp file directory."""
import os
import tempfile
import unittest
from pathlib import Path

os.environ.setdefault('DATABASE_URL', 'sqlite://')
os.environ.setdefault('JWT_SECRET', 'test-secret')
os.environ.setdefault('REDIS_URL', 'redis://localhost:6379/0')

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config.database import Base
from src.models.dataset import Dataset
from src.models.drill import Drill
from src.schemas.drill import DrillCheckpointInput, DrillCreateInput, DrillGroupInput
from src.services import drill as drill_service
from src.services.durable_store import NotWritable

G1 = [
    {'op': 'put', 'key': 'station-01', 'value': '18.2'},
    {'op': 'put', 'key': 'station-02', 'value': '19.1'},
]
G2 = [
    {'op': 'put', 'key': 'station-03', 'value': '22.4'},
    {'op': 'delete', 'key': 'station-01'},
]


class DrillServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix='drill-service-'))
        self.previous_root = drill_service.DRILL_ROOT
        drill_service.DRILL_ROOT = self.tmp
        drill_service._stores.clear()
        engine = create_engine('sqlite://')
        Base.metadata.create_all(engine)
        self.db = sessionmaker(engine, expire_on_commit=False)()
        self.db.add(
            Dataset(
                name='drill input',
                description='grouped script',
                entries=[
                    {'group': 'g1', **G1[0]},
                    {'group': 'g1', **G1[1]},
                    {'group': 'g2', **G2[0]},
                    {'group': 'g2', **G2[1]},
                ],
            )
        )
        self.db.commit()

    def tearDown(self):
        self.db.close()
        drill_service.DRILL_ROOT = self.previous_root

    def new_drill(self, name='drill'):
        return drill_service.create(
            self.db, DrillCreateInput(name=name, dataset_id=1, segment_limit=1)
        )

    def test_drill_starts_pending_and_refuses_writes(self):
        created = self.new_drill()
        self.assertEqual(created['status'], 'pending_recovery')
        with self.assertRaises(drill_service.HTTPException) as caught:
            drill_service.write_group(self.db, created['id'], DrillGroupInput(group_id='g1', operations=G1))
        self.assertEqual(caught.exception.status_code, 409)

    def test_recovery_then_writes_and_checkpoint(self):
        created = self.new_drill()
        drill_service.recover(self.db, created['id'])
        drill_service.write_group(self.db, created['id'], DrillGroupInput(group_id='g1', operations=G1))
        result = drill_service.write_group(
            self.db, created['id'], DrillGroupInput(group_id='g2', operations=G2)
        )
        self.assertEqual(result['outcome'], 'committed')
        self.assertEqual(result['kv'], {'station-02': '19.1', 'station-03': '22.4'})

        cp = drill_service.checkpoint(self.db, created['id'])
        self.assertEqual(cp['outcome'], 'published')
        self.assertEqual(cp['lsn'], 2)

        drill_service.crash(self.db, created['id'])
        recovered = drill_service.recover(self.db, created['id'])
        self.assertEqual(recovered['kv'], {'station-02': '19.1', 'station-03': '22.4'})
        self.assertEqual(recovered['checkpoint_lsn'], 2)
        row = self.db.get(Drill, created['id'])
        self.assertEqual(row.status, 'writable')

    def test_duplicate_same_and_conflicting_content(self):
        created = self.new_drill()
        drill_service.recover(self.db, created['id'])
        first = drill_service.write_group(self.db, created['id'], DrillGroupInput(group_id='g1', operations=G1))
        again = drill_service.write_group(self.db, created['id'], DrillGroupInput(group_id='g1', operations=G1))
        self.assertEqual(first['lsn'], 1)
        self.assertEqual(again['outcome'], 'duplicate')
        self.assertEqual(again['lsn'], 1)
        with self.assertRaises(drill_service.HTTPException) as caught:
            drill_service.write_group(self.db, created['id'], DrillGroupInput(group_id='g1', operations=G2))
        self.assertEqual(caught.exception.status_code, 409)
        self.assertIn('不同内容', caught.exception.detail)

    def test_crash_before_fsync_requires_recovery(self):
        created = self.new_drill()
        drill_service.recover(self.db, created['id'])
        drill_service.write_group(self.db, created['id'], DrillGroupInput(group_id='g1', operations=G1))
        drill_service.write_group(
            self.db,
            created['id'],
            DrillGroupInput(group_id='g2', operations=G2, crash='before_fsync'),
        )
        row = self.db.get(Drill, created['id'])
        self.assertEqual(row.status, 'pending_recovery')
        with self.assertRaises(drill_service.HTTPException):
            drill_service.write_group(self.db, created['id'], DrillGroupInput(group_id='g3', operations=G2))
        report = drill_service.recover(self.db, created['id'])
        self.assertEqual(report['kv'], {'station-01': '18.2', 'station-02': '19.1'})
        self.assertEqual([g['group_id'] for g in report['applied_groups']], ['g1'])

    def test_interrupted_checkpoint_reports_and_recovers(self):
        created = self.new_drill()
        drill_service.recover(self.db, created['id'])
        drill_service.write_group(self.db, created['id'], DrillGroupInput(group_id='g1', operations=G1))
        result = drill_service.checkpoint(self.db, created['id'], interrupt=True)
        self.assertEqual(result['outcome'], 'checkpoint_interrupted')
        self.assertEqual(self.db.get(Drill, created['id']).status, 'pending_recovery')
        report = drill_service.recover(self.db, created['id'])
        self.assertTrue(report['ignored_incomplete_checkpoint'])
        self.assertEqual(report['kv'], {'station-01': '18.2', 'station-02': '19.1'})

    def test_recover_stops_on_middle_corruption_and_keeps_scene(self):
        created = self.new_drill()
        drill_service.recover(self.db, created['id'])
        drill_service.write_group(self.db, created['id'], DrillGroupInput(group_id='g1', operations=G1))
        drill_service.write_group(self.db, created['id'], DrillGroupInput(group_id='g2', operations=G2))
        root = self.tmp / str(created['id'])
        segment = root / 'wal-000001.log'
        data = bytearray(segment.read_bytes())
        data[12] ^= 0xFF
        segment.write_bytes(bytes(data))
        before = segment.read_bytes()
        with self.assertRaises(drill_service.HTTPException) as caught:
            drill_service.recover(self.db, created['id'])
        self.assertEqual(caught.exception.status_code, 409)
        self.assertEqual(caught.exception.detail['error_code'], 'LOG_CORRUPT')
        self.assertEqual(segment.read_bytes(), before)
        self.assertEqual(self.db.get(Drill, created['id']).status, 'stopped')
        with self.assertRaises(drill_service.HTTPException) as blocked:
            drill_service.write_group(self.db, created['id'], DrillGroupInput(group_id='g3', operations=G2))
        self.assertIn('保留现场', blocked.exception.detail)

    def test_script_groups_operations_by_group_key(self):
        created = self.new_drill()
        detail = drill_service.detail(self.db, created['id'])
        self.assertEqual([row['group'] for row in detail['script']], ['g1', 'g2'])
        self.assertEqual(detail['script'][1]['ops'][1]['op'], 'delete')

    def test_listing_and_export(self):
        created = self.new_drill()
        rows = drill_service.listing(self.db)
        self.assertEqual(rows[0]['id'], created['id'])
        exported = drill_service.export(self.db, created['id'])
        self.assertEqual(exported['drill']['status'], 'pending_recovery')


if __name__ == '__main__':
    unittest.main()
