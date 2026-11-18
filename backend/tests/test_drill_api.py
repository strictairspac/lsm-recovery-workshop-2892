"""HTTP-level drill tests with SQLite metadata and a temp artifact directory."""
import os
import tempfile
import unittest
from pathlib import Path

os.environ.setdefault('JWT_SECRET', 'test-secret')
os.environ.setdefault('REDIS_URL', 'redis://localhost:6379/0')
os.environ['DATABASE_URL'] = 'sqlite:///' + tempfile.mktemp(suffix='.db')

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.config.database import Base, get_db
from src.main import app
from src.models.dataset import Dataset
from src.models.user import User
from src.services import auth as auth_service
from src.services import drill as drill_service

G1 = [
    {'op': 'put', 'key': 'station-01', 'value': '18.2'},
    {'op': 'put', 'key': 'station-02', 'value': '19.1'},
]
G2 = [
    {'op': 'put', 'key': 'station-03', 'value': '22.4'},
    {'op': 'delete', 'key': 'station-01'},
]


class ApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = Path(tempfile.mkdtemp(prefix='drill-api-'))
        cls.previous_root = drill_service.DRILL_ROOT
        drill_service.DRILL_ROOT = cls.tmp
        engine = create_engine(os.environ['DATABASE_URL'], connect_args={'check_same_thread': False})
        Base.metadata.create_all(engine)
        cls.Session = sessionmaker(engine, expire_on_commit=False)
        with cls.Session() as db:
            db.add(User(email='operator@beiling.test', role='operator', password_hash='x'))
            db.add(User(email='viewer@beiling.test', role='viewer', password_hash='x'))
            db.add(
                Dataset(
                    name='drill groups',
                    description='grouped',
                    entries=[
                        {'group': 'g1', **G1[0]},
                        {'group': 'g1', **G1[1]},
                        {'group': 'g2', **G2[0]},
                        {'group': 'g2', **G2[1]},
                    ],
                )
            )
            db.commit()

        def override_db():
            with cls.Session() as db:
                yield db

        def operator():
            with cls.Session() as db:
                return db.query(User).filter_by(email='operator@beiling.test').one()

        def viewer():
            with cls.Session() as db:
                return db.query(User).filter_by(email='viewer@beiling.test').one()

        cls.actor = {'role': 'operator'}

        def current():
            return operator() if cls.actor['role'] == 'operator' else viewer()

        def writer():
            user = current()
            if user.role not in ('admin', 'operator'):
                from fastapi import HTTPException

                raise HTTPException(403, '观察员只能查看与导出')
            return user

        app.dependency_overrides[get_db] = override_db
        app.dependency_overrides[auth_service.current_user] = current
        app.dependency_overrides[auth_service.writer] = writer
        cls.client = TestClient(app)

    @classmethod
    def tearDownClass(cls):
        app.dependency_overrides.clear()
        drill_service.DRILL_ROOT = cls.previous_root

    def setUp(self):
        drill_service._stores.clear()
        self.name = f"api-drill-{os.urandom(4).hex()}"
        response = self.client.post(
            '/drills',
            json={'name': self.name, 'dataset_id': 1, 'segment_limit': 1},
        )
        self.assertEqual(response.status_code, 201, response.text)
        self.ident = response.json()['id']

    def group(self, group_id, ops, crash='none'):
        return self.client.post(
            f'/drills/{self.ident}/groups',
            json={'group_id': group_id, 'operations': ops, 'crash': crash},
        )

    def test_full_drill_flow(self):
        listed = self.client.get('/drills').json()
        self.assertEqual(listed[0]['status'], 'pending_recovery')
        blocked = self.group('g1', G1)
        self.assertEqual(blocked.status_code, 409)

        recovered = self.client.post(f'/drills/{self.ident}/recover')
        self.assertEqual(recovered.status_code, 201, recovered.text)
        self.assertEqual(recovered.json()['status'], 'writable')
        self.assertEqual(recovered.json()['kv'], {})

        self.assertEqual(self.group('g1', G1).json()['outcome'], 'committed')
        second = self.group('g2', G2)
        self.assertEqual(second.status_code, 201)
        self.assertEqual(second.json()['kv'], {'station-02': '19.1', 'station-03': '22.4'})

        duplicate = self.group('g1', G1)
        self.assertEqual(duplicate.json()['outcome'], 'duplicate')
        self.assertEqual(duplicate.json()['lsn'], 1)
        conflict = self.group('g2', G1)
        self.assertEqual(conflict.status_code, 409)
        self.assertIn('不同内容', conflict.json()['detail'])

        checkpoint = self.client.post(f'/drills/{self.ident}/checkpoint', json={'interrupt': False})
        self.assertEqual(checkpoint.json()['outcome'], 'published')
        self.assertEqual(checkpoint.json()['lsn'], 2)
        self.assertEqual(checkpoint.json()['reclaimed_segments'], [1, 2])

        crash = self.client.post(f'/drills/{self.ident}/crash')
        self.assertEqual(crash.json()['status'], 'pending_recovery')
        self.assertIsNone(crash.json()['kv'])

        recovery = self.client.post(f'/drills/{self.ident}/recover')
        body = recovery.json()
        self.assertEqual(body['status'], 'writable')
        self.assertEqual(body['kv'], {'station-02': '19.1', 'station-03': '22.4'})
        self.assertEqual(body['checkpoint_lsn'], 2)

    def test_crash_before_fsync_and_discarded_tail(self):
        self.client.post(f'/drills/{self.ident}/recover')
        self.group('g1', G1)
        torn = self.group('g2', G2, crash='before_fsync')
        self.assertEqual(torn.json()['outcome'], 'lost_before_fsync')
        self.assertFalse(torn.json()['durable'])
        self.assertIsNone(torn.json()['kv'], 'abandoned memory must not look readable')
        self.assertEqual(self.client.get(f'/drills/{self.ident}').json()['status'], 'pending_recovery')

        recovery = self.client.post(f'/drills/{self.ident}/recover').json()
        self.assertEqual(recovery['kv'], {'station-01': '18.2', 'station-02': '19.1'})
        self.assertEqual([row['group_id'] for row in recovery['applied_groups']], ['g1'])
        self.assertEqual(recovery['discarded_tail']['action'], '已丢弃尾部半条记录')
        self.assertEqual(recovery['status'], 'writable')

        self.assertEqual(self.group('g3', G2).json()['outcome'], 'committed')
        again = self.client.post(f'/drills/{self.ident}/crash')
        self.assertEqual(again.status_code, 201)
        final = self.client.post(f'/drills/{self.ident}/recover').json()
        self.assertEqual(final['last_lsn'], 2)
        # g1 was never checkpointed, so it is replayed again; the half record
        # from the torn g2 write is gone and only the new g3 follows it.
        self.assertEqual([row['group_id'] for row in final['applied_groups']], ['g1', 'g3'])
        self.assertEqual(final['discarded_tail'], None)

    def test_interrupted_checkpoint_recovers_with_previous_state(self):
        self.client.post(f'/drills/{self.ident}/recover')
        self.group('g1', G1)
        interrupted = self.client.post(f'/drills/{self.ident}/checkpoint', json={'interrupt': True})
        self.assertEqual(interrupted.json()['outcome'], 'checkpoint_interrupted')
        self.assertEqual(interrupted.json()['reclaimed_segments'], [])
        body = self.client.post(f'/drills/{self.ident}/recover').json()
        self.assertTrue(body['ignored_incomplete_checkpoint'])
        self.assertEqual(body['kv'], {'station-01': '18.2', 'station-02': '19.1'})

    def test_middle_corruption_returns_reason_and_preserves(self):
        self.client.post(f'/drills/{self.ident}/recover')
        self.group('g1', G1)
        self.group('g2', G2)
        segment = self.tmp / str(self.ident) / 'wal-000001.log'
        data = bytearray(segment.read_bytes())
        data[12] ^= 0xFF
        segment.write_bytes(bytes(data))
        before = segment.read_bytes()
        response = self.client.post(f'/drills/{self.ident}/recover')
        self.assertEqual(response.status_code, 409)
        detail = response.json()['detail']
        self.assertEqual(detail['error_code'], 'LOG_CORRUPT')
        self.assertTrue(detail['preserved'])
        self.assertIsNone(detail['kv'])
        self.assertEqual(segment.read_bytes(), before)
        blocked = self.group('g3', G2)
        self.assertEqual(blocked.status_code, 409)

    def test_viewer_cannot_write(self):
        self.actor['role'] = 'viewer'
        try:
            response = self.client.post(f'/drills/{self.ident}/recover')
            self.assertEqual(response.status_code, 403)
            self.assertEqual(self.client.get('/drills').status_code, 200)
        finally:
            self.actor['role'] = 'operator'

    def test_export_contains_report_and_script(self):
        self.client.post(f'/drills/{self.ident}/recover')
        self.group('g1', G1)
        exported = self.client.get(f'/drills/{self.ident}/export').json()
        self.assertEqual(exported['drill']['last_lsn'], 1)
        self.assertEqual([row['group'] for row in exported['script']], ['g1', 'g2'])

    def test_status_get_does_not_touch_files(self):
        self.client.post(f'/drills/{self.ident}/recover')
        self.group('g1', G1)
        self.group('g2', G2, crash='before_fsync')
        root = self.tmp / str(self.ident)
        before = {path.name: path.read_bytes() for path in sorted(root.iterdir())}
        payload = self.client.get(f'/drills/{self.ident}/status').json()
        self.assertEqual(payload['disk_view']['status'], 'recovered')
        self.assertIsNotNone(payload['disk_view']['discarded_tail'])
        self.assertEqual(payload['disk_view']['discarded_tail']['action'], '待恢复时丢弃')
        after = {path.name: path.read_bytes() for path in sorted(root.iterdir())}
        self.assertEqual(before, after)
        detail = self.client.get(f'/drills/{self.ident}').json()
        self.assertEqual(detail['status'], 'pending_recovery')
        self.assertIsNone(detail['last_report']['kv'])
        self.assertEqual(detail['last_report']['group_count'], 1)

    def test_original_pages_still_work(self):
        datasets = self.client.get('/datasets').json()
        self.assertTrue(datasets)
        self.assertEqual(datasets[0]['operation_count'], 4)
        self.assertEqual(self.client.get('/configs').json(), [])
        self.assertEqual(self.client.get('/trials').json(), [])


if __name__ == '__main__':
    unittest.main()
