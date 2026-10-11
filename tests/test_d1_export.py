"""scripts/d1_export.py against an in-memory SQLite copy of the D1 schema
(no network): paging, the latest-inspection join, and the row format."""
import gzip
import json
import os
import sqlite3
import sys
import tempfile
import unittest
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))
sys.path.insert(0, os.path.join(os.path.dirname(HERE), 'scripts'))

import dinescores_pipeline as dp  # noqa: E402
import d1_export  # noqa: E402


class ExportTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.db.row_factory = sqlite3.Row
        self.db.executescript(dp.D1_SCHEMA_SQL)
        for n in range(7):
            rid = f'r{n:02d}'
            self.db.execute(
                'INSERT INTO restaurants (id, name, city, source, inspection_date, risk_score, weighted_score,'
                ' vetted_grade, infractions, summaries, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)',
                (rid, f'Place {n}', 'Town', 'Test Source', '2026-10-02', 95, 93, 'A', '["pests"]', '[]',
                 '2026-10-03T00:00:00+00:00'))
            if n == 3:
                continue                      # a restaurant whose latest row is missing
            for date, text in (('2026-10-02', f'Line {n}'), ('2026-04-01', 'Older line')):
                self.db.execute(
                    'INSERT INTO inspections (id, restaurant_id, inspection_date, risk_score, violations)'
                    ' VALUES (?,?,?,?,?)',
                    (dp.make_inspection_id(rid, date), rid, date, 95, json.dumps([['pest_control', 'core', text]])))

    def fake_query(self, url, token, sql, params, tries=6):
        return [dict(r) for r in self.db.execute(sql, params).fetchall()]

    def test_pages_join_the_latest_inspection_and_keep_every_restaurant(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, 'export.jsonl.gz')
            env = {'CLOUDFLARE_API_TOKEN': 'x', 'CLOUDFLARE_ACCOUNT_ID': 'y'}
            with mock.patch.dict(os.environ, env), mock.patch.object(d1_export, 'query', self.fake_query), \
                    mock.patch('builtins.print'):
                stats = d1_export.export(out, page_size=3)
            with gzip.open(out, 'rt') as f:
                header, *rows = [json.loads(line) for line in f]
        self.assertIn('exported_at', header)
        self.assertEqual([r['id'] for r in rows], [f'r{n:02d}' for n in range(7)])
        self.assertEqual(stats['restaurants'], 7)
        self.assertEqual(stats['no_latest_inspection'], 1)
        self.assertIsNone(rows[3]['latest'])
        latest = rows[0]['latest']
        self.assertEqual(latest['id'], 'r00_20261002')
        self.assertEqual(json.loads(latest['violations']), [['pest_control', 'core', 'Line 0']])
        self.assertEqual((rows[0]['vetted_grade'], rows[0]['infractions'], rows[0]['updated_at']),
                         ('A', '["pests"]', '2026-10-03T00:00:00+00:00'))

    def test_needs_both_credentials(self):
        with mock.patch.dict(os.environ, {'CLOUDFLARE_API_TOKEN': '', 'CLOUDFLARE_ACCOUNT_ID': ''}):
            with self.assertRaises(SystemExit):
                d1_export.export(os.devnull)


if __name__ == '__main__':
    unittest.main()
