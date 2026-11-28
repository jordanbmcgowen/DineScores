"""The precomputed city index /api/cities reads (D1_CITY_INDEX_SQL)."""
import sqlite3
import tempfile
import unittest
from pathlib import Path

import dinescores_pipeline as dp


def record(name, city, lat, score, date='2026-11-01'):
    return {
        'name': name, 'address': f'{len(name)} Main St', 'city': city, 'state': 'TX',
        'zip': '75201', 'latitude': lat, 'longitude': -96.8 if lat else None,
        'inspection_date': date, 'original_score': score, 'risk_score': score,
        'violations': [], 'inspection_type': 'Routine', 'results': '',
        'source': 'Test', 'source_url': '', 'source_id': f'{name}-{date}', 'metro': '',
    }


class CityIndexTest(unittest.TestCase):
    def build(self, records):
        db = sqlite3.connect(':memory:')
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'out.sql'
            dp.write_d1_sql(records, str(path))
            sql = path.read_text()
        db.executescript(sql)
        return db, sql

    def test_every_sql_file_rebuilds_the_index(self):
        rows = [record(f'Place {i}', 'Dallas', 32.7 + i / 1000, 95) for i in range(30)]
        rows += [record(f'Town {i}', 'Tiny', 33.0, 95) for i in range(3)]
        rows.append(record('No Pin', 'Dallas', None, 60))
        db, sql = self.build(rows)
        self.assertTrue(sql.rstrip().endswith(dp.D1_CITY_INDEX_SQL.rstrip()))
        self.assertEqual(db.execute('SELECT total FROM city_index_meta').fetchone()[0], 34)
        # cities under 25 pinned restaurants stay out, unpinned rows are not counted
        self.assertEqual(db.execute('SELECT city, restaurant_count FROM city_index').fetchall(),
                         [('Dallas', 30)])

    def test_rebuild_replaces_not_appends(self):
        rows = [record(f'Place {i}', 'Dallas', 32.7, 95) for i in range(30)]
        db, sql = self.build(rows)
        db.executescript(sql)
        self.assertEqual(db.execute('SELECT COUNT(*) FROM city_index').fetchone()[0], 1)
        self.assertEqual(db.execute('SELECT COUNT(*) FROM city_index_meta').fetchone()[0], 1)

    def test_statements_stay_under_the_d1_cap(self):
        for statement in dp.D1_CITY_INDEX_SQL.split(';'):
            self.assertLess(len(statement.encode()), 100_000)


if __name__ == '__main__':
    unittest.main()
