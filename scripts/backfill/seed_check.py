"""seed_check.py <seed.sql.gz>: D1 statement cap check and an in-memory sqlite
replay of a committed-style seed, with row counts."""
import gzip
import sqlite3
import sys

path = sys.argv[1]
text = gzip.open(path, 'rt').read()
sizes = [len(x.encode()) for x in text.split(';\n')]
print(f"seed {path}: {len(sizes)} statements, largest {max(sizes):,} bytes (cap 100,000)")
assert max(sizes) < 100_000, 'a statement is over the D1 cap'
db = sqlite3.connect(':memory:')
db.executescript(text)


def q(sql):
    return db.execute(sql).fetchone()[0]


grades = ' '.join(f"{g} {q('SELECT COUNT(*) FROM restaurants WHERE vetted_grade=' + repr(g))}" for g in 'ABCF')
print(f"  restaurants {q('SELECT COUNT(*) FROM restaurants')}, inspections {q('SELECT COUNT(*) FROM inspections')}, "
      f"no coords {q('SELECT COUNT(*) FROM restaurants WHERE lat IS NULL')}, grades {grades}, "
      f"dates {q('SELECT MIN(inspection_date) FROM inspections')}..{q('SELECT MAX(inspection_date) FROM inspections')}")
