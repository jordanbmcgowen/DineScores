#!/usr/bin/env python3
"""
d1_export.py: export every restaurant's stored row plus its latest inspection
from the live D1 database to gzipped JSONL, for offline regrades and audits.
Read-only: it only runs SELECTs. Stdlib only.

    CLOUDFLARE_API_TOKEN=... CLOUDFLARE_ACCOUNT_ID=... \\
        python3 scripts/d1_export.py --out d1_export.jsonl.gz [--summary FILE]

The committed seeds lag D1 (every weekly run writes straight to it), so a
regrade has to read the live rows. One line per restaurant:

    {"id", "name", "city", "state", "metro", "source", "inspection_date",
     "original_score", "risk_score", "weighted_score", "vetted_grade",
     "infractions", "summaries", "inspection_count", "updated_at",
     "latest": {"id", "risk_score", "original_score", "violations"} | null}

`latest` is the inspection row the restaurant row was built from (same
restaurant, same date). Stored violation text is what write_d1_sql kept: at
most 20 lines per inspection, each cut at 400 characters.
"""
import argparse
import gzip
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
API = 'https://api.cloudflare.com/client/v4/accounts/{account}/d1/database/{database}/query'

# Restaurants are paged by primary key in a subquery so a page boundary never
# splits one restaurant's inspection rows; inspections join on the index
# (restaurant_id, inspection_date).
PAGE_SQL = """
SELECT r.id, r.name, r.city, r.state, r.metro, r.source, r.inspection_date,
       r.original_score, r.risk_score, r.weighted_score, r.vetted_grade,
       r.infractions, r.summaries, r.inspection_count, r.updated_at,
       i.id AS insp_id, i.risk_score AS insp_risk_score,
       i.original_score AS insp_original_score, i.violations AS insp_violations
FROM (SELECT * FROM restaurants WHERE id > ?1 ORDER BY id LIMIT ?2) AS r
LEFT JOIN inspections AS i
  ON i.restaurant_id = r.id AND i.inspection_date = r.inspection_date
ORDER BY r.id
"""

REST_COLS = ('id', 'name', 'city', 'state', 'metro', 'source', 'inspection_date',
             'original_score', 'risk_score', 'weighted_score', 'vetted_grade',
             'infractions', 'summaries', 'inspection_count', 'updated_at')


def database_id(path=os.path.join(REPO, 'wrangler.toml')):
    with open(path, encoding='utf-8') as f:
        m = re.search(r'database_id\s*=\s*"([0-9a-f-]{36})"', f.read())
    if not m:
        raise SystemExit('no database_id in wrangler.toml')
    return m.group(1)


def query(url, token, sql, params, tries=6):
    body = json.dumps({'sql': sql, 'params': params}).encode('utf-8')
    for attempt in range(tries):
        req = urllib.request.Request(url, data=body, method='POST', headers={
            'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'})
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                payload = json.loads(resp.read().decode('utf-8'))
            if not payload.get('success'):
                raise RuntimeError(f"D1 query failed: {payload.get('errors')}")
            return payload['result'][0]['results']
        except urllib.error.HTTPError as e:
            # never echo the request (it carries nothing secret, but the
            # Authorization header must not reach a log either)
            if e.code in (429, 500, 502, 503, 504) and attempt < tries - 1:
                time.sleep(min(60, 2 ** attempt * 2))
                continue
            raise RuntimeError(f'D1 query HTTP {e.code}: {e.read()[:300]!r}') from None
        except (urllib.error.URLError, TimeoutError) as e:
            if attempt < tries - 1:
                time.sleep(min(60, 2 ** attempt * 2))
                continue
            raise RuntimeError(f'D1 query failed: {type(e).__name__}') from None


def export(out, page_size=1000, summary=None):
    token = os.environ.get('CLOUDFLARE_API_TOKEN', '')
    account = os.environ.get('CLOUDFLARE_ACCOUNT_ID', '')
    if not token or not account:
        raise SystemExit('CLOUDFLARE_API_TOKEN and CLOUDFLARE_ACCOUNT_ID are required')
    url = API.format(account=account, database=database_id())
    started = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
    stats = Counter()
    by_source = Counter()
    last = ''
    t0 = time.time()
    with gzip.open(out, 'wt', encoding='utf-8') as f:
        f.write(json.dumps({'exported_at': started, 'page_size': page_size}) + '\n')
        while True:
            rows = query(url, token, PAGE_SQL, [last, page_size])
            if not rows:
                break
            grouped = {}
            for row in rows:
                rec = grouped.get(row['id'])
                if rec is None:
                    rec = grouped[row['id']] = {k: row.get(k) for k in REST_COLS}
                    rec['latest'] = None
                    rec['_extra'] = 0
                if row.get('insp_id'):
                    if rec['latest'] is None:
                        rec['latest'] = {'id': row['insp_id'], 'risk_score': row.get('insp_risk_score'),
                                         'original_score': row.get('insp_original_score'),
                                         'violations': row.get('insp_violations')}
                    else:
                        rec['_extra'] += 1
            for rid in sorted(grouped):
                rec = grouped[rid]
                stats['restaurants'] += 1
                by_source[rec.get('source') or '?'] += 1
                if rec['latest'] is None:
                    stats['no_latest_inspection'] += 1
                else:
                    try:
                        vio = json.loads(rec['latest']['violations'] or '[]')
                    except ValueError:
                        vio = []
                        stats['unreadable_violations'] += 1
                    if len(vio) >= 20:
                        stats['latest_with_20_lines'] += 1
                    if any(isinstance(v, list) and len(v) >= 3 and len(str(v[2])) >= 400 for v in vio):
                        stats['latest_with_a_400_char_line'] += 1
                if rec.pop('_extra'):
                    stats['restaurants_with_extra_same_day_rows'] += 1
                f.write(json.dumps(rec, ensure_ascii=False, separators=(',', ':')) + '\n')
            last = max(grouped)
            if stats['restaurants'] % 20000 < page_size:
                print(f"  {stats['restaurants']} restaurants ({time.time() - t0:.0f}s)", flush=True)
            if len(grouped) < page_size:
                break
    lines = [f"Exported {stats['restaurants']} restaurants at {started} in {time.time() - t0:.0f}s",
             '', '| check | count |', '|---|---|']
    lines += [f'| {k} | {v} |' for k, v in sorted(stats.items())]
    lines += ['', '| source | restaurants |', '|---|---|']
    lines += [f'| {s} | {n} |' for s, n in by_source.most_common()]
    print('\n'.join(lines))
    if summary:
        with open(summary, 'a', encoding='utf-8') as f:
            f.write('\n'.join(lines) + '\n')
    return stats


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('--out', default='d1_export.jsonl.gz')
    ap.add_argument('--page-size', type=int, default=1000)
    ap.add_argument('--summary', default=None, help='append a markdown summary here (e.g. $GITHUB_STEP_SUMMARY)')
    args = ap.parse_args(argv)
    export(args.out, args.page_size, args.summary)


if __name__ == '__main__':
    sys.exit(main())
