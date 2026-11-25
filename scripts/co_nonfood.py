#!/usr/bin/env python3
"""
co_nonfood.py: remove the places that are not restaurants from the Colorado
Front Range sources. Stdlib only.

    python3 scripts/co_nonfood.py --export d1_export.jsonl.gz \\
        [--seed data/d1_seed_<date>_co_nonfood_cleanup.sql.gz] \\
        [--data-js public/data.js --write-data-js]

--export is the per-source history export (export-d1.yml with `sources`).

El Paso County, Adams, Weld and Larimer publish Child Care, Body Art, Waste
Tire and pool inspections on the same portal slugs as retail food, and until
2026-11-25 the fetcher shipped them as restaurants (the configs are
`programs: ['Retail Food']` since). The stored inspection type names the
program on three of the four ('Child Care - Routine', 'Body Art', 'Waste Tire
Generator', 'Water Recreation'); Larimer leaves it empty on its child-care
rows and writes the visit's purpose on its food rows, which was checked
against the portal row by row (LARIMER_UNTYPED_IS_CHILD_CARE below).

What is removed:
  - A restaurant whose every stored inspection is a non-food one is deleted
    with its inspections. The DELETE is guarded in SQL as well: a place that
    has gained a food inspection since the export stays.
  - A place with both kinds (a school with a cafeteria and a child-care
    licence at one address) keeps its row and its food inspections; the
    non-food inspections are deleted, and if the row was built from one of
    them it is rebuilt from the newest food inspection.
Loading the seed twice changes nothing more. The same records leave data.js.
"""
import argparse
import gzip
import json
import os
import re
import sys
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, REPO)
sys.path.insert(0, HERE)

import dinescores_pipeline as dp   # noqa: E402
import jev_reader as jr            # noqa: E402
import jev_regrade as jg           # noqa: E402

# The sources whose portals mix programs, and the stored inspection types of
# their food inspections (what the SQL guard looks for).
SOURCES = ('El Paso County CO Health Dept', 'Adams County Health Dept', 'Weld County Health Dept',
           'Larimer County Health Dept')
FOOD_TYPES = ('Retail Food', 'Routine', 'Re-Inspection', 'Imminent Health Hazard')
NON_FOOD_TYPE_RE = re.compile(r'child care|body art|waste tire|water recreation|\bpool|\bspa\b', re.I)
# Larimer's stored rows with an empty inspection type, checked against the
# portal one by one on 2026-11-25 (a search of each row's day; the search row
# carries programName): 218 of 220 are 'Child Care'. One is a 'Retail Food'
# row of a portal test record ("Deb's Test Record Brewhause"), excepted by
# its inspection id below. One is no longer on the portal (a Boys & Girls
# Club; its two lines are about toy sanitising and a first-aid kit) and
# counts as the child-care row it reads as.
LARIMER_UNTYPED_IS_CHILD_CARE = True
LARIMER_UNTYPED_FOOD_IDS = frozenset({'42DB2A09-4819-427C-84D5-AF36D06F99F4'})
IDS_PER_STATEMENT = 400


def is_non_food(source, inspection_type, source_id=None):
    """True for a stored inspection of one of the mixed portals that is not a
    retail-food inspection."""
    if source not in SOURCES:
        return False
    t = (inspection_type or '').strip()
    if NON_FOOD_TYPE_RE.search(t):
        return True
    return (LARIMER_UNTYPED_IS_CHILD_CARE and source == 'Larimer County Health Dept' and t == ''
            and source_id not in LARIMER_UNTYPED_FOOD_IDS)


def kind_of(inspection_type):
    t = (inspection_type or '').lower()
    for word, kind in (('child care', 'child care'), ('body art', 'body art'), ('waste tire', 'waste tyre'),
                       ('water recreation', 'pool'), ('pool', 'pool'), ('spa', 'pool')):
        if word in t:
            return kind
    return 'child care' if not t.strip() else 'other'


def load_export(path):
    with gzip.open(path, 'rt', encoding='utf-8') as f:
        header = json.loads(f.readline())
        rows = [json.loads(line) for line in f if line.strip()]
    if not header.get('history'):
        raise SystemExit('this needs the per-source history export (d1_export.py --sources)')
    return header, rows


def plan(rows, judgments=None):
    """What to do with every exported restaurant of the mixed portals:
    {'delete': [row, ...], 'mixed': [{...}, ...], 'stats': Counter}."""
    judgments = judgments or {}
    out = {'delete': [], 'mixed': [], 'stats': Counter()}
    for row in rows:
        src = row.get('source') or ''
        if src not in SOURCES:
            continue
        insps = row.get('inspections') or []
        flags = [is_non_food(src, i.get('inspection_type'), i.get('source_id')) for i in insps]
        if not insps or not any(flags):
            out['stats']['restaurants kept'] += 1
            continue
        if all(flags):
            out['delete'].append(row)
            kinds = {kind_of(i.get('inspection_type')) for i in insps}
            out['stats'][f"delete: {'/'.join(sorted(kinds))}"] += 1
            out['stats'][f"delete: graded {row.get('vetted_grade')}"] += 1
            continue
        food = [i for i, nf in zip(insps, flags) if not nf]          # newest first, as exported
        gone = [i for i, nf in zip(insps, flags) if nf]
        item = {'row': row, 'remove': gone, 'rebuild': None}
        if row.get('inspection_date') != food[0].get('inspection_date'):
            item['rebuild'] = _rebuild(row, food, judgments)
        out['mixed'].append(item)
        out['stats']['mixed: non-food inspections removed'] += len(gone)
        out['stats']['mixed: row rebuilt from its food inspection' if item['rebuild']
                     else 'mixed: row already on a food inspection'] += 1
    return out


def _rebuild(row, food, judgments):
    """The restaurant row as the weekly run would have written it from the
    newest food inspection alone."""
    latest = food[0]
    try:
        vio = json.loads(latest.get('violations') or '[]')
    except ValueError:
        vio = []
    texts = [str(v[2]) for v in vio if isinstance(v, (list, tuple)) and len(v) >= 3 and str(v[2]).strip()]
    jm = {}
    for t in texts:
        j = judgments.get(jr.normalize_text(t))
        if j:
            jm[jr.normalize_text(t)] = j
    joined = '|||'.join(texts)
    if jm:
        text_f = jr.has_active_pest_or_sewage(texts, jm, use=jg.USE)
        chips = jr.detect_infractions(texts, jm, use=jg.USE)
        summaries = jr.summarize_violations(texts, jm, use=jg.USE)
    else:
        text_f = dp.has_active_pest_or_sewage(joined)
        chips = dp.detect_infractions(joined)
        summaries = dp.summarize_violations(joined)
    for s in summaries:
        s['verbatim'] = s['verbatim'][:200]
    rs = latest.get('risk_score') or 0
    ws = dp._weighted_from_history([[i.get('inspection_date'), i.get('risk_score')] for i in food])
    vg = dp.calculate_vetted_grade(ws)
    if dp.calculate_vetted_grade(rs) == 'F' or text_f:
        vg = 'F'
    return {'inspection_id': latest['id'], 'inspection_date': latest.get('inspection_date'),
            'original_score': latest.get('original_score'), 'risk_score': rs, 'weighted_score': ws,
            'vetted_grade': vg, 'infractions': chips, 'summaries': summaries,
            'history': [[i.get('inspection_date'), i.get('risk_score')] for i in food][:dp.MAX_HISTORY]}


def _chunks(items, n=IDS_PER_STATEMENT):
    for i in range(0, len(items), n):
        yield items[i:i + n]


def seed_sql(p):
    """Guarded DELETEs and the rebuilt rows. Restaurants first: the
    inspection DELETE then takes only rows whose restaurant is gone."""
    q = dp._sql_quote
    food = ','.join(q(t) for t in FOOD_TYPES)
    sources = ','.join(q(s) for s in SOURCES)
    parts = []
    ids = sorted(r['id'] for r in p['delete'])
    for chunk in _chunks(ids):
        lst = ','.join(q(i) for i in chunk)
        parts.append(
            f"DELETE FROM restaurants WHERE id IN ({lst}) AND source IN ({sources}) "
            f"AND NOT EXISTS (SELECT 1 FROM inspections WHERE inspections.restaurant_id = restaurants.id "
            f"AND inspections.inspection_type IN ({food}));\n")
    for chunk in _chunks(ids):
        lst = ','.join(q(i) for i in chunk)
        parts.append(
            f"DELETE FROM inspections WHERE restaurant_id IN ({lst}) "
            f"AND NOT EXISTS (SELECT 1 FROM restaurants WHERE restaurants.id = inspections.restaurant_id);\n")
    for item in p['mixed']:
        row = item['row']
        for i in item['remove']:
            parts.append(f"DELETE FROM inspections WHERE id = {q(i['id'])} "
                         f"AND restaurant_id = {q(row['id'])} "
                         f"AND COALESCE(inspection_type, '') = {q(i.get('inspection_type') or '')};\n")
        nb = item['rebuild']
        if nb:
            parts.append(
                "UPDATE restaurants SET "
                f"inspection_date = {q(nb['inspection_date'])}, original_score = {q(nb['original_score'])}, "
                f"risk_score = {q(nb['risk_score'])}, weighted_score = {q(nb['weighted_score'])}, "
                f"vetted_grade = {q(nb['vetted_grade'])}, infractions = {dp._sql_json(nb['infractions'])}, "
                f"summaries = {dp._sql_json(nb['summaries'])}, "
                f"source_url = COALESCE((SELECT source_url FROM inspections WHERE id = {q(nb['inspection_id'])}), "
                "source_url) "
                f"WHERE id = {q(row['id'])} AND inspection_date = {q(row.get('inspection_date'))} "
                f"AND updated_at IS {q(row.get('updated_at'))};\n")
        parts.append("UPDATE restaurants SET inspection_count = (SELECT COUNT(*) FROM inspections "
                     f"WHERE inspections.restaurant_id = restaurants.id) WHERE id = {q(row['id'])};\n")
    for stmt in parts:
        if len(stmt.encode('utf-8')) >= 100_000:
            raise RuntimeError('statement over the D1 100KB limit')
    return ''.join(parts), {'restaurants deleted': len(ids), 'statements': len(parts),
                            'mixed restaurants': len(p['mixed'])}


def clean_data_js(content, p):
    """(new content, stats): the deleted restaurants leave the file; a mixed
    one's record is rebuilt from its food inspections."""
    head, body = content.split('window.DATA = ', 1)
    recs = json.loads(body.rstrip(';\n'))
    gone = {r['id'] for r in p['delete']}
    mixed = {m['row']['id']: m for m in p['mixed']}
    stats = Counter()
    out = []
    for rec in recs:
        rid = rec.get('i')
        if rid in gone:
            stats['records removed'] += 1
            continue
        m = mixed.get(rid)
        if m:
            dates = {i.get('inspection_date') for i in m['remove']}
            nb = m['rebuild']
            if nb and rec.get('d') in dates:
                rec['d'], rec['rs'], rec['ws'], rec['vg'] = (nb['inspection_date'], nb['risk_score'],
                                                           nb['weighted_score'], nb['vetted_grade'])
                rec['os'] = nb['original_score']
                rec['inf'] = list(nb['infractions'])
                rec['h'] = [list(x) for x in nb['history']]
                if 'vs' in rec:
                    rec['vs'] = [dict(s) for s in nb['summaries']]
                stats['mixed records rebuilt'] += 1
            elif any(pair[0] in dates for pair in rec.get('h') or []):
                rec['h'] = [pair for pair in rec['h'] if pair[0] not in dates]
                rec['ws'] = dp._weighted_from_history(rec['h'] or [[rec.get('d', ''), rec.get('rs', 0)]])
                stats['mixed records trimmed'] += 1
            if rec.get('ic'):
                rec['ic'] = max(1, len(m['row'].get('inspections') or []) - len(m['remove']))
        out.append(rec)
    return head + 'window.DATA = ' + json.dumps(out, separators=(',', ':'), default=str) + ';\n', stats


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('--export', required=True)
    ap.add_argument('--cache-dir', default=jr.CACHE_DIR)
    ap.add_argument('--seed', default=None)
    ap.add_argument('--data-js', default=None)
    ap.add_argument('--write-data-js', action='store_true')
    args = ap.parse_args(argv)

    header, rows = load_export(args.export)
    cache = jr.JudgmentCache(args.cache_dir).load()
    judgments = {}
    for row in rows:
        for i in row.get('inspections') or []:
            if is_non_food(row.get('source') or '', i.get('inspection_type'), i.get('source_id')):
                continue
            try:
                vio = json.loads(i.get('violations') or '[]')
            except ValueError:
                continue
            for v in vio:
                nt = jr.normalize_text(str(v[2])) if isinstance(v, (list, tuple)) and len(v) >= 3 else None
                if nt and nt not in judgments:
                    rec = cache.get(jr.judgment_key(nt))
                    j = jr.judgment_from_record(rec) if rec else None
                    if j:
                        judgments[nt] = j
    p = plan(rows, judgments)
    print(f"{len(rows)} restaurants in the export taken {header.get('exported_at')}")
    for k, v in sorted(p['stats'].items()):
        print(f'  {k}: {v}')
    by_source = Counter(r['source'] for r in p['delete'])
    print('  to delete by source: ' + ', '.join(f'{s} {n}' for s, n in by_source.most_common()))
    for m in p['mixed']:
        r, nb = m['row'], m['rebuild']
        print(f"  mixed: {r['name']} | {r['city']}: {len(m['remove'])} non-food inspection(s) removed" +
              (f"; row {r['inspection_date']} rs {r['risk_score']} {r['vetted_grade']} -> "
               f"{nb['inspection_date']} rs {nb['risk_score']} {nb['vetted_grade']}" if nb else ''))
    if args.seed:
        sql, n = seed_sql(p)
        with gzip.open(args.seed, 'wt', encoding='utf-8') as f:
            f.write(f"-- Colorado Front Range: {n['restaurants deleted']} stored places that are not restaurants "
                    f"(child care, body art, waste tyre, pools); export {header.get('exported_at')}.\n")
            f.write('-- Guarded: a place with a food inspection is never deleted; safe to load twice.\n')
            f.write(sql)
        print(f"wrote {n['statements']} statements to {args.seed}")
    if args.data_js:
        with open(args.data_js, encoding='utf-8') as f:
            content = f.read()
        new_content, stats = clean_data_js(content, p)
        print('data.js: ' + (', '.join(f'{k} {v}' for k, v in sorted(stats.items())) or 'nothing to change')
              + f'; {len(content):,} -> {len(new_content):,} bytes')
        if args.write_data_js:
            with open(args.data_js, 'w', encoding='utf-8') as f:
                f.write(new_content)
            print('data.js written')


if __name__ == '__main__':
    sys.exit(main())
