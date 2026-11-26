#!/usr/bin/env python3
"""
unscored_cleanup.py: remove the stored inspections a published score of 0
turned into false F's (Portland-metro lodging, parks and camps; Dallas 'No
Score' visits). Stdlib only.

    python3 scripts/unscored_cleanup.py --export d1_export.jsonl.gz \\
        [--seed data/d1_seed_<date>_unscored_zero_cleanup.sql.gz] \\
        [--data-js public/data.js --write-data-js]

--export is a per-source history export (export-d1.yml with `sources`, or
the same shape built from the site's own /api/restaurants/:id and /history).

Until 2026-11-23 a MyHealthDepartment row whose 0-100 score field said 0
shipped as risk 0, an F with nothing cited. Two kinds of row did that:
  - Multnomah, Washington and Clackamas counties publish their hotel, RV
    park, organised camp and pool inspections on the restaurant slugs with a
    score of 0. They are not food inspections at all (the configs exclude
    those inspection types since 2026-11-26); every one of them goes,
    whatever its score.
  - Dallas's 'Retail Food Establishment (No Score)' visits (and two
    Multnomah 'Food School' visits) are food visits the city did not score.
    One with nothing cited goes (the fetcher leaves those out now, as
    `unscored_zero`); one with cited lines is graded from them
    (100 - 5P - 2PF - C), as the fetcher does now.

What changes:
  - A restaurant whose every stored inspection is removed is deleted with
    its inspections; the DELETE is guarded in SQL (a place that has gained
    any other inspection since the export stays).
  - Any other affected restaurant loses those inspections (each DELETE
    guarded on the row still being a score-0 or non-food one) and its row is
    rebuilt from what is left: from its newest remaining inspection when the
    row stood on a removed or regraded one, otherwise only the weighted
    score and grade move (the history lost a 0). The row UPDATE is guarded
    on the exported date, weighted score and grade (and updated_at when the
    export has it), so a row the weekly rewrote since is left alone.
Loading the seed twice changes nothing more. The same records change in
data.js.
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
import co_nonfood as cn            # noqa: E402

OREGON_SOURCES = ('Multnomah County Health Dept', 'Washington County OR Health Dept',
                  'Clackamas County Health Dept')
SOURCES = OREGON_SOURCES + ('Dallas Health Dept',)
NON_FOOD_TYPE_RE = re.compile(dp.MHD_OR_NON_FOOD_TYPE_RE)
# The same test in SQL, for the guards (inspection_type LIKE prefixes).
NON_FOOD_SQL = ("(inspection_type LIKE 'Traveler%' OR inspection_type LIKE 'Recreational Park%' "
                "OR inspection_type LIKE 'Organizational Camp%' OR inspection_type LIKE 'Pool%')")
ZERO_SQL = "(original_score = 0 AND COALESCE(risk_score, 0) = 0)"
IDS_PER_STATEMENT = 400


def _lines(insp):
    try:
        vio = json.loads(insp.get('violations') or '[]')
    except ValueError:
        return []
    return [v for v in vio if isinstance(v, (list, tuple)) and len(v) >= 3 and str(v[2]).strip()]


def verdict(source, insp):
    """'remove', 'regrade' or '' (keep) for one stored inspection."""
    if source not in SOURCES:
        return ''
    if source in OREGON_SOURCES and NON_FOOD_TYPE_RE.search(insp.get('inspection_type') or ''):
        return 'remove'
    if insp.get('original_score') == 0 and not insp.get('risk_score'):
        return 'regrade' if _lines(insp) else 'remove'
    return ''


def regraded_risk(insp):
    """The fetcher's grade for an unscored visit with cited lines."""
    vio = [(str(v[0]), str(v[1]), str(v[2])) for v in _lines(insp)]
    return dp.calc_risk_score(vio)[0]


def text_forces_f(insp, judgments):
    """The text-forced F of one stored inspection, read as the weekly run
    reads it (Jev's cached decision where there is one, else the regex)."""
    texts = [str(v[2]) for v in _lines(insp)]
    jm = {jr.normalize_text(t): judgments[jr.normalize_text(t)] for t in texts
          if jr.normalize_text(t) in judgments}
    if jm:
        return bool(jr.has_active_pest_or_sewage(texts, jm, use=cn.jg.USE))
    return bool(dp.has_active_pest_or_sewage('|||'.join(texts)))


def plan(rows, judgments=None):
    judgments = judgments or {}
    out = {'delete': [], 'mixed': [], 'regrade': [], 'stats': Counter()}
    for row in rows:
        src = row.get('source') or ''
        insps = row.get('inspections') or []
        verdicts = [verdict(src, i) for i in insps]
        if not any(verdicts):
            continue
        if all(v == 'remove' for v in verdicts):
            out['delete'].append(row)
            out['stats'][f"delete: {src}"] += 1
            out['stats'][f"delete: graded {row.get('vetted_grade')}"] += 1
            continue
        kept = []
        for i, v in zip(insps, verdicts):
            if v == 'regrade':
                i = dict(i, risk_score=regraded_risk(i), original_score=None)
                out['regrade'].append((row, i))
            if v != 'remove':
                kept.append(i)
        kept.sort(key=lambda i: i.get('inspection_date') or '', reverse=True)   # newest first
        gone = [i for i, v in zip(insps, verdicts) if v == 'remove']
        touched = {i.get('inspection_date') for i, v in zip(insps, verdicts) if v}
        item = {'row': row, 'remove': gone, 'kept': kept, 'rebuild': None, 'regrade_only': None}
        if row.get('inspection_date') in touched:
            item['rebuild'] = cn._rebuild(row, kept, judgments)
        else:
            # The row stands on an inspection that stays. Its stored weighted
            # score counts a removed 0 only when it equals the weighted score
            # of the full stored history (rows written from one run's fetch
            # often carry the latest score alone); otherwise it stays.
            old_hist = [[i.get('inspection_date'), i.get('risk_score')] for i in
                        sorted(insps, key=lambda i: i.get('inspection_date') or '', reverse=True)]
            new_hist = [[i.get('inspection_date'), i.get('risk_score')] for i in kept]
            ws = row.get('weighted_score')
            if ws == dp._weighted_from_history(old_hist):
                ws = dp._weighted_from_history(new_hist)
            # The grade is worked out again: an F the 0 left behind is sticky
            # (a kept data.js record never upgrades an F without new text),
            # so the text decision is read again from the latest lines.
            text_f = text_forces_f(kept[0], judgments)
            vg = dp.calculate_vetted_grade(ws or 0)
            if dp.calculate_vetted_grade(row.get('risk_score') or 0) == 'F' or text_f:
                vg = 'F'
            item['regrade_only'] = {'weighted_score': ws, 'vetted_grade': vg, 'text_f': text_f,
                                    'removed_dates': {i.get('inspection_date') for i in gone},
                                    'regraded': {i.get('inspection_date'): i.get('risk_score')
                                                 for r_, i in out['regrade'] if r_ is row}}
        out['mixed'].append(item)
        new = item['rebuild'] or item['regrade_only']
        out['stats'][f"keep: {src}"] += 1
        out['stats']['keep: inspections removed'] += len(gone)
        out['stats']['keep: row rebuilt from its newest remaining inspection' if item['rebuild']
                     else 'keep: weighted score recomputed'] += 1
        out['stats'][f"keep: grade {row.get('vetted_grade')} -> {new['vetted_grade']}"] += 1
    return out


def _chunks(items, n=IDS_PER_STATEMENT):
    for i in range(0, len(items), n):
        yield items[i:i + n]


def _row_guard(row):
    q = dp._sql_quote
    g = (f"id = {q(row['id'])} AND inspection_date IS {q(row.get('inspection_date'))} "
         f"AND weighted_score IS {q(row.get('weighted_score'))} AND vetted_grade IS {q(row.get('vetted_grade'))}")
    if row.get('updated_at'):
        g += f" AND updated_at = {q(row['updated_at'])}"
    return g


def seed_sql(p):
    q = dp._sql_quote
    sources = ','.join(q(s) for s in SOURCES)
    removable = f"({NON_FOOD_SQL} OR ({ZERO_SQL} AND COALESCE(violations, '') IN ('', '[]')))"
    parts = []
    ids = sorted(r['id'] for r in p['delete'])
    for chunk in _chunks(ids):
        lst = ','.join(q(i) for i in chunk)
        parts.append(
            f"DELETE FROM restaurants WHERE id IN ({lst}) AND source IN ({sources}) "
            f"AND NOT EXISTS (SELECT 1 FROM inspections WHERE inspections.restaurant_id = restaurants.id "
            f"AND NOT {removable});\n")
    for chunk in _chunks(ids):
        lst = ','.join(q(i) for i in chunk)
        parts.append(
            f"DELETE FROM inspections WHERE restaurant_id IN ({lst}) "
            f"AND NOT EXISTS (SELECT 1 FROM restaurants WHERE restaurants.id = inspections.restaurant_id);\n")
    for row, i in p['regrade']:
        parts.append(f"UPDATE inspections SET risk_score = {q(i['risk_score'])}, original_score = NULL "
                     f"WHERE id = {q(i['id'])} AND restaurant_id = {q(row['id'])} AND {ZERO_SQL};\n")
    for item in p['mixed']:
        row = item['row']
        for i in item['remove']:
            parts.append(f"DELETE FROM inspections WHERE id = {q(i['id'])} AND restaurant_id = {q(row['id'])} "
                         f"AND {removable};\n")
        nb, ro = item['rebuild'], item['regrade_only']
        if nb:
            parts.append(
                "UPDATE restaurants SET "
                f"inspection_date = {q(nb['inspection_date'])}, original_score = {q(nb['original_score'])}, "
                f"risk_score = {q(nb['risk_score'])}, weighted_score = {q(nb['weighted_score'])}, "
                f"vetted_grade = {q(nb['vetted_grade'])}, infractions = {dp._sql_json(nb['infractions'])}, "
                f"summaries = {dp._sql_json(nb['summaries'])}, "
                f"source_url = COALESCE((SELECT source_url FROM inspections WHERE id = {q(nb['inspection_id'])}), "
                f"source_url) WHERE {_row_guard(row)};\n")
        elif ro and (ro['weighted_score'], ro['vetted_grade']) != (row.get('weighted_score'),
                                                                   row.get('vetted_grade')):
            parts.append(f"UPDATE restaurants SET weighted_score = {q(ro['weighted_score'])}, "
                         f"vetted_grade = {q(ro['vetted_grade'])} WHERE {_row_guard(row)};\n")
        parts.append("UPDATE restaurants SET inspection_count = (SELECT COUNT(*) FROM inspections "
                     f"WHERE inspections.restaurant_id = restaurants.id) WHERE id = {q(row['id'])};\n")
    for stmt in parts:
        if len(stmt.encode('utf-8')) >= 100_000:
            raise RuntimeError('statement over the D1 100KB limit')
    return ''.join(parts), {'restaurants deleted': len(ids), 'statements': len(parts),
                            'restaurants kept': len(p['mixed'])}


def clean_data_js(content, p):
    """(new content, stats): deleted restaurants leave the file; a kept one's
    record is rebuilt the same way as its D1 row."""
    head, body = content.split('window.DATA = ', 1)
    recs = json.loads(body.rstrip().rstrip(';'))
    gone = {r['id'] for r in p['delete']}
    kept = {m['row']['id']: m for m in p['mixed']}
    stats = Counter()
    out = []
    for rec in recs:
        rid = rec.get('i')
        if rid in gone:
            stats['records removed'] += 1
            continue
        m = kept.get(rid)
        if m:
            nb, ro = m['rebuild'], m['regrade_only']
            if nb:
                rec['d'], rec['rs'], rec['ws'], rec['vg'] = (nb['inspection_date'], nb['risk_score'],
                                                           nb['weighted_score'], nb['vetted_grade'])
                rec['os'] = nb['original_score']
                rec['inf'] = list(nb['infractions'])
                rec['h'] = [list(x) for x in nb['history']]
                if 'vs' in rec:
                    rec['vs'] = [dict(s) for s in nb['summaries']]
                stats['records rebuilt'] += 1
            else:
                # The record's own history, less the removed visits.
                h = [[d, ro['regraded'].get(d, sc)] for d, sc in (rec.get('h') or [])
                     if d not in ro['removed_dates']]
                rec['h'] = h or [[rec.get('d', ''), rec.get('rs', 0)]]
                rec['ws'] = dp._weighted_from_history(rec['h'])
                vg = dp.calculate_vetted_grade(rec['ws'])
                if dp.calculate_vetted_grade(rec.get('rs') or 0) == 'F' or ro['text_f']:
                    vg = 'F'
                rec['vg'] = vg
                stats['records re-weighted'] += 1
            if rec.get('ic'):
                rec['ic'] = max(1, len(m['kept']))
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

    header, rows = cn.load_export(args.export)
    cache = jr.JudgmentCache(args.cache_dir).load()
    judgments = {}
    for row in rows:
        for i in row.get('inspections') or []:
            for v in _lines(i):
                nt = jr.normalize_text(str(v[2]))
                if nt and nt not in judgments:
                    rec = cache.get(jr.judgment_key(nt))
                    j = jr.judgment_from_record(rec) if rec else None
                    if j:
                        judgments[nt] = j
    p = plan(rows, judgments)
    print(f"{len(rows)} restaurants in the export taken {header.get('exported_at')}")
    for k, v in sorted(p['stats'].items()):
        print(f'  {k}: {v}')
    print(f"  inspections regraded from their lines: {len(p['regrade'])}")
    if args.seed:
        sql, n = seed_sql(p)
        with gzip.open(args.seed, 'wt', encoding='utf-8') as f:
            f.write(f"-- Score-0 cleanup: {n['restaurants deleted']} stored places deleted (Portland-metro "
                    f"hotels, RV parks, camps, pools; Dallas unscored visits), {n['restaurants kept']} "
                    f"rebuilt; export {header.get('exported_at')}.\n")
            f.write('-- Guarded: only score-0 or non-food inspections are deleted; safe to load twice.\n')
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
