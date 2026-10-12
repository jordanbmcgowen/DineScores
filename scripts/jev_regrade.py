#!/usr/bin/env python3
"""
jev_regrade.py: regrade every stored restaurant from its stored latest
inspection under the weekly run's decide rules (JEV_DECIDE=hazard,summary),
so the rows written before Jev read the violations agree with the rows
written after. Stdlib only.

    python3 scripts/jev_regrade.py --export d1_export.jsonl.gz --report-dir OUT \\
        [--judge --deadline-minutes M --max-ask N] \\
        [--seed data/d1_seed_<date>_jev_regrade.sql.gz] \\
        [--data-js public/data.js --write-data-js]

--export comes from the export-d1 workflow (scripts/d1_export.py). Without
--judge only cached judgments are used; with it, every uncached line is asked
first (TYPESAFE_API_KEY) and the answers go to ONE new cache file. The
report (report.md, transitions.csv, changes.jsonl.gz) is written either way;
--seed and --write-data-js produce the change itself.

What moves and what never does:
  - Scores never move: category stays in shadow, so the stored weighted and
    risk scores stand. Only the text-forced F, the chips and the summaries
    are recomputed, with the same functions the weekly run uses.
  - A text-forced F changes only when the stored text explains the stored
    decision. D1 keeps at most 20 lines per inspection, each cut at 400
    characters (write_d1_sql), so: today's regex must reproduce the stored F
    decision from the stored lines (the reader that wrote the row), and no
    line may be cut or missing. A cut line counts as complete when it is a
    cited pest item, or comes from a feed that prints only standard wording
    (STANDARD_TEXT_SOURCES), since only more of that wording follows. Rows the
    weekly run already wrote with Jev reproduce themselves (same text, same
    cached answer), so they only change where a rule changed.
  - Chips and summaries change only on complete text too: a pest named after
    the cut must not lose its chip to a reading of the first 400 characters.
  - Every UPDATE is guarded by id + inspection_date + updated_at: a row the
    weekly run rewrote after the export is left alone, and loading the seed
    twice changes nothing more.
  - data.js records get the same decision when they hold the same inspection
    (same date) and their own stored F decision matches D1's; their own
    weighted score decides their letter.
"""
import argparse
import csv
import gzip
import json
import os
import sys
import time
from collections import Counter, defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, REPO)

import dinescores_pipeline as dp   # noqa: E402
import jev_reader as jr            # noqa: E402

USE = frozenset({'hazard', 'summary'})   # the weekly run's JEV_DECIDE
LINE_CHARS = 400                         # write_d1_sql: str(text)[:400]
LINES_PER_INSPECTION = 20                # write_d1_sql: violations[:20]


# ─── LOADING ─────────────────────────────────────────────────────────────────

def load_export(path):
    """(header, rows) from d1_export.py's gzipped JSONL."""
    with gzip.open(path, 'rt', encoding='utf-8') as f:
        header = json.loads(f.readline())
        rows = [json.loads(line) for line in f if line.strip()]
    return header, rows


def stored_texts(row):
    """The stored violation lines of the row's latest inspection, or None."""
    latest = row.get('latest') or {}
    try:
        vio = json.loads(latest.get('violations') or '[]')
    except ValueError:
        return None
    if not isinstance(vio, list):
        return None
    return [str(v[2]) for v in vio if isinstance(v, (list, tuple)) and len(v) >= 3 and str(v[2]).strip()]


def _json_or_empty(s):
    if not s:
        return []
    try:
        v = json.loads(s)
    except ValueError:
        return None
    return v if isinstance(v, list) else None


# ─── ONE ROW ─────────────────────────────────────────────────────────────────

# Feeds that print only the code's standard wording of each cited item, never
# an inspector's note (checked on the export: every line is a code
# description, 392 distinct in San Francisco, 119 in NYC, 769 in Houston).
# A line cut at 400 characters there hides only more of the same wording.
STANDARD_TEXT_SOURCES = frozenset({'DataSF', 'NYC Open Data', 'Houston Health Dept'})


def complete(texts, source=''):
    """True when no stored line can be hiding text: under the line cap, and
    every line shorter than the cut, a cited pest item's boilerplate, or from
    a feed that prints only standard wording."""
    if len(texts) >= LINES_PER_INSPECTION:
        return False
    if source in STANDARD_TEXT_SOURCES:
        return True
    return all(len(t) < LINE_CHARS or jr.cited_pest_item(t) for t in texts)


def score_f(ws, rs):
    return dp.calculate_vetted_grade(ws or 0) == 'F' or dp.calculate_vetted_grade(rs or 0) == 'F'


def regrade_row(row, judgments):
    """The regraded values for one exported row, with the reasons for any
    part left as stored. `judgments` maps normalized text -> judgment."""
    out = {'id': row['id'], 'source': row.get('source') or '', 'inspection_date': row.get('inspection_date'),
           'updated_at': row.get('updated_at'), 'skip': None, 'kept': []}
    texts = stored_texts(row)
    if not row.get('latest'):
        out['skip'] = 'no_latest_inspection'
        return out
    if texts is None:
        out['skip'] = 'unreadable_violations'
        return out
    if not texts:
        out['skip'] = 'no_violations'
        return out
    if not row.get('inspection_date') or row.get('weighted_score') is None:
        out['skip'] = 'missing_date_or_score'
        return out
    old_inf = _json_or_empty(row.get('infractions'))
    old_sum = _json_or_empty(row.get('summaries'))
    if old_inf is None or old_sum is None:
        out['skip'] = 'unreadable_stored_json'
        return out

    ws, rs, vg = row.get('weighted_score'), row.get('risk_score'), row.get('vetted_grade')
    jm = {}
    for t in texts:
        j = judgments.get(jr.normalize_text(t))
        if j:
            jm[jr.normalize_text(t)] = j
    joined = '|||'.join(texts)
    f_regex = dp.has_active_pest_or_sewage(joined)
    if jm:
        f_new = jr.has_active_pest_or_sewage(texts, jm, use=USE)
        new_inf = jr.detect_infractions(texts, jm, use=USE)
        new_sum = jr.summarize_violations(texts, jm, use=USE)
    else:                                   # nothing judged: the regex, as the weekly would
        f_new = f_regex
        new_inf = dp.detect_infractions(joined)
        new_sum = dp.summarize_violations(joined)
        out['kept'].append('no_judgment')
    for s in new_sum:
        s['verbatim'] = s['verbatim'][:200]

    scored_f = score_f(ws, rs)
    stored_text_f = vg == 'F' and not scored_f
    consistent = vg == 'F' or vg == dp.calculate_vetted_grade(ws)
    text_f = stored_text_f
    if f_new != stored_text_f and not scored_f:
        if not consistent:
            out['kept'].append('grade_not_from_scores')
        elif f_regex != stored_text_f:
            out['kept'].append('f_not_explained_by_stored_text')
        elif not complete(texts, out['source']):
            out['kept'].append('f_text_cut')
        else:
            text_f = f_new
    new_vg = vg if not consistent else ('F' if (scored_f or text_f) else dp.calculate_vetted_grade(ws))

    if not complete(texts, out['source']):
        out['kept'].append('chips_text_cut')
        new_inf, new_sum = old_inf, old_sum

    out.update({
        'old_vg': vg, 'new_vg': new_vg, 'scored_f': scored_f,
        'old_text_f': stored_text_f, 'new_text_f': text_f, 'f_regex': f_regex, 'f_jev': f_new,
        'old_inf': old_inf, 'new_inf': new_inf, 'old_sum': old_sum, 'new_sum': new_sum,
        'judged_lines': len(jm), 'lines': len(texts),
    })
    out['changed'] = new_vg != vg or new_inf != old_inf or new_sum != old_sum
    return out


# ─── SQL ─────────────────────────────────────────────────────────────────────

UPDATE_HEAD = 'UPDATE restaurants SET vetted_grade = v.column3, infractions = v.column4, summaries = v.column5 FROM (VALUES\n'
UPDATE_TAIL = ('\n) AS v WHERE restaurants.id = v.column1 AND restaurants.inspection_date = v.column2 '
               'AND restaurants.updated_at IS v.column6;\n')


def seed_sql(results):
    """Guarded multi-row UPDATEs (40 rows / 80KB per statement) for the
    changed rows."""
    rows = []
    for r in results:
        if r.get('changed'):
            rows.append('(' + ','.join([
                dp._sql_quote(r['id']), dp._sql_quote(r['inspection_date']), dp._sql_quote(r['new_vg']),
                dp._sql_json(r['new_inf']), dp._sql_json(r['new_sum']), dp._sql_quote(r['updated_at']),
            ]) + ')')
    parts = []
    for batch in dp._d1_row_batches(rows):
        stmt = UPDATE_HEAD + ',\n'.join(batch) + UPDATE_TAIL
        if len(stmt.encode('utf-8')) >= 100_000:
            raise RuntimeError('statement over the D1 100KB limit')
        parts.append(stmt)
    return ''.join(parts), len(rows)


# ─── DATA.JS ─────────────────────────────────────────────────────────────────

def regrade_data_js(content, results_by_id):
    """(new content, stats). Applies a row's decision to the embedded record
    holding the same inspection."""
    head, body = content.split('window.DATA = ', 1)
    recs = json.loads(body.rstrip(';\n'))
    stats = Counter()
    for rec in recs:
        r = results_by_id.get(rec.get('i'))
        if not r or r.get('skip') or 'new_vg' not in r:
            continue
        if rec.get('d') != r['inspection_date']:
            stats['other_inspection'] += 1
            continue
        stats['same_inspection'] += 1
        ws, rs, vg = rec.get('ws'), rec.get('rs'), rec.get('vg')
        scored = score_f(ws, rs)
        text_f = vg == 'F' and not scored
        if r['new_text_f'] != r['old_text_f'] and text_f == r['old_text_f'] and not scored \
                and (vg == 'F' or vg == dp.calculate_vetted_grade(ws or 0)):
            new_vg = 'F' if r['new_text_f'] else dp.calculate_vetted_grade(ws or 0)
            if new_vg != vg:
                stats[f'grade {vg}->{new_vg}'] += 1
                rec['vg'] = new_vg
        if 'chips_text_cut' not in r['kept']:
            if rec.get('inf') != r['new_inf']:
                stats['chips_changed'] += 1
                rec['inf'] = list(r['new_inf'])
            # only the newest summaries_budget records carry 'vs' (write_data_js);
            # the rest load findings from the API and must stay slim
            if 'vs' in rec and rec.get('vs') != r['new_sum']:
                stats['summaries_changed'] += 1
                rec['vs'] = [dict(s) for s in r['new_sum']]
    return head + 'window.DATA = ' + json.dumps(recs, separators=(',', ':'), default=str) + ';\n', stats


# ─── REPORT ──────────────────────────────────────────────────────────────────

def report(results, header, judge_stats, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    skip = Counter(r['skip'] for r in results if r.get('skip'))
    kept = Counter(k for r in results if not r.get('skip') for k in r['kept'])
    trans = Counter()
    pests = Counter()
    chips = Counter()
    summ = 0
    for r in results:
        if r.get('skip'):
            continue
        if r['old_vg'] != r['new_vg']:
            trans[(r['source'], r['old_vg'], r['new_vg'])] += 1
        po, pn = 'pests' in r['old_inf'], 'pests' in r['new_inf']
        if po != pn:
            pests[(r['source'], 'added' if pn else 'removed')] += 1
        if r['old_inf'] != r['new_inf']:
            chips[r['source']] += 1
        if r['old_sum'] != r['new_sum']:
            summ += 1
    changed = sum(1 for r in results if r.get('changed'))
    tot = Counter()
    for (s, a, b), n in trans.items():
        tot[(a, b)] += n
    lines = [f"# Regrade dry run", '',
             f"Export taken {header.get('exported_at')}; {len(results)} restaurants; "
             f"{changed} rows change.", '']
    lines += ['| | restaurants |', '|---|---|']
    for k, v in sorted(skip.items()):
        lines.append(f'| skipped: {k} | {v} |')
    for k, v in sorted(kept.items()):
        lines.append(f'| kept as stored: {k} | {v} |')
    lines.append(f'| chips change | {sum(chips.values())} |')
    lines.append(f'| summaries change | {summ} |')
    lines += ['', '## Grade transitions', '', '| from | to | restaurants |', '|---|---|---|']
    lines += [f'| {a} | {b} | {n} |' for (a, b), n in sorted(tot.items())]
    lines += ['', '## By source (changed grades only)', '', '| source | from | to | restaurants |', '|---|---|---|---|']
    lines += [f'| {s} | {a} | {b} | {n} |' for (s, a, b), n in trans.most_common()]
    lines += ['', '## Pest chip', '', '| source | change | restaurants |', '|---|---|---|']
    lines += [f'| {s} | {c} | {n} |' for (s, c), n in pests.most_common()]
    if judge_stats:
        lines += ['', '## Jev', ''] + ['    ' + l for l in judge_stats]
    with open(os.path.join(out_dir, 'report.md'), 'w') as f:
        f.write('\n'.join(lines) + '\n')
    with open(os.path.join(out_dir, 'transitions.csv'), 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['source', 'grade_before', 'grade_after', 'restaurants'])
        for (s, a, b), n in trans.most_common():
            w.writerow([s, a, b, n])
    with gzip.open(os.path.join(out_dir, 'changes.jsonl.gz'), 'wt', encoding='utf-8') as f:
        for r in results:
            if r.get('changed') or r.get('kept'):
                f.write(json.dumps(r, ensure_ascii=False) + '\n')
    return '\n'.join(lines)


# ─── MAIN ────────────────────────────────────────────────────────────────────

def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('--export', required=True)
    ap.add_argument('--report-dir', required=True)
    ap.add_argument('--cache-dir', default=jr.CACHE_DIR)
    ap.add_argument('--judge', action='store_true', help='ask Jev for every uncached line first')
    ap.add_argument('--max-ask', type=int, default=None, help='cap on lines sent to Jev')
    ap.add_argument('--deadline-minutes', type=float, default=None)
    ap.add_argument('--label', default='regrade')
    ap.add_argument('--seed', default=None, help='write the guarded UPDATE seed here (.sql.gz)')
    ap.add_argument('--data-js', default=None)
    ap.add_argument('--write-data-js', action='store_true')
    args = ap.parse_args(argv)

    header, rows = load_export(args.export)
    print(f"{len(rows)} restaurants in the export taken {header.get('exported_at')}", flush=True)
    wanted = []
    seen = set()
    for row in rows:
        texts = stored_texts(row) or []
        if not complete(texts, row.get('source') or ''):
            continue                # cut text changes nothing, so it is never asked
        for t in texts:
            nt = jr.normalize_text(t)
            if nt and nt not in seen:
                seen.add(nt)
                wanted.append(nt)
    cache = jr.JudgmentCache(args.cache_dir).load()
    uncached = [t for t in wanted if cache.get(jr.judgment_key(t)) is None]
    print(f'{len(wanted)} distinct lines on complete stored inspections; {len(uncached)} not yet judged', flush=True)

    judge_stats = None
    judgments = {}
    if args.judge and uncached:
        ask = uncached if args.max_ask is None else uncached[:args.max_ask]
        deadline = time.time() + args.deadline_minutes * 60 if args.deadline_minutes else None
        reader = jr.JevReader(mode='shadow', client=jr.JevClient(deadline=deadline), cache=cache,
                              deadline=deadline)
        reader._cache_loaded = True
        if not reader.client.configured:
            raise SystemExit('--judge needs TYPESAFE_API_KEY')
        print(f'asking Jev about {len(ask)} lines...', flush=True)
        t0 = time.time()
        judgments.update(reader.judge_texts(ask, label=args.label))
        path = reader.flush_cache(args.label)
        judge_stats = reader.summary_lines() + [f'  elapsed {time.time() - t0:.0f}s, cache file {path}']
        print('\n'.join(judge_stats), flush=True)
    for t in wanted:
        if t not in judgments or judgments[t] is None:
            rec = cache.get(jr.judgment_key(t))
            judgments[t] = jr.judgment_from_record(rec) if rec else None
    judgments = {t: j for t, j in judgments.items() if j}
    print(f'{len(judgments)} of {len(wanted)} lines have a judgment', flush=True)

    results = [regrade_row(row, judgments) for row in rows]
    print(report(results, header, judge_stats, args.report_dir), flush=True)

    if args.seed:
        sql, n = seed_sql(results)
        with gzip.open(args.seed, 'wt', encoding='utf-8') as f:
            f.write(f"-- Jev regrade from stored text: export {header.get('exported_at')}, {n} rows.\n")
            f.write('-- Guarded by id + inspection_date + updated_at; safe to load twice.\n')
            f.write(sql)
        print(f'wrote {n} guarded row updates to {args.seed}', flush=True)
    if args.data_js:
        with open(args.data_js, encoding='utf-8') as f:
            content = f.read()
        new_content, stats = regrade_data_js(content, {r['id']: r for r in results})
        print('data.js: ' + ', '.join(f'{k} {v}' for k, v in sorted(stats.items())), flush=True)
        print(f'data.js: {len(content):,} -> {len(new_content):,} bytes', flush=True)
        if len(new_content) > dp.DATA_JS_MAX_BYTES:
            raise SystemExit(f'data.js would be over {dp.DATA_JS_MAX_BYTES:,} bytes')
        if args.write_data_js:
            with open(args.data_js, 'w', encoding='utf-8') as f:
                f.write(new_content)
            print(f'data.js written', flush=True)


if __name__ == '__main__':
    sys.exit(main())
