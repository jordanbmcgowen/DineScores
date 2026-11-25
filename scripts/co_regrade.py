#!/usr/bin/env python3
"""
co_regrade.py: regrade the stored inspections of the Colorado Front Range
counties on the tier the state gives each cited provision, and make a stored
Closed inspection an F. Stdlib only.

    python3 scripts/co_regrade.py --export d1_export.jsonl.gz --report-dir OUT \\
        [--seed data/d1_seed_<date>_co_regrade.sql.gz] \\
        [--data-js public/data.js --write-data-js]

--export is the per-source history export (export-d1.yml with `sources`,
scripts/d1_export.py --sources): each restaurant with every stored
inspection. The report (report.md, changes.jsonl.gz) is written either way;
--seed and --write-data-js produce the change itself.

Why: until 2026-11-25 these counties' lines were tiered on keywords
(classify_violation). Where a line cites a section of the state's violation
library, or prints the designation as El Paso County's do, the state's own
tier is known (dinescores_pipeline._co_line_tier). Where it states nothing,
the form item it is filed under has a usual tier (_co_item_tier: right on
nine stated lines in ten, against four in ten for keywords). The fetcher
reads lines this way (_co_tier) from that date on; this brings the rows
stored before into line. It ran twice that day: first on stated tiers only,
then with the item tiers.

What moves and what never does:
  - A line's tier moves to the tier it states, else to its form item's usual
    tier. A line with neither (no item number, or item 29) keeps its stored
    tier.
  - Inspections of another program (child care, body art, waste tyres,
    pools: scripts/co_nonfood.py) are never read as food-form lines, and a
    restaurant that has one is left to that cleanup.
  - An inspection's risk score is recomputed (100 - 5P - 2PF - C) only when
    its stored score is exactly what its stored tiers give, which holds for
    every stored inspection of these sources but one. An inspection at the
    20-line cap moves by the difference its stored lines make.
  - An inspection whose stored result is Closed is capped at
    MHD_CLOSED_RISK_CAP.
  - A restaurant's weighted score is rebuilt from as many of its newest
    inspections as reproduce the stored one (the run that wrote the row saw
    one, two or three of them); where none does, the row is left alone.
  - The text-forced F is not re-decided: the text has not changed. A stored F
    that the stored scores do not explain stays an F. A stored F the scores
    did explain needs the text decision only if the new scores no longer make
    it an F. Complete stored text is read as the weekly run reads it (Jev's
    cached judgment, else the regex). Cut text (D1 keeps 400 characters a
    line, and El Paso County's lines open with the code's wording, so the
    observation is what gets cut) is not read at all: the chips and summaries
    on the row were written by the run that read the full text, and they
    answer it. A roach, rodent or sewage summary keeps the F; a hazard-level
    summary with no known phrase, or a pest chip nothing explains, keeps the
    F as undecidable; otherwise no pest or sewage was found and the scores
    decide.
  - Chips and summaries do not move.
  - Every UPDATE is guarded: a restaurant row by id + inspection_date +
    updated_at, an inspection row by id + its stored risk score, so a row the
    weekly run rewrote after the export is left alone and loading the seed
    twice changes nothing more.
  - data.js records are moved the same way where they hold the same scores:
    a history pair or a latest score changes only when it equals the stored
    score of that inspection in the export.
"""
import argparse
import gzip
import json
import os
import sys
from collections import Counter, defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, REPO)
sys.path.insert(0, HERE)

import dinescores_pipeline as dp   # noqa: E402
import jev_reader as jr            # noqa: E402
import jev_regrade as jg           # noqa: E402
import co_nonfood as nf            # noqa: E402

POINTS = {'priority': 5, 'priority_foundation': 2, 'core': 1}
LINES_PER_INSPECTION = jg.LINES_PER_INSPECTION
SUMMARIES_KEPT = 5       # summarize_violations keeps the five most severe

# Summary phrases as they are stored (display names included).
_shown = dp.display_summary_text
FORCING_PHRASES = frozenset(q for p, h in jr.HAZARD_REQUIRED_BY_PHRASE.items() if h in jr.F_FORCING_HAZARDS
                            for q in (p, _shown(p)))
# hazards that set the pest chip and force nothing (flies, a cited pest item)
PEST_CHIP_PHRASES = frozenset(q for p in ('Flies/Gnats present', 'Pests found') for q in (p, _shown(p)))
KNOWN_PHRASES = frozenset(q for p in jr.SUMMARY_LEVEL_BY_PHRASE for q in (p, _shown(p)))


def stored_hazard(row):
    """What the run that read the full text recorded about pests and sewage:
    'forcing' (a roach, rodent or sewage summary), 'none', or 'unclear'."""
    chips = jg._json_or_empty(row.get('infractions'))
    summ = jg._json_or_empty(row.get('summaries'))
    if chips is None or summ is None or not all(isinstance(x, dict) for x in summ):
        return 'unclear'
    texts = [str(x.get('text') or '') for x in summ]
    if any(t in FORCING_PHRASES for t in texts):
        return 'forcing'
    critical = [x for x in summ if x.get('severity') == 'CRITICAL']
    if len(critical) >= SUMMARIES_KEPT:
        return 'unclear'                 # a sixth hazard-level summary may have been dropped
    if any(str(x.get('text') or '') not in KNOWN_PHRASES for x in critical):
        return 'unclear'                 # a hazard-level line with no phrase: may be vermin or sewage
    if 'pests' in chips and not any(t in PEST_CHIP_PHRASES for t in texts):
        return 'unclear'                 # a pest chip that no stored summary explains
    return 'none'


# ─── LOADING ─────────────────────────────────────────────────────────────────

def load_export(path):
    with gzip.open(path, 'rt', encoding='utf-8') as f:
        header = json.loads(f.readline())
        rows = [json.loads(line) for line in f if line.strip()]
    if not header.get('history'):
        raise SystemExit('this needs the per-source history export (d1_export.py --sources)')
    return header, rows


def _lines(insp):
    """[[category, tier, text], ...] of a stored inspection, or None."""
    try:
        vio = json.loads(insp.get('violations') or '[]')
    except ValueError:
        return None
    if not isinstance(vio, list):
        return None
    out = []
    for v in vio:
        if not (isinstance(v, (list, tuple)) and len(v) >= 3):
            return None
        out.append([v[0], v[1], str(v[2])])
    return out


def _risk(lines):
    return max(0, 100 - sum(POINTS.get(v[1], 1) for v in lines))


# ─── ONE INSPECTION ──────────────────────────────────────────────────────────

def regrade_inspection(insp):
    """New tiers and risk score for one stored inspection."""
    out = {'id': insp['id'], 'date': insp.get('inspection_date'), 'old_risk': insp.get('risk_score'),
           'new_risk': insp.get('risk_score'), 'lines': None, 'retiered': 0, 'stated': 0, 'item': 0, 'n': 0,
           'note': None, 'closed': False, 'changed': False}
    old = insp.get('risk_score')
    lines = _lines(insp)
    if lines is None or old is None:
        out['note'] = 'unreadable'
        return out
    out['n'] = len(lines)
    new_lines = []
    for cat, tier, text in lines:
        read, basis = dp._co_tier(text)
        if read:
            out[basis] += 1
            if read != tier:
                out['retiered'] += 1
        new_lines.append([cat, read or tier, text])
    risk = old
    if out['retiered']:
        if len(lines) < LINES_PER_INSPECTION and _risk(lines) == old:
            risk = _risk(new_lines)
        elif len(lines) >= LINES_PER_INSPECTION and old > 0:
            # lines beyond the cap are not stored: move by what the stored ones change
            delta = sum(POINTS.get(a[1], 1) - POINTS.get(b[1], 1) for a, b in zip(lines, new_lines))
            risk = max(0, min(100, old + delta))
            out['note'] = 'at_line_cap'
        else:
            out['note'] = 'score_not_from_stored_tiers'
            new_lines = lines
            out['retiered'] = 0
    if dp.MHD_CLOSED_RESULT_RE.match(str(insp.get('results') or '').strip()):
        out['closed'] = True
        risk = min(risk, dp.MHD_CLOSED_RISK_CAP)
    out['new_risk'] = risk
    if out['retiered']:
        out['lines'] = new_lines
    out['changed'] = risk != old or bool(out['retiered'])
    return out


# ─── ONE RESTAURANT ──────────────────────────────────────────────────────────

def _text_f(texts, source, judgments):
    """(decision, known): the text-forced F of the stored lines, read as the
    weekly run reads them; known is False when the stored text is cut."""
    joined = '|||'.join(texts)
    jm = {}
    for t in texts:
        j = judgments.get(jr.normalize_text(t))
        if j:
            jm[jr.normalize_text(t)] = j
    decision = jr.has_active_pest_or_sewage(texts, jm, use=jg.USE) if jm else dp.has_active_pest_or_sewage(joined)
    return bool(decision), jg.complete(texts, source)


def regrade_restaurant(row, judgments):
    """Regraded values for one exported restaurant and its inspections."""
    out = {'id': row['id'], 'source': row.get('source') or '', 'city': row.get('city'),
           'inspection_date': row.get('inspection_date'), 'updated_at': row.get('updated_at'),
           'old_rs': row.get('risk_score'), 'old_ws': row.get('weighted_score'), 'old_vg': row.get('vetted_grade'),
           'skip': None, 'kept': [], 'changed': False}
    exported = row.get('inspections') or []
    if any(nf.is_non_food(out['source'], i.get('inspection_type'), i.get('source_id')) for i in exported):
        # not a food-form report: its item numbers mean something else. The
        # non-food cleanup deletes or rebuilds these rows.
        out.update(inspections=[], insp_by_date={}, new_rs=out['old_rs'], new_ws=out['old_ws'],
                   new_vg=out['old_vg'], text_f=None, skip='has_non_food_inspections')
        return out
    insps = [regrade_inspection(i) for i in exported]
    out['inspections'] = insps
    out['insp_by_date'] = {i['date']: i for i in insps}
    ws, rs, vg = out['old_ws'], out['old_rs'], out['old_vg']
    out.update(new_rs=rs, new_ws=ws, new_vg=vg, text_f=None)
    if not insps or not out['inspection_date'] or ws is None or rs is None:
        out['skip'] = 'no_inspections_or_scores'
        return out
    latest = out['insp_by_date'].get(out['inspection_date'])
    if latest is None or latest['old_risk'] != rs:
        out['skip'] = 'latest_inspection_not_the_stored_one'
        return out
    if not any(i.get('changed') for i in insps):
        return out
    # newest first, as exported; the stored weighted score came from the 1-3 newest
    old_risks = [i['old_risk'] for i in insps]
    new_risks = [i['new_risk'] for i in insps]
    fits = [k for k in (1, 2, 3)
            if len(old_risks) >= k and dp._weighted_from_history([[None, s] for s in old_risks[:k]]) == ws]
    if not fits:
        out['skip'] = 'weighted_score_not_from_history'
        return out
    k = max(fits)
    new_ws = dp._weighted_from_history([[None, s] for s in new_risks[:k]])
    new_rs = latest['new_risk']
    scored_old = jg.score_f(ws, rs)
    scored_new = jg.score_f(new_ws, new_rs)
    if not (vg == 'F' or vg == dp.calculate_vetted_grade(ws)):
        out['skip'] = 'grade_not_from_scores'
        return out
    stored_lines = _lines(row_latest(row)) or []
    texts = [v[2] for v in stored_lines if v[2].strip()]
    decision, known = _text_f(texts, out['source'], judgments) if texts else (False, True)
    if not known:
        hazard = stored_hazard(row)
        decision = {'forcing': True, 'none': False}.get(hazard)    # None: undecidable
    out['text_f'] = decision
    if vg != 'F':
        text_f = False
    elif not scored_old:
        text_f = True                       # an F the scores never explained: text-forced, kept
    elif scored_new:
        text_f = False                      # still an F on its scores
    elif decision is None:
        text_f = True
        out['kept'].append('f_kept_undecidable')
    else:
        text_f = decision
        if decision:
            out['kept'].append('f_kept_pest_or_sewage' if known else 'f_kept_stored_hazard_summary')
        elif not known:
            out['kept'].append('f_lifted_on_stored_summaries')
    new_vg = 'F' if (scored_new or text_f) else dp.calculate_vetted_grade(new_ws)
    out.update(new_rs=new_rs, new_ws=new_ws, new_vg=new_vg, k=k)
    out['changed'] = (new_rs, new_ws, new_vg) != (rs, ws, vg)
    return out


def row_latest(row):
    for i in row.get('inspections') or []:
        if i.get('inspection_date') == row.get('inspection_date'):
            return i
    return {}


# ─── SQL ─────────────────────────────────────────────────────────────────────

REST_HEAD = ('UPDATE restaurants SET risk_score = v.column3, weighted_score = v.column4, '
             'vetted_grade = v.column5 FROM (VALUES\n')
REST_TAIL = ('\n) AS v WHERE restaurants.id = v.column1 AND restaurants.inspection_date = v.column2 '
             'AND restaurants.updated_at IS v.column6;\n')
INSP_HEAD = 'UPDATE inspections SET risk_score = v.column3, violations = v.column4 FROM (VALUES\n'
INSP_TAIL = '\n) AS v WHERE inspections.id = v.column1 AND inspections.risk_score = v.column2;\n'
RISK_HEAD = 'UPDATE inspections SET risk_score = v.column3 FROM (VALUES\n'
RISK_TAIL = '\n) AS v WHERE inspections.id = v.column1 AND inspections.risk_score = v.column2;\n'


def _statements(head, rows, tail):
    parts = []
    for batch in dp._d1_row_batches(rows):
        stmt = head + ',\n'.join(batch) + tail
        if len(stmt.encode('utf-8')) >= 100_000:
            raise RuntimeError('statement over the D1 100KB limit')
        parts.append(stmt)
    return parts


def seed_sql(results):
    """(sql, counts): guarded UPDATEs for the changed inspection and
    restaurant rows. Inspections first, like write_d1_sql."""
    with_lines, risk_only, rest = [], [], []
    for r in results:
        if r.get('skip'):
            continue
        for i in r['inspections']:
            if not i.get('changed'):
                continue
            if i['lines'] is not None:
                with_lines.append('(' + ','.join([
                    dp._sql_quote(i['id']), dp._sql_quote(i['old_risk']), dp._sql_quote(i['new_risk']),
                    dp._sql_json(i['lines'])]) + ')')
            else:
                risk_only.append('(' + ','.join([
                    dp._sql_quote(i['id']), dp._sql_quote(i['old_risk']), dp._sql_quote(i['new_risk'])]) + ')')
        if r.get('changed'):
            rest.append('(' + ','.join([
                dp._sql_quote(r['id']), dp._sql_quote(r['inspection_date']), dp._sql_quote(r['new_rs']),
                dp._sql_quote(r['new_ws']), dp._sql_quote(r['new_vg']), dp._sql_quote(r['updated_at'])]) + ')')
    parts = (_statements(INSP_HEAD, with_lines, INSP_TAIL) + _statements(RISK_HEAD, risk_only, RISK_TAIL)
             + _statements(REST_HEAD, rest, REST_TAIL))
    return ''.join(parts), {'inspections': len(with_lines) + len(risk_only), 'restaurants': len(rest),
                            'statements': len(parts)}


# ─── DATA.JS ─────────────────────────────────────────────────────────────────

def regrade_data_js(content, results_by_id):
    """(new content, stats). Moves the embedded records of the regraded
    restaurants: a score changes only where it equals the stored score of
    that inspection in the export."""
    head, body = content.split('window.DATA = ', 1)
    recs = json.loads(body.rstrip(';\n'))
    stats = Counter()
    for rec in recs:
        r = results_by_id.get(rec.get('i'))
        if not r or r.get('skip'):
            continue
        stats['records_of_these_sources'] += 1
        by_date = r['insp_by_date']
        ws, rs, vg = rec.get('ws'), rec.get('rs'), rec.get('vg')
        new_h = []
        moved = False
        for pair in rec.get('h') or []:
            d, score = pair[0], pair[1]
            i = by_date.get(d)
            if i is not None and i['old_risk'] == score and i['new_risk'] != score:
                new_h.append([d, i['new_risk']])
                moved = True
            else:
                new_h.append([d, score])
        new_rs = rs
        i = by_date.get(rec.get('d'))
        if i is not None and i['old_risk'] == rs and i['new_risk'] != rs:
            new_rs = i['new_risk']
            moved = True
        if not moved:
            continue
        new_ws = dp._weighted_from_history(new_h or [[rec.get('d', ''), new_rs]])
        scored_old = jg.score_f(ws, rs)
        scored_new = jg.score_f(new_ws, new_rs)
        if not (vg == 'F' or vg == dp.calculate_vetted_grade(ws or 0)):
            stats['grade_not_from_scores'] += 1
            continue
        if vg != 'F':
            text_f = False
        elif not scored_old:
            text_f = True
        elif scored_new:
            text_f = False
        elif rec.get('d') == r['inspection_date'] and r.get('text_f') is not None:
            text_f = bool(r['text_f'])
        else:
            text_f = True                  # another inspection, or undecidable: the F stays
            stats['f_kept_undecidable'] += 1
        new_vg = 'F' if (scored_new or text_f) else dp.calculate_vetted_grade(new_ws)
        rec['h'], rec['rs'], rec['ws'] = new_h, new_rs, new_ws
        stats['records_moved'] += 1
        if new_vg != vg:
            stats[f'grade {vg}->{new_vg}'] += 1
            rec['vg'] = new_vg
    return head + 'window.DATA = ' + json.dumps(recs, separators=(',', ':'), default=str) + ';\n', stats


# ─── REPORT ──────────────────────────────────────────────────────────────────

def report(results, header, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    by = defaultdict(Counter)
    trans = Counter()
    for r in results:
        b = by[r['source']]
        b['restaurants'] += 1
        if r.get('skip'):
            b[f"skipped: {r['skip']}"] += 1
        for k in r['kept']:
            b[f'kept: {k}'] += 1
        for i in r['inspections']:
            b['inspections'] += 1
            b['lines'] += i['n']
            b['lines on the state tier'] += i['stated']
            b['lines on the item tier'] += i['item']
            b['lines re-tiered'] += i['retiered']
            if i.get('note'):
                b[f"inspection: {i['note']}"] += 1
            if not r.get('skip') and i.get('changed'):
                b['inspections changed'] += 1
                if i['new_risk'] > i['old_risk']:
                    b['inspection score up'] += 1
                elif i['new_risk'] < i['old_risk']:
                    b['inspection score down'] += 1
                if i['closed'] and i['new_risk'] != i['old_risk']:
                    b['closed inspections capped'] += 1
        b[f"grade before {r['old_vg']}"] += 1
        b[f"grade after {r['new_vg']}"] += 1
        if r.get('changed'):
            b['restaurants changed'] += 1
        if r['old_vg'] != r['new_vg']:
            trans[(r['source'], r['old_vg'], r['new_vg'])] += 1
    lines = ['# Colorado Front Range regrade', '',
             f"Export taken {header.get('exported_at')}; {len(results)} restaurants.", '',
             '| source | restaurants | inspections | lines | tier stated | item tier | re-tiered '
             '| inspections changed | restaurants changed | A | B | C | F | A after | B after | C after | F after |',
             '|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|']
    for src in sorted(by):
        b = by[src]
        lines.append('| ' + ' | '.join(str(x) for x in [
            src, b['restaurants'], b['inspections'], b['lines'], b['lines on the state tier'],
            b['lines on the item tier'], b['lines re-tiered'], b['inspections changed'], b['restaurants changed'],
            b['grade before A'], b['grade before B'], b['grade before C'], b['grade before F'],
            b['grade after A'], b['grade after B'], b['grade after C'], b['grade after F']]) + ' |')
    lines += ['', '## Grade moves', '', '| source | from | to | restaurants |', '|---|---|---|---|']
    lines += [f'| {s} | {a} | {b} | {n} |' for (s, a, b), n in sorted(trans.items())]
    lines += ['', '## Notes', '', '| source | note | count |', '|---|---|---|']
    for src in sorted(by):
        for k, v in sorted(by[src].items()):
            if k.startswith(('skipped', 'kept', 'inspection:', 'inspection score', 'closed')):
                lines.append(f'| {src} | {k} | {v} |')
    text = '\n'.join(lines) + '\n'
    with open(os.path.join(out_dir, 'report.md'), 'w') as f:
        f.write(text)
    with gzip.open(os.path.join(out_dir, 'changes.jsonl.gz'), 'wt', encoding='utf-8') as f:
        for r in results:
            if r.get('changed') or r.get('skip') or r['kept']:
                slim = {k: v for k, v in r.items() if k not in ('inspections', 'insp_by_date')}
                slim['inspections'] = [{k: v for k, v in i.items() if k != 'lines'} for i in r['inspections']]
                f.write(json.dumps(slim, ensure_ascii=False) + '\n')
    return text


# ─── MAIN ────────────────────────────────────────────────────────────────────

def load_judgments(rows, cache_dir):
    """Cached Jev judgments for the latest inspection's lines of every row."""
    cache = jr.JudgmentCache(cache_dir).load()
    out = {}
    for row in rows:
        for v in _lines(row_latest(row)) or []:
            nt = jr.normalize_text(v[2])
            if nt and nt not in out:
                rec = cache.get(jr.judgment_key(nt))
                j = jr.judgment_from_record(rec) if rec else None
                if j:
                    out[nt] = j
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('--export', required=True)
    ap.add_argument('--report-dir', required=True)
    ap.add_argument('--cache-dir', default=jr.CACHE_DIR)
    ap.add_argument('--seed', default=None, help='write the guarded UPDATE seed here (.sql.gz)')
    ap.add_argument('--data-js', default=None)
    ap.add_argument('--write-data-js', action='store_true')
    args = ap.parse_args(argv)

    header, rows = load_export(args.export)
    print(f"{len(rows)} restaurants in the export taken {header.get('exported_at')}", flush=True)
    judgments = load_judgments(rows, args.cache_dir)
    print(f'{len(judgments)} cached judgments for their latest inspections', flush=True)
    results = [regrade_restaurant(row, judgments) for row in rows]
    print(report(results, header, args.report_dir), flush=True)

    if args.seed:
        sql, n = seed_sql(results)
        with gzip.open(args.seed, 'wt', encoding='utf-8') as f:
            f.write(f"-- Colorado Front Range regrade on the state's tiers: export {header.get('exported_at')}, "
                    f"{n['inspections']} inspections, {n['restaurants']} restaurants.\n")
            f.write('-- Guarded (restaurants: id + inspection_date + updated_at; inspections: id + stored '
                    'risk score); safe to load twice.\n')
            f.write(sql)
        print(f"wrote {n['inspections']} inspection and {n['restaurants']} restaurant updates "
              f"({n['statements']} statements) to {args.seed}", flush=True)
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
            print('data.js written', flush=True)


if __name__ == '__main__':
    sys.exit(main())
