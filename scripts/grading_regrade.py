#!/usr/bin/env python3
"""
grading_regrade.py: bring the stored rows in line with the grading rules of
2026-11-27 (docs/grading-review-2026-11-27.md). Stdlib only.

    python3 scripts/grading_regrade.py --history hist.jsonl.gz --full full.jsonl.gz \\
        [--seed data/d1_seed_<date>_grading_rules.sql.gz] \\
        [--data-js public/data.js --write-data-js] [--report docs/...md]

--history is export-d1.yml with `sources` = SCORE_SOURCES joined by '|' (every
stored inspection of those sources); --full is the plain export (every
restaurant's row and latest inspection). Both read D1 itself.

What it changes, and nothing else:
  1. Fort Worth, Frisco: every stored inspection with the city's demerit
     total is rescored to the Texas form score, 100 - demerits, and its lines
     take their form item's tier (the fetcher's rule now).
  2. Rows that were never inspections are removed: Chicago 'Out of Business',
     'No Entry', 'Not Ready', 'Business Not Located'; Sacramento 'Not
     Applicable'; Boston 'HE_OutBus'. A restaurant whose NEWEST stored row
     says it is out of business is deleted (the city's last word is that it
     is gone); one left with no inspection is deleted; any other is rebuilt
     from its newest remaining inspection.
  3. The agency's verdict caps the inspection's score: Chicago 'Fail', San
     Francisco 'Closure', New York City 'Establishment Closed by DOHMH',
     Florida emergency orders at 65 (an F); San Francisco 'Conditional Pass'
     at 79 (a C).
  4. Every restaurant whose F came from its text alone is read again: when
     every pest line of its latest inspection shows only dead or trapped
     pests, or ants, the grade becomes no better than C. An F stays wherever
     the stored text cannot settle it (no pest line among the 20 stored, or
     a pest line cut at 400 characters).

Every UPDATE and DELETE is guarded on what the export saw (the row's date,
weighted score, grade and updated_at; an inspection's old score), so a row
the weekly run rewrote since is left alone and loading the seed twice
changes nothing more. The same records change in data.js.
"""
import argparse
import gzip
import json
import os
import sys
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, REPO)
sys.path.insert(0, HERE)

import dinescores_pipeline as dp   # noqa: E402
import jev_reader as jr            # noqa: E402
import jev_regrade as jg           # noqa: E402
import co_nonfood as cn            # noqa: E402

TX_FORM_SOURCES = ('Fort Worth Health Dept', 'Frisco Health Dept')
SCORE_SOURCES = TX_FORM_SOURCES + (
    'Chicago Open Data', 'DataSF', 'NYC Open Data', 'Florida DBPR',
    'Sacramento County Open Data', 'Boston Open Data')
OUT_OF_BUSINESS = {'Chicago Open Data': 'out of business', 'Boston Open Data': 'he_outbus'}


def _lines(insp):
    try:
        vio = json.loads((insp or {}).get('violations') or '[]')
    except (ValueError, TypeError):
        return []
    return [[str(v[0]), str(v[1]), str(v[2])] for v in vio
            if isinstance(v, (list, tuple)) and len(v) >= 3 and str(v[2]).strip()]


def _int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def not_inspected(source, results):
    r = (results or '').strip()
    if source == 'Chicago Open Data':
        return r.lower() in dp.CHICAGO_NOT_INSPECTED
    if source == 'Sacramento County Open Data':
        return r.upper() in dp.SACRAMENTO_NOT_INSPECTED
    if source == 'Boston Open Data':
        return r in dp.BOSTON_NOT_INSPECTED
    return False


def verdict_cap(source, results):
    r = (results or '').strip()
    if source == 'Chicago Open Data':
        return dp.VERDICT_FAIL_RISK_CAP if r.lower() == 'fail' else None
    if source == 'DataSF':
        return dp.SF_VERDICT_CAPS.get(r.lower())
    if source == 'NYC Open Data':
        return dp.VERDICT_FAIL_RISK_CAP if dp.NYC_CLOSED_RE.match(r) else None
    if source == 'Florida DBPR':
        return dp.VERDICT_FAIL_RISK_CAP if dp.FL_EMERGENCY_ORDER_RE.match(r) else None
    return None


def regrade_inspection(source, insp):
    """(new risk, new lines or None) for one stored inspection."""
    risk = _int(insp.get('risk_score'))
    new_lines = None
    if source in TX_FORM_SOURCES:
        lines = _lines(insp)
        retiered = [[c, dp._tx_form_tier(t)[0] or s, t] for c, s, t in lines]
        if retiered != lines:
            new_lines = retiered
        demerits = _int(insp.get('original_score'))
        if demerits is not None:
            risk = max(0, min(100, 100 - demerits))
        elif lines and len(lines) < jg.LINES_PER_INSPECTION:
            form = dp._tx_form_score([t for _, _, t in lines])
            if form is not None:
                risk = form
    cap = verdict_cap(source, insp.get('results'))
    if cap is not None and risk is not None:
        risk = min(risk, cap)
    return risk, new_lines


def read_text(lines, source, judgments):
    """(cap, settled) for the stored lines of one inspection: cap is 'F', 'C'
    or None as the weekly run would read it; settled is False when the stored
    text cannot be trusted to carry the whole finding."""
    texts = [t for _, _, t in lines]
    flagged = []
    for t in texts:
        j = judgments.get(jr.normalize_text(t))
        flagged.append((t, bool(jr.decide(t, j, None, None, jg.USE)['forces_f'])))
    cap = dp.text_cap_of_lines(flagged)
    cut = any(f and len(t) >= jg.LINE_CHARS for t, f in flagged)
    full = len(texts) >= jg.LINES_PER_INSPECTION
    return cap, not cut and not (cap is None and full) and not (cap == 'C' and full)


def stored_cap(row, latest_lines, judgments, latest_unchanged=True):
    """The text cap of a stored restaurant row under the new rule."""
    vg, rs, ws = row.get('vetted_grade'), _int(row.get('risk_score')) or 0, _int(row.get('weighted_score')) or 0
    cap, settled = read_text(latest_lines, row.get('source') or '', judgments)
    if not latest_unchanged:
        return cap                                   # a rebuilt row is read like a fresh one
    if vg != 'F':
        return None                                  # the run that read the full text found nothing
    if rs >= 70 and ws >= 70:                        # an F from the text alone
        return 'C' if (cap == 'C' and settled) else 'F'
    # an F the score explains: the text decision was never stored, read it
    return cap if settled or cap == 'F' else ('F' if cap is None and not settled else cap)


def grade(rs, ws, cap):
    vg = dp.cap_grade(dp.calculate_vetted_grade(ws), cap)
    return 'F' if dp.calculate_vetted_grade(rs) == 'F' else vg


def plan_history(rows, judgments):
    out = []
    for row in rows:
        src = row.get('source') or ''
        insps = sorted(row.get('inspections') or [], key=lambda i: i.get('inspection_date') or '', reverse=True)
        if not insps:
            continue
        item = {'row': row, 'delete': False, 'remove': [], 'insp_updates': [], 'new': None, 'rebuild': None,
                'why': Counter()}
        newest_gone = (OUT_OF_BUSINESS.get(src) is not None
                       and (insps[0].get('results') or '').strip().lower() == OUT_OF_BUSINESS[src])
        kept = []
        for i in insps:
            if not_inspected(src, i.get('results')):
                item['remove'].append(i)
                item['why']['not an inspection'] += 1
                continue
            risk, new_lines = regrade_inspection(src, i)
            old = _int(i.get('risk_score'))
            if risk != old or new_lines is not None:
                item['insp_updates'].append((i, old, risk, new_lines))
                if risk != old:
                    item['why']['verdict cap' if src not in TX_FORM_SOURCES else 'Texas form score'] += 1
            kept.append(dict(i, risk_score=risk, violations=json.dumps(new_lines) if new_lines is not None
                             else i.get('violations')))
        if newest_gone or not kept:
            item['delete'] = True
            item['why']['out of business' if newest_gone else 'no inspection left'] += 1
            out.append(item)
            continue
        latest_unchanged = kept[0].get('inspection_date') == row.get('inspection_date')
        hist = [[i.get('inspection_date'), i.get('risk_score')] for i in kept]
        if not latest_unchanged:
            nb = cn._rebuild(row, kept, judgments)
            cap = stored_cap(row, _lines(kept[0]), judgments, latest_unchanged=False)
            nb['vetted_grade'] = grade(nb['risk_score'], nb['weighted_score'], cap)
            nb['tc'] = cap == 'C' and nb['vetted_grade'] == 'C'
            item['rebuild'] = nb
            new = {'risk_score': nb['risk_score'], 'weighted_score': nb['weighted_score'],
                   'vetted_grade': nb['vetted_grade']}
        else:
            rs = kept[0].get('risk_score') or 0
            old_hist = [[i.get('inspection_date'), _int(i.get('risk_score'))] for i in insps]
            ws = _int(row.get('weighted_score'))
            # Rows written from one run's fetch often carry the latest score
            # alone: the stored weighted score moves only as far as the
            # history it was built from did.
            if ws == dp._weighted_from_history(old_hist) or src in TX_FORM_SOURCES:
                ws = dp._weighted_from_history(hist)
            elif ws == _int(row.get('risk_score')):
                ws = rs
            cap = stored_cap(row, _lines(kept[0]), judgments)
            vg = grade(rs, ws, cap)
            new = {'risk_score': rs, 'weighted_score': ws, 'vetted_grade': vg}
            item['tc'] = cap == 'C' and vg == 'C'
        item['new'] = new
        item['history'] = hist[:dp.MAX_HISTORY]
        item['dates_removed'] = {i.get('inspection_date') for i in item['remove']}
        changed = (item['remove'] or item['insp_updates'] or
                   (new['risk_score'], new['weighted_score'], new['vetted_grade']) !=
                   (_int(row.get('risk_score')), _int(row.get('weighted_score')), row.get('vetted_grade')))
        if changed:
            out.append(item)
    return out


def plan_pests(rows, judgments, skip_sources):
    """Restaurants outside the history sources whose F came from text alone
    and whose stored pest lines are all minor: the new grade."""
    out = []
    for row in rows:
        if row.get('source') in skip_sources or row.get('vetted_grade') != 'F':
            continue
        rs, ws = _int(row.get('risk_score')) or 0, _int(row.get('weighted_score')) or 0
        if rs < 70 or ws < 70:
            continue
        if stored_cap(row, _lines(row.get('latest')), judgments) != 'C':
            continue
        out.append({'row': row, 'new': {'risk_score': rs, 'weighted_score': ws,
                                        'vetted_grade': grade(rs, ws, 'C')}, 'tc': True})
    return out


def _guard(row):
    q = dp._sql_quote
    g = (f"id = {q(row['id'])} AND inspection_date IS {q(row.get('inspection_date'))} "
         f"AND weighted_score IS {q(_int(row.get('weighted_score')))} "
         f"AND vetted_grade IS {q(row.get('vetted_grade'))}")
    if row.get('updated_at'):
        g += f" AND updated_at = {q(row['updated_at'])}"
    return g


def seed_sql(history, pests):
    q = dp._sql_quote
    parts = []
    for it in history:
        row = it['row']
        if it['delete']:
            parts.append(f"DELETE FROM inspections WHERE restaurant_id = {q(row['id'])} "
                         f"AND EXISTS (SELECT 1 FROM restaurants WHERE {_guard(row)});\n")
            parts.append(f"DELETE FROM restaurants WHERE {_guard(row)};\n")
            continue
        for i in it['remove']:
            parts.append(f"DELETE FROM inspections WHERE id = {q(i['id'])} AND restaurant_id = {q(row['id'])} "
                         f"AND results = {q(i.get('results'))};\n")
        for i, old, risk, new_lines in it['insp_updates']:
            sets = [f"risk_score = {q(risk)}"]
            if new_lines is not None:
                sets.append(f"violations = {dp._sql_json(new_lines)}")
            parts.append(f"UPDATE inspections SET {', '.join(sets)} WHERE id = {q(i['id'])} "
                         f"AND restaurant_id = {q(row['id'])} AND risk_score IS {q(old)};\n")
        nb, new = it['rebuild'], it['new']
        if nb:
            parts.append(
                "UPDATE restaurants SET "
                f"inspection_date = {q(nb['inspection_date'])}, original_score = {q(nb['original_score'])}, "
                f"risk_score = {q(nb['risk_score'])}, weighted_score = {q(nb['weighted_score'])}, "
                f"vetted_grade = {q(nb['vetted_grade'])}, infractions = {dp._sql_json(nb['infractions'])}, "
                f"summaries = {dp._sql_json(nb['summaries'])}, "
                f"source_url = COALESCE((SELECT source_url FROM inspections WHERE id = {q(nb['inspection_id'])}), "
                f"source_url) WHERE {_guard(row)};\n")
        elif (new['risk_score'], new['weighted_score'], new['vetted_grade']) != (
                _int(row.get('risk_score')), _int(row.get('weighted_score')), row.get('vetted_grade')):
            parts.append(f"UPDATE restaurants SET risk_score = {q(new['risk_score'])}, "
                         f"weighted_score = {q(new['weighted_score'])}, vetted_grade = {q(new['vetted_grade'])} "
                         f"WHERE {_guard(row)};\n")
        if it['remove']:
            parts.append("UPDATE restaurants SET inspection_count = (SELECT COUNT(*) FROM inspections "
                         f"WHERE inspections.restaurant_id = restaurants.id) WHERE id = {q(row['id'])};\n")
    for it in pests:
        parts.append(f"UPDATE restaurants SET vetted_grade = {q(it['new']['vetted_grade'])} "
                     f"WHERE {_guard(it['row'])};\n")
    for stmt in parts:
        if len(stmt.encode('utf-8')) >= 100_000:
            raise RuntimeError('statement over the D1 100KB limit')
    return ''.join(parts), len(parts)


def regrade_data_js(content, history, pests):
    head, body = content.split('window.DATA = ', 1)
    recs = json.loads(body.rstrip().rstrip(';'))
    by_id = {it['row']['id']: it for it in history}
    pest_by_id = {it['row']['id']: it for it in pests}
    stats = Counter()
    out = []
    for rec in recs:
        rid = rec.get('i')
        it = by_id.get(rid)
        if it is None:
            p = pest_by_id.get(rid)
            if p and rec.get('vg') == 'F' and rec.get('d') == p['row'].get('inspection_date'):
                rec['vg'] = p['new']['vetted_grade']
                rec['tc'] = 1
                stats['pest finding now a C cap'] += 1
            out.append(rec)
            continue
        if it['delete']:
            stats['records removed'] += 1
            continue
        nb = it['rebuild']
        if nb:
            rec['d'], rec['rs'], rec['ws'], rec['vg'] = (nb['inspection_date'], nb['risk_score'],
                                                       nb['weighted_score'], nb['vetted_grade'])
            rec['os'] = nb['original_score']
            rec['inf'] = list(nb['infractions'])
            rec['h'] = [list(x) for x in nb['history']]
            if 'vs' in rec:
                rec['vs'] = [dict(s) for s in nb['summaries']]
            tc = nb['tc']
            stats['records rebuilt'] += 1
        elif rec.get('d') == it['row'].get('inspection_date'):
            scores = dict(it['history'])
            h = [[d, scores.get(d, sc)] for d, sc in (rec.get('h') or []) if d not in it['dates_removed']]
            rec['h'] = h or [[rec.get('d', ''), it['new']['risk_score']]]
            rec['rs'] = it['new']['risk_score']
            rec['ws'] = dp._weighted_from_history(rec['h'])
            cap = 'F' if it['new']['vetted_grade'] == 'F' and dp.calculate_vetted_grade(
                it['new']['weighted_score']) != 'F' else ('C' if it.get('tc') else None)
            rec['vg'] = grade(rec['rs'], rec['ws'], cap)
            tc = it.get('tc') and rec['vg'] == 'C'
            stats['records regraded'] += 1
        else:
            stats['records on another inspection, left alone'] += 1
            out.append(rec)
            continue
        if tc:
            rec['tc'] = 1
        else:
            rec.pop('tc', None)
        if it['remove'] and rec.get('ic'):
            rec['ic'] = max(1, rec['ic'] - len(it['remove']))
        out.append(rec)
    return head + 'window.DATA = ' + json.dumps(out, separators=(',', ':'), default=str) + ';\n', stats


def load(path, need_history):
    with gzip.open(path, 'rt', encoding='utf-8') as f:
        header = json.loads(f.readline())
        rows = [json.loads(line) for line in f if line.strip()]
    if bool(header.get('history')) != need_history:
        raise SystemExit(f'{path}: wrong kind of export')
    return header, rows


def load_judgments(line_sets, cache_dir):
    cache = jr.JudgmentCache(cache_dir).load()
    out = {}
    for lines in line_sets:
        for _, _, t in lines:
            nt = jr.normalize_text(t)
            if nt and nt not in out:
                rec = cache.get(jr.judgment_key(nt))
                j = jr.judgment_from_record(rec) if rec else None
                if j:
                    out[nt] = j
    return out


def summarize(history, pests):
    s = Counter()
    moves = Counter()
    examples = {}
    for it in history:
        src = it['row'].get('source')
        if it['delete']:
            s[f'{src}: restaurants deleted ({", ".join(sorted(it["why"]))})'] += 1
            continue
        old, new = it['row'].get('vetted_grade'), it['new']['vetted_grade']
        if it['remove']:
            s[f'{src}: non-inspection rows removed'] += len(it['remove'])
        if it['insp_updates']:
            s[f'{src}: inspections rescored or re-tiered'] += len(it['insp_updates'])
        moves[(src, old, new)] += 1
        if old != new:
            examples.setdefault((src, old, new), []).append(it)
    for it in pests:
        src = it['row'].get('source')
        moves[(src, 'F', it['new']['vetted_grade'])] += 1
        s['pest rule: F from text alone, now capped at C'] += 1
        examples.setdefault(('pest', src), []).append(it)
    return s, moves, examples


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('--history', required=True)
    ap.add_argument('--full', required=True)
    ap.add_argument('--cache-dir', default=jr.CACHE_DIR)
    ap.add_argument('--seed', default=None)
    ap.add_argument('--data-js', default=None)
    ap.add_argument('--write-data-js', action='store_true')
    ap.add_argument('--dump', default=None, help='write the plan summary as JSON (for the report)')
    args = ap.parse_args(argv)

    hheader, hrows = load(args.history, True)
    fheader, frows = load(args.full, False)
    missing = set(SCORE_SOURCES) - set(hheader.get('sources') or [])
    if missing:
        raise SystemExit(f'the history export lacks {sorted(missing)}')
    hrows = [r for r in hrows if r.get('source') in SCORE_SOURCES]
    judgments = load_judgments(
        [_lines(i) for r in hrows for i in (r.get('inspections') or [])[:1]] +
        [_lines(r.get('latest')) for r in frows if r.get('vetted_grade') == 'F'], args.cache_dir)
    print(f'{len(hrows)} history rows, {len(frows)} rows in all, {len(judgments)} cached readings')
    history = plan_history(hrows, judgments)
    pests = plan_pests(frows, judgments, set(SCORE_SOURCES))
    s, moves, examples = summarize(history, pests)
    for k in sorted(s):
        print(f'  {s[k]:>6}  {k}')
    by_move = Counter()
    for (src, old, new), n in moves.items():
        if old != new:
            by_move[f'{old} -> {new}'] += n
    print('grade moves:', dict(sorted(by_move.items())))
    if args.dump:
        with open(args.dump, 'w') as f:
            json.dump({'stats': s, 'moves': [[a, b, c, n] for (a, b, c), n in sorted(moves.items())],
                       'examples': {' | '.join(map(str, k)): [
                           {'name': it['row'].get('name'), 'city': it['row'].get('city'),
                            'date': it['row'].get('inspection_date'),
                            'old': [it['row'].get('risk_score'), it['row'].get('weighted_score'),
                                    it['row'].get('vetted_grade')],
                            'new': it['new'],
                            'lines': [t[:160] for _, _, t in (_lines(it['row'].get('latest')) or _lines(
                                (it['row'].get('inspections') or [{}])[0]))
                                      if dp.has_active_pest_or_sewage(t)][:3]}
                           for it in v[:6]] for k, v in examples.items()}}, f, indent=1, default=str)
    if args.seed:
        sql, n = seed_sql(history, pests)
        with gzip.open(args.seed, 'wt', encoding='utf-8') as f:
            f.write(sql)
        print(f'seed: {n} statements, {os.path.getsize(args.seed)} bytes -> {args.seed}')
    if args.data_js:
        with open(args.data_js, encoding='utf-8') as f:
            content, stats = regrade_data_js(f.read(), history, pests)
        print('data.js:', dict(stats), f'{len(content.encode())} bytes')
        if len(content.encode('utf-8')) > dp.DATA_JS_MAX_BYTES:
            raise SystemExit('data.js would pass the size cap')
        if args.write_data_js:
            with open(args.data_js, 'w', encoding='utf-8') as f:
                f.write(content)


if __name__ == '__main__':
    main()
