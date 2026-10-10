#!/usr/bin/env python3
"""
jev_shadow.py: shadow the Jev reader over the committed D1 seeds + data.js
and measure what it would change. Publishes nothing; changes no grade.

    python3 scripts/jev_shadow.py --out data/jev_shadow \
        [--sample 20000] [--max-ask N] [--no-network] [--deadline-minutes M]
        [--floor category=0.5 --floor hazard=0.5 ...] [--verdicts data/jev_shadow/handcheck_verdicts.csv]

What it judges (deduped by normalized text): every text behind a text-forced
F, every text summarized "Sewage backup/issue", every regex-unclassified
text, and a stratified sample (default ~20,000) of the rest weighted by line
count. Judgments come from data/jev_judgments/ when cached, else from the API
(JEV_MODE is forced to shadow here; TYPESAFE_API_KEY must be set).

Outputs (CSV + JSON) in --out, plus report_tables.md with the numbers the
report quotes. Grade impact is computed in code with today's cutoffs:

  - hazard: Jev's hazard (above its floor) replaces the pest/sewage regex per
    line; the F-forcing set is jev_reader.F_FORCING_HAZARDS.
  - score: re-scored ONLY where DineScores computes the score from regex
    tiers (SCORE_POLICY below, verified against the stored rows); official
    scores and source-stated tiers are left alone.

Stdlib only.
"""
import argparse
import csv
import datetime
import gzip
import json
import math
import os
import random
import re
import sys
import time
from collections import Counter, defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, REPO)

import dinescores_pipeline as dp   # noqa: E402
import jev_reader as jr            # noqa: E402

TOKEN_RE = re.compile(r"'((?:[^']|'')*)'|(NULL)|(-?\d+(?:\.\d+)?)")
TIERS = ('core', 'priority_foundation', 'priority')
TIER_RANK = {t: i for i, t in enumerate(TIERS)}
GRADES = ('A', 'B', 'C', 'F')

# Where does the stored risk_score come from?  'computed' = calc_risk_score
# over regex tiers (Jev's category changes points); 'official' = the health
# department's score, counts, result or grade (points do not move);
# 'source_tiers' = computed, but from tiers the SOURCE stated (points do not
# move either). 'mhd' is decided per record: official when original_score is
# present and the jurisdiction is 0-100, computed otherwise. Verified against
# the seeds by recompute_check() and printed in score_policy.csv.
SCORE_POLICY = {
    'Chicago Open Data': 'computed',        # tiers bounded by item number: core stays core
    'NYC Open Data': 'computed',            # critical flag bumps core to PF
    'DataSF': 'computed',
    'Houston Health Dept': 'computed',
    'Cincinnati Open Data': 'computed',
    'Delaware Open Data': 'computed',
    'Montgomery County Open Data': 'computed',
    'Boston Open Data': 'source_tiers',
    'Detroit Open Data': 'source_tiers',
    'King County Open Data': 'official',
    'Austin Open Data': 'official',
    'Florida DBPR': 'official',
    'NYS Department of Health': 'official',
    'Wake County Open Data': 'official',
    'LA County Open Data': 'official',
    'Louisville Open Data': 'official',
    'Minneapolis Open Data': 'official',
    'Sacramento County Open Data': 'official',
    'SC DHEC': 'official',
    'Fairfax County': 'official',
    'Southern Nevada Health District': 'official',
    'DC Health': 'official',
    'Richardson Health Dept': 'official',
    'Arlington Open Data': 'official',
    'SC DPH Food Grades': 'official',          # letter grade only, no violation text
    'Louisville Metro Open Data': 'official',  # score only, no violation text
    'Fairfax County Open Data': 'official',    # result only, no violation text
}
MHD_DEMERIT_OR_NONE = {'Frisco', 'Fort Worth', 'El Paso County CO', 'Larimer County', 'Weld County',
                       'Adams County', 'Utah County', 'Clark County WA', 'Yolo County'}
# Sources whose stored tier came from the regex (SEVERITY_MAP) and may be
# re-tiered by Jev's category; everything else stated its own tier.
TIER_FLOOR_SOURCES = {'Chicago Open Data': 'bounded', 'NYC Open Data': 'bounded'}


def policy_for(source, original_score):
    if source in SCORE_POLICY:
        return SCORE_POLICY[source]
    if source.endswith(' Health Dept'):
        juris = source[:-len(' Health Dept')]
        if juris in MHD_DEMERIT_OR_NONE or original_score is None:
            return 'computed'
        return 'official'
    return 'unknown'


# ─── LOADING ─────────────────────────────────────────────────────────────────

def tokenize(line, ncols):
    vals = []
    for m in TOKEN_RE.finditer(line):
        if m.group(1) is not None:
            vals.append(m.group(1).replace("''", "'"))
        elif m.group(2) is not None:
            vals.append(None)
        else:
            s = m.group(3)
            vals.append(float(s) if '.' in s else int(s))
        if len(vals) == ncols:
            break
    return vals


def seed_order(path):
    base = os.path.basename(path)
    if base == 'd1_seed.sql.gz':
        return (0, base)
    m = re.match(r'd1_seed_(\d)_', base)
    return (int(m.group(1)), base) if m else (9, base)


class Index:
    """Texts, inspections and restaurants from the seeds (last file wins per id)."""

    def __init__(self):
        self.texts = []
        self.text_id = {}
        self.inspections = {}   # id -> (rid, date, risk, orig, violations tuple of (sev, tid))
        self.restaurants = {}   # rid -> dict
        self.datajs = {}

    def tid(self, t):
        i = self.text_id.get(t)
        if i is None:
            i = len(self.texts)
            self.text_id[t] = i
            self.texts.append(t)
        return i

    def load_seeds(self, data_dir):
        files = sorted((os.path.join(data_dir, f) for f in os.listdir(data_dir)
                        if f.startswith('d1_seed') and f.endswith('.sql.gz')), key=seed_order)
        for path in files:
            print(f'  reading {os.path.basename(path)}', flush=True)
            table = cols = None
            buf = ''
            with gzip.open(path, 'rt', encoding='utf-8', errors='replace') as f:
                for line in f:
                    if buf:
                        line, buf = buf + line, ''
                    if line.startswith('INSERT'):
                        m = re.match(r'INSERT(?: OR IGNORE)? INTO (\w+) \(([^)]*)\) VALUES', line)
                        table, cols = m.group(1), [c.strip() for c in m.group(2).split(',')]
                        continue
                    if table is None or not line.startswith('('):
                        continue
                    vals = tokenize(line[1:], len(cols))
                    if len(vals) < len(cols):
                        buf = line.rstrip('\n') + ' '
                        if len(buf) > 200000:
                            buf = ''
                        continue
                    row = dict(zip(cols, vals))
                    if table == 'inspections':
                        vlist = []
                        if row.get('violations'):
                            try:
                                parsed = json.loads(row['violations'])
                            except ValueError:
                                parsed = []
                            for v in parsed:
                                if isinstance(v, list) and len(v) >= 3:
                                    t = jr.normalize_text(v[2])
                                    if t:
                                        vlist.append((str(v[1]), self.tid(t)))
                        self.inspections[row['id']] = (row.get('restaurant_id'), row.get('inspection_date') or '',
                                                       row.get('risk_score'), row.get('original_score'), tuple(vlist))
                    elif table == 'restaurants':
                        rid = row['id']
                        prev = self.restaurants.get(rid)
                        if prev is None or (row.get('inspection_date') or '') >= prev['inspection_date']:
                            self.restaurants[rid] = {
                                'metro': row.get('metro') or '', 'state': row.get('state') or '',
                                'city': row.get('city') or '', 'source': row.get('source') or '',
                                'inspection_date': row.get('inspection_date') or '',
                                'original_score': row.get('original_score'), 'risk_score': row.get('risk_score'),
                                'weighted_score': row.get('weighted_score'), 'vetted_grade': row.get('vetted_grade') or '',
                                'summaries': row.get('summaries') or '', 'infractions': row.get('infractions') or '',
                            }
        print(f'  {len(self.inspections)} inspections, {len(self.restaurants)} restaurants, '
              f'{len(self.texts)} unique texts', flush=True)

    def load_datajs(self, path):
        with open(path, encoding='utf-8') as f:
            content = f.read()
        recs = json.loads(content.split('window.DATA = ', 1)[1].rstrip(';\n'))
        for r in recs:
            self.datajs[r.get('i')] = r
        print(f'  data.js: {len(self.datajs)} embedded restaurants', flush=True)

    def by_restaurant(self):
        out = defaultdict(list)
        for iid, insp in self.inspections.items():
            out[insp[0]].append(insp)
        for rid in out:
            out[rid].sort(key=lambda i: i[1], reverse=True)
        return out


# ─── TEXT-LEVEL READINGS ─────────────────────────────────────────────────────

def regex_reading_cache(texts):
    """Today's per-text answers, computed once."""
    cache = {}
    for t in texts:
        cache[t] = jr.regex_reading(t)
    return cache


def select_judge_set(idx, line_count, regex, sample_n, rng):
    forcing = {i for i, t in enumerate(idx.texts) if regex[t]['forces_f']}
    sewage = {i for i, t in enumerate(idx.texts) if regex[t]['summary'] == 'Sewage backup/issue'}
    uncl = {i for i, t in enumerate(idx.texts) if regex[t]['category'] == 'unclassified'}
    must = forcing | sewage | uncl
    rest = [i for i in range(len(idx.texts)) if i not in must]
    weights = [line_count[i] for i in rest]
    sample = set()
    if rest and sample_n > 0:
        # weighted sampling without replacement (Efraimidis-Spirakis keys)
        keyed = sorted(((rng.random() ** (1.0 / max(w, 1)), i) for i, w in zip(rest, weights)), reverse=True)
        sample = {i for _, i in keyed[:sample_n]}
    strata = {'forced_f': forcing, 'sewage_summary': sewage, 'unclassified': uncl, 'sample': sample}
    return must | sample, strata


# ─── GRADE IMPACT ────────────────────────────────────────────────────────────

def line_tier_new(stored_sev, reading, source):
    """New tier for a line in a computed-score source. Only regex-derived
    tiers move; Chicago/NYC's stored bound is honoured conservatively."""
    new = reading['severity']
    if TIER_FLOOR_SOURCES.get(source) == 'bounded':
        if stored_sev == 'core':
            return 'core'                      # Chicago item >= 30 / NYC non-critical: core stays core
        if TIER_RANK[new] < TIER_RANK['priority_foundation']:
            return 'priority_foundation'       # a bumped item never drops below PF
    return new


def score_from_tiers(tiers):
    pv = sum(1 for s in tiers if s == 'priority')
    pfv = sum(1 for s in tiers if s == 'priority_foundation')
    cv = len(tiers) - pv - pfv
    return max(0, 100 - 5 * pv - 2 * pfv - cv)


def weighted(scores):
    w = [0.6, 0.3, 0.1]
    tot = sum(s * w[i] for i, s in enumerate(scores[:3]))
    tw = sum(w[:len(scores[:3])])
    return round(tot / tw) if tw else 0


def grade(ws, rs_latest, forces_f):
    g = dp.calculate_vetted_grade(ws)
    if forces_f or dp.calculate_vetted_grade(rs_latest) == 'F':
        g = 'F'
    return g


# ─── MAIN ────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--out', default=os.path.join(REPO, 'data', 'jev_shadow'))
    ap.add_argument('--data-dir', default=os.path.join(REPO, 'data'))
    ap.add_argument('--data-js', default=os.path.join(REPO, 'public', 'data.js'))
    ap.add_argument('--cache-dir', default=jr.CACHE_DIR)
    ap.add_argument('--sample', type=int, default=20000)
    ap.add_argument('--max-ask', type=int, default=None, help='cap on texts sent to the API this run')
    ap.add_argument('--no-network', action='store_true', help='cached judgments only')
    ap.add_argument('--deadline-minutes', type=float, default=None)
    ap.add_argument('--in-flight', type=int, default=jr.MAX_IN_FLIGHT)
    ap.add_argument('--floor', action='append', default=[], help='question=floor, e.g. hazard=0.6')
    ap.add_argument('--handcheck', type=int, default=100)
    ap.add_argument('--verdicts', default=None, help='filled-in handcheck CSV (who_is_right column)')
    ap.add_argument('--seed', type=int, default=20261010)
    args = ap.parse_args()

    floors = dict(jr.CONFIDENCE_FLOORS)
    for f in args.floor:
        q, v = f.split('=')
        floors[q.strip()] = float(v)
    rng = random.Random(args.seed)
    os.makedirs(args.out, exist_ok=True)
    t_start = time.time()

    print('Loading seeds...', flush=True)
    idx = Index()
    idx.load_seeds(args.data_dir)
    idx.load_datajs(args.data_js)
    texts = idx.texts
    line_count = [0] * len(texts)
    text_sources = [Counter() for _ in texts]
    text_sevs = [Counter() for _ in texts]
    for iid, (rid, date, risk, orig, vio) in idx.inspections.items():
        src = idx.restaurants.get(rid, {}).get('source', 'unknown')
        for sev, tid in vio:
            line_count[tid] += 1
            text_sources[tid][src] += 1
            text_sevs[tid][sev] += 1
    n_lines = sum(line_count)
    print(f'  {n_lines} violation lines', flush=True)

    print('Regex readings...', flush=True)
    regex = regex_reading_cache(texts)

    judge_ids, strata = select_judge_set(idx, line_count, regex, args.sample, rng)
    judge_ids = sorted(judge_ids, key=lambda i: -line_count[i])      # most common lines first
    print(f'Judge set: {len(judge_ids)} texts covering {sum(line_count[i] for i in judge_ids)} lines; '
          + ', '.join(f'{k}={len(v)}' for k, v in strata.items()), flush=True)

    # ── ask Jev (cache first) ──────────────────────────────────────────────
    deadline = time.time() + args.deadline_minutes * 60 if args.deadline_minutes else None
    reader = jr.JevReader(mode='shadow', cache=jr.JudgmentCache(args.cache_dir), deadline=deadline,
                          max_in_flight=args.in_flight, floors=floors)
    to_judge = [texts[i] for i in judge_ids]
    if args.no_network:
        reader._disabled = 'no-network run'
    elif args.max_ask is not None:
        # only the first N uncached texts go to the API this run
        reader.cache.load()
        reader._cache_loaded = True
        uncached = [t for t in to_judge if reader.cache.get(jr.judgment_key(t)) is None]
        allowed = set(uncached[:args.max_ask])
        to_judge = [t for t in to_judge if reader.cache.get(jr.judgment_key(t)) is not None or t in allowed]
        print(f'  --max-ask: {len(uncached)} uncached, asking {len(allowed)}', flush=True)
    print('Judging...', flush=True)
    t0 = time.time()
    judgments = reader.judge_texts(to_judge, label='shadow')
    judge_time = time.time() - t0
    cache_path = reader.flush_cache('shadow')
    for line in reader.summary_lines():
        print('  ' + line)
    s = reader.stats
    judged = {t: j for t, j in judgments.items() if j}
    print(f'  {len(judged)} judged texts available ({judge_time:.0f}s)', flush=True)

    # ── per-text decisions ─────────────────────────────────────────────────
    readings = {}
    for i in judge_ids:
        t = texts[i]
        readings[t] = jr.decide(t, judged.get(t), None, floors)

    # agreement / confusion / confidence
    agree = {q: Counter() for q in jr.QUESTIONS}
    conf_buckets = {q: Counter() for q in jr.QUESTIONS}
    conf_lines = {q: Counter() for q in jr.QUESTIONS}
    confusion = Counter()
    confusion_lines = Counter()
    hazard_vs_regex = Counter()
    summary_changes = Counter()
    summary_examples = {}
    chip_changes = Counter()
    chip_examples = defaultdict(list)
    disagreements = {q: [] for q in jr.QUESTIONS}
    marker_lines = Counter()
    for i in judge_ids:
        t = texts[i]
        d = readings[t]
        j = judged.get(t)
        cnt = line_count[i]
        r = d['regex']
        if not j:
            for q in jr.QUESTIONS:
                agree[q]['no_judgment'] += 1
            continue
        for q in jr.QUESTIONS:
            c, p = jr.answer(j, q)
            b = f'{math.floor(p * 10) / 10:.1f}' if p < 1 else '1.0'
            conf_buckets[q][b] += 1
            conf_lines[q][b] += cnt
        jc, _ = jr.answer(j, 'category')
        confusion[(r['category'], jc)] += 1
        confusion_lines[(r['category'], jc)] += cnt
        agree['category']['agree' if jc == r['category'] else 'disagree'] += 1
        if jc != r['category']:
            disagreements['category'].append(i)
        jh, _ = jr.answer(j, 'hazard')
        jf = jh in jr.F_FORCING_HAZARDS
        hazard_vs_regex[(r['forces_f'], jh)] += 1
        agree['hazard']['agree' if jf == r['forces_f'] else 'disagree'] += 1
        if jf != r['forces_f']:
            disagreements['hazard'].append(i)
        js, _ = jr.answer(j, 'summary')
        new_sum = jr._truncate(t) if js == jr.SUMMARY_NO_MATCH else js
        agree['summary']['agree' if new_sum == r['summary'] else 'disagree'] += 1
        if new_sum != r['summary']:
            disagreements['summary'].append(i)
            key = (r['summary'] if r['summary'] in jr.SUMMARY_PHRASES else '<truncated text>',
                   js if js != jr.SUMMARY_NO_MATCH else '<truncated text>')
            summary_changes[key] += cnt
            summary_examples.setdefault(key, t)
        # chips: today's per-line chips vs the code map (only when Jev decided)
        if any(v == 'jev' for v in d['used'].values()):
            before, after = set(r['chips']), set(d['chips'])
            for c in after - before:
                chip_changes[(c, 'added')] += cnt
                if len(chip_examples[(c, 'added')]) < 3:
                    chip_examples[(c, 'added')].append(t)
            for c in before - after:
                chip_changes[(c, 'removed')] += cnt
                if len(chip_examples[(c, 'removed')]) < 3:
                    chip_examples[(c, 'removed')].append(t)
        if d['tier_marker']:
            marker_lines[(d['tier_marker'], tuple(sorted(text_sevs[i])))] += cnt

    # ── marker stats over ALL texts (markers are code, not Jev) ───────────
    marker_total = 0
    marker_disagree = 0
    marker_by_source = Counter()
    for i, t in enumerate(texts):
        mk = jr.parse_tier_marker(t)
        if not mk:
            continue
        marker_total += line_count[i]
        for sev, c in text_sevs[i].items():
            if sev != mk:
                marker_disagree += c
        marker_by_source.update(text_sources[i])

    # ── grade impact ───────────────────────────────────────────────────────
    # Three grades per restaurant, all computed here with today's cutoffs:
    #   g_today  today's functions over the stored rows (joined-text regex F)
    #   g_nojev  the new reader with every judgment withheld: per-line F,
    #            tier markers: the reader's mechanics alone
    #   g_jev    the new reader with Jev's judgments
    # Transitions are reported g_today -> g_jev, and decomposed into
    # g_today -> g_nojev (mechanics) and g_nojev -> g_jev (Jev).
    by_rest = idx.by_restaurant()
    policy_rows = Counter()
    recompute_match = Counter()
    nojev_readings = {}

    alt_readings = {}

    def reading_for(tid, with_jev, forcing=None):
        t = texts[tid]
        if with_jev and forcing is not None:
            r = alt_readings.get(t)
            if r is None:
                r = alt_readings[t] = jr.decide(t, judged.get(t), None, floors, forcing=forcing)
            return r
        if with_jev:
            return readings.get(t) or jr.decide(t, None, None, floors)
        r = nojev_readings.get(t)
        if r is None:
            r = nojev_readings[t] = jr.decide(t, None, None, floors)
        return r

    def grade_pass(with_jev, forcing=None):
        out = {}
        acct = Counter()
        examples = defaultdict(list)
        for rid, insps in by_rest.items():
            rest = idx.restaurants.get(rid)
            if not rest:
                continue
            source = rest['source'] or 'unknown'
            latest = insps[0]
            pol = policy_for(source, latest[3])
            if not with_jev:
                policy_rows[(source, pol)] += 1
                if latest[4]:
                    recompute_match[(source, pol, score_from_tiers([sev for sev, _ in latest[4]]) == latest[2])] += 1
            scores_today = [i[2] or 0 for i in insps]
            ws_today = weighted(scores_today)
            joined = '|||'.join(texts[tid] for _, tid in latest[4])
            forced_today = dp.has_active_pest_or_sewage(joined)
            g_today = grade(ws_today, latest[2] or 0, forced_today)
            lines = [reading_for(tid, with_jev, forcing) for _, tid in latest[4]]
            forced_new = any(l['forces_f'] for l in lines)
            flies_new = any(l['hazard'] == 'flies_or_gnats_observed' for l in lines)
            unspec_new = any(l['hazard'] == 'pests_unspecified' for l in lines)
            if pol == 'computed':
                new_scores = []
                for insp in insps[:3]:
                    if insp[4] and score_from_tiers([sv for sv, _ in insp[4]]) == insp[2]:
                        tiers = [line_tier_new(sev, reading_for(tid, with_jev), source) for sev, tid in insp[4]]
                        new_scores.append(score_from_tiers(tiers))
                    else:
                        new_scores.append(insp[2] or 0)
            else:
                new_scores = scores_today
            ws_new = weighted(new_scores)
            g_new = grade(ws_new, new_scores[0], forced_new)
            score_f = dp.calculate_vetted_grade(ws_today) == 'F' or dp.calculate_vetted_grade(latest[2] or 0) == 'F'
            out[rid] = (source, g_today, g_new, rest['vetted_grade'])
            if forced_today and not score_f:
                acct[(source, 'today_text_forced_f')] += 1
                acct[(source, 'kept' if forced_new else 'dropped')] += 1
                if not forced_new and not any(dp.has_active_pest_or_sewage(texts[tid]) for _, tid in latest[4]):
                    acct[(source, 'dropped_cross_line_artifact')] += 1
            elif not forced_today and forced_new and not score_f:
                acct[(source, 'added')] += 1
            if flies_new and not forced_new and not score_f and g_new != 'F':
                acct[(source, 'flies_would_add_f')] += 1
            if unspec_new and not forced_new and not score_f and g_new != 'F':
                acct[(source, 'unspecified_would_add_f')] += 1
            if g_today != g_new and len(examples[(source, g_today, g_new)]) < 3:
                examples[(source, g_today, g_new)].append(
                    (rest['city'], ws_today, ws_new, [texts[tid][:140] for _, tid in latest[4]][:4]))
        return out, acct, examples

    print('Grading (reader mechanics only)...', flush=True)
    g_nojev, forced_nojev, _ = grade_pass(False)
    print('Grading (with Jev)...', flush=True)
    g_jev, forced, changed_examples = grade_pass(True)
    print('Grading (with Jev, pests_unspecified forcing an F)...', flush=True)
    g_alt, forced_alt, _ = grade_pass(True, forcing=jr.F_FORCING_HAZARDS_WITH_UNSPECIFIED)
    alt_transitions = Counter()
    alt_grade_total = Counter()
    for rid, (source, g_t, g_n, g_stored) in g_alt.items():
        alt_transitions[(source, g_t, g_n)] += 1
        alt_grade_total[g_n] += 1

    transitions = Counter()
    mech_transitions = Counter()
    jev_transitions = Counter()
    datajs_transitions = Counter()
    grade_today_total = Counter()
    grade_new_total = Counter()
    mismatch_stored = 0
    for rid, (source, g_t, g_n, g_stored) in g_jev.items():
        g_m = g_nojev[rid][2]
        transitions[(source, g_t, g_n)] += 1
        mech_transitions[(source, g_t, g_m)] += 1
        jev_transitions[(source, g_m, g_n)] += 1
        grade_today_total[g_t] += 1
        grade_new_total[g_n] += 1
        if g_t != g_stored:
            mismatch_stored += 1
        if rid in idx.datajs:
            datajs_transitions[(g_t, g_n)] += 1
    n_scored = sum(n for (src_, pol), n in policy_rows.items() if pol == 'computed')

    # ── write outputs ──────────────────────────────────────────────────────
    def write_csv(name, header, rows):
        with open(os.path.join(args.out, name), 'w', newline='', encoding='utf-8') as f:
            w = csv.writer(f)
            w.writerow(header)
            for r in rows:
                w.writerow(r)

    judged_n = len(judged)
    FKEYS = ('today_text_forced_f', 'kept', 'dropped', 'dropped_cross_line_artifact', 'added', 'flies_would_add_f',
             'unspecified_would_add_f')
    write_csv('agreement.csv', ['question', 'judged_texts', 'agree', 'disagree', 'agree_pct', 'floor',
                                'texts_at_or_above_floor', 'pct_above_floor'],
              [[q, judged_n, agree[q]['agree'], agree[q]['disagree'],
                round(100.0 * agree[q]['agree'] / max(1, judged_n), 2), floors[q],
                sum(1 for t in judged if jr.confident_answer(judged[t], q, floors)),
                round(100.0 * sum(1 for t in judged if jr.confident_answer(judged[t], q, floors)) / max(1, judged_n), 2)]
               for q in jr.QUESTIONS])
    cats = jr.category_options()
    write_csv('category_confusion.csv', ['regex_category', 'jev_category', 'texts', 'lines'],
              [[a, b, confusion[(a, b)], confusion_lines[(a, b)]] for a in cats for b in cats if confusion[(a, b)]])
    write_csv('hazard_vs_regex.csv', ['regex_forces_f', 'jev_hazard', 'texts'],
              [[k[0], k[1], v] for k, v in sorted(hazard_vs_regex.items(), key=lambda kv: -kv[1])])
    write_csv('confidence_distribution.csv', ['question', 'confidence_bucket', 'texts', 'lines'],
              [[q, b, conf_buckets[q][b], conf_lines[q][b]] for q in jr.QUESTIONS for b in sorted(conf_buckets[q])])
    write_csv('summary_changes.csv', ['regex_summary', 'jev_summary', 'lines', 'example'],
              [[k[0], k[1], v, summary_examples[k]] for k, v in summary_changes.most_common()])
    write_csv('chip_changes.csv', ['chip', 'change', 'lines', 'examples'],
              [[k[0], k[1], v, ' || '.join(chip_examples[k])] for k, v in chip_changes.most_common()])
    write_csv('grade_transitions.csv', ['source', 'grade_today', 'grade_new', 'restaurants'],
              [[s, a, b, n] for (s, a, b), n in sorted(transitions.items())])
    write_csv('grade_transitions_alt_unspecified_forces_f.csv', ['source', 'grade_today', 'grade_new', 'restaurants'],
              [[s, a, b, n] for (s, a, b), n in sorted(alt_transitions.items())])
    write_csv('forced_f_alt_unspecified_forces_f.csv', ['source'] + list(FKEYS),
              [[s] + [forced_alt[(s, k)] for k in FKEYS] for s in sorted({s for s, _ in forced_alt})])
    write_csv('grade_transitions_datajs.csv', ['grade_today', 'grade_new', 'restaurants'],
              [[a, b, n] for (a, b), n in sorted(datajs_transitions.items())])
    sources = sorted({s for s, _ in forced} | {s for s, _ in forced_nojev})
    write_csv('forced_f.csv', ['source'] + list(FKEYS) + [k + '_nojev' for k in FKEYS],
              [[s] + [forced[(s, k)] for k in FKEYS] + [forced_nojev[(s, k)] for k in FKEYS] for s in sources])
    write_csv('score_policy.csv', ['source', 'policy', 'restaurants', 'recompute_matches_stored', 'recompute_differs'],
              [[s, p, n, recompute_match[(s, p, True)], recompute_match[(s, p, False)]]
               for (s, p), n in sorted(policy_rows.items(), key=lambda kv: -kv[1])])
    write_csv('grade_transitions_decomposed.csv', ['step', 'source', 'grade_from', 'grade_to', 'restaurants'],
              [['reader_mechanics_no_jev', s, a, b, n] for (s, a, b), n in sorted(mech_transitions.items()) if a != b]
              + [['jev_judgments', s, a, b, n] for (s, a, b), n in sorted(jev_transitions.items()) if a != b])
    write_csv('marker_lines.csv', ['marker_tier', 'stored_tiers', 'lines'],
              [[k[0], '|'.join(k[1]), v] for k, v in marker_lines.most_common()])
    write_csv('changed_examples.csv', ['source', 'grade_today', 'grade_new', 'city', 'ws_today', 'ws_new', 'latest_lines'],
              [[s, a, b, c, w1, w2, ' || '.join(ls)] for (s, a, b), exs in sorted(changed_examples.items())
               for (c, w1, w2, ls) in exs])

    # hand-check lists: 100 random disagreements per question
    for q in jr.QUESTIONS:
        pool = disagreements[q][:]
        rng.shuffle(pool)
        rows = []
        for i in pool[:args.handcheck]:
            t = texts[i]
            j = judged[t]
            r = regex[t]
            if q == 'category':
                reg, jv = r['category'], jr.answer(j, 'category')[0]
            elif q == 'hazard':
                reg, jv = ('forces F' if r['forces_f'] else 'no F'), jr.answer(j, 'hazard')[0]
            else:
                reg, jv = r['summary'], jr.answer(j, 'summary')[0]
            rows.append([q, i, line_count[i], ';'.join(f'{s}:{c}' for s, c in text_sources[i].most_common(2)),
                         reg, jv, round(jr.answer(j, q)[1], 3), '', '', t])
        write_csv(f'handcheck_{q}.csv', ['question', 'text_id', 'lines', 'sources', 'regex', 'jev', 'jev_confidence',
                                         'who_is_right', 'why', 'text'], rows)

    # optional: verdicts -> floor recommendation
    floor_rows = []
    if args.verdicts and os.path.exists(args.verdicts):
        with open(args.verdicts, newline='', encoding='utf-8') as f:
            verdicts = list(csv.DictReader(f))
        for q in jr.QUESTIONS:
            vs = [v for v in verdicts if v['question'] == q and v.get('who_is_right')]
            for floor in (0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9):
                above = [v for v in vs if float(v['jev_confidence']) >= floor]
                jev_right = sum(1 for v in above if v['who_is_right'].strip().lower().startswith('jev'))
                regex_right = sum(1 for v in above if v['who_is_right'].strip().lower().startswith('regex'))
                cover = sum(1 for t in judged if jr.answer(judged[t], q)[1] >= floor)
                floor_rows.append([q, floor, len(above), jev_right, regex_right,
                                   round(100.0 * jev_right / max(1, jev_right + regex_right), 1),
                                   round(100.0 * cover / max(1, judged_n), 1)])
        write_csv('floor_sweep.csv', ['question', 'floor', 'handchecked_disagreements_above', 'jev_right',
                                      'regex_right', 'jev_precision_pct', 'coverage_pct_of_judged'], floor_rows)

    # cost
    tokens_per_line = s['input_tokens'] / s['lines_asked'] if s['lines_asked'] else None
    price_per_mtok = 0.042
    cost_per_line = tokens_per_line * price_per_mtok / 1e6 if tokens_per_line else None
    # weekly new-text estimate: distinct texts first seen per ISO week over the last 12 full weeks
    first_seen = {}
    for iid, (rid, date, risk, orig, vio) in idx.inspections.items():
        for _, tid in vio:
            if date and (tid not in first_seen or date < first_seen[tid]):
                first_seen[tid] = date
    weeks = Counter()
    for tid, d in first_seen.items():
        try:
            y, w, _ = datetime.date.fromisoformat(d[:10]).isocalendar()
        except (ValueError, TypeError):
            continue
        weeks[(y, w)] += 1
    recent = sorted(weeks)[-14:-2] if len(weeks) > 14 else sorted(weeks)
    weekly_new_texts = sum(weeks[k] for k in recent) / max(1, len(recent))
    summary = {
        'inspections': len(idx.inspections), 'violation_lines': n_lines, 'unique_texts': len(texts),
        'restaurants': len(idx.restaurants), 'datajs_restaurants': len(idx.datajs),
        'judge_set_texts': len(judge_ids), 'judge_set_lines': sum(line_count[i] for i in judge_ids),
        'strata': {k: len(v) for k, v in strata.items()},
        'judged_texts': judged_n, 'judged_lines': sum(line_count[i] for i in judge_ids if texts[i] in judged),
        'reader_stats': {k: v for k, v in s.items() if k != 'cache_files_written'},
        'cache_file': os.path.basename(cache_path) if cache_path else None,
        'floors': floors, 'model': jr.JEV_MODEL, 'question_set': jr.QUESTION_SET_VERSION,
        'tokens_per_line': tokens_per_line, 'cost_per_line_usd': cost_per_line,
        'projected_backfill_usd': cost_per_line * len(texts) if cost_per_line else None,
        'weekly_new_unique_texts_estimate': weekly_new_texts,
        'projected_weekly_usd': cost_per_line * weekly_new_texts if cost_per_line else None,
        'agreement': {q: dict(agree[q]) for q in jr.QUESTIONS},
        'grade_today': dict(grade_today_total), 'grade_new': dict(grade_new_total),
        'grade_changes': sum(n for (s_, a, b), n in transitions.items() if a != b),
        'datajs_grade_changes': sum(n for (a, b), n in datajs_transitions.items() if a != b),
        'today_recomputed_grade_mismatches_vs_stored': mismatch_stored,
        'restaurants_rescored_computed_policy': n_scored,
        'text_forced_f_today': sum(v for (s_, k), v in forced.items() if k == 'today_text_forced_f'),
        'text_forced_f_kept': sum(v for (s_, k), v in forced.items() if k == 'kept'),
        'text_forced_f_dropped': sum(v for (s_, k), v in forced.items() if k == 'dropped'),
        'text_forced_f_added': sum(v for (s_, k), v in forced.items() if k == 'added'),
        'text_forced_f_dropped_cross_line_artifact': sum(v for (s_, k), v in forced.items() if k == 'dropped_cross_line_artifact'),
        'nojev_text_forced_f_dropped': sum(v for (s_, k), v in forced_nojev.items() if k == 'dropped'),
        'nojev_text_forced_f_added': sum(v for (s_, k), v in forced_nojev.items() if k == 'added'),
        'grade_changes_reader_mechanics_only': sum(n for (s_, a, b), n in mech_transitions.items() if a != b),
        'grade_changes_jev_only': sum(n for (s_, a, b), n in jev_transitions.items() if a != b),
        'flies_would_add_f': sum(v for (s_, k), v in forced.items() if k == 'flies_would_add_f'),
        'unspecified_would_add_f': sum(v for (s_, k), v in forced.items() if k == 'unspecified_would_add_f'),
        'alt_unspecified_forces_f': {
            'grade_new': dict(alt_grade_total),
            'grade_changes': sum(n for (s_, a, b), n in alt_transitions.items() if a != b),
            'text_forced_f_kept': sum(v for (s_, k), v in forced_alt.items() if k == 'kept'),
            'text_forced_f_dropped': sum(v for (s_, k), v in forced_alt.items() if k == 'dropped'),
            'text_forced_f_added': sum(v for (s_, k), v in forced_alt.items() if k == 'added'),
            'flies_would_add_f': sum(v for (s_, k), v in forced_alt.items() if k == 'flies_would_add_f'),
        },
        'marker_lines': marker_total, 'marker_lines_disagreeing_with_stored_tier': marker_disagree,
        'marker_lines_by_source': dict(marker_by_source.most_common(12)),
        'elapsed_seconds': round(time.time() - t_start),
    }
    with open(os.path.join(args.out, 'summary.json'), 'w') as f:
        json.dump(summary, f, indent=1, default=str)

    # markdown tables for the report
    md = []
    md.append(f"Judge set: {summary['judge_set_texts']} texts / {summary['judge_set_lines']} lines; judged {judged_n}.\n")
    md.append('| question | judged | agree | disagree | agree % | floor | % at/above floor |\n|---|---|---|---|---|---|---|')
    for q in jr.QUESTIONS:
        above = sum(1 for t in judged if jr.confident_answer(judged[t], q, floors))
        md.append(f"| {q} | {judged_n} | {agree[q]['agree']} | {agree[q]['disagree']} | "
                  f"{100.0 * agree[q]['agree'] / max(1, judged_n):.1f} | {floors[q]} | {100.0 * above / max(1, judged_n):.1f} |")
    md.append('\nGrade totals (restaurants):\n\n| grade | today | with Jev |\n|---|---|---|')
    for g in GRADES:
        md.append(f'| {g} | {grade_today_total[g]} | {grade_new_total[g]} |')
    md.append('\nText-forced F accounting:\n')
    md.append(f"- today: {summary['text_forced_f_today']} text-forced F's; Jev keeps {summary['text_forced_f_kept']}, "
              f"drops {summary['text_forced_f_dropped']}, adds {summary['text_forced_f_added']} new F's; "
              f"flies/gnats would add {summary['flies_would_add_f']} more; pests_unspecified would add "
              f"{summary['unspecified_would_add_f']} more.")
    alt = summary['alt_unspecified_forces_f']
    md.append(f"- if pests_unspecified forces an F: keeps {alt['text_forced_f_kept']}, drops {alt['text_forced_f_dropped']}, "
              f"adds {alt['text_forced_f_added']}; F total {alt['grade_new'].get('F', 0)}; {alt['grade_changes']} grade changes.")
    md.append('\nTransitions by source (changed only):\n\n| source | today | new | restaurants |\n|---|---|---|---|')
    for (src, a, b), n in sorted(transitions.items(), key=lambda kv: -kv[1]):
        if a != b:
            md.append(f'| {src} | {a} | {b} | {n} |')
    with open(os.path.join(args.out, 'report_tables.md'), 'w') as f:
        f.write('\n'.join(md) + '\n')
    print('\n'.join(md))
    print(f"\nDone in {summary['elapsed_seconds']}s. Outputs in {args.out}")


if __name__ == '__main__':
    main()
