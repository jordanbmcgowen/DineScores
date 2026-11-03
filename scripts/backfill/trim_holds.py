"""trim_holds.py [--horizon-days N] <group>...

After a backfill, a hold the chunk left on a NEW group pins the first weekly
runs at the oldest unreadable record of the whole history: Pima's 746
unpublished checklists reached back to 2024-12, so each of the next three
Sunday runs would have re-read 400 days of Pima before giving them up. Only
failures from the last N days (default 45: the span in which a late or
transient failure can still be read) are kept; the older ones are recorded
as given up, exactly as _record_group does after three failed runs, so a
re-read that meets them again does not restart a count. A source's latest
becomes the day before its earliest kept failure, or its group's newest
shipped inspection when none is kept; the group's latest follows its
sources. Groups whose latest is already inside the horizon are left alone.
Run from the main checkout; edits data/source_freshness.json in place."""
import argparse
import json
from datetime import datetime, timedelta, timezone

ap = argparse.ArgumentParser()
ap.add_argument('--horizon-days', type=int, default=45)
ap.add_argument('groups', nargs='+')
args = ap.parse_args()
path = 'data/source_freshness.json'
d = json.load(open(path))
today = datetime.now(timezone.utc).date()
cutoff = (today - timedelta(days=args.horizon_days)).isoformat()
keep_given_up_from = (today - timedelta(days=400)).isoformat()   # FRESHNESS_LOOKBACK_API_DAYS


def day_before(day):
    return (datetime.strptime(day, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')


for g in args.groups:
    ge = d['groups'].get(g)
    if not ge:
        print(f"{g}: no such group"); continue
    if (ge.get('latest') or '') >= cutoff:
        print(f"{g}: latest {ge.get('latest')} is inside the horizon; left alone"); continue
    newest = (ge.get('last_run') or {}).get('latest') or ge.get('latest')
    latests = []
    for s, se in d['sources'].items():
        if se.get('group') != g:
            continue
        hold = se.get('hold') or {}
        failures = hold.get('failures') or {}
        kept = {k: n for k, n in failures.items() if k[:10] >= cutoff}
        old = sorted(k for k in failures if k[:10] < cutoff)
        given_up = sorted(set(se.get('given_up') or []) | {k for k in old if k[:10] >= keep_given_up_from})
        if given_up:
            se['given_up'] = given_up[-500:]
        if kept:
            first = min(k[:10] for k in kept)
            se['hold'] = {'date': first, 'failures': kept}
            se['latest'] = min(newest, day_before(first))
        else:
            se.pop('hold', None)
            se['latest'] = max(se.get('latest') or '', newest) if (se.get('latest') or '') >= cutoff else newest
        latests.append(se['latest'])
        print(f"{g}: {s}: {len(failures)} held failures -> {len(kept)} kept (from {cutoff}), "
              f"{len(old)} given up; latest {se['latest']}")
    if latests:
        ge['latest'] = min(latests)
    print(f"{g}: group latest {ge['latest']} (newest shipped {newest})")
with open(path, 'w') as f:
    json.dump(d, f, indent=1, sort_keys=True)
    f.write('\n')
