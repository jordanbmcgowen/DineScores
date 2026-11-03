"""freshness_copy.py <worktree freshness.json> <records.json.gz>: copy a backfill
chunk's freshness entries into the main checkout's data/source_freshness.json
(cwd = the main checkout).

The chunk's groups are the groups of the sources its records carry (not the
clock). Rules:
- a NEW group (no entry in the main file) is copied as the chunk left it,
  hold included: the first weekly runs re-read from the hold until the held
  records are read or given up; a pinned entry is reported;
- an EXISTING group never moves backwards and never picks up a backfill hold;
- a source entry follows its group: copied when its group was copied or when
  the main file has no entry for it, otherwise only when it moves forward,
  and never with a hold (for the MHD groups the source entries ARE the
  weekly window);
- NO_FRESHNESS_COPY=1 copies nothing (a re-read of sources whose freshness
  is already right)."""
import datetime
import gzip
import json
import os
import sys

wt_path, rec_path = sys.argv[1], sys.argv[2]
main = json.load(open('data/source_freshness.json'))
wt = json.load(open(wt_path))
recs = json.load(gzip.open(rec_path, 'rt'))
labels = {r.get('source') for r in recs if r.get('source')}
if os.environ.get('NO_FRESHNESS_COPY'):
    print(f"  freshness: not copied (NO_FRESHNESS_COPY); sources in the chunk {sorted(labels)}")
    sys.exit(0)
groups = {wt['sources'][s].get('group') for s in labels if s in wt['sources']} - {None}
copied = []
for g in sorted(groups):
    v = wt['groups'].get(g)
    if not v or main['groups'].get(g) == v:
        continue
    held = {s: sv['hold'] for s, sv in wt['sources'].items() if sv.get('group') == g and sv.get('hold')}
    pinned = bool(held) or ((v.get('latest') or '') < ((v.get('last_run') or {}).get('latest') or ''))
    old = main['groups'].get(g)
    if old and old.get('latest'):
        if pinned or (v.get('latest') or '') < old['latest']:
            print(f"  NOTE: group {g} kept as it was (chunk latest {v.get('latest')}, holds {json.dumps(held)[:160]})")
            continue
    main['groups'][g] = v
    copied.append(g)
    if pinned:
        print(f"  NOTE: new group {g} is pinned at {v.get('latest')} by a backfill hold "
              f"(newest shipped {(v.get('last_run') or {}).get('latest')}): {json.dumps(held)[:300]}")
for s in sorted(labels):
    v = wt['sources'].get(s)
    if not v or main['sources'].get(s) == v:
        continue
    old = main['sources'].get(s)
    if old is None or v.get('group') in copied:
        main['sources'][s] = v
    elif (v.get('latest') or '') > (old.get('latest') or '') and not v.get('hold'):
        main['sources'][s] = dict(v)
    else:
        print(f"  NOTE: source {s} kept as it was (chunk latest {v.get('latest')}, hold {bool(v.get('hold'))})")
main['updated'] = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')
with open('data/source_freshness.json', 'w') as f:
    json.dump(main, f, indent=1, sort_keys=True)
    f.write('\n')
print(f"  freshness: groups copied {copied}; sources in the chunk {sorted(labels)}")
for g in copied:
    print(f"    {g}: latest {main['groups'][g].get('latest')} status {(main['groups'][g].get('last_run') or {}).get('status')}")
