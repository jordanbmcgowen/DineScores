"""freshness_copy.py <worktree freshness.json> <records.json.gz>: copy the chunk's group and source
entries into the main repo's data/source_freshness.json (cwd = main repo). An existing group never
moves backwards or picks up a backfill hold; NO_FRESHNESS_COPY=1 skips everything."""
import gzip, json, os, sys, datetime
wt_path, rec_path = sys.argv[1], sys.argv[2]
main = json.load(open('data/source_freshness.json'))
wt = json.load(open(wt_path))
recs = json.load(gzip.open(rec_path, 'rt'))
labels = {r.get('source') for r in recs}
today = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')
copied = []
if os.environ.get('NO_FRESHNESS_COPY'):
    wt = {'groups': {}, 'sources': {}}
    print('  freshness: not copied (NO_FRESHNESS_COPY)')
for g, v in wt['groups'].items():
    if (v.get('last_run') or {}).get('at') != today or main['groups'].get(g) == v:
        continue
    # holds live on the SOURCE entries (_record_group caps the group's latest
    # at the day before the earliest held drop and records the hold under
    # sources[label]['hold']); a group whose latest sits behind its last run's
    # newest shipped date is pinned by one
    held = {s: sv['hold'] for s, sv in wt['sources'].items()
            if sv.get('group') == g and sv.get('hold')}
    pinned = bool(held) or ((v.get('latest') or '') < ((v.get('last_run') or {}).get('latest') or ''))
    old = main['groups'].get(g)
    if old and old.get('latest'):
        if pinned or (v.get('latest') or '') < old['latest']:
            print(f"  NOTE: group {g} kept as it was (backfill latest {v.get('latest')}, holds {json.dumps(held)[:160]})")
            continue
    main['groups'][g] = v
    copied.append(g)
    if pinned:
        print(f"  NOTE: new group {g} is pinned at {v.get('latest')} by a backfill hold "
              f"(newest shipped {(v.get('last_run') or {}).get('latest')}); the weekly re-reads from there "
              f"until the held records are read or given up: {json.dumps(held)[:300]}")
for s, v in wt['sources'].items():
    if s in labels and main['sources'].get(s) != v:
        main['sources'][s] = v
main['updated'] = today
with open('data/source_freshness.json', 'w') as f:
    json.dump(main, f, indent=1, sort_keys=True)
    f.write('\n')
print(f"  freshness: groups copied {copied}; sources {sorted(x for x in labels if x)}")
for g in copied:
    print(f"    {g}: latest {main['groups'][g].get('latest')} status {(main['groups'][g].get('last_run') or {}).get('status')}")
