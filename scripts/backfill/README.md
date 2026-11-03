# Backfill tooling

A new source's history is fetched once, in chunks, and absorbed into the
repository without refetching. Each chunk runs the pipeline in `--mode full`
over a date window and writes three things: every inspection record it
fetched (`--output-records`, gzipped JSON), the D1 SQL, and the worktree's
copy of `data/source_freshness.json`. The absorb step then re-runs the
pipeline in the main checkout with `--input-records`, which merges the
records into `public/data.js`, lets Jev read their violation lines (the
weekly job's decide mode) and writes the D1 seed from the same records, so
data.js and D1 carry identical decisions; it copies the chunk's freshness
entries over, commits and (optionally) pushes.

```
scripts/backfill/chunk.sh <worktree> <tag> <since> [until] -- <slug...>
    runs one chunk in a git worktree detached at the integration commit
    (so several sources fetch in parallel) and absorbs it under a lock
scripts/backfill/absorb.sh <tag> <since> <records.json.gz> <d1.sql> <wt freshness.json> <wt jev dir>
    the absorb step on its own (chunk.sh calls it)
scripts/backfill/seed_check.py <seed.sql.gz>
    D1 statement cap and an in-memory sqlite replay with row counts
scripts/backfill/freshness_copy.py <wt freshness.json> <records.json.gz>
    copies the chunk's group/source entries into data/source_freshness.json
scripts/backfill/geocode_backlog.py [--patch] <out.records.json.gz> <nominatim cap> <records.json.gz>...
    one more geocoding pass over the restaurants a backfill left without
    coordinates (absorb.sh runs it with --patch on every chunk)
scripts/backfill/trim_holds.py [--horizon-days 45] <group>...
    after a long backfill, keep only recent held failures so the first
    weekly runs do not re-read the whole history (older ones are given up)
```

Environment: `BACKFILL_DIR` (where chunk outputs and logs go, default
`/tmp/dinescores-backfills`), `DEADLINE_MINUTES` (pipeline fetch deadline
per chunk, default 300), `EXTRA_ARGS` (extra pipeline flags, e.g.
`--skip-known-inspections FILE`), `NO_FRESHNESS_COPY=1` (absorb the records
but leave the freshness file alone — for a re-read of sources whose
freshness is already right), `JEV_BUDGET_MINUTES` (default 90), `GEOCODE_CAP`
(Nominatim lookups the absorb step spends on restaurants the Census batch
could not place, default 2000, 0 = skip), `PUSH=1` (push after the absorb
commit), plus any fetcher knobs
(`MCESD_DETAIL_WORKERS`, `LA_PRINTABLE_MAX_PER_RUN`, ...).

Rules the scripts enforce: a seed statement stays under D1's 100 KB cap;
data.js stays at or under `DATA_JS_MAX_BYTES`, and is written to a scratch
path and moved into place only after the checks pass; an existing freshness
group never moves backwards or picks up a backfill hold (a new group keeps
its hold and the first weekly runs re-read from it, which is reported).
Seeds are named `data/d1_seed_<date>_<tag>_<since>.sql.gz`; several seeds in
one push load in path order (`load-d1-seeds.yml`), so a chunk that must win
(narratives over a titles-only history) needs a tag that sorts last.

The 2026-11 expansion ran this way (see `docs/expansion-2026-11/`): one
worktree per vendor family, chunks of 14-90 days for portals that ban detail
pages, whole history for the JSON APIs.
