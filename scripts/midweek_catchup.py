#!/usr/bin/env python3
"""Sunday MyHealthDepartment slugs the Wednesday run should fetch again.

The Sunday refresh fetches DFW and the 'MHD metros' group (Portland, the CO
Front Range, Utah County, Yolo, Gwinnett, Tennessee, Fort Bend...) through
one portal that can block a whole run (2026-11-22: one blocked search wrote
off all of them). Before this, a lost Sunday waited a full week for the next
Sunday. The Wednesday run now adds the slugs of every such group whose last
run, within the last few days, was blocked, skipped or errored, so a lost
Sunday heals three days later. A group that ran fine adds nothing, so a
normal Wednesday is unchanged.

Usage: midweek_catchup.py FRESHNESS_FILE SUNDAY_SLUG...
Prints the extra slugs, space-separated (nothing when none are due).
"""
import json
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
import dinescores_pipeline as dp  # noqa: E402

FAILED = ('blocked', 'skipped', 'error')
MAX_AGE_DAYS = 5      # only the Sunday just before this Wednesday


def group_slugs(sunday_slugs):
    """{group: [Sunday slugs that fetch it]} for the Sunday MHD groups."""
    out = {'DFW': [s for s in sunday_slugs if s in dp.DFW_JURISDICTIONS or s == 'dfw']}
    for slug in sunday_slugs:
        keys = dp._mhd_metro_keys([slug])
        if not keys:
            continue
        for group in dp._mhd_metro_groups(keys):
            out.setdefault(group, [])
            if slug not in out[group]:
                out[group].append(slug)
    return {g: s for g, s in out.items() if s}


def catchup(freshness, sunday_slugs, today=None):
    today = today or datetime.now()
    floor = (today - timedelta(days=MAX_AGE_DAYS)).strftime('%Y-%m-%d')
    extra = []
    for group, slugs in group_slugs(sunday_slugs).items():
        run = ((freshness.get('groups') or {}).get(group) or {}).get('last_run') or {}
        if run.get('status') in FAILED and (run.get('at') or '') >= floor:
            extra.extend(s for s in slugs if s not in extra)
    return extra


def main(argv):
    if len(argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2
    try:
        with open(argv[0]) as f:
            freshness = json.load(f)
    except (OSError, ValueError):
        return 0          # no freshness file: nothing to catch up
    print(' '.join(catchup(freshness, argv[1:])))
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
