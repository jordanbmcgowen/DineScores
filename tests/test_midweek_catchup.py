"""scripts/midweek_catchup.py: the Wednesday run re-fetches a Sunday MHD
group that came back blocked, skipped or errored."""
import os
import sys
import unittest
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'scripts'))
import midweek_catchup as mc  # noqa: E402

SUNDAY = ('chicago florida portland coloradosprings gwinnett nashville dallas plano frisco '
          'fort-worth-texas tarrant arlington richardson georgia').split()
WEDNESDAY = datetime(2026, 11, 25)


class CatchupTests(unittest.TestCase):
    def test_groups_are_the_sunday_mhd_groups_only(self):
        groups = mc.group_slugs(SUNDAY)
        self.assertEqual(groups['DFW'], ['dallas', 'plano', 'frisco', 'fort-worth-texas', 'tarrant'])
        self.assertEqual(groups['MHD metros'], ['portland', 'coloradosprings', 'gwinnett', 'nashville'])
        self.assertEqual(set(groups), {'DFW', 'MHD metros'})

    def test_a_blocked_sunday_is_fetched_again_on_wednesday(self):
        f = {'groups': {'DFW': {'last_run': {'status': 'blocked', 'at': '2026-11-22'}},
                        'MHD metros': {'last_run': {'status': 'ok', 'at': '2026-11-22'}}}}
        self.assertEqual(mc.catchup(f, SUNDAY, WEDNESDAY),
                         ['dallas', 'plano', 'frisco', 'fort-worth-texas', 'tarrant'])

    def test_a_good_or_old_sunday_adds_nothing(self):
        ok = {'groups': {'DFW': {'last_run': {'status': 'ok', 'at': '2026-11-22'}}}}
        old = {'groups': {'DFW': {'last_run': {'status': 'blocked', 'at': '2026-11-08'}}}}
        self.assertEqual(mc.catchup(ok, SUNDAY, WEDNESDAY), [])
        self.assertEqual(mc.catchup(old, SUNDAY, WEDNESDAY), [])
        self.assertEqual(mc.catchup({}, SUNDAY, WEDNESDAY), [])


if __name__ == '__main__':
    unittest.main()
