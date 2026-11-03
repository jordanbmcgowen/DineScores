"""data.js budget trimming (write_data_js / _embed_trim) and the D1 line cut."""
import json
import os
import tempfile
import unittest

import dinescores_pipeline as dp


def _rec(city, state, day, name):
    return {
        'name': name, 'address': f'{day} Main St', 'city': city, 'state': state, 'zip': '00000',
        'latitude': 33.0, 'longitude': -97.0, 'inspection_date': f'2026-{day:02d}-01',
        'original_score': 95, 'risk_score': 95, 'priority_violations': 0,
        'priority_foundation_violations': 0, 'core_violations': 0, 'total_violations': 0,
        'violations': [], 'inspection_type': 'Routine', 'results': '', 'source': 'Test',
        'source_url': '', 'source_id': name, 'metro': '',
    }


class EmbedTrimTests(unittest.TestCase):
    def _compact(self, city, state, n):
        # newest first, like write_data_js builds by_city
        return [{'i': f'{city}{state}{k}', 'c': city, 's': state, 'd': f'2026-{12 - k // 28:02d}-{28 - k % 28:02d}'}
                for k in range(n)]

    def test_drops_from_the_deepest_cities_never_below_the_floor(self):
        big = self._compact('Dallas', 'TX', 300)
        mid = self._compact('Plano', 'TX', 100)
        town = self._compact('Rome', 'GA', 25)
        by_city = {('Dallas', 'TX'): list(big), ('Plano', 'TX'): list(mid), ('Rome', 'GA'): list(town)}
        final = big + mid + town
        kept = dp._embed_trim(final, by_city, 250)
        counts = {}
        for r in kept:
            counts[r['c']] = counts.get(r['c'], 0) + 1
        self.assertEqual(len(kept), 425 - 250)
        # water-filling: Dallas and Plano level out at the same depth, Rome untouched
        self.assertEqual(counts['Rome'], 25)
        self.assertLessEqual(abs(counts['Dallas'] - counts['Plano']), 1)
        # the newest records of every city survive
        for city, recs in (('Dallas', big), ('Plano', mid)):
            kept_ids = {r['i'] for r in kept if r['c'] == city}
            self.assertTrue(all(r['i'] in kept_ids for r in recs[:counts[city]]))
        # asking for more than the floors allow stops at the floor
        kept = dp._embed_trim(final, by_city, 10_000)
        self.assertEqual({r['c'] for r in kept}, {'Dallas', 'Plano', 'Rome'})
        self.assertTrue(all(sum(1 for r in kept if r['c'] == c) == dp.EMBED_CITY_FLOOR for c in ('Dallas', 'Plano', 'Rome')))

    def test_same_named_towns_in_two_states_have_their_own_floor(self):
        dallas_tx = self._compact('Dallas', 'TX', 200)
        dallas_ga = self._compact('Dallas', 'GA', 10)
        by_city = {('Dallas', 'TX'): list(dallas_tx), ('Dallas', 'GA'): list(dallas_ga)}
        kept = dp._embed_trim(dallas_tx + dallas_ga, by_city, 150)
        self.assertEqual(sum(1 for r in kept if r['s'] == 'GA'), 10)
        self.assertEqual(sum(1 for r in kept if r['s'] == 'TX'), 50)

    def test_write_data_js_keeps_the_floor_of_a_small_town_under_the_budget(self):
        old_budget = dp.EMBED_BUDGET
        dp.EMBED_BUDGET = 60
        try:
            recs = [_rec('Dallas', 'TX', 1 + k % 12, f'Dallas {k}') for k in range(80)]
            recs += [_rec('Dallas', 'GA', 1 + k % 12, f'Dallas GA {k}') for k in range(5)]
            with tempfile.TemporaryDirectory() as d:
                out = os.path.join(d, 'data.js')
                dp.write_data_js(recs, out)
                data = json.loads(open(out).read().split('window.DATA = ', 1)[1].rstrip(';\n'))
            self.assertEqual(len(data), 60)
            self.assertEqual(sum(1 for r in data if r['s'] == 'GA'), 5)
            self.assertEqual(sum(1 for r in data if r['s'] == 'TX'), 55)
        finally:
            dp.EMBED_BUDGET = old_budget


class D1LineCutTests(unittest.TestCase):
    def test_short_lists_keep_form_order_and_long_lists_keep_the_worst_tier(self):
        short = [['a', 'core', 'c1'], ['b', 'priority', 'p1'], ['c', 'core', 'c2']]
        self.assertEqual(dp._d1_violation_lines(short), short)
        long = [['x', 'core', f'c{k}'] for k in range(25)] + [['y', 'priority', 'p1'], ['z', 'priority_foundation', 'pf1']]
        cut = dp._d1_violation_lines(long)
        self.assertEqual(len(cut), dp.D1_VIOLATION_LINES)
        self.assertEqual(cut[0], ['y', 'priority', 'p1'])
        self.assertEqual(cut[1], ['z', 'priority_foundation', 'pf1'])
        self.assertEqual([v[2] for v in cut[2:]], [f'c{k}' for k in range(18)])   # stable within a tier


if __name__ == '__main__':
    unittest.main()
