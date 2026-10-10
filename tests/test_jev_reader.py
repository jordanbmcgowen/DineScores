#!/usr/bin/env python3
"""Unit tests for jev_reader.py. No network: every HTTP exchange goes through
a scripted fake opener. Run with:  python3 -m unittest tests.test_jev_reader -v
"""
import gzip
import inspect
import io
import json
import logging
import os
import socket
import sys
import tempfile
import unittest
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import jev_reader as jr          # noqa: E402
import dinescores_pipeline as dp  # noqa: E402

SECRET = 'sk-test-SECRET-0123456789abcdef-DO-NOT-LEAK'

GOLDEN_GAP = ('No Evidence of Insect contamination, rodent/other animals: There is a hole '
              'or gap along the floor, wall or ceiling.')
GOLDEN_ROACH = ('INSECTS, RODENTS, & ANIMALS NOT PRESENT: Found live cockroaches in rear '
                'storage room around water heater')
GOLDEN_BACKFLOW = ('Anti-siphonage or back-flow prevention device not provided where required; '
                   'equipment or floor not properly drained; sewage disposal system in disrepair '
                   'or not functioning properly. Condensation or liquid waste improperly disposed of.')


class FakeResponse:
    def __init__(self, status, body, headers=None):
        self.status = status
        self._body = body if isinstance(body, bytes) else json.dumps(body).encode('utf-8')
        self.headers = headers or {}

    def read(self):
        return self._body

    def getcode(self):
        return self.status


def good_answers(n, cat='general_cleanliness', haz='none', summ=jr.SUMMARY_NO_MATCH, conf=0.9,
                 tokens_per_line=1500):
    answers = {}
    for i in range(n):
        for q, choice in (('category', cat), ('hazard', haz), ('summary', summ)):
            answers[f'{q}_{i}'] = {'type': 'choice', 'choice': choice, 'confidence': conf,
                                   'probabilities': {choice: conf, 'other': round(1 - conf, 3)}}
    return {'model': jr.JEV_MODEL, 'answers': answers,
            'usage': {'input_tokens': tokens_per_line * n, 'output_tokens': 12 * n}}


class ScriptedOpener:
    """open_fn replaying outcomes in order; records every request.
    Steps: ('ok', body) | ('http', status, body_bytes) | ('timeout',) |
           ('urlerror', reason) | ('ok_fn', callable(request_body_dict) -> body)."""

    def __init__(self, steps):
        self.steps = list(steps)
        self.requests = []

    def __call__(self, req, timeout):
        self.requests.append(req)
        if not self.steps:
            raise AssertionError('unexpected extra request')
        step = self.steps.pop(0)
        kind = step[0]
        if kind == 'ok':
            return FakeResponse(200, step[1])
        if kind == 'ok_fn':
            body = json.loads(req.data.decode('utf-8'))
            return FakeResponse(200, step[1](body))
        if kind == 'http':
            body = step[2] if isinstance(step[2], bytes) else json.dumps(step[2]).encode()
            raise urllib.error.HTTPError(req.full_url, step[1], 'error', {}, io.BytesIO(body))
        if kind == 'timeout':
            raise socket.timeout('timed out')
        if kind == 'urlerror':
            raise urllib.error.URLError(step[1])
        raise AssertionError(f'bad step {step}')


def make_reader(steps, mode='decide', cache_dir=None, **kw):
    opener = ScriptedOpener(steps)
    client = jr.JevClient(api_key=SECRET, open_fn=opener, sleep_fn=lambda s: None)
    cache = jr.JudgmentCache(cache_dir or tempfile.mkdtemp())
    reader = jr.JevReader(mode=mode, client=client, cache=cache, **kw)
    return reader, opener


class FallbackTests(unittest.TestCase):

    def test_timeout_falls_back_to_regex(self):
        reader, opener = make_reader([('timeout',)] * 10)
        res = reader.judge_texts([GOLDEN_ROACH])
        nt = jr.normalize_text(GOLDEN_ROACH)
        self.assertIsNone(res[nt])
        self.assertEqual(reader.stats['lines_fallback'], 1)
        self.assertIn('JevUnavailable', ''.join(reader.stats['fallback_reasons']))
        # retried MAX_RETRIES times then gave up
        self.assertEqual(len(opener.requests), jr.MAX_RETRIES + 1)
        d = jr.decide(GOLDEN_ROACH, res[nt])
        self.assertEqual(d['used'], {'category': 'regex', 'hazard': 'regex', 'summary': 'regex'})
        self.assertEqual(d['category'], dp.classify_violation(GOLDEN_ROACH)[0])
        self.assertEqual(d['forces_f'], dp.has_active_pest_or_sewage(GOLDEN_ROACH))
        self.assertEqual(d['summary'], dp._paraphrase_violation(GOLDEN_ROACH))
        self.assertEqual(d['chips'], dp.detect_infractions(GOLDEN_ROACH))

    def test_429_then_success(self):
        slept = []
        opener = ScriptedOpener([('http', 429, b'{"error":"rate limited"}'),
                                 ('ok', good_answers(1, cat='pest_control', haz='vermin_observed',
                                                     summ='Live roaches found'))])
        client = jr.JevClient(api_key=SECRET, open_fn=opener, sleep_fn=slept.append)
        reader = jr.JevReader(mode='decide', client=client, cache=jr.JudgmentCache(tempfile.mkdtemp()))
        res = reader.judge_texts([GOLDEN_ROACH])
        j = res[jr.normalize_text(GOLDEN_ROACH)]
        self.assertEqual(j['hazard']['choice'], 'vermin_observed')
        self.assertEqual(len(slept), 1)
        self.assertGreater(slept[0], 0)
        self.assertEqual(reader.stats['requests'], 1)
        self.assertEqual(reader.stats['lines_judged'], 1)
        self.assertEqual(reader.stats['lines_fallback'], 0)

    def test_529_exhausts_retries_then_falls_back(self):
        reader, opener = make_reader([('http', 529, b'overloaded')] * 10)
        res = reader.judge_texts(['Floor dirty under the fryer'])
        self.assertIsNone(list(res.values())[0])
        self.assertEqual(len(opener.requests), jr.MAX_RETRIES + 1)
        self.assertEqual(reader.stats['request_failures'], 1)

    def test_401_disables_jev_for_the_run(self):
        reader, opener = make_reader([('http', 401, b'{"error":"unauthorized"}')])
        res = reader.judge_texts([GOLDEN_GAP, GOLDEN_ROACH])
        self.assertTrue(all(v is None for v in res.values()))
        self.assertEqual(reader.stats['fallback_reasons'].get('auth error (401)'), 2)
        self.assertFalse(reader.active)
        # a second call makes no request at all
        res2 = reader.judge_texts(['Another line about cold holding at 50F'])
        self.assertTrue(all(v is None for v in res2.values()))
        self.assertEqual(len(opener.requests), 1)

    def test_malformed_json_falls_back(self):
        reader, opener = make_reader([('ok', b'this is not json')])
        res = reader.judge_texts([GOLDEN_GAP])
        self.assertIsNone(list(res.values())[0])
        self.assertIn('JevMalformed', ''.join(reader.stats['fallback_reasons']))

    def test_malformed_answer_falls_back_for_that_question_only(self):
        body = good_answers(1, cat='general_cleanliness', haz='conditions_only',
                            summ='Gaps/Openings (Risk of entry)')
        body['answers']['category_0']['choice'] = 'not_a_real_category'
        reader, opener = make_reader([('ok', body)])
        res = reader.judge_texts([GOLDEN_GAP])
        j = res[jr.normalize_text(GOLDEN_GAP)]
        self.assertNotIn('category', j)
        self.assertEqual(j['hazard']['choice'], 'conditions_only')
        d = jr.decide(GOLDEN_GAP, j)
        self.assertEqual(d['used']['category'], 'regex')
        self.assertEqual(d['used']['hazard'], 'jev')
        self.assertFalse(d['forces_f'])          # today's regex says F; Jev's conditions_only wins
        self.assertEqual(reader.stats['fallback_reasons'].get('malformed answer'), 1)

    def test_low_confidence_falls_back(self):
        body = good_answers(1, cat='maintenance', haz='conditions_only',
                            summ='Floors/Walls dirty or damaged', conf=0.2)
        reader, opener = make_reader([('ok', body)])
        res = reader.judge_texts([GOLDEN_GAP])
        d = jr.decide(GOLDEN_GAP, res[jr.normalize_text(GOLDEN_GAP)])
        self.assertEqual(d['used'], {'category': 'regex', 'hazard': 'regex', 'summary': 'regex'})
        self.assertEqual(d['category'], 'pest_control')         # regex
        self.assertTrue(d['forces_f'])                           # regex (title match) stands
        # at the floor, Jev decides in either direction
        body2 = good_answers(1, cat='maintenance', haz='conditions_only',
                             summ='Floors/Walls dirty or damaged', conf=jr.CONFIDENCE_FLOORS['hazard'])
        reader2, _ = make_reader([('ok', body2)])
        res2 = reader2.judge_texts([GOLDEN_GAP])
        d2 = jr.decide(GOLDEN_GAP, res2[jr.normalize_text(GOLDEN_GAP)])
        self.assertEqual(d2['used']['hazard'], 'jev')
        self.assertFalse(d2['forces_f'])

    def test_deadline_in_the_past_asks_nothing(self):
        reader, opener = make_reader([('ok', good_answers(1))], deadline=1.0)
        res = reader.judge_texts([GOLDEN_GAP])
        self.assertIsNone(list(res.values())[0])
        self.assertEqual(len(opener.requests), 0)
        self.assertIn('deadline reached', reader.stats['fallback_reasons'])

    def test_422_splits_the_batch(self):
        def answer_fn(body):
            n = len(body['state'])
            return good_answers(n)
        reader, opener = make_reader([('http', 422, b'{"detail":"too long"}'),
                                      ('ok_fn', answer_fn), ('ok_fn', answer_fn)])
        texts = [f'Observed line number {i} with grease on the floor' for i in range(4)]
        res = reader.judge_texts(texts)
        self.assertTrue(all(v is not None for v in res.values()))
        self.assertEqual(len(opener.requests), 3)
        self.assertEqual(reader.stats['lines_judged'], 4)

    def test_mode_off_makes_no_requests(self):
        reader, opener = make_reader([('ok', good_answers(1))], mode='off')
        self.assertEqual(reader.judge_texts([GOLDEN_GAP]), {})
        self.assertEqual(len(opener.requests), 0)
        # and decide with no judgment is exactly today's reading
        d = jr.decide(GOLDEN_GAP, None)
        self.assertEqual(d['regex']['category'], d['category'])
        self.assertEqual(jr.summarize_violations([GOLDEN_GAP, GOLDEN_ROACH], None),
                         dp.summarize_violations('|||'.join([GOLDEN_GAP, GOLDEN_ROACH])))
        self.assertEqual(jr.detect_infractions([GOLDEN_GAP, GOLDEN_ROACH], None),
                         dp.detect_infractions('|||'.join([GOLDEN_GAP, GOLDEN_ROACH])))


class VerifyAloneTests(unittest.TestCase):

    def test_pest_hazard_from_a_batch_is_reasked_alone_and_the_single_answer_wins(self):
        benign = 'Item 11B: Wiping cloths dirty, not stored properly in sanitizing solutions'
        batch_body = good_answers(2, haz='none')
        batch_body['answers']['hazard_1'] = {'type': 'choice', 'choice': 'vermin_observed', 'confidence': 1.0,
                                             'probabilities': {'vermin_observed': 1.0}}
        single_body = good_answers(1, haz='none', conf=0.9)

        def single_fn(body):
            self.assertEqual(list(body['state']), ['line_0'])
            self.assertEqual(body['state']['line_0'], benign)
            return single_body
        reader, opener = make_reader([('ok_fn', lambda body: batch_body), ('ok_fn', single_fn)])
        res = reader.judge_texts(['Floors soiled under the fryer', benign])
        self.assertEqual(len(opener.requests), 2)
        self.assertEqual(res[jr.normalize_text(benign)]['hazard']['choice'], 'none')
        self.assertEqual(reader.stats['hazard_verified_alone'], 1)
        self.assertEqual(reader.stats['hazard_changed_on_verify'], 1)
        path = reader.flush_cache('t')
        with gzip.open(path, 'rt') as f:
            recs = {json.loads(l)['t']: json.loads(l) for l in f}
        self.assertEqual(recs[benign]['a']['hazard']['c'], 'none')          # the cache holds the verified answer

    def test_none_on_a_text_naming_a_pest_is_reasked_alone(self):
        body = good_answers(2, haz='none')
        single = good_answers(1, haz='vermin_observed')
        reader, opener = make_reader([('ok', body), ('ok', single)])
        res = reader.judge_texts(['Floors soiled under the fryer', GOLDEN_ROACH])
        self.assertEqual(len(opener.requests), 2)
        self.assertEqual(res[jr.normalize_text(GOLDEN_ROACH)]['hazard']['choice'], 'vermin_observed')
        self.assertEqual(res[jr.normalize_text('Floors soiled under the fryer')]['hazard']['choice'], 'none')

    def test_single_line_requests_are_not_reverified(self):
        reader, opener = make_reader([('ok', good_answers(1, haz='vermin_observed'))], lines_per_request=1)
        reader.judge_texts([GOLDEN_ROACH])
        self.assertEqual(len(opener.requests), 1)
        self.assertEqual(reader.stats['hazard_verified_alone'], 0)

    def test_lines_per_request_is_a_cap_even_after_usage_is_measured(self):
        seen = []

        def fn(body):
            seen.append(len(body['state']))
            return good_answers(len(body['state']), tokens_per_line=10)   # tiny usage would allow huge batches
        reader, opener = make_reader([('ok_fn', fn)] * 6, lines_per_request=1)
        reader.judge_texts([f'Floor soiled in area {i}' for i in range(6)])
        self.assertEqual(seen, [1] * 6)
        reader2, _ = make_reader([('ok_fn', fn)] * 6, lines_per_request=2)
        seen.clear()
        reader2.judge_texts([f'Floor soiled in area {i}' for i in range(6)])
        self.assertEqual(seen, [2, 2, 2])

    def test_failed_verification_falls_back_for_that_line_only(self):
        body = good_answers(2, haz='none')
        body['answers']['hazard_1']['choice'] = 'sewage_observed'
        reader, opener = make_reader([('ok', body)] + [('http', 529, b'x')] * 10)
        res = reader.judge_texts(['Floors soiled under the fryer', GOLDEN_BACKFLOW])
        self.assertIsNotNone(res[jr.normalize_text('Floors soiled under the fryer')])
        self.assertIsNone(res[jr.normalize_text(GOLDEN_BACKFLOW)])
        self.assertIn('hazard verification failed', reader.stats['fallback_reasons'])
        self.assertEqual(reader.cache.pending, 1)                              # the unverified line is not cached


class CacheTests(unittest.TestCase):

    def test_cache_hit_makes_zero_requests(self):
        cache_dir = tempfile.mkdtemp()
        reader, opener = make_reader([('ok', good_answers(2, haz='conditions_only'))], cache_dir=cache_dir)
        res = reader.judge_texts([GOLDEN_GAP, '  ' + GOLDEN_BACKFLOW.replace(' ', '  ') + '\n'])
        self.assertEqual(len(opener.requests), 1)
        self.assertEqual(len(res), 2)
        path = reader.flush_cache('test')
        self.assertTrue(path and path.endswith('.jsonl.gz') and os.path.exists(path))
        self.assertIsNone(reader.flush_cache('test'))          # nothing new -> no file

        def boom(req, timeout):
            raise AssertionError('network must not be touched on a cache hit')
        client = jr.JevClient(api_key=SECRET, open_fn=boom)
        reader2 = jr.JevReader(mode='decide', client=client, cache=jr.JudgmentCache(cache_dir))
        res2 = reader2.judge_texts([GOLDEN_GAP, GOLDEN_BACKFLOW])
        self.assertEqual(reader2.stats['lines_cached'], 2)
        self.assertEqual(reader2.stats['requests'], 0)
        for nt, j in res2.items():
            self.assertEqual(j['hazard']['choice'], 'conditions_only')
            self.assertEqual(res[nt]['hazard']['choice'], j['hazard']['choice'])

    def test_key_is_over_model_and_question_set(self):
        k1 = jr.judgment_key('a  b', 'qs1', 'jev-1.13.0')
        self.assertEqual(k1, jr.judgment_key(' a b ', 'qs1', 'jev-1.13.0'))   # normalized
        self.assertNotEqual(k1, jr.judgment_key('a b', 'qs2', 'jev-1.13.0'))  # version bump misses
        self.assertNotEqual(k1, jr.judgment_key('a b', 'qs1', 'jev-1.14.0'))  # model bump misses
        self.assertNotEqual(k1, jr.judgment_key('A b', 'qs1', 'jev-1.13.0'))  # case kept

    def test_cache_file_holds_only_this_runs_new_answers(self):
        cache_dir = tempfile.mkdtemp()
        reader, _ = make_reader([('ok', good_answers(1))], cache_dir=cache_dir)
        reader.judge_texts(['Line one about shelving dust'])
        reader.flush_cache('a')
        reader2, _ = make_reader([('ok', good_answers(1))], cache_dir=cache_dir)
        reader2.judge_texts(['Line one about shelving dust', 'Line two about shelving dust'])
        path = reader2.flush_cache('b')
        with gzip.open(path, 'rt') as f:
            recs = [json.loads(l) for l in f]
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0]['t'], 'Line two about shelving dust')
        self.assertEqual(recs[0]['m'], jr.JEV_MODEL)
        self.assertEqual(recs[0]['q'], jr.QUESTION_SET_VERSION)
        self.assertEqual(len(os.listdir(cache_dir)), 2)


class SecrecyTests(unittest.TestCase):

    def test_key_never_appears_in_logs_or_exceptions(self):
        echo = json.dumps({'error': 'bad token', 'you_sent': f'Bearer {SECRET}'}).encode()
        steps = ([('http', 401, echo), ('http', 422, echo)]
                 + [('urlerror', f'proxy said {SECRET}')] * 4      # retried 3x, then JevUnavailable
                 + [('http', 503, echo)] * 4                         # retried 3x, then JevUnavailable
                 + [('http', 418, echo)])                            # unexpected status -> JevError
        opener = ScriptedOpener(steps)
        client = jr.JevClient(api_key=SECRET, open_fn=opener, sleep_fn=lambda s: None, max_retries=3)
        self.assertNotIn(SECRET, repr(client))
        state = {'line_0': 'x'}
        qs = jr.build_questions([0])
        with self.assertLogs('dinescores.jev', level='DEBUG') as cm:
            for expected in (jr.JevAuthError, jr.JevRequestError, jr.JevUnavailable, jr.JevUnavailable, jr.JevError):
                with self.assertRaises(expected) as ctx:
                    client.system_one(state, qs)
                self.assertNotIn(SECRET, str(ctx.exception))
                self.assertNotIn(SECRET, repr(ctx.exception))
            logging.getLogger('dinescores.jev').debug('done')
        self.assertNotIn(SECRET, '\n'.join(cm.output))
        # the header did carry it (sanity: we are testing scrubbing, not absence)
        self.assertEqual(opener.requests[0].get_header('Authorization'), f'Bearer {SECRET}')

    def test_reader_logs_never_carry_the_key_or_request_bodies(self):
        echo = f'{{"detail":"{SECRET}"}}'.encode()
        reader, opener = make_reader([('http', 401, echo)])
        with self.assertLogs('dinescores.jev', level='DEBUG') as cm:
            reader.judge_texts(['Observed mice droppings under the prep table'])
        out = '\n'.join(cm.output) + '\n'.join(reader.summary_lines())
        self.assertNotIn(SECRET, out)
        self.assertNotIn('Observed mice droppings', out)       # request bodies are never logged

    def test_state_holds_only_the_lines(self):
        captured = {}

        def answer_fn(body):
            captured.update(body)
            return good_answers(len(body['state']))
        reader, opener = make_reader([('ok_fn', answer_fn)])
        reader.judge_texts([GOLDEN_GAP, GOLDEN_ROACH])
        self.assertEqual(set(captured['state']), {'line_0', 'line_1'})
        self.assertEqual(sorted(captured['state'].values()), sorted([GOLDEN_GAP, GOLDEN_ROACH]))
        self.assertEqual(captured['model'], jr.JEV_MODEL)
        self.assertEqual(set(captured) , {'model', 'state', 'questions'})
        self.assertEqual(len(captured['questions']), 6)


class MarkerTests(unittest.TestCase):

    def test_explicit_markers(self):
        cases = {
            'Good Repair and Proper Adjustment-Equipment (C): Repair leak under the dish machine.': 'core',
            'Proper Date Marking and Disposition (Pf): Items not date marked.': 'priority_foundation',
            'Proper Cold Holding (P): Milk at 48F.': 'priority',
            '**P** Observed raw chicken stored over lettuce.': 'priority',
            '** PF ** Observed no test strips.': 'priority_foundation',
            'Food contact surfaces; Priority Foundation; cutting boards scored': 'priority_foundation',
            'Floors soiled; Core; clean floors': 'core',
            'Hot holding; Priority; chicken at 120F': 'priority',
        }
        for text, tier in cases.items():
            self.assertEqual(jr.parse_tier_marker(text), tier, text)

    def test_traps_are_not_markers(self):
        for text in ('3717-1-04.8(P) - Violation - Food contact surfaces not clean',
                     '4(P) - Equipment not cleaned at required frequency',
                     '3717-1-06.4(A) - Violation - 4(P) - floors',
                     'Food Protection (A)-(P): raw beef over produce',
                     'Non-Food Contact Surfaces (A)-(P): soiled shelving',
                     'Something (P) with no colon after it',
                     'Vermin present (p) see notes'):
            self.assertIsNone(jr.parse_tier_marker(text), text)

    def test_marker_sets_tier_but_source_tier_wins(self):
        text = 'Good Repair and Proper Adjustment-Equipment (C): Repair leak under the dish machine at the basement level bar.'
        d = jr.decide(text, None)
        self.assertEqual(d['severity'], 'core')
        self.assertEqual(d['tier_from'], 'marker')
        d2 = jr.decide(text, None, source_severity='priority_foundation')
        self.assertEqual(d2['severity'], 'priority_foundation')
        self.assertEqual(d2['tier_from'], 'source')
        # Cincinnati's paragraph letter must not override the category tier
        cin = '4(P) - Food contact surfaces of equipment not cleaned and sanitized at required frequency'
        d3 = jr.decide(cin, None)
        self.assertEqual(d3['tier_from'], 'category')
        self.assertEqual(d3['severity'], dp.SEVERITY_MAP[d3['category']])
        self.assertEqual(jr.classify_violation(cin), dp.classify_violation(cin))


class PolicyTests(unittest.TestCase):

    def judged(self, haz, cat='pest_control', summ=jr.SUMMARY_NO_MATCH, conf=0.95):
        return {'category': {'choice': cat, 'confidence': conf, 'probabilities': {}},
                'hazard': {'choice': haz, 'confidence': conf, 'probabilities': {}},
                'summary': {'choice': summ, 'confidence': conf, 'probabilities': {}}}

    def test_only_policy_hazards_force_an_f(self):
        text = 'INSECTS, RODENTS, & ANIMALS NOT PRESENT: Insects were present. Fruit flies in back area'
        self.assertTrue(jr.decide(text, self.judged('vermin_observed'))['forces_f'])
        self.assertTrue(jr.decide(text, self.judged('sewage_observed'))['forces_f'])
        self.assertFalse(jr.decide(text, self.judged('flies_or_gnats_observed'))['forces_f'])
        self.assertFalse(jr.decide(text, self.judged('conditions_only'))['forces_f'])
        self.assertFalse(jr.decide(text, self.judged('none'))['forces_f'])
        self.assertEqual(jr.F_FORCING_HAZARDS, {'vermin_observed', 'sewage_observed'})
        # pests without a named kind do not force an F by default; the alternative set does
        self.assertFalse(jr.decide(text, self.judged('pests_unspecified'))['forces_f'])
        self.assertTrue(jr.decide(text, self.judged('pests_unspecified'),
                                  forcing=jr.F_FORCING_HAZARDS_WITH_UNSPECIFIED)['forces_f'])
        self.assertTrue(jr.decide(text, self.judged('flies_or_gnats_observed'),
                                  forcing=jr.F_FORCING_HAZARDS_WITH_FLIES)['forces_f'])
        self.assertEqual(jr.decide(text, self.judged('pests_unspecified'))['chips'], ['pests'])
        self.assertEqual(jr.decide(text, self.judged('pests_unspecified'))['summary_severity'], 'CRITICAL')

    def test_points_and_cutoffs_are_untouched(self):
        # Jev output never reaches calc_risk_score: it gets (cat, sev, text) tuples
        vio = [jr.classify_violation(GOLDEN_GAP, {jr.normalize_text(GOLDEN_GAP): self.judged('conditions_only')}),
               jr.classify_violation('Milk at 48F in reach-in', {jr.normalize_text('Milk at 48F in reach-in'):
                                                                  self.judged('none', cat='temperature_control')})]
        score, pv, pfv, cv = dp.calc_risk_score(vio)
        self.assertEqual((score, pv, pfv, cv), (100 - 5 - 1, 1, 0, 1))
        self.assertEqual(dp.calculate_vetted_grade(94), 'A')
        self.assertEqual(dp.calculate_vetted_grade(69), 'F')

    def test_pest_or_sewage_phrase_needs_its_hazard(self):
        backflow = GOLDEN_BACKFLOW
        # Jev's own summary says sewage but its hazard (confident) says conditions_only -> truncation
        j = self.judged('conditions_only', cat='water_sewage', summ='Sewage backup/issue')
        d = jr.decide(backflow, j)
        self.assertEqual(d['summary'], backflow[:45] + '...')
        self.assertEqual(d['used']['summary'], 'hazard_override')
        # summary below floor -> regex fallback says "Sewage backup/issue" -> still overridden
        j2 = self.judged('conditions_only', cat='water_sewage', summ='Sewage backup/issue')
        j2['summary']['confidence'] = 0.1
        d2 = jr.decide(backflow, j2)
        self.assertNotEqual(d2['summary'], 'Sewage backup/issue')
        self.assertEqual(d2['used']['summary'], 'hazard_override')
        # hazard not confident -> no override, regex phrase stands
        j3 = dict(j2); j3['hazard'] = {'choice': 'conditions_only', 'confidence': 0.2, 'probabilities': {}}
        self.assertEqual(jr.decide(backflow, j3)['summary'], 'Sewage backup/issue')
        # matching hazard keeps the phrase
        j4 = self.judged('vermin_observed', summ='Live roaches found')
        self.assertEqual(jr.decide(GOLDEN_ROACH, j4)['summary'], 'Live roaches found')

    def test_summary_none_of_these_keeps_truncation(self):
        long_text = 'x' * 120
        d = jr.decide(long_text, self.judged('none', cat='unclassified'))
        self.assertEqual(d['summary'], long_text[:45] + '...')
        self.assertEqual(d['used']['summary'], 'jev')

    def test_summaries_and_chips_from_code_maps(self):
        j = {jr.normalize_text(GOLDEN_GAP): self.judged('conditions_only', summ='Gaps/Openings (Risk of entry)'),
             jr.normalize_text(GOLDEN_ROACH): self.judged('vermin_observed', summ='Live roaches found')}
        items = jr.summarize_violations([GOLDEN_GAP, GOLDEN_ROACH], j)
        self.assertEqual([s['text'] for s in items], ['Live roaches found', 'Gaps/Openings (Risk of entry)'])
        self.assertEqual(items[0]['severity'], 'CRITICAL')
        self.assertEqual(items[1]['category'], 'Structural Risk')
        self.assertEqual(set(items[0]), {'text', 'verbatim', 'severity', 'category'})
        self.assertEqual(jr.detect_infractions([GOLDEN_GAP], j), ['equipment'])
        self.assertEqual(jr.detect_infractions([GOLDEN_ROACH], j), ['pests'])
        self.assertTrue(jr.has_active_pest_or_sewage([GOLDEN_GAP, GOLDEN_ROACH], j))
        self.assertFalse(jr.has_active_pest_or_sewage([GOLDEN_GAP], j))
        ware = 'WAREWASHING AND SANITIZING PROCEDURES'
        jw = {jr.normalize_text(ware): self.judged('none', cat='equipment_utensils')}
        self.assertEqual(jr.detect_infractions([ware], jw), ['equipment'])   # no hygiene chip


class PurityTests(unittest.TestCase):

    def test_scoring_makes_no_requests(self):
        def boom(*a, **k):
            raise AssertionError('network touched')
        saved = (urllib.request.urlopen, socket.create_connection, socket.socket.connect)
        urllib.request.urlopen = boom
        socket.create_connection = boom
        socket.socket.connect = boom
        try:
            score, pv, pfv, cv = dp.calc_risk_score([('pest_control', 'core', GOLDEN_ROACH),
                                                     ('temperature_control', 'priority', 'milk 48F')])
            self.assertEqual(score, 94)
            self.assertEqual(dp.calculate_vetted_grade(94, GOLDEN_ROACH), 'F')
            self.assertEqual(dp.calculate_vetted_grade(94, GOLDEN_GAP), 'F')   # today's title misfire
            self.assertEqual(dp.calculate_vetted_grade(94, 'floor dirty'), 'A')
            j = {'hazard': {'choice': 'conditions_only', 'confidence': 1.0, 'probabilities': {}}}
            self.assertFalse(jr.decide(GOLDEN_GAP, j)['forces_f'])
            jr.classify_violation(GOLDEN_GAP, {jr.normalize_text(GOLDEN_GAP): j})
            jr.summarize_violations([GOLDEN_GAP], {jr.normalize_text(GOLDEN_GAP): j})
        finally:
            urllib.request.urlopen, socket.create_connection, socket.socket.connect = saved

    def test_scoring_source_has_no_network_code(self):
        for fn in (dp.calc_risk_score, dp.calculate_vetted_grade, dp.has_active_pest_or_sewage,
                   jr.decide, jr.classify_violation, jr.summarize_violations, jr.detect_infractions,
                   jr.has_active_pest_or_sewage):
            src = inspect.getsource(fn)
            for token in ('urlopen', 'requests.', 'system_one', 'JevClient', 'socket'):
                self.assertNotIn(token, src, f'{fn.__name__} contains {token}')

    def test_question_set_is_versioned(self):
        # Any wording change to the questions must come with a QUESTION_SET_VERSION
        # bump (the cache key depends on it). Update BOTH values here when you bump.
        self.assertEqual(jr.QUESTION_SET_VERSION, jr.PINNED_QUESTION_SET_VERSION)
        self.assertEqual(jr.question_set_fingerprint(), jr.PINNED_QUESTION_SET_FINGERPRINT,
                         'the question wording changed: bump QUESTION_SET_VERSION and re-pin the fingerprint')
        self.assertEqual(jr.JEV_MODEL, 'jev-1.13.0')

    def test_options_match_the_pipeline(self):
        self.assertEqual(jr.category_options(), list(dp.VIOLATION_CATEGORIES) + ['unclassified'])
        self.assertEqual(len(jr.SUMMARY_PHRASES), 27)
        src = inspect.getsource(dp._paraphrase_violation)
        for phrase in jr.SUMMARY_PHRASES:
            self.assertIn(f'return "{phrase}"', src, phrase)
        qs = jr.build_questions([3])
        self.assertEqual(set(qs), {'category_3', 'hazard_3', 'summary_3'})
        for q in qs.values():
            self.assertIn('`line_3`', q['instructions']['question'])
        self.assertEqual(set(qs['hazard_3']['criteria']), set(jr.HAZARD_OPTIONS))
        self.assertIn(jr.SUMMARY_NO_MATCH, qs['summary_3']['criteria'])


class ModeTests(unittest.TestCase):

    def test_mode_and_decide_parsing(self):
        self.assertEqual(jr.jev_mode({}), 'off')
        self.assertEqual(jr.jev_mode({'JEV_MODE': 'Shadow'}), 'shadow')
        self.assertEqual(jr.jev_mode({'JEV_MODE': 'bogus'}), 'off')
        self.assertEqual(jr.decide_questions({}), {'category', 'hazard', 'summary'})
        self.assertEqual(jr.decide_questions({'JEV_DECIDE': 'hazard, summary'}), {'hazard', 'summary'})
        self.assertEqual(jr.decide_questions({'JEV_DECIDE': 'hazard,bogus'}), {'hazard'})

    def test_from_env_without_key_is_inactive(self):
        reader = jr.JevReader.from_env({'JEV_MODE': 'decide', 'TYPESAFE_API_KEY': ''},
                                       cache_dir=tempfile.mkdtemp())
        self.assertFalse(reader.active)
        self.assertEqual(reader.judge_texts([GOLDEN_GAP]), {jr.normalize_text(GOLDEN_GAP): None})

    def test_shadow_use_set_keeps_regex_decisions(self):
        j = {'hazard': {'choice': 'conditions_only', 'confidence': 1.0, 'probabilities': {}}}
        d = jr.decide(GOLDEN_GAP, j, use=set())
        self.assertTrue(d['forces_f'])             # regex still decides
        self.assertEqual(d['jev']['hazard']['choice'], 'conditions_only')   # but the shadow answer is visible
        d2 = jr.decide(GOLDEN_GAP, j, use={'hazard'})
        self.assertFalse(d2['forces_f'])


if __name__ == '__main__':
    unittest.main()
