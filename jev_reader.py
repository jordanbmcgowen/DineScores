#!/usr/bin/env python3
"""
jev_reader.py: the DineScores violation READER on TypeSafe's Jev model.

Jev reads, code scores. Jev only ever picks options from fixed lists (a
category, a hazard, a summary phrase). Points (5/2/1), grade cutoffs
(A/B/C/F), the 60/30/10 weighting and the set of hazards that force an F all
live in code, here and in dinescores_pipeline.py, and never change because
of anything Jev says.

    JEV_MODE=off     (default) nothing here runs; the regex functions decide.
    JEV_MODE=shadow  Jev is asked and every answer is cached, but decisions
                     still come from the regex functions.
    JEV_MODE=decide  Jev decides the questions named in JEV_DECIDE
                     (default: all three) when its answer clears that
                     question's confidence floor; everything else falls
                     back to the regex functions.

The pipeline runs apply_to_inspections() once over every fetched record
after the fetch phase (its own --jev-deadline-minutes budget) and the
output paths read the per-record `_jev` result it leaves behind.

Fail open: any error, timeout, 429/529 after retries, malformed answer or a
top answer below its confidence floor means today's regex answers that line.

Same text, same answer, every week: the model id is pinned, every answer is
cached under sha256(model + question-set version + normalized text) in
append-only gzipped JSONL files under data/jev_judgments/, and a cached line
is never re-asked. Changing the questions or the model is a deliberate
version bump (QUESTION_SET_VERSION / JEV_MODEL), which simply misses the
old cache.

Only the violation line goes to the API: no name, address, id, city or
date. The API key lives only in TYPESAFE_API_KEY; it is never logged, never
part of an exception message, and request bodies are never logged either.

Stdlib only (urllib, json, hashlib, gzip, concurrent.futures): the weekly
GitHub runner installs nothing for the pipeline.
"""

import gzip
import hashlib
import http.client
import json
import logging
import os
import random
import re
import socket
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

log = logging.getLogger('dinescores.jev')

# ─── CONSTANTS (the things a reviewer should look at) ────────────────────────

API_URL = 'https://api.typesafe.ai/v1/systemone'
MODELS_URL = 'https://api.typesafe.ai/v1/models'

# Pinned. `jev-latest` moves when a release ships; a model change is a
# deliberate bump here (it changes every cache key, so nothing is reused).
JEV_MODEL = 'jev-1.13.0'

# Bump whenever ANY instruction or criteria text below changes. Part of the
# cache key, so a changed question set never reuses an answer to the old one.
QUESTION_SET_VERSION = 'qs3'

QUESTIONS = ('category', 'hazard', 'summary')

# Pinned by tests/test_jev_reader.py: a wording change must come with a
# version bump, or the cache would hand back answers to the old questions.
PINNED_QUESTION_SET_VERSION = 'qs3'
PINNED_QUESTION_SET_FINGERPRINT = '11d075f8490356fb922cc0e5e1d5bae948e70f0e89d99c5376e32c0486c86416'

# Below the floor, today's regex answers that line. At or above it, Jev
# decides in either direction. Confidence is TypeSafe's choice confidence:
# (p_max - 1/n) / (1 - 1/n). Set from the shadow report's hand-check and
# option-order test (docs/jev-shadow-report.md): below 0.6 the top answer
# flipped on 20-60% of texts when the options were reordered; at or above
# it on 3-4%, and Jev was right on 97-99% of hand-checked disagreements.
CONFIDENCE_FLOORS = {'category': 0.60, 'hazard': 0.60, 'summary': 0.60}

# POLICY: which hazards force an automatic F. Today's stated policy:
# observed rodents, roaches, ants, maggots/larvae or other named vermin, or
# sewage actually backing up, overflowing, leaking or exposed. Flies and
# gnats do not force an F today; the shadow report counts what they would
# add. `pests_unspecified` (pests reported without a kind: "Insects, rodents
# present", "Evidence of rodents or insects observed") is NOT in the set:
# today's regex forces an F on some of those labels and not others, purely
# by wording, so there is no consistent "today" to keep. The shadow report
# shows both outcomes; flipping the policy is a one-line change here.
F_FORCING_HAZARDS = frozenset({'vermin_observed', 'sewage_observed'})
F_FORCING_HAZARDS_WITH_UNSPECIFIED = F_FORCING_HAZARDS | {'pests_unspecified'}
F_FORCING_HAZARDS_WITH_FLIES = F_FORCING_HAZARDS | {'flies_or_gnats_observed'}

HAZARD_OPTIONS = ('vermin_observed', 'pests_unspecified', 'flies_or_gnats_observed',
                  'sewage_observed', 'conditions_only', 'none')

# The 27 fixed phrases _paraphrase_violation can return, in its order.
SUMMARY_PHRASES = (
    'Gaps/Openings (Risk of entry)',
    'Live roaches found',
    'Rodent activity detected',
    'Flies/Gnats present',
    'Sewage backup/issue',
    'Food not kept cold enough',
    'Food not kept hot enough',
    'Food spoiled/unsafe',
    'Dented/Damaged cans',
    'Raw food touching ready-to-eat',
    'Missing thermometers',
    'Hand sink blocked/dirty',
    'Improper hand washing',
    'Worker eating/drinking near food',
    'Hair restraints missing',
    'Long nails/jewelry on staff',
    'Chemicals unsafe/unlabeled',
    'Dirty food contact surfaces',
    'Dishwasher not sanitizing',
    'Mold/Slime in ice machine or bin',
    'No pest control records',
    'Plumbing leaks',
    'Floors/Walls dirty or damaged',
    'Dirty equipment surfaces',
    'Lights missing shields',
    'Vent hood grease buildup',
    'Permit not posted',
)
SUMMARY_NO_MATCH = 'none_of_these'

CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'jev_judgments')

REQUEST_TIMEOUT = 20.0        # seconds per request
MAX_IN_FLIGHT = 4             # concurrent requests
LINES_PER_REQUEST = 20        # cap on lines per request; shrunk from usage.input_tokens
MAX_LINES_PER_REQUEST = 40
TOKEN_BUDGET = 48000          # per request; the API caps a request at 64k tokens
MAX_RETRIES = 4               # on 429/529/5xx/timeouts: 1s, 2s, 4s, 8s (+jitter)
RETRY_BASE_SECONDS = 1.0
RETRY_CAP_SECONDS = 20.0
CHARS_PER_TOKEN = 3.2         # conservative pre-flight estimate; measured usage takes over

# A hazard answer that forces an F (or could, under another policy) is never
# taken from a batched request: it is re-asked with the line alone and the
# single-line answer is what gets cached. Measured 2026-10-10: in 14-line
# batches Jev occasionally answered the hazard question about a neighbouring
# line ("Wiping cloths dirty" came back vermin_observed at confidence 1.0;
# alone it is "none" at 0.9 every time), and a cached misfire on a common
# text would have put a false F on ~1,000 restaurants. The same re-ask
# happens for a "none" on a text that names a pest or sewage outright, so a
# real roach line cannot lose its F to the same confusion.
VERIFY_HAZARDS_ALONE = True
PEST_OR_SEWAGE_HAZARDS = frozenset({'vermin_observed', 'pests_unspecified',
                                    'flies_or_gnats_observed', 'sewage_observed'})
STRONG_HAZARD_WORD_RE = re.compile(
    r'roach|rodent|\bmice\b|\bmouse\b|\brats?\b|dropping|infest|maggot|larva|vermin|'
    r'\bflies\b|\bfly\b|gnat|\bants\b|sewage|sewer|septic', re.I)

# Chips (data.js `inf`) from code maps over category and hazard, never from
# word fragments again ("WAREWASHING" set hygiene, "prep cooler" set temp).
CHIP_ORDER = ('pests', 'temp', 'hygiene', 'equipment', 'docs')
CHIP_BY_CATEGORY = {
    'temperature_control': 'temp',
    'personal_hygiene': 'hygiene',
    'equipment_utensils': 'equipment',
    'facility_design': 'equipment',
    'water_sewage': 'equipment',
    'general_cleanliness': 'equipment',
    'maintenance': 'equipment',
    'employee_training': 'docs',
    # pest_control: the chip comes from the hazard (a door gap is not "pests")
    # cross_contamination, food_source, food_handling, waste_management,
    # unclassified: no chip, as today (today they only fire on stray words)
}
CHIP_BY_HAZARD = {
    'vermin_observed': 'pests',
    'pests_unspecified': 'pests',
    'flies_or_gnats_observed': 'pests',
    'sewage_observed': 'equipment',
    'conditions_only': 'equipment',
}
CHIP_BY_SUMMARY = {
    'Permit not posted': 'docs',
    'No pest control records': 'docs',
    'Missing thermometers': 'temp',
}

# A pest or sewage summary phrase is only true when the hazard question saw
# that hazard. When the hazard answer is confident and says otherwise, the
# phrase is dropped (today's truncation is used instead), whichever side
# proposed it, Jev's summary or the regex fallback.
HAZARD_REQUIRED_BY_PHRASE = {
    'Live roaches found': 'vermin_observed',
    'Rodent activity detected': 'vermin_observed',
    'Flies/Gnats present': 'flies_or_gnats_observed',
    'Sewage backup/issue': 'sewage_observed',
}

# Summary severity/category (the vs[].severity / vs[].category fields) from
# code maps. Precedence: an observed hazard, then the phrase, then the
# category. Mirrors summarize_violations' four levels.
ACTIVE = ('CRITICAL', 'Active Hazard')
RISK = ('WARNING', 'Risk Factor')
STRUCTURAL = ('INFO', 'Structural Risk')
MAINT = ('INFO', 'Maintenance')
DOCS = ('INFO', 'Documentation')
SUMMARY_LEVEL_BY_HAZARD = {
    'vermin_observed': ACTIVE,
    'pests_unspecified': ACTIVE,
    'flies_or_gnats_observed': ACTIVE,
    'sewage_observed': ACTIVE,
}
SUMMARY_LEVEL_BY_PHRASE = {
    'Gaps/Openings (Risk of entry)': STRUCTURAL,
    'Live roaches found': ACTIVE,
    'Rodent activity detected': ACTIVE,
    'Flies/Gnats present': ACTIVE,
    'Sewage backup/issue': ACTIVE,
    'Food spoiled/unsafe': ACTIVE,
    'Raw food touching ready-to-eat': ACTIVE,
    'Mold/Slime in ice machine or bin': ACTIVE,
    'Chemicals unsafe/unlabeled': ACTIVE,
    'Food not kept cold enough': RISK,
    'Food not kept hot enough': RISK,
    'Dented/Damaged cans': RISK,
    'Missing thermometers': RISK,
    'Hand sink blocked/dirty': RISK,
    'Improper hand washing': RISK,
    'Worker eating/drinking near food': RISK,
    'Hair restraints missing': RISK,
    'Long nails/jewelry on staff': RISK,
    'Dirty food contact surfaces': RISK,
    'Dishwasher not sanitizing': RISK,
    'No pest control records': DOCS,
    'Permit not posted': DOCS,
    'Plumbing leaks': MAINT,
    'Floors/Walls dirty or damaged': MAINT,
    'Dirty equipment surfaces': MAINT,
    'Lights missing shields': MAINT,
    'Vent hood grease buildup': MAINT,
}
SUMMARY_LEVEL_BY_CATEGORY = {
    'temperature_control': RISK,
    'cross_contamination': RISK,
    'food_source': RISK,
    'personal_hygiene': RISK,
    'food_handling': RISK,
    'equipment_utensils': RISK,
    'employee_training': DOCS,
    'pest_control': STRUCTURAL,
}

# ─── THE QUESTIONS ───────────────────────────────────────────────────────────
# Jev never sees question ids: the full meaning is in the instructions and
# descriptions, and each question names its line in backticks (`line_7`).

CATEGORY_CRITERIA = {
    'pest_control': {
        'what': 'Pests seen or found (rodents, roaches, ants, maggots, flies, gnats, droppings), '
                'and pest-conducive conditions: gaps or holes where pests could enter, missing '
                'door sweeps or screens, harborage, pest-control service or records.',
        'not_for': 'Openings or damage not tied to pests (maintenance).',
    },
    'temperature_control': {
        'what': 'Food held, cooled, reheated, thawed or stored at an unsafe temperature; a cooler '
                'or hot-holding unit not holding temperature; no or inaccurate food thermometer; '
                'time/temperature controls not followed.',
        'not_for': 'Hot water supply (water_sewage); dish machine or sanitizer temperature '
                   '(equipment_utensils).',
    },
    'cross_contamination': {
        'what': 'Raw meat, poultry, seafood or eggs over or touching ready-to-eat food; utensils '
                'or surfaces shared between raw and cooked; allergen controls; toxic chemicals '
                'stored over or beside food or in unlabeled containers.',
        'not_for': 'Surfaces merely unclean (equipment_utensils, general_cleanliness).',
    },
    'personal_hygiene': {
        'what': 'Employees not washing hands or washing wrong, bare-hand contact with ready-to-eat '
                'food, glove misuse, eating, drinking or smoking in prep areas, missing hair '
                'restraints, jewelry or long nails, ill employees, no employee health policy.',
        'not_for': 'The handwashing sink itself blocked, missing or lacking soap, towels or hot '
                   'water (facility_design).',
    },
    'food_source': {
        'what': 'Food from an unapproved source, no shellfish tags or parasite records, spoiled, '
                'moldy, expired or adulterated food, unlabeled or mislabeled packaged food, '
                'home-prepared food.',
        'not_for': 'Date marking of food made on site (food_handling); chemical labels '
                   '(cross_contamination).',
    },
    'water_sewage': {
        'what': 'Water and plumbing: no hot or cold water, low pressure, non-potable water, '
                'backflow prevention device or air gap missing or wrong, leaking or broken '
                'plumbing, drains not working, sewage or wastewater backing up or leaking, '
                'grease interceptor overflow.',
        'not_for': 'A handwashing sink blocked or missing (facility_design).',
    },
    'food_handling': {
        'what': 'Food storage and handling: date marking missing or expired on food made on site, '
                'food uncovered or unprotected, stored on the floor, scoops or utensils left in '
                'food, food past its discard date, consumer advisory missing.',
        'not_for': 'Temperature (temperature_control); raw over ready-to-eat (cross_contamination).',
    },
    'equipment_utensils': {
        'what': 'Food-contact surfaces and equipment: cutting boards, slicers, utensils, prep '
                'tables, ice machines or nozzles not clean or sanitized; dish machines and '
                'warewashing, sanitizer concentration or temperature, test strips, wiping cloths; '
                'equipment not approved or not in good repair.',
        'not_for': 'Non-food-contact surfaces, floors, walls (general_cleanliness); hand washing '
                   '(personal_hygiene).',
    },
    'employee_training': {
        'what': 'Certified food manager or food handler certificate missing or expired, person in '
                'charge lacking knowledge, no training or training records.',
        'not_for': 'Permits or licenses (unclassified); pest-control records (pest_control).',
    },
    'facility_design': {
        'what': 'Fixed facilities: handwashing sink missing, blocked or lacking soap, towels or '
                'hot water; restrooms; mop sink; three-compartment sink; ventilation or hood; '
                'grease trap; lighting too dim; no designated employee area; layout or materials '
                'not approved.',
        'not_for': 'Dirt (general_cleanliness) or disrepair (maintenance) of floors, walls and '
                   'ceilings.',
    },
    'general_cleanliness': {
        'what': 'Non-food-contact surfaces, equipment exteriors, shelving, cooler interiors or '
                'gaskets, floors, walls, ceilings, vents or storage areas dirty, soiled, greasy, '
                'dusty, cluttered or with debris.',
        'not_for': 'Food-contact surfaces (equipment_utensils); structural damage (maintenance).',
    },
    'maintenance': {
        'what': 'Facilities or equipment in disrepair: damaged or missing tiles, peeling paint, '
                'broken doors or fixtures, unsealed surfaces, missing light shields, torn gaskets, '
                'rust.',
        'not_for': 'Openings cited as pest entry or harborage (pest_control); plumbing leaks '
                   '(water_sewage).',
    },
    'waste_management': {
        'what': 'Garbage and refuse: trash overflowing or uncovered, dumpster lids open or area '
                'dirty, grease container issues, recycling, litter.',
        'not_for': 'Sewage or wastewater (water_sewage); food to be discarded (food_handling).',
    },
    'unclassified': {
        'what': 'No category above fits: permit or license postings, consumer advisory signs, '
                'administrative notes, or a line too short or vague to tell.',
    },
}

HAZARD_CRITERIA = {
    'vermin_observed': (
        'On this visit the inspector saw or found a named vermin: rodents (rats, mice, rodent '
        'droppings, gnaw or rub marks, burrows, nesting), cockroaches live or dead, ants, maggots '
        'or larvae, weevils or another crawling insect named by kind, or an infestation of one of '
        'these. Evidence found counts (droppings, dead roaches). Not: flies alone '
        '(flies_or_gnats_observed); "insects" or "pests" with no kind named (pests_unspecified); '
        'pesticide cans, traps, bait or glue boards by themselves (conditions_only); dogs, cats, '
        'birds or other animals (none).'),
    'pests_unspecified': (
        'On this visit pests were reported present, found, observed or infesting without saying '
        'which kind: "insects", "pests", "bugs", "insect carcasses", "rodents or insects", '
        '"Insects, rodents present", "pest infestation", "infestation/activity of '
        'cockroaches/rodents/flies/vermin". The line asserts pests are or were there but does not '
        'name a vermin.'),
    'flies_or_gnats_observed': (
        'On this visit the inspector saw flies, fruit flies, drain flies, gnats or other small '
        'flying insects (dead or alive), and no rodents, roaches, ants, maggots or other named '
        'vermin.'),
    'sewage_observed': (
        'Sewage actually present on this visit: a sewer line, floor drain or toilet backing up or '
        'overflowing, sewage or raw waste on a floor or the ground, an open or broken sewer line, '
        'a failing septic system with sewage present, or a closure for a sewage backup. Not: a '
        'sink, dish machine, ice machine or prep-sink drain line leaking or dripping, even onto '
        'the floor (conditions_only); a grey-water tank or exhaust hood overflowing '
        '(conditions_only); rule text about how sewage shall be conveyed or removed (conditions_only).'),
    'conditions_only': (
        'No pest and no sewage was observed on this visit. The line is a condition, a '
        'requirement, a record, a corrective instruction, or the code\'s own title or definition: '
        'gaps, holes or openings, missing door sweeps or screens, harborage, pest-control service '
        'or records, pest-control products or devices present with no pest reported (Raid or other '
        'pesticide cans, bait, poison, glue boards, traps, fly strips), backflow prevention device '
        'or air gap requirements, a slow or clogged drain with no sewage, a leaking or dripping '
        'drain pipe, sink drain or faucet, wastewater stored in containers, condensation, '
        'instructions such as "remove dead pests", titles such as "Removing Dead or Trapped '
        'Birds, Insects, Rodents and other Pests", and rule text saying what "shall" be done or '
        'when pests "are seen" or "are visible" (for example "pest control shall be required '
        'when live insects are seen").'),
    'none': (
        'The line is about something else: temperature, hygiene, cleaning, equipment, labeling, '
        'training, permits, or live animals such as dogs, cats or birds, which are not pests.'),
}

SUMMARY_CRITERIA = {
    'Gaps/Openings (Risk of entry)': 'Gaps, holes or openings, or missing door sweeps, screens or '
                                     'weather stripping, that could let pests in.',
    'Live roaches found': 'Cockroaches were observed, live or dead.',
    'Rodent activity detected': 'Rodents, rats, mice, droppings, gnaw or rub marks or nesting were '
                                'observed.',
    'Flies/Gnats present': 'Flies, fruit flies, drain flies or gnats were observed.',
    'Sewage backup/issue': 'Sewage was observed backing up, overflowing, pooling or exposed on this '
                           'visit. Not a backflow device, air gap or drainage requirement, and not '
                           'rule text about sewage disposal.',
    'Food not kept cold enough': 'Cold food held above the cold-holding limit, a cooler too warm, or '
                                 'food not cooled fast enough.',
    'Food not kept hot enough': 'Hot food held below the hot-holding limit or reheated insufficiently.',
    'Food spoiled/unsafe': 'Spoiled, moldy, expired, adulterated or contaminated food, or food from '
                           'an unapproved source.',
    'Dented/Damaged cans': 'Dented, swollen or damaged cans or packaging.',
    'Raw food touching ready-to-eat': 'Raw meat, poultry, seafood or eggs stored over, touching or '
                                      'cross-contaminating ready-to-eat food.',
    'Missing thermometers': 'No thermometer in a cooler or hot unit, no probe thermometer, or an '
                            'inaccurate thermometer.',
    'Hand sink blocked/dirty': 'A handwashing sink blocked, inaccessible, used for other purposes, '
                               'dirty, or lacking soap, towels or hot water.',
    'Improper hand washing': 'Employees not washing hands when required, or washing wrong.',
    'Worker eating/drinking near food': 'Employee eating, drinking from an open cup, or using tobacco '
                                        'in a prep or storage area.',
    'Hair restraints missing': 'No hair restraints or beard guards.',
    'Long nails/jewelry on staff': 'Long or artificial nails, or jewelry, while handling food.',
    'Chemicals unsafe/unlabeled': 'Toxic chemicals stored over or beside food, unlabeled chemical '
                                  'containers, or sanitizer far too strong.',
    'Dirty food contact surfaces': 'Food-contact surfaces such as cutting boards, slicers, prep '
                                   'tables, utensils or the ice machine interior not clean or not '
                                   'sanitized.',
    'Dishwasher not sanitizing': 'A dish machine or three-compartment sink with sanitizer '
                                 'concentration or temperature wrong, or no test strips.',
    'Mold/Slime in ice machine or bin': 'Mold, pink slime or black buildup inside an ice machine, '
                                        'ice bin or soda nozzle.',
    'No pest control records': 'Pest-control service or records missing or unavailable.',
    'Plumbing leaks': 'Leaking pipes, faucets or drains; clean-water leaks, not sewage.',
    'Floors/Walls dirty or damaged': 'Floors, walls or ceilings dirty or damaged, missing tiles, or '
                                     'holes not cited as pest entry.',
    'Dirty equipment surfaces': 'Non-food-contact surfaces such as shelves, cooler exteriors, '
                                'gaskets or hood filters dirty or greasy.',
    'Lights missing shields': 'Light shields or end caps missing, unshielded bulbs, or dim lighting.',
    'Vent hood grease buildup': 'Grease on the hood or vent, or dirty hood filters.',
    'Permit not posted': 'Permit, license or inspection report not posted or expired.',
    SUMMARY_NO_MATCH: 'No phrase above is true of what the inspector observed.',
}

CATEGORY_INSTRUCTIONS = {
    'question': 'Which food-safety category best fits what the inspector observed or required in `{line}`?',
    'how_to_read': 'Many lines are shaped "CODE TITLE: observation". Classify the observation '
                   '(the part after the colon), and use the title only to break a tie. When '
                   'the line is a bare code title with no observation, classify the title.',
}
HAZARD_INSTRUCTIONS = {
    'question': 'What does `{line}` report about pests and sewage?',
    'rules': 'Judge only what the inspector observed or found on this visit. An observation says '
             'something was observed, found, noted, seen or present. A code title, a rule, a '
             'corrective instruction, or the code\'s definition of a violation ("premises not '
             'free of insects", "area contaminated by sewage or liquid waste", "dead pests shall '
             'be removed") is not an observation, even when it names insects, rodents or sewage. '
             'If more than one applies, pick the first that applies in this order: '
             'vermin_observed, sewage_observed, pests_unspecified, flies_or_gnats_observed, '
             'conditions_only.',
}
SUMMARY_INSTRUCTIONS = {
    'question': 'Which public summary phrase is true of what the inspector observed in `{line}`?',
    'rules': 'Pick a phrase only if the observation itself supports it. A code title, a rule, a '
             'corrective instruction or the code\'s definition of a violation is not an '
             'observation of it, even when it names pests or sewage. If no phrase is true of '
             'the observation, pick none_of_these.',
}


def category_options():
    """The 13 VIOLATION_CATEGORIES (pipeline order) plus unclassified."""
    return list(_pipeline().VIOLATION_CATEGORIES) + ['unclassified']


def build_questions(indices):
    """The three questions for every `line_<i>` in indices. Pure."""
    cats = category_options()
    missing = [c for c in cats if c not in CATEGORY_CRITERIA]
    if missing:
        raise RuntimeError(f'CATEGORY_CRITERIA lacks {missing}')
    questions = {}
    for i in indices:
        line = f'line_{i}'
        questions[f'category_{i}'] = {
            'type': 'choice',
            'instructions': {k: v.format(line=line) for k, v in CATEGORY_INSTRUCTIONS.items()},
            'criteria': {c: CATEGORY_CRITERIA[c] for c in cats},
        }
        questions[f'hazard_{i}'] = {
            'type': 'choice',
            'instructions': {k: v.format(line=line) for k, v in HAZARD_INSTRUCTIONS.items()},
            'criteria': {h: HAZARD_CRITERIA[h] for h in HAZARD_OPTIONS},
        }
        questions[f'summary_{i}'] = {
            'type': 'choice',
            'instructions': {k: v.format(line=line) for k, v in SUMMARY_INSTRUCTIONS.items()},
            'criteria': {p: SUMMARY_CRITERIA[p] for p in SUMMARY_PHRASES + (SUMMARY_NO_MATCH,)},
        }
    return questions


_Q_CHARS = None


def _question_chars_per_line():
    """JSON size of the three questions for one line (cached)."""
    global _Q_CHARS
    if _Q_CHARS is None:
        _Q_CHARS = len(json.dumps(build_questions([0]), separators=(',', ':')))
    return _Q_CHARS


def question_set_fingerprint():
    """sha256 of the question set text: a guard that QUESTION_SET_VERSION was
    bumped when the wording changed (the tests pin this)."""
    blob = json.dumps(build_questions([7]), sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(blob.encode('utf-8')).hexdigest()


# ─── MODE ────────────────────────────────────────────────────────────────────

MODES = ('off', 'shadow', 'decide')


def jev_mode(env=None):
    env = os.environ if env is None else env
    mode = (env.get('JEV_MODE') or 'off').strip().lower()
    if mode not in MODES:
        log.warning(f"JEV_MODE={mode!r} is not one of {MODES}; using 'off'")
        return 'off'
    return mode


def decide_questions(env=None):
    """JEV_DECIDE=hazard,summary,category: the questions Jev decides in
    decide mode. Anything not listed stays shadow. Default: all three."""
    env = os.environ if env is None else env
    raw = (env.get('JEV_DECIDE') or '').strip()
    if not raw:
        return set(QUESTIONS)
    chosen = {q.strip().lower() for q in raw.split(',') if q.strip()}
    unknown = chosen - set(QUESTIONS)
    if unknown:
        log.warning(f'JEV_DECIDE names unknown questions {sorted(unknown)}; ignoring them')
    return chosen & set(QUESTIONS)


# ─── NORMALIZATION, KEYS, TIER MARKERS ───────────────────────────────────────

_WS_RE = re.compile(r'\s+')


def normalize_text(text):
    """Whitespace-collapsed, stripped. Case is kept (it carries meaning)."""
    return _WS_RE.sub(' ', str(text or '')).strip()


def judgment_key(text, question_set=QUESTION_SET_VERSION, model=JEV_MODEL):
    """sha256 over (model id, question-set version, normalized text)."""
    h = hashlib.sha256()
    h.update(model.encode('utf-8'))
    h.update(b'\x1f')
    h.update(question_set.encode('utf-8'))
    h.update(b'\x1f')
    h.update(normalize_text(text).encode('utf-8'))
    return h.hexdigest()


# Explicit tier markers that some sources embed in the text. A tier the
# source states wins over SEVERITY_MAP. Traps that are NOT markers:
# Cincinnati's Ohio-code paragraph letters "3717-1-04.8(P) -" / "4(P) -"
# (a digit right before the parenthesis) and Boston's paragraph ranges
# "(A)-(P)" (a hyphen right before it). Neither is followed by the colon
# that the real "(P):" marker carries, and the lookbehind rejects them even
# when one is.
TIER_MARKER_RE = re.compile(
    r'(?<![\w)\]\-])\((P|Pf|C)\)\s*:'                 # "(P):", "(Pf):", "(C):"
    r'|\*\*\s*(P|PF|Pf|C)\s*\*\*'                      # "**P**"
    r'|;\s*(Priority Foundation|Priority|Core)\s*;',  # "; Priority;"
)
_MARKER_TIER = {
    'p': 'priority', 'pf': 'priority_foundation', 'c': 'core',
    'priority': 'priority', 'priority foundation': 'priority_foundation', 'core': 'core',
}


def parse_tier_marker(text):
    """'priority' | 'priority_foundation' | 'core' | None from an explicit
    marker in the text. Pure."""
    m = TIER_MARKER_RE.search(text or '')
    if not m:
        return None
    token = next(g for g in m.groups() if g)
    return _MARKER_TIER.get(token.strip().lower())


# ─── PIPELINE ACCESS (lazy: the pipeline may import this module later) ───────

_PIPELINE = None


def _pipeline():
    global _PIPELINE
    if _PIPELINE is None:
        import dinescores_pipeline
        _PIPELINE = dinescores_pipeline
    return _PIPELINE


# ─── ERRORS ──────────────────────────────────────────────────────────────────

class JevError(Exception):
    """Base: anything that means 'fall back to the regex for these lines'."""


class JevAuthError(JevError):
    """401: the key is missing or invalid. Not retried; disables Jev for the run."""


class JevRequestError(JevError):
    """422: our request failed validation. Not retried as-is."""


class JevUnavailable(JevError):
    """429/529/5xx after retries, timeouts, connection errors."""


class JevMalformed(JevError):
    """The response was not the documented shape."""


# ─── CLIENT ──────────────────────────────────────────────────────────────────

class JevClient:
    """Thin urllib client for POST /v1/systemone with bounded retries.

    Test seams: open_fn(request, timeout) replaces urllib.request.urlopen;
    sleep_fn and rng replace time.sleep and the jitter source.
    """

    def __init__(self, api_key=None, model=JEV_MODEL, timeout=REQUEST_TIMEOUT,
                 max_retries=MAX_RETRIES, open_fn=None, sleep_fn=time.sleep,
                 rng=None, deadline=None, time_fn=time.time):
        key = os.environ.get('TYPESAFE_API_KEY', '') if api_key is None else api_key
        self._key = (key or '').strip()
        self.model = model
        self.timeout = timeout
        self.max_retries = max_retries
        self._open = open_fn or (lambda req, timeout: urllib.request.urlopen(req, timeout=timeout))
        self._sleep = sleep_fn
        self._rng = rng or random.Random()
        self.deadline = deadline          # epoch seconds; retries never sleep past it
        self._time = time_fn

    def __repr__(self):
        return f'JevClient(model={self.model!r}, configured={self.configured})'

    @property
    def configured(self):
        return bool(self._key)

    def _scrub(self, s):
        """Remove the key from anything that could be logged or raised."""
        s = str(s)
        if self._key and self._key in s:
            s = s.replace(self._key, '[REDACTED]')
        return s

    def _headers(self):
        return {'Authorization': f'Bearer {self._key}', 'Content-Type': 'application/json',
                'Accept': 'application/json', 'User-Agent': 'dinescores-jev-reader/1'}

    def _do(self, request):
        """One HTTP exchange. Returns (status, body_text, headers). Raises
        JevUnavailable on network-level failure. Never logs the body."""
        try:
            resp = self._open(request, self.timeout)
            status = getattr(resp, 'status', None) or resp.getcode()
            body = resp.read()
            headers = getattr(resp, 'headers', None) or {}
            return status, body.decode('utf-8', 'replace') if isinstance(body, bytes) else str(body), headers
        except urllib.error.HTTPError as e:
            try:
                body = e.read()
            except Exception:
                body = b''
            return e.code, body.decode('utf-8', 'replace') if isinstance(body, bytes) else str(body), e.headers or {}
        except (urllib.error.URLError, socket.timeout, TimeoutError, ConnectionError,
                http.client.HTTPException, OSError) as e:
            raise JevUnavailable(f'network error: {type(e).__name__}: {self._scrub(e)[:200]}')

    def list_models(self):
        """GET /v1/models (spends no tokens). Returns the parsed JSON."""
        if not self.configured:
            raise JevAuthError('TYPESAFE_API_KEY is not set')
        req = urllib.request.Request(MODELS_URL, headers=self._headers(), method='GET')
        status, text, _ = self._do(req)
        if status == 401:
            raise JevAuthError('401 unauthorized from /v1/models (check TYPESAFE_API_KEY)')
        if status != 200:
            raise JevUnavailable(f'HTTP {status} from /v1/models')
        try:
            return json.loads(text)
        except ValueError:
            raise JevMalformed('/v1/models did not return JSON')

    def system_one(self, state, questions):
        """POST /v1/systemone. Returns the parsed response dict or raises a
        JevError subclass. 429/529/5xx and network errors are retried with
        exponential backoff and jitter, honoring Retry-After, never past the
        deadline."""
        if not self.configured:
            raise JevAuthError('TYPESAFE_API_KEY is not set')
        body = json.dumps({'model': self.model, 'state': state, 'questions': questions},
                          ensure_ascii=False, separators=(',', ':')).encode('utf-8')
        attempt = 0
        while True:
            req = urllib.request.Request(API_URL, data=body, headers=self._headers(), method='POST')
            try:
                status, text, headers = self._do(req)
                err = None
            except JevUnavailable as e:
                status, text, headers, err = None, '', {}, e
            if err is None:
                if status == 200:
                    return self._parse(text)
                if status == 401:
                    raise JevAuthError('401 unauthorized (check TYPESAFE_API_KEY)')
                if status == 422:
                    raise JevRequestError(f'422 unprocessable: {self._scrub(text)[:300]}')
                if status in (429, 529) or (status is not None and 500 <= status < 600):
                    err = JevUnavailable(f'HTTP {status}')
                else:
                    raise JevError(f'HTTP {status}: {self._scrub(text)[:200]}')
            # retryable
            if attempt >= self.max_retries:
                raise JevUnavailable(f'{err} after {attempt} retries')
            delay = min(RETRY_CAP_SECONDS, RETRY_BASE_SECONDS * (2 ** attempt))
            delay *= 0.5 + self._rng.random()          # jitter: 0.5x .. 1.5x
            retry_after = _retry_after_seconds(headers)
            if retry_after is not None:
                delay = max(delay, min(retry_after, RETRY_CAP_SECONDS * 3))
            if self.deadline is not None and self._time() + delay > self.deadline:
                raise JevUnavailable(f'{err}; deadline reached before retry')
            log.info(f'Jev: {err}; retry {attempt + 1}/{self.max_retries} in {delay:.1f}s')
            self._sleep(delay)
            attempt += 1

    def _parse(self, text):
        try:
            data = json.loads(text)
        except ValueError:
            raise JevMalformed('response is not JSON')
        if not isinstance(data, dict) or not isinstance(data.get('answers'), dict):
            raise JevMalformed('response lacks an answers map')
        return data


def _retry_after_seconds(headers):
    try:
        v = headers.get('Retry-After') if hasattr(headers, 'get') else None
    except Exception:
        v = None
    if not v:
        return None
    try:
        return float(v)
    except ValueError:
        return None


# ─── CACHE ───────────────────────────────────────────────────────────────────

class JudgmentCache:
    """Append-only: load every data/jev_judgments/*.jsonl.gz at start; each
    run writes ONE new file holding only the answers it had to ask for.

    Record: {"k": key, "m": model, "q": question-set version, "t": normalized
    text, "a": {question: {"c": choice, "p": confidence, "pr": {option:
    probability >= 0.005}}}, "ts": utc iso}.
    """

    def __init__(self, directory=CACHE_DIR):
        self.directory = directory
        self._mem = {}
        self._new = {}
        self._lock = threading.Lock()
        self.loaded_files = 0

    def load(self):
        if not os.path.isdir(self.directory):
            return self
        for name in sorted(os.listdir(self.directory)):
            if not name.endswith('.jsonl.gz'):
                continue
            path = os.path.join(self.directory, name)
            try:
                with gzip.open(path, 'rt', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            rec = json.loads(line)
                        except ValueError:
                            continue
                        k = rec.get('k')
                        if k and isinstance(rec.get('a'), dict):
                            self._mem[k] = rec
                self.loaded_files += 1
            except (OSError, EOFError) as e:
                log.warning(f'Jev cache: could not read {name}: {e}')
        return self

    def __len__(self):
        return len(self._mem)

    def get(self, key):
        return self._mem.get(key)

    def put(self, key, rec):
        with self._lock:
            self._mem[key] = rec
            self._new[key] = rec

    @property
    def pending(self):
        return len(self._new)

    def flush(self, label='run'):
        """Write this run's new answers to one new gzipped JSONL file.
        Returns the path, or None when nothing new was asked."""
        with self._lock:
            if not self._new:
                return None
            recs = list(self._new.values())
            self._new = {}
        os.makedirs(self.directory, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        safe = re.sub(r'[^A-Za-z0-9_-]+', '-', label)[:40] or 'run'
        path = os.path.join(self.directory, f'jev_{stamp}_{safe}.jsonl.gz')
        n = 0
        while os.path.exists(path):
            n += 1
            path = os.path.join(self.directory, f'jev_{stamp}_{safe}-{n}.jsonl.gz')
        with gzip.open(path, 'wt', encoding='utf-8') as f:
            for rec in recs:
                f.write(json.dumps(rec, ensure_ascii=False, separators=(',', ':')) + '\n')
        log.info(f'Jev cache: wrote {len(recs)} new judgments to {os.path.basename(path)}')
        return path


def judgment_from_record(rec):
    """Cache record -> judgment dict {question: {'choice','confidence','probabilities'}}."""
    out = {}
    for q, a in (rec.get('a') or {}).items():
        if isinstance(a, dict) and 'c' in a:
            out[q] = {'choice': a['c'], 'confidence': float(a.get('p') or 0.0),
                      'probabilities': dict(a.get('pr') or {})}
    return out


# ─── READER (batching, concurrency, fail-open orchestration) ─────────────────

class JevReader:
    """Asks Jev about violation lines, through the cache, in batches, with
    bounded concurrency. judge_texts() never raises: anything that goes wrong
    leaves those lines unjudged (None), and the pure decide() functions then
    use today's regex for them."""

    def __init__(self, mode='off', client=None, cache=None, floors=None,
                 lines_per_request=LINES_PER_REQUEST, max_in_flight=MAX_IN_FLIGHT,
                 token_budget=TOKEN_BUDGET, deadline=None, decide=None, time_fn=time.time):
        self.mode = mode if mode in MODES else 'off'
        self.client = client or JevClient(deadline=deadline)
        self.cache = cache if cache is not None else JudgmentCache()
        self._cache_loaded = cache is not None and len(cache) > 0
        self.floors = dict(CONFIDENCE_FLOORS)
        if floors:
            self.floors.update(floors)
        self.lines_per_request = max(1, min(MAX_LINES_PER_REQUEST, int(lines_per_request)))
        self.max_in_flight = max(1, int(max_in_flight))
        self.token_budget = token_budget
        self.deadline = deadline
        self.decide_set = set(QUESTIONS) if decide is None else set(decide)
        self._time = time_fn
        self._disabled = None     # reason string once Jev is switched off for the run
        self._lock = threading.Lock()
        self._tokens_per_line = None
        self.stats = {
            'mode': self.mode, 'requests': 0, 'request_failures': 0, 'lines_asked': 0,
            'lines_judged': 0, 'lines_cached': 0, 'lines_fallback': 0,
            'fallback_reasons': {}, 'input_tokens': 0, 'output_tokens': 0,
            'latency_total': 0.0, 'latency_max': 0.0, 'cache_files_written': [],
            'hazard_verified_alone': 0, 'hazard_changed_on_verify': 0,
        }

    @classmethod
    def from_env(cls, env=None, deadline=None, cache_dir=None):
        env = os.environ if env is None else env
        mode = jev_mode(env)
        reader = cls(mode=mode, client=JevClient(api_key=env.get('TYPESAFE_API_KEY'), deadline=deadline),
                     cache=JudgmentCache(cache_dir or CACHE_DIR), deadline=deadline,
                     decide=decide_questions(env) if mode == 'decide' else set())
        if mode != 'off' and not reader.client.configured:
            log.warning(f'JEV_MODE={mode} but TYPESAFE_API_KEY is not set: Jev is off for this run')
            reader._disabled = 'no key'
        return reader

    # -- public ---------------------------------------------------------------

    @property
    def active(self):
        return self.mode != 'off' and self._disabled is None and self.client.configured

    def judge_texts(self, texts, label='run'):
        """Judge every distinct line in `texts`. Returns {normalized text:
        judgment or None}. Never raises. In mode 'off' makes no requests and
        returns an empty map."""
        if self.mode == 'off':
            return {}
        if not self._cache_loaded:
            self.cache.load()
            self._cache_loaded = True
            log.info(f'Jev cache: {len(self.cache)} judgments from {self.cache.loaded_files} files')
        wanted = {}
        for t in texts:
            nt = normalize_text(t)
            if nt and nt not in wanted:
                wanted[nt] = judgment_key(nt, QUESTION_SET_VERSION, self.client.model)
        results = {}
        misses = []
        for nt, key in wanted.items():
            rec = self.cache.get(key)
            if rec is not None:
                results[nt] = judgment_from_record(rec)
                self.stats['lines_cached'] += 1
            else:
                results[nt] = None
                misses.append(nt)
        if misses and self.active:
            self._ask_all(misses, results)
        elif misses:
            self._count_fallback(len(misses), self._disabled or 'jev inactive')
        return results

    def flush_cache(self, label='run'):
        path = self.cache.flush(label)
        if path:
            self.stats['cache_files_written'].append(path)
        return path

    def summary_lines(self):
        """Human-readable coverage for the run summary."""
        s = self.stats
        lines = [f"Jev mode={s['mode']} model={self.client.model} qs={QUESTION_SET_VERSION}",
                 f"  lines: {s['lines_cached']} cached, {s['lines_judged']} judged this run, "
                 f"{s['lines_fallback']} fell back",
                 f"  requests: {s['requests']} ({s['request_failures']} failed), "
                 f"{s['input_tokens']} input tokens, {s['output_tokens']} output tokens"]
        if s['requests']:
            lines.append(f"  latency: mean {s['latency_total'] / s['requests']:.2f}s, max {s['latency_max']:.2f}s")
        if s['hazard_verified_alone']:
            lines.append(f"  hazards re-asked alone: {s['hazard_verified_alone']}, "
                         f"answer changed on {s['hazard_changed_on_verify']}")
        for reason, n in sorted(s['fallback_reasons'].items(), key=lambda kv: -kv[1]):
            lines.append(f'  fallback: {n} lines: {reason}')
        if self._disabled:
            lines.append(f'  Jev disabled for the run: {self._disabled}')
        return lines

    # -- internals ------------------------------------------------------------

    def _count_fallback(self, n, reason):
        with self._lock:
            self.stats['lines_fallback'] += n
            self.stats['fallback_reasons'][reason] = self.stats['fallback_reasons'].get(reason, 0) + n

    def _past_deadline(self):
        return self.deadline is not None and self._time() > self.deadline

    def _ask_all(self, misses, results):
        """Batch the misses and run up to max_in_flight requests at once.
        Batches are formed up front at the current size; the size adapts
        from usage.input_tokens for later batches."""
        pending = list(misses)
        pos = 0
        with ThreadPoolExecutor(max_workers=self.max_in_flight) as pool:
            futures = {}
            while pos < len(pending) or futures:
                while pos < len(pending) and len(futures) < self.max_in_flight and self.active \
                        and not self._past_deadline():
                    n = self._batch_size(pending[pos:pos + MAX_LINES_PER_REQUEST])
                    batch = pending[pos:pos + n]
                    pos += n
                    futures[pool.submit(self._ask_batch, batch)] = batch
                if not futures:
                    break
                done = next(as_completed(list(futures)))
                batch = futures.pop(done)
                try:
                    judged = done.result()
                except Exception as e:          # belt and braces: never raise out
                    log.error(f'Jev: unexpected {type(e).__name__} judging a batch; falling back')
                    judged = {}
                for nt in batch:
                    if nt in judged:
                        results[nt] = judged[nt]
            if pos < len(pending):
                reason = self._disabled or ('deadline reached' if self._past_deadline() else 'stopped')
                self._count_fallback(len(pending) - pos, reason)

    def _batch_size(self, upcoming):
        """Lines for the next request: from measured usage.input_tokens once
        we have a response, else from the real JSON size of the questions
        (CHARS_PER_TOKEN is deliberately conservative)."""
        n = self.lines_per_request                      # configured cap
        if self._tokens_per_line:
            n = min(n, int(self.token_budget / self._tokens_per_line))
        n = max(1, min(MAX_LINES_PER_REQUEST, n, len(upcoming)))
        per_line_q = _question_chars_per_line() / CHARS_PER_TOKEN
        while n > 1 and sum(per_line_q + (len(t) + 20) / CHARS_PER_TOKEN
                            for t in upcoming[:n]) > self.token_budget:
            n -= 1
        return n

    def _ask_batch(self, batch, depth=0):
        """One request for `batch` (normalized texts). Returns {text:
        judgment} for the lines that came back well-formed. Fallback lines
        are counted, never raised. A 422 on a multi-line batch splits it."""
        if not batch:
            return {}
        state = {f'line_{i}': t for i, t in enumerate(batch)}
        questions = build_questions(range(len(batch)))
        t0 = self._time()
        try:
            data = self.client.system_one(state, questions)
        except JevAuthError as e:
            self._disable(f'auth error: {e}')
            self._count_fallback(len(batch), 'auth error (401)')
            return {}
        except JevRequestError as e:
            if len(batch) > 1 and depth < 6:
                log.warning(f'Jev: 422 on a {len(batch)}-line batch; splitting')
                half = len(batch) // 2
                out = self._ask_batch(batch[:half], depth + 1)
                out.update(self._ask_batch(batch[half:], depth + 1))
                return out
            log.warning(f'Jev: {e}')
            self._count_fallback(len(batch), 'request rejected (422)')
            with self._lock:
                self.stats['request_failures'] += 1
            return {}
        except JevError as e:
            log.warning(f'Jev: {type(e).__name__}: {e}')
            self._count_fallback(len(batch), f'{type(e).__name__}')
            with self._lock:
                self.stats['request_failures'] += 1
            return {}
        latency = self._time() - t0
        usage = data.get('usage') or {}
        in_tok = int(usage.get('input_tokens') or 0)
        out_tok = int(usage.get('output_tokens') or 0)
        with self._lock:
            self.stats['requests'] += 1
            self.stats['input_tokens'] += in_tok
            self.stats['output_tokens'] += out_tok
            self.stats['latency_total'] += latency
            self.stats['latency_max'] = max(self.stats['latency_max'], latency)
            self.stats['lines_asked'] += len(batch)
            if in_tok and len(batch):
                per_line = in_tok / len(batch)
                self._tokens_per_line = per_line if self._tokens_per_line is None \
                    else 0.7 * self._tokens_per_line + 0.3 * per_line
        model = data.get('model') or self.client.model
        if model != self.client.model:
            log.warning(f'Jev answered with model {model!r}, not the pinned {self.client.model!r}')
        answers = data['answers']
        out = {}
        for i, nt in enumerate(batch):
            judgment, rec_answers, bad = self._parse_line(answers, i)
            if bad:
                # A malformed answer is a fallback for that question only; the
                # record is cached without it, so the line is not re-asked
                # forever for one bad field. (Cache never holds a bad answer.)
                self._count_fallback(len(bad), 'malformed answer')
            if not judgment:
                self._count_fallback(1, 'no usable answers')
                continue
            if VERIFY_HAZARDS_ALONE and len(batch) > 1 and self._needs_verification(nt, judgment):
                verified = self._verify_alone(nt)
                if verified is None:
                    # the single-line request failed: keep nothing for this
                    # line (not cached), today's regex answers it this run
                    self._count_fallback(1, 'hazard verification failed')
                    continue
                if verified[0].get('hazard', {}).get('choice') != judgment.get('hazard', {}).get('choice'):
                    with self._lock:
                        self.stats['hazard_changed_on_verify'] += 1
                judgment, rec_answers = verified
            out[nt] = judgment
            self._store(nt, rec_answers)
        return out

    def _parse_line(self, answers, i):
        judgment, rec_answers, bad = {}, {}, []
        for q in QUESTIONS:
            parsed = _parse_choice(answers.get(f'{q}_{i}'), _options_for(q))
            if parsed is None:
                bad.append(q)
                continue
            judgment[q] = parsed
            rec_answers[q] = {'c': parsed['choice'], 'p': round(parsed['confidence'], 4),
                              'pr': {k: round(v, 3) for k, v in parsed['probabilities'].items()
                                     if v >= 0.005}}
        return judgment, rec_answers, bad

    def _store(self, nt, rec_answers):
        key = judgment_key(nt, QUESTION_SET_VERSION, self.client.model)
        self.cache.put(key, {'k': key, 'm': self.client.model, 'q': QUESTION_SET_VERSION, 't': nt,
                             'a': rec_answers,
                             'ts': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')})
        with self._lock:
            self.stats['lines_judged'] += 1

    @staticmethod
    def _needs_verification(nt, judgment):
        hz = judgment.get('hazard', {}).get('choice')
        if hz in PEST_OR_SEWAGE_HAZARDS:
            return True
        return hz == 'none' and bool(STRONG_HAZARD_WORD_RE.search(nt))

    def _verify_alone(self, nt):
        """Ask about one line by itself. Returns (judgment, rec_answers) or
        None when the request failed or the answers were unusable."""
        with self._lock:
            self.stats['hazard_verified_alone'] += 1
        t0 = self._time()
        try:
            data = self.client.system_one({'line_0': nt}, build_questions([0]))
        except JevAuthError as e:
            self._disable(f'auth error: {e}')
            return None
        except JevError as e:
            log.warning(f'Jev: verification request failed: {type(e).__name__}: {e}')
            with self._lock:
                self.stats['request_failures'] += 1
            return None
        usage = data.get('usage') or {}
        with self._lock:
            self.stats['requests'] += 1
            self.stats['input_tokens'] += int(usage.get('input_tokens') or 0)
            self.stats['output_tokens'] += int(usage.get('output_tokens') or 0)
            latency = self._time() - t0
            self.stats['latency_total'] += latency
            self.stats['latency_max'] = max(self.stats['latency_max'], latency)
        judgment, rec_answers, bad = self._parse_line(data.get('answers') or {}, 0)
        if bad:
            self._count_fallback(len(bad), 'malformed answer')
        return (judgment, rec_answers) if judgment else None

    def _disable(self, reason):
        with self._lock:
            if self._disabled is None:
                self._disabled = reason
                log.error(f'Jev disabled for the rest of this run: {reason}')


def _options_for(question):
    if question == 'category':
        return set(category_options())
    if question == 'hazard':
        return set(HAZARD_OPTIONS)
    return set(SUMMARY_PHRASES) | {SUMMARY_NO_MATCH}


def _parse_choice(answer, options):
    """Validate one choice answer against its option set. None if malformed."""
    if not isinstance(answer, dict) or answer.get('type') != 'choice':
        return None
    choice = answer.get('choice')
    if choice not in options:
        return None
    try:
        conf = float(answer.get('confidence'))
    except (TypeError, ValueError):
        return None
    if not (0.0 <= conf <= 1.0) or conf != conf:
        return None
    probs = answer.get('probabilities')
    if not isinstance(probs, dict):
        return None
    clean = {}
    for k, v in probs.items():
        try:
            clean[str(k)] = float(v)
        except (TypeError, ValueError):
            return None
    return {'choice': choice, 'confidence': conf, 'probabilities': clean}


# ─── PURE DECISIONS (no network, ever) ───────────────────────────────────────

def answer(judgment, question):
    """(choice, confidence) from a judgment, or (None, 0.0)."""
    if not judgment:
        return None, 0.0
    a = judgment.get(question)
    if not a:
        return None, 0.0
    return a.get('choice'), float(a.get('confidence') or 0.0)


def confident_answer(judgment, question, floors=None):
    """The choice when its confidence clears the floor, else None."""
    floors = CONFIDENCE_FLOORS if floors is None else floors
    choice, conf = answer(judgment, question)
    if choice is None or conf < floors.get(question, CONFIDENCE_FLOORS[question]):
        return None
    return choice


def _truncate(text):
    """Today's summary fallback: truncate if long."""
    return text[:45] + '...' if len(text) > 50 else text


def summary_level(hazard, phrase, category):
    """(severity, category) for a summary item from the code maps."""
    if hazard in SUMMARY_LEVEL_BY_HAZARD:
        return SUMMARY_LEVEL_BY_HAZARD[hazard]
    if phrase in SUMMARY_LEVEL_BY_PHRASE:
        return SUMMARY_LEVEL_BY_PHRASE[phrase]
    return SUMMARY_LEVEL_BY_CATEGORY.get(category, MAINT)


def chips_for(category, hazard, phrase=None):
    chips = set()
    if category in CHIP_BY_CATEGORY:
        chips.add(CHIP_BY_CATEGORY[category])
    if hazard in CHIP_BY_HAZARD:
        chips.add(CHIP_BY_HAZARD[hazard])
    if phrase in CHIP_BY_SUMMARY:
        chips.add(CHIP_BY_SUMMARY[phrase])
    return [c for c in CHIP_ORDER if c in chips]


def regex_reading(text):
    """Today's answers for one line, exactly as the pipeline computes them."""
    dp = _pipeline()
    cat, sev, _ = dp.classify_violation(text)
    items = dp.summarize_violations(text)
    item = items[0] if items else None
    return {
        'category': cat,
        'severity': sev,
        'forces_f': dp.has_active_pest_or_sewage(text),
        'summary': dp._paraphrase_violation(text),
        'summary_severity': item['severity'] if item else 'INFO',
        'summary_category': item['category'] if item else 'Maintenance',
        'chips': dp.detect_infractions(text),
    }


def decide(text, judgment=None, source_severity=None, floors=None, use=None, forcing=None):
    """Read one violation line. Pure: never makes a request.

    text:            the exact slice the pipeline classifies
    judgment:        Jev's answers for this text (from JevReader.judge_texts), or None
    source_severity: a tier the source states (Boston viol_level, Seattle
                     RED/BLUE, demerit bands ...); always wins
    use:             which questions Jev may decide (default: all three);
                     pass an empty set for shadow/off behaviour
    forcing:         the set of hazards that force an F (default F_FORCING_HAZARDS);
                     the shadow report uses it to cost out the alternatives

    Returns a dict with the final reading plus both sides ('regex', 'jev')
    and which side was used per question.
    """
    dp = _pipeline()
    floors = CONFIDENCE_FLOORS if floors is None else floors
    use = set(QUESTIONS) if use is None else set(use)
    reg = regex_reading(text)
    jev = {q: answer(judgment, q) for q in QUESTIONS}
    used = {}

    jcat = confident_answer(judgment, 'category', floors) if 'category' in use else None
    category = jcat or reg['category']
    used['category'] = 'jev' if jcat else 'regex'

    marker = parse_tier_marker(text)
    if source_severity:
        severity, tier_from = source_severity, 'source'
    elif marker:
        severity, tier_from = marker, 'marker'
    else:
        severity, tier_from = dp.SEVERITY_MAP.get(category, 'core'), 'category'

    forcing = F_FORCING_HAZARDS if forcing is None else forcing
    jhaz = confident_answer(judgment, 'hazard', floors) if 'hazard' in use else None
    forces_f = (jhaz in forcing) if jhaz else reg['forces_f']
    used['hazard'] = 'jev' if jhaz else 'regex'

    jsum = confident_answer(judgment, 'summary', floors) if 'summary' in use else None
    if jsum:
        summary = _truncate(text) if jsum == SUMMARY_NO_MATCH else jsum
        used['summary'] = 'jev'
    else:
        summary = reg['summary']
        used['summary'] = 'regex'
    needed = HAZARD_REQUIRED_BY_PHRASE.get(summary)
    if needed and jhaz and jhaz != needed:
        summary = _truncate(text)
        used['summary'] = 'hazard_override'
        jsum = None

    any_jev = any(v == 'jev' for v in used.values())
    if any_jev:
        phrase = jsum if (jsum and jsum != SUMMARY_NO_MATCH) else (summary if summary in SUMMARY_LEVEL_BY_PHRASE else None)
        sum_sev, sum_cat = summary_level(jhaz, phrase, category)
        chips = chips_for(category, jhaz, phrase)
    else:
        sum_sev, sum_cat, chips = reg['summary_severity'], reg['summary_category'], reg['chips']

    return {
        'text': text,
        'category': category,
        'severity': severity,
        'tier_from': tier_from,
        'tier_marker': marker,
        'forces_f': bool(forces_f),
        'hazard': jhaz,
        'summary': summary,
        'summary_severity': sum_sev,
        'summary_category': sum_cat,
        'chips': chips,
        'used': used,
        'regex': reg,
        'jev': {q: {'choice': c, 'confidence': p} for q, (c, p) in jev.items()},
    }


# -- pipeline-shaped helpers --------------------------------------------------

def _lines(texts_or_joined):
    if isinstance(texts_or_joined, str):
        return [p for p in texts_or_joined.split('|||') if p.strip()]
    return [str(t) for t in texts_or_joined if t]


def _lookup(judgments, text):
    return (judgments or {}).get(normalize_text(text))


def classify_violation(text, judgments=None, source_severity=None, floors=None, use=None):
    """(category, severity, text): the shape classify_violation returns."""
    if not text:
        return ('unclassified', source_severity or 'core', text)
    d = decide(text, _lookup(judgments, text), source_severity, floors, use)
    return (d['category'], d['severity'], text)


def has_active_pest_or_sewage(texts_or_joined, judgments=None, floors=None, use=None):
    """True when any line forces an F. With no Jev decisions in play this is
    exactly today's regex over the joined text."""
    use = set(QUESTIONS) if use is None else set(use)
    joined = texts_or_joined if isinstance(texts_or_joined, str) else '|||'.join(_lines(texts_or_joined))
    if 'hazard' not in use or not judgments:
        return _pipeline().has_active_pest_or_sewage(joined)
    return any(decide(t, _lookup(judgments, t), None, floors, use)['forces_f'] for t in _lines(joined))


def detect_infractions(texts_or_joined, judgments=None, floors=None, use=None):
    """Chips, in today's order."""
    use = set(QUESTIONS) if use is None else set(use)
    joined = texts_or_joined if isinstance(texts_or_joined, str) else '|||'.join(_lines(texts_or_joined))
    if not use or not judgments:
        return _pipeline().detect_infractions(joined)
    chips = set()
    for t in _lines(joined):
        chips.update(decide(t, _lookup(judgments, t), None, floors, use)['chips'])
    return [c for c in CHIP_ORDER if c in chips]


def summarize_violations(texts_or_joined, judgments=None, floors=None, use=None):
    """List of {text, verbatim, severity, category}, CRITICAL first, deduped
    by summary text, max 5: the shape summarize_violations returns."""
    use = set(QUESTIONS) if use is None else set(use)
    joined = texts_or_joined if isinstance(texts_or_joined, str) else '|||'.join(_lines(texts_or_joined))
    if not use or not judgments:
        return _pipeline().summarize_violations(joined)
    items = []
    for raw in _lines(joined):
        clean_raw = re.sub(r'^\d+\.\s*', '', raw).strip()
        if len(clean_raw) < 5:
            continue
        d = decide(clean_raw, _lookup(judgments, clean_raw), None, floors, use)
        items.append({'text': d['summary'], 'verbatim': clean_raw,
                      'severity': d['summary_severity'], 'category': d['summary_category']})
    order = {'CRITICAL': 3, 'WARNING': 2, 'INFO': 1}
    items.sort(key=lambda s: order.get(s['severity'], 0), reverse=True)
    seen, unique = set(), []
    for s in items:
        if s['text'] not in seen:
            seen.add(s['text'])
            unique.append(s)
    return unique[:5]


# ─── PIPELINE PASS (phase 2) ─────────────────────────────────────────────────
# Where a source's stored risk_score comes from, and where its per-line tier
# comes from. Read off the fetchers and verified against the committed seeds
# (docs/jev-shadow-report.md, "Where the score is re-scored"): for every
# "computed" source calc_risk_score over the stored tiers reproduces the
# stored risk_score on 95% or more of inspections. Jev's category may move
# points only on computed sources; a source-stated tier never moves.

SCORE_FROM_TIERS_SOURCES = frozenset({
    'Chicago Open Data', 'NYC Open Data', 'DataSF', 'Houston Health Dept',
    'Cincinnati Open Data', 'Delaware Open Data', 'Montgomery County Open Data',
})
# Computed from tiers the SOURCE stated (levels, demerit bands): the score is
# a function of the stored tiers, but Jev's category must not move them.
SOURCE_TIER_SCORE_SOURCES = frozenset({'Boston Open Data', 'Detroit Open Data'})
# Sources whose per-line tier is stated by the source (a level, points, a
# demerit band, a critical flag).
SOURCE_TIER_SOURCES = frozenset({
    'Boston Open Data', 'Detroit Open Data', 'Minneapolis Open Data', 'LA County Open Data',
    'Wake County Open Data', 'King County Open Data', 'Southern Nevada Health District',
    'Richardson Health Dept', 'NYS Department of Health',
})
# Sources whose tier was bounded by a per-item rule the stored row no longer
# carries (Chicago's item number, NYC's critical flag): a stored core line
# stays core and a stored non-core line never drops below priority_foundation.
BOUNDED_TIER_SOURCES = frozenset({'Chicago Open Data', 'NYC Open Data'})
MHD_SOURCE_SUFFIX = ' Health Dept'
_TIER_RANK = {'core': 0, 'priority_foundation': 1, 'core_': 0, 'priority': 2}
_MHD_DEMERIT = None


def _mhd_demerit_or_none_jurisdictions():
    """Display names of MyHealthDepartment jurisdictions whose portal publishes
    demerits or no score at all: their risk_score is computed from the
    scraped violations (from the pipeline's jurisdiction configs)."""
    global _MHD_DEMERIT
    if _MHD_DEMERIT is None:
        dp = _pipeline()
        names = set()
        for cfg in list(dp.DFW_JURISDICTIONS.values()) + list(dp.MHD_METRO_JURISDICTIONS.values()):
            if cfg.get('score_scale', '100') in ('demerit', 'none'):
                names.add(cfg['display_name'])
        _MHD_DEMERIT = frozenset(names)
    return _MHD_DEMERIT


def score_policy(source, original_score=None):
    """'computed' (points move with the tiers), 'source_tiers' (computed, but
    from tiers the source stated) or 'official'."""
    source = source or ''
    if source in SCORE_FROM_TIERS_SOURCES:
        return 'computed'
    if source in SOURCE_TIER_SCORE_SOURCES:
        return 'source_tiers'
    if source in SOURCE_TIER_SOURCES:
        return 'official'
    if source.endswith(MHD_SOURCE_SUFFIX):
        juris = source[:-len(MHD_SOURCE_SUFFIX)]
        if juris in _mhd_demerit_or_none_jurisdictions() or original_score is None:
            return 'computed'
        return 'official'
    return 'official'


def tier_policy(source):
    """'fixed' (source-stated, never re-tiered), 'bounded' (Chicago, NYC) or 'free'."""
    source = source or ''
    if source in SOURCE_TIER_SOURCES:
        return 'fixed'
    if source in BOUNDED_TIER_SOURCES:
        return 'bounded'
    return 'free'


def bounded_tier(stored_sev, new_sev):
    """The conservative bound for Chicago/NYC lines (see BOUNDED_TIER_SOURCES)."""
    if stored_sev == 'core':
        return 'core'
    if _TIER_RANK.get(new_sev, 0) < _TIER_RANK['priority_foundation']:
        return 'priority_foundation'
    return new_sev


def _line_parts(v):
    """(category, severity, text) from a violation entry (list/tuple or dict)."""
    if isinstance(v, dict):
        return (v.get('category', 'unclassified'), v.get('severity', 'core'),
                str(v.get('description', '') or ''))
    if isinstance(v, (list, tuple)) and len(v) >= 3:
        return (str(v[0]), str(v[1]), str(v[2]))
    return None


def apply_to_inspections(inspections, reader, use=None, label='run'):
    """Phase 2: read every violation line of the fetched inspection records
    with Jev and apply the decisions in `use` (default: the reader's decide
    set in decide mode, nothing in shadow mode). Mutates records in place:

      violations[i]        -> [category, severity, text] (category/tier only
                              when 'category' is decided; a source-stated tier
                              never moves; Chicago/NYC stay within their bound)
      risk_score           -> recomputed only where the source computes it
                              from tiers AND the stored score still matches
                              the stored tiers
      pv/pfv/cv/tv counts  -> from the final tiers
      _jev                 -> {'forces_f': bool|None, 'chips': list|None,
                              'summaries': list|None} for the output paths;
                              None means "Jev did not decide, use today's
                              functions"

    Fail open at every level: a line without a judgment gets today's regex,
    a record that raises is left untouched and counted. Returns a stats dict
    for the run summary. Never raises for reader or network reasons."""
    dp = _pipeline()
    t0 = time.time()
    if use is None:
        use = set(reader.decide_set) if reader.mode == 'decide' else set()
    use = set(use) & set(QUESTIONS)
    stats = {
        'mode': reader.mode, 'decide': sorted(use), 'model': reader.client.model,
        'question_set': QUESTION_SET_VERSION, 'records': len(inspections),
        'records_with_violations': 0, 'lines': 0, 'distinct_texts': 0, 'lines_with_judgment': 0,
        'decisions': {q: {'jev': 0, 'regex': 0} for q in QUESTIONS},
        'f_vs_regex': {'agree': 0, 'regex_only': 0, 'jev_only': 0},
        'rescored_records': 0, 'retiered_lines': 0, 'record_errors': 0,
    }
    texts = []
    for rec in inspections:
        try:
            for v in rec.get('violations') or []:
                parts = _line_parts(v)
                if parts and parts[2].strip():
                    texts.append(parts[2])
        except Exception as e:                   # a record we cannot read is left to the regex
            stats['record_errors'] += 1
            log.warning(f'Jev pass: unreadable violations on a record ({type(e).__name__})')
    stats['lines'] = len(texts)
    stats['distinct_texts'] = len({normalize_text(t) for t in texts})
    judgments = reader.judge_texts(texts, label=label) if texts else {}
    stats['lines_with_judgment'] = sum(1 for t in texts if judgments.get(normalize_text(t)))

    for rec in inspections:
        vio = rec.get('violations') or []
        if not vio or not isinstance(vio, (list, tuple)):
            continue
        stats['records_with_violations'] += 1
        try:
            _apply_to_record(rec, vio, judgments, use, stats, dp)
        except Exception as e:                   # one bad record never stops the run
            stats['record_errors'] += 1
            log.warning(f'Jev pass: record left as read by the regex ({type(e).__name__})')
    stats['reader'] = {k: v for k, v in reader.stats.items() if k != 'cache_files_written'}
    stats['summary_lines'] = reader.summary_lines() + [
        f"  decisions: " + ', '.join(f"{q} {stats['decisions'][q]['jev']} jev / {stats['decisions'][q]['regex']} regex"
                                     for q in QUESTIONS),
        f"  F vs regex on judged records: {stats['f_vs_regex']['agree']} agree, "
        f"{stats['f_vs_regex']['regex_only']} regex-only F, {stats['f_vs_regex']['jev_only']} jev-only F",
        f"  re-tiered lines: {stats['retiered_lines']}, re-scored records: {stats['rescored_records']}, "
        f"record errors: {stats['record_errors']}",
    ]
    stats['elapsed_s'] = round(time.time() - t0, 1)
    return stats


def _apply_to_record(rec, vio, judgments, use, stats, dp):
    source = rec.get('source') or ''
    tp = tier_policy(source)
    sp = score_policy(source, rec.get('original_score'))
    parts = [_line_parts(v) for v in vio]
    parts = [p for p in parts if p]
    if not parts:
        return
    line_texts = [p[2] for p in parts]
    joined = '|||'.join(t for t in line_texts if t)
    readings = []
    shadow_f = False
    any_judged = False
    for (c0, s0, t) in parts:
        j = judgments.get(normalize_text(t))
        if j:
            any_judged = True
        d = decide(t, j, source_severity=(s0 if tp == 'fixed' else None), use=use)
        readings.append(d)
        for q in QUESTIONS:
            if q in use:
                stats['decisions'][q]['jev' if d['used'][q] == 'jev' else 'regex'] += 1
        # what the hazard question would decide, whatever this run's mode
        shadow_f = shadow_f or decide(t, j, None, None, {'hazard'})['forces_f']
    if any_judged:
        regex_f = dp.has_active_pest_or_sewage(joined)
        if regex_f and shadow_f:
            stats['f_vs_regex']['agree'] += 1
        elif regex_f:
            stats['f_vs_regex']['regex_only'] += 1
        elif shadow_f:
            stats['f_vs_regex']['jev_only'] += 1
        else:
            stats['f_vs_regex']['agree'] += 1

    result = {'forces_f': None, 'chips': None, 'summaries': None}

    if 'category' in use:
        old_tiers = [s0 for (_, s0, _) in parts]
        new_lines = []
        for d, (c0, s0, t) in zip(readings, parts):
            sev = d['severity']
            if tp == 'bounded':
                sev = bounded_tier(s0, sev)
            if sev != s0:
                stats['retiered_lines'] += 1
            new_lines.append([d['category'], sev, t])
        if sp == 'computed':
            old_score, _, _, _ = dp.calc_risk_score([(c0, s0, t) for (c0, s0, t) in parts])
            if old_score == rec.get('risk_score'):
                new_score, _, _, _ = dp.calc_risk_score([tuple(x) for x in new_lines])
                if new_score != rec.get('risk_score'):
                    stats['rescored_records'] += 1
                rec['risk_score'] = new_score
        rec['violations'] = new_lines
        _, pv, pfv, cv = dp.calc_risk_score([tuple(x) for x in new_lines])
        rec['priority_violations'] = pv
        rec['priority_foundation_violations'] = pfv
        rec['core_violations'] = cv
        rec['total_violations'] = len(new_lines)

    if any_judged:
        if 'hazard' in use:
            result['forces_f'] = any(d['forces_f'] for d in readings)
        if use:
            result['chips'] = detect_infractions(line_texts, judgments, use=use)
            summaries = summarize_violations(line_texts, judgments, use=use)
            for s in summaries:
                s['verbatim'] = s['verbatim'][:200]
            result['summaries'] = summaries
    rec['_jev'] = result


# ─── CLI (debugging aid; prints no secrets) ──────────────────────────────────

def main(argv=None):
    import argparse
    p = argparse.ArgumentParser(description='DineScores Jev reader')
    p.add_argument('--check', action='store_true', help='GET /v1/models with the configured key (spends no tokens)')
    p.add_argument('--line', nargs='*', help='judge these violation lines (uses and extends the cache)')
    p.add_argument('--cache-dir', default=CACHE_DIR)
    p.add_argument('--mode', default=None, help='override JEV_MODE for this call')
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    client = JevClient()
    print(f'key configured: {client.configured}; model: {client.model}; '
          f'question set: {QUESTION_SET_VERSION} ({question_set_fingerprint()[:12]}); '
          f'mode: {args.mode or jev_mode()}')
    if args.check:
        models = client.list_models()
        for m in models.get('models', []):
            print(f"  {m.get('name')}: {m.get('description')}")
    if args.line:
        reader = JevReader(mode=args.mode or jev_mode() or 'shadow', client=client,
                           cache=JudgmentCache(args.cache_dir))
        if reader.mode == 'off':
            reader.mode = 'shadow'
        judgments = reader.judge_texts(args.line, label='cli')
        for t in args.line:
            d = decide(t, judgments.get(normalize_text(t)))
            print(json.dumps({k: d[k] for k in ('text', 'category', 'severity', 'tier_from', 'forces_f',
                                                 'hazard', 'summary', 'chips', 'used', 'jev')},
                             ensure_ascii=False, indent=1))
        reader.flush_cache('cli')
        for line in reader.summary_lines():
            print(line)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
