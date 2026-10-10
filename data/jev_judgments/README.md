# Jev judgment cache

Append-only cache of TypeSafe Jev's answers about violation lines, read by
`jev_reader.py`. Every file is gzipped JSONL; every run that had to ask Jev
anything writes ONE new file holding only the answers it asked for, and every
run loads all files at start. A cached line is never re-asked.

One record per line:

```
{"k": "<sha256 of model id + question-set version + normalized text>",
 "m": "jev-1.13.0", "q": "qs1", "t": "<normalized violation text>",
 "a": {"category": {"c": "general_cleanliness", "p": 0.91, "pr": {"general_cleanliness": 0.93, "equipment_utensils": 0.05}},
       "hazard":   {"c": "none", "p": 0.99, "pr": {"none": 0.99}},
       "summary":  {"c": "Dirty equipment surfaces", "p": 0.80, "pr": {...}}},
 "ts": "2026-10-10T01:23:45Z"}
```

`c` is Jev's top option, `p` its confidence ((p_max - 1/n)/(1 - 1/n)), `pr`
the probabilities at or above 0.005. The text is the only thing that was
sent: no restaurant name, address, id, city or date.

Files load in name order and a later file wins for a key that appears
twice. That is how an answer gets corrected without editing history: a
re-verification run writes a later file (see `VERIFY_HAZARDS_ALONE` in
`jev_reader.py`).

Rules:

- Never edit or rewrite a file. Add files; delete only a whole file whose
  question set or model is retired.
- `JEV_MODEL` and `QUESTION_SET_VERSION` in `jev_reader.py` are part of the
  key. Changing either is a deliberate version bump and simply misses this
  cache; the next run refills it.
- The weekly refresh commits its new file alongside `public/data.js`, so
  the same text gets the same answer every week.
