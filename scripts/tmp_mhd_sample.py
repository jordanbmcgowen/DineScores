"""Temporary: sample raw MHD rows + a few detail pages for candidate slugs."""
import datetime, json, os, sys, time, random
sys.path.insert(0, os.getcwd())
import dinescores_pipeline as p
slugs = sys.argv[1].split(',')
out = {}
s = p._mhd_session()
today = datetime.date.today()
for slug in slugs:
    rows_all = []
    days = {}
    for back in range(1, 15):
        d = (today - datetime.timedelta(days=back)).isoformat()
        rows = p._mhd_search_all_pages(s, slug, f"{d} to {d}", sort=p.MHD_DEFAULT_SORT, label=slug)
        days[d] = None if rows is None else len(rows)
        if rows: rows_all.extend(rows)
        time.sleep(2)
    details = {}
    sample = random.Random(1).sample(rows_all, min(12, len(rows_all)))
    for r in sample:
        iid = r.get('inspectionID')
        try:
            h = s.get(f"https://inspections.myhealthdepartment.com/{slug}/inspection/?inspectionID={iid}", timeout=40)
            details[iid] = {'status': h.status_code, 'html': h.text[:60000]}
        except Exception as e:
            details[iid] = {'error': str(e)}
        time.sleep(3)
    out[slug] = {'days': days, 'rows': rows_all, 'details': details}
    print(slug, days, len(rows_all), flush=True)
json.dump(out, open('mhd_sample.json', 'w'))
