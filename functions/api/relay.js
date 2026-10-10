// TEMPORARY egress diagnostic (deleted after one measurement): can a
// Cloudflare Pages Function's own fetch reach the two inspection portals
// that answer 403 to every GitHub Actions runner? Nothing here is caller
// controlled — the targets, methods and bodies are fixed below — and the
// response is only the status code and the first bytes of each body.
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36';

function mhdSearchBody(path, range) {
  return JSON.stringify({
    data: {
      path, programName: '',
      filters: { date: range, purpose: '' },
      start: 0, count: 25, searchStr: '', lat: 0, lng: 0, sort: {},
    },
    task: 'searchInspections',
  });
}
const MHD_HEADERS = {
  'User-Agent': UA, 'Content-Type': 'application/json',
  'Accept': 'application/json, text/plain, */*',
  'Origin': 'https://inspections.myhealthdepartment.com',
  'Referer': 'https://inspections.myhealthdepartment.com/',
};
const mhdSearch = (name, path, range) => ({
  name, url: 'https://inspections.myhealthdepartment.com/',
  init: { method: 'POST', body: mhdSearchBody(path, range), headers: MHD_HEADERS },
});

const PROBES = [
  { name: 'egress-ip', url: 'https://api.ipify.org', init: { headers: { 'User-Agent': UA } } },
  mhdSearch('mhd-search-dallas-oct1-8', 'dallas', '2026-10-01 to 2026-10-08'),
  mhdSearch('mhd-search-dallas-oct6', 'dallas', '2026-10-06 to 2026-10-06'),
  mhdSearch('mhd-search-plano-oct1-8', 'plano', '2026-10-01 to 2026-10-08'),
  {
    name: 'mhd-detail-dallas',
    url: 'https://inspections.myhealthdepartment.com/dallas/inspection/?inspectionID=05A6A6C5-462F-4418-9279-6BE4966D1CBF',
    init: { headers: { 'User-Agent': UA } },
  },
  {
    name: 'snhd-list',
    url: 'https://www.southernnevadahealthdistrict.org/wp-json/snhd-eh-restaurants/v1/restaurants?per_page=1&page=1',
    init: { headers: { 'User-Agent': UA } },
  },
];

// SNHD rejected GitHub runners with 401 after ~60 list pages in ~70 s; see
// whether Cloudflare's egress survives the same burst.
async function snhdBurst() {
  const statuses = {};
  for (let page = 1; page <= 70; page++) {
    const r = await fetch(`https://www.southernnevadahealthdistrict.org/wp-json/snhd-eh-restaurants/v1/restaurants?per_page=100&page=${page}`,
      { headers: { 'User-Agent': UA } });
    await r.arrayBuffer();
    statuses[r.status] = (statuses[r.status] || 0) + 1;
    if (r.status !== 200) break;
    await new Promise(res => setTimeout(res, 150));
  }
  return statuses;
}

export async function onRequestGet() {
  const results = [];
  for (const p of PROBES) {
    try {
      const r = await fetch(p.url, { ...p.init, redirect: 'follow' });
      const text = await r.text();
      let rows = null;
      try { const j = JSON.parse(text); rows = Array.isArray(j) ? j.length : null; } catch {}
      results.push({ name: p.name, status: r.status, rows, head: text.slice(0, 160) });
    } catch (e) {
      results.push({ name: p.name, error: String(e) });
    }
  }
  try { results.push({ name: 'snhd-burst-70-pages', statuses: await snhdBurst() }); }
  catch (e) { results.push({ name: 'snhd-burst-70-pages', error: String(e) }); }
  return new Response(JSON.stringify(results, null, 1), {
    headers: { 'content-type': 'application/json', 'cache-control': 'no-store' },
  });
}
