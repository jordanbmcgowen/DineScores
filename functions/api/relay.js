// TEMPORARY egress diagnostic (deleted after one measurement): can a
// Cloudflare Pages Function's own fetch reach the two inspection portals
// that answer 403 to every GitHub Actions runner? Nothing here is caller
// controlled — the targets, methods and bodies are fixed below — and the
// response is only the status code and the first bytes of each body.
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36';

function mhdSearchBody() {
  const today = new Date();
  const weekAgo = new Date(today.getTime() - 7 * 86400000);
  const iso = d => d.toISOString().slice(0, 10);
  return JSON.stringify({
    data: {
      path: 'dallas', programName: '',
      filters: { date: `${iso(weekAgo)} to ${iso(today)}`, purpose: '' },
      start: 0, count: 25, searchStr: '', lat: 0, lng: 0, sort: {},
    },
    task: 'searchInspections',
  });
}

const PROBES = [
  { name: 'egress-ip', url: 'https://api.ipify.org', init: { headers: { 'User-Agent': UA } } },
  {
    name: 'mhd-search-dallas', url: 'https://inspections.myhealthdepartment.com/',
    init: {
      method: 'POST', body: mhdSearchBody(),
      headers: {
        'User-Agent': UA, 'Content-Type': 'application/json',
        'Accept': 'application/json, text/plain, */*',
        'Origin': 'https://inspections.myhealthdepartment.com',
        'Referer': 'https://inspections.myhealthdepartment.com/',
      },
    },
  },
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

export async function onRequestGet() {
  const results = [];
  for (const p of PROBES) {
    try {
      const r = await fetch(p.url, { ...p.init, redirect: 'follow' });
      const text = await r.text();
      results.push({ name: p.name, status: r.status, server: r.headers.get('server'), head: text.slice(0, 160) });
    } catch (e) {
      results.push({ name: p.name, error: String(e) });
    }
  }
  return new Response(JSON.stringify(results, null, 1), {
    headers: { 'content-type': 'application/json', 'cache-control': 'no-store' },
  });
}
