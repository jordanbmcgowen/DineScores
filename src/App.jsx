import React, { useEffect, useState, useMemo, useCallback, useRef } from 'react';
import { ensureVettedFields } from './grading.js';
import { probeApi, fetchBboxFromApi, fetchAreaFromApi } from './api.js';
import GradeBadge from './components/GradeBadge.jsx';
import RestaurantMap from './components/RestaurantMap.jsx';
import InspectionModal from './components/InspectionModal.jsx';
import FilterBar from './components/FilterBar.jsx';
import BottomSheet from './components/BottomSheet.jsx';

// Below this zoom the embedded overview is enough; above it, lazy-load the
// uncapped records for whatever is in view from the D1 API.
const VIEWPORT_ZOOM = 9;

export default function App() {
  const [allData, setAllData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [debouncedSearch, setDebouncedSearch] = useState('');
  const [selectedRestaurant, setSelectedRestaurant] = useState(null);
  // Default ordering: most recent inspections; upgraded to nearest-first the
  // moment a GPS fix lands (unless the user has picked a sort themselves).
  const [sortBy, setSortBy] = useState('date-desc');
  const sortTouchedRef = useRef(false);

  // Filters
  const [cityFilter, setCityFilter] = useState('all');
  const [metroFilter, setMetroFilter] = useState(null);
  const [gradeFilter, setGradeFilter] = useState([]);
  const [infractionFilter, setInfractionFilter] = useState([]);

  // Progressive loading from the D1 API when it is reachable
  const [mapView, setMapView] = useState(null); // { zoom, bounds:{w,s,e,n} }
  const [apiReady, setApiReady] = useState(false);
  const [loadingArea, setLoadingArea] = useState(null); // city/metro being fetched
  const apiAvailableRef = useRef(false);
  const knownIdsRef = useRef(new Set());
  const coveredBoxesRef = useRef([]); // boxes fully fetched (not row-capped)
  const inFlightRef = useRef(new Set());
  const loadedAreasRef = useRef(new Set()); // cities/metros fully loaded

  // Open the map on the user's own location (mobile-first): ask for a GPS
  // fix once on load, and center there when it's near covered data.
  const [userPos, setUserPos] = useState(null);
  const [flyTo, setFlyTo] = useState(null);

  // Debounce search
  useEffect(() => {
    const timer = setTimeout(() => setDebouncedSearch(searchTerm), 300);
    return () => clearTimeout(timer);
  }, [searchTerm]);

  useEffect(() => {
    if (!navigator.geolocation) return;
    navigator.geolocation.getCurrentPosition(
      pos => setUserPos({ lat: pos.coords.latitude, lng: pos.coords.longitude }),
      () => {}, // denied/unavailable — keep the default framing
      { timeout: 8000, maximumAge: 300000 },
    );
  }, []);

  // Once we know where the user is, "nearest first" is the most useful
  // default ordering — but never override a sort they chose explicitly.
  useEffect(() => {
    if (userPos && !sortTouchedRef.current) setSortBy('distance');
  }, [userPos]);

  // Center on the user only when we actually cover their area (~within
  // 100km of any loaded restaurant) — recentring onto an empty map would
  // be worse than the default nationwide view.
  useEffect(() => {
    if (!userPos || flyTo || allData.length === 0) return;
    const near = allData.some(r =>
      r.lt && r.ln &&
      Math.abs(r.lt - userPos.lat) < 0.9 &&
      Math.abs(r.ln - userPos.lng) < 0.9);
    if (near) setFlyTo({ lng: userPos.lng, lat: userPos.lat, zoom: 12.5, key: Date.now() });
  }, [userPos, allData, flyTo]);

  // Load data
  useEffect(() => {
    async function load() {
      setLoading(true);
      try {
        // Embedded data.js is the only bulk source: it is refreshed weekly by
        // CI and deployed with the site. The D1 API layers uncapped records
        // on top once probed.
        if (window.DATA && Array.isArray(window.DATA) && window.DATA.length > 0) {
          const seeded = window.DATA.map(ensureVettedFields);
          for (const r of seeded) if (r.i) knownIdsRef.current.add(r.i);
          setAllData(seeded);
        } else {
          setError('Failed to load restaurant data. Please refresh.');
        }
      } catch (err) {
        console.error('Data load failed:', err);
        setError('Failed to load restaurant data. Please refresh.');
      } finally {
        setLoading(false);
        // Remove loading overlay
        const overlay = document.getElementById('loading-overlay');
        if (overlay) {
          overlay.classList.add('fade-out');
          setTimeout(() => overlay.remove(), 500);
        }
      }
    }
    load();
    // Probe the D1 API once; when reachable it enables progressive loading.
    probeApi().then(ok => {
      if (ok) {
        apiAvailableRef.current = true;
        setApiReady(true);
      }
    });
  }, []);

  // Merge lazily-loaded viewport records into the pool (dedup by id)
  const mergeRecords = useCallback((records) => {
    const fresh = [];
    for (const r of records) {
      if (r.i && !knownIdsRef.current.has(r.i)) {
        knownIdsRef.current.add(r.i);
        fresh.push(ensureVettedFields(r));
      }
    }
    if (fresh.length) setAllData(prev => [...prev, ...fresh]);
  }, []);

  // Selecting a city or metro eagerly loads its COMPLETE roster (lite records,
  // one time per area) so the view isn't limited to the embedded per-city cap.
  useEffect(() => {
    const area = metroFilter ? { metro: metroFilter } : (cityFilter !== 'all' ? { city: cityFilter } : null);
    if (!area || !apiAvailableRef.current) return;
    const key = metroFilter ? `m:${metroFilter}` : `c:${cityFilter}`;
    if (loadedAreasRef.current.has(key)) return;
    loadedAreasRef.current.add(key);
    setLoadingArea(area.metro || area.city);
    fetchAreaFromApi(area).then(records => {
      mergeRecords(records);
      setLoadingArea(prev => (prev === (area.metro || area.city) ? null : prev));
      if (records.length === 0) loadedAreasRef.current.delete(key); // allow retry
    }).catch(() => {
      loadedAreasRef.current.delete(key);
      setLoadingArea(prev => (prev === (area.metro || area.city) ? null : prev));
    });
  }, [cityFilter, metroFilter, mergeRecords, apiReady]);

  // A lite record was opened and its full detail fetched: upgrade it in place.
  const upgradeRecord = useCallback((full) => {
    const rec = ensureVettedFields(full);
    setAllData(prev => prev.map(x => (x.i === rec.i ? { ...x, ...rec } : x)));
  }, []);

  // Viewport changed: remember it, and (when zoomed in) lazy-load its records.
  const handleViewportChange = useCallback(({ zoom, bounds }) => {
    setMapView({ zoom, bounds });
    if (!apiAvailableRef.current || zoom < VIEWPORT_ZOOM) return;

    // Pad the box ~18% so small pans don't re-fetch
    const padX = (bounds.e - bounds.w) * 0.18;
    const padY = (bounds.n - bounds.s) * 0.18;
    const box = {
      w: bounds.w - padX, s: bounds.s - padY,
      e: bounds.e + padX, n: bounds.n + padY,
    };
    const contains = (a, b) => a.w <= b.w && a.s <= b.s && a.e >= b.e && a.n >= b.n;
    if (coveredBoxesRef.current.some(c => contains(c, box))) return;

    const key = [box.w, box.s, box.e, box.n].map(v => v.toFixed(3)).join(',');
    if (inFlightRef.current.has(key)) return;
    inFlightRef.current.add(key);

    fetchBboxFromApi(box).then(({ records, truncated }) => {
      inFlightRef.current.delete(key);
      mergeRecords(records);
      // Only mark fully covered when the API returned everything in the box;
      // a capped result means denser-than-one-page, so allow a tighter refetch.
      if (!truncated) {
        coveredBoxesRef.current.push(box);
        if (coveredBoxesRef.current.length > 60) coveredBoxesRef.current.shift();
      }
    }).catch(() => { inFlightRef.current.delete(key); });
  }, [mergeRecords]);

  // City/metro chips derived from the live dataset, largest first
  const cityOptions = useMemo(() => {
    const cityCounts = new Map();
    const metroCounts = new Map();
    for (const r of allData) {
      if (r.m) {
        metroCounts.set(r.m, (metroCounts.get(r.m) || 0) + 1);
      } else if (r.c) {
        cityCounts.set(r.c, (cityCounts.get(r.c) || 0) + 1);
      }
    }
    const entries = [
      ...[...cityCounts].map(([c, n]) => ({ label: c, city: c, metro: null, n })),
      ...[...metroCounts].map(([m, n]) => ({ label: m, city: 'all', metro: m, n })),
    ].filter(e => e.n >= 50).sort((a, b) => b.n - a.n);
    // Metro member cities as drill-down chips after the aggregate chips
    const metroCities = new Map();
    for (const r of allData) {
      if (r.m && r.c) metroCities.set(r.c, (metroCities.get(r.c) || 0) + 1);
    }
    const drill = [...metroCities]
      .filter(([, n]) => n >= 200)
      .sort((a, b) => b[1] - a[1])
      .map(([c]) => ({ label: c, city: c, metro: null }));
    return [{ label: 'All Cities', city: 'all', metro: null }, ...entries, ...drill];
  }, [allData]);

  // Filtered + sorted data
  const filtered = useMemo(() => {
    let data = allData;

    // City filter
    if (cityFilter !== 'all') {
      data = data.filter(r => r.c === cityFilter);
    }
    // Metro filter
    if (metroFilter) {
      data = data.filter(r => r.m === metroFilter);
    }

    // Grade filter
    if (gradeFilter.length > 0) {
      data = data.filter(r => gradeFilter.includes(r.vg));
    }

    // Infraction filter
    if (infractionFilter.length > 0) {
      data = data.filter(r =>
        r.inf && infractionFilter.some(cat => r.inf.includes(cat))
      );
    }

    // Search
    if (debouncedSearch) {
      const q = debouncedSearch.toLowerCase();
      data = data.filter(r =>
        (r.n && r.n.toLowerCase().includes(q)) ||
        (r.a && r.a.toLowerCase().includes(q)) ||
        (r.z && r.z.includes(q))
      );
    }

    // Sort
    const sorted = [...data];
    switch (sortBy) {
      case 'distance': {
        if (!userPos) {
          sorted.sort((a, b) => (b.d || '').localeCompare(a.d || ''));
          break;
        }
        // Squared equirectangular distance — monotonic with true distance,
        // cheap enough to run over the whole result set. No coords sorts last.
        const cosLat = Math.cos((userPos.lat * Math.PI) / 180);
        const dist = r => (r.lt && r.ln)
          ? (r.lt - userPos.lat) ** 2 + ((r.ln - userPos.lng) * cosLat) ** 2
          : Infinity;
        sorted.sort((a, b) => dist(a) - dist(b));
        break;
      }
      case 'score-asc':
        sorted.sort((a, b) => (a.rs || 0) - (b.rs || 0));
        break;
      case 'score-desc':
        sorted.sort((a, b) => (b.rs || 0) - (a.rs || 0));
        break;
      case 'name-asc':
        sorted.sort((a, b) => (a.n || '').localeCompare(b.n || ''));
        break;
      default: // date-desc
        sorted.sort((a, b) => (b.d || '').localeCompare(a.d || ''));
    }

    return sorted;
  }, [allData, cityFilter, metroFilter, gradeFilter, infractionFilter, debouncedSearch, sortBy, userPos]);

  // The list and header count are viewport-aware: once zoomed in, they reflect
  // what's actually on screen (which is where lazy-loaded records show up).
  // Zoomed out, they mirror the full filtered set. The MAP always gets the full
  // filtered set — it clusters, so density isn't a problem.
  const visible = useMemo(() => {
    if (!mapView || mapView.zoom < VIEWPORT_ZOOM) return filtered;
    const { w, s, e, n } = mapView.bounds;
    return filtered.filter(r => r.ln >= w && r.ln <= e && r.lt >= s && r.lt <= n);
  }, [filtered, mapView]);

  // Camera signals: a city/metro change re-frames the map to that area
  // (fitSignal); grade/issue/search changes only ever zoom OUT to the nearest
  // match if none is on screen (narrowSignal). Sort and lazy-load merges
  // never move the camera.
  const fitSignal = `${cityFilter}|${metroFilter}`;
  const narrowSignal = `${gradeFilter.join('')}|${infractionFilter.join('')}|${debouncedSearch}`;

  const handleMarkerClick = useCallback((restaurant) => {
    setSelectedRestaurant(restaurant);
  }, []);

  const toggleGrade = useCallback((grade) => {
    setGradeFilter(prev => {
      if (grade === 'B') {
        const hasB = prev.includes('B');
        if (hasB) return prev.filter(g => g !== 'B' && g !== 'C');
        return [...prev, 'B', 'C'];
      }
      return prev.includes(grade) ? prev.filter(g => g !== grade) : [...prev, grade];
    });
  }, []);

  const toggleInfraction = useCallback((cat) => {
    setInfractionFilter(prev =>
      prev.includes(cat) ? prev.filter(c => c !== cat) : [...prev, cat]
    );
  }, []);

  function formatDate(d) {
    if (!d) return 'N/A';
    const parts = d.split('-');
    if (parts.length < 3) return d;
    const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    return months[parseInt(parts[1], 10) - 1] + ' ' + parseInt(parts[2], 10) + ', ' + parts[0];
  }

  // Loading state — the HTML overlay handles it
  if (loading) return null;

  const sortSelect = (
    <select
      value={sortBy}
      onChange={e => { sortTouchedRef.current = true; setSortBy(e.target.value); }}
      className="text-xs font-semibold bg-transparent text-slate-500 dark:text-slate-400 border-0 outline-none cursor-pointer pr-1"
      aria-label="Sort results"
    >
      {userPos && <option value="distance">Nearest first</option>}
      <option value="date-desc">Recently inspected</option>
      <option value="score-asc">Worst score first</option>
      <option value="score-desc">Best score first</option>
      <option value="name-asc">Name A–Z</option>
    </select>
  );

  return (
    <div className="h-dvh relative overflow-hidden font-sans text-slate-900 dark:text-slate-100">
      {/* Map canvas */}
      <div className="absolute inset-0">
        <RestaurantMap
          restaurants={filtered}
          onMarkerClick={handleMarkerClick}
          onViewportChange={handleViewportChange}
          fitSignal={fitSignal}
          narrowSignal={narrowSignal}
          flyTo={flyTo}
        />
      </div>

      {/* Floating header */}
      <div className="absolute top-0 inset-x-0 z-20 pt-safe pointer-events-none">
        <div className="px-3 md:px-4 pt-3 pb-2 flex items-center gap-2 pointer-events-auto">
          {/* Brand */}
          <div className="hidden sm:flex items-center gap-2 h-11 px-3.5 rounded-2xl bg-white/90 dark:bg-slate-800/90 backdrop-blur shadow-lg ring-1 ring-slate-900/5 dark:ring-white/10 shrink-0">
            <svg className="w-6 h-6" viewBox="0 0 32 32" fill="none" aria-hidden="true">
              <path d="M16 2L4 8v8c0 7.18 5.12 13.88 12 16 6.88-2.12 12-8.82 12-16V8L16 2z" fill="#0d9488"/>
              <text x="16" y="21" textAnchor="middle" fill="white" fontSize="12" fontWeight="700" fontFamily="system-ui">A</text>
            </svg>
            <span className="text-[17px] font-extrabold tracking-tight">DineScores</span>
          </div>

          {/* Search */}
          <div className="relative flex-1 max-w-xl">
            <svg className="absolute left-3.5 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400 pointer-events-none" viewBox="0 0 20 20" fill="none" aria-hidden="true">
              <circle cx="8.5" cy="8.5" r="5.5" stroke="currentColor" strokeWidth="1.8"/>
              <path d="M13 13l4 4" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round"/>
            </svg>
            <input
              type="search"
              placeholder="Search restaurants, addresses…"
              className="w-full h-11 bg-white/90 dark:bg-slate-800/90 backdrop-blur text-slate-900 dark:text-white pl-10 pr-20 rounded-2xl shadow-lg ring-1 ring-slate-900/5 dark:ring-white/10 focus:ring-2 focus:ring-brand-500 outline-none text-[15px] placeholder:text-slate-400"
              value={searchTerm}
              onChange={e => setSearchTerm(e.target.value)}
            />
            {searchTerm ? (
              <button
                onClick={() => setSearchTerm('')}
                className="absolute right-3 top-1/2 -translate-y-1/2 p-1 text-slate-400 hover:text-slate-600 dark:hover:text-slate-200"
                aria-label="Clear search"
              >
                <svg viewBox="0 0 20 20" width="16" height="16" fill="none">
                  <path d="M6 6l8 8M14 6l-8 8" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round"/>
                </svg>
              </button>
            ) : (
              // Same count the results list reports, so the two never disagree
              <span className="absolute right-3.5 top-1/2 -translate-y-1/2 text-[11px] font-bold text-slate-400 tabular-nums whitespace-nowrap">
                {visible.length.toLocaleString()}
              </span>
            )}
          </div>
        </div>

        {/* Filter rail */}
        <FilterBar
          cityOptions={cityOptions}
          cityFilter={cityFilter}
          setCityFilter={setCityFilter}
          metroFilter={metroFilter}
          setMetroFilter={setMetroFilter}
          gradeFilter={gradeFilter}
          toggleGrade={toggleGrade}
          infractionFilter={infractionFilter}
          toggleInfraction={toggleInfraction}
        />
      </div>

      {/* Desktop results panel */}
      <aside className="hidden md:flex flex-col absolute left-4 top-[118px] bottom-4 w-[400px] z-10 rounded-3xl bg-white/95 dark:bg-slate-900/95 backdrop-blur shadow-2xl ring-1 ring-slate-900/5 dark:ring-white/10 overflow-hidden">
        <div className="px-4 h-11 border-b border-slate-100 dark:border-slate-800 flex items-center justify-between shrink-0">
          <span className="text-xs font-bold text-slate-500 dark:text-slate-400 tabular-nums flex items-center gap-2">
            {visible.length.toLocaleString()} restaurants
            {loadingArea && (
              <span className="inline-flex items-center gap-1 text-brand-600 dark:text-brand-500 normal-case">
                <span className="w-3 h-3 border-2 border-brand-200 border-t-brand-600 rounded-full animate-spin" />
                loading {loadingArea}…
              </span>
            )}
          </span>
          {sortSelect}
        </div>
        <div className="flex-1 overflow-y-auto">
          {visible.slice(0, 100).map(r => (
            <RestaurantCard
              key={r.i}
              restaurant={r}
              formatDate={formatDate}
              onClick={() => setSelectedRestaurant(r)}
              selected={selectedRestaurant?.i === r.i}
            />
          ))}
          {visible.length > 100 && (
            <p className="p-4 text-center text-xs text-slate-400">
              Showing 100 of {visible.length.toLocaleString()} — zoom in or search to narrow.
            </p>
          )}
          {visible.length === 0 && <EmptyState />}
        </div>
      </aside>

      {/* Mobile bottom sheet (drag the header to resize) */}
      <BottomSheet
        header={
          <div className="w-full px-4 pb-2 flex items-center justify-between">
            <span className="text-sm font-bold tabular-nums">
              {visible.length.toLocaleString()} restaurants
            </span>
            <span onClick={e => e.stopPropagation()} onTouchStart={e => e.stopPropagation()}>
              {sortSelect}
            </span>
          </div>
        }
      >
        {visible.slice(0, 50).map(r => (
          <RestaurantCard
            key={r.i}
            restaurant={r}
            formatDate={formatDate}
            onClick={() => setSelectedRestaurant(r)}
            selected={false}
          />
        ))}
        {visible.length === 0 && <EmptyState />}
      </BottomSheet>

      {/* Inspection Modal */}
      {selectedRestaurant && (
        <InspectionModal
          restaurant={selectedRestaurant}
          onClose={() => setSelectedRestaurant(null)}
          formatDate={formatDate}
          onUpgrade={upgradeRecord}
        />
      )}

      {/* Error banner */}
      {error && (
        <div className="fixed bottom-4 left-4 right-4 md:left-auto md:right-4 md:w-96 bg-red-50 dark:bg-red-900/60 border border-red-200 dark:border-red-800 rounded-2xl p-4 shadow-xl z-50 animate-slide-up">
          <p className="text-sm font-medium text-red-700 dark:text-red-200">{error}</p>
        </div>
      )}
    </div>
  );
}

function EmptyState() {
  return (
    <div className="p-10 text-center text-slate-400">
      <svg className="w-10 h-10 mx-auto mb-3 opacity-60" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5">
        <circle cx="11" cy="11" r="7"/><path d="M20 20l-3-3"/>
      </svg>
      <p className="font-semibold text-slate-500 dark:text-slate-400">No restaurants found</p>
      <p className="text-xs mt-1">Try adjusting your search or filters</p>
    </div>
  );
}

// Paperwork (docs) is deliberately absent: not decision-relevant enough for
// the result cards. It still appears in the detail modal.
const CARD_TAGS = {
  pests: 'bg-red-50 text-red-700 dark:bg-red-500/10 dark:text-red-400',
  temp: 'bg-sky-50 text-sky-700 dark:bg-sky-500/10 dark:text-sky-400',
  hygiene: 'bg-violet-50 text-violet-700 dark:bg-violet-500/10 dark:text-violet-400',
  equipment: 'bg-slate-100 text-slate-600 dark:bg-slate-500/10 dark:text-slate-400',
};
const CARD_TAG_LABELS = {
  pests: 'Pests', temp: 'Temp', hygiene: 'Hygiene', equipment: 'Equipment',
};

function RestaurantCard({ restaurant: r, formatDate, onClick, selected }) {
  return (
    <button
      onClick={onClick}
      className={`w-full text-left px-4 py-3 border-b border-slate-100 dark:border-slate-800 hover:bg-slate-50 dark:hover:bg-slate-800/60 transition-colors flex items-start gap-3 ${
        selected ? 'bg-brand-50 dark:bg-brand-900/20' : ''
      }`}
    >
      <GradeBadge grade={r.vg} size="sm" />
      <div className="flex-1 min-w-0">
        <h3 className="font-bold text-[15px] leading-snug truncate">{r.n}</h3>
        <p className="text-[13px] text-slate-500 dark:text-slate-400 truncate mt-0.5">
          {r.a}{r.c ? ` · ${r.c}` : ''}
        </p>
        <div className="flex items-center gap-1.5 mt-1.5 flex-wrap">
          <span className="text-[11px] text-slate-400 tabular-nums">
            Inspected {formatDate(r.d)}
          </span>
          {(r.inf || []).filter(cat => CARD_TAGS[cat]).slice(0, 3).map(cat => (
            <span key={cat} className={`text-[10px] px-1.5 py-0.5 rounded-md font-bold ${CARD_TAGS[cat]}`}>
              {CARD_TAG_LABELS[cat]}
            </span>
          ))}
        </div>
      </div>
    </button>
  );
}
