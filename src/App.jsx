import React, { useEffect, useState, useMemo, useCallback, useRef } from 'react';
import { fetchAllRestaurants } from './firebase.js';
import { ensureVettedFields } from './grading.js';
import GradeBadge from './components/GradeBadge.jsx';
import RestaurantMap from './components/RestaurantMap.jsx';
import InspectionModal from './components/InspectionModal.jsx';
import FilterBar from './components/FilterBar.jsx';

export default function App() {
  const [allData, setAllData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [debouncedSearch, setDebouncedSearch] = useState('');
  const [selectedRestaurant, setSelectedRestaurant] = useState(null);
  const [sortBy, setSortBy] = useState('score-desc');

  // Filters
  const [cityFilter, setCityFilter] = useState('all');
  const [metroFilter, setMetroFilter] = useState(null);
  const [riskFilter, setRiskFilter] = useState('all');
  const [gradeFilter, setGradeFilter] = useState([]);
  const [infractionFilter, setInfractionFilter] = useState([]);

  // Mobile bottom sheet
  const [sheetState, setSheetState] = useState('collapsed');
  const sheetRef = useRef(null);

  // Debounce search
  useEffect(() => {
    const timer = setTimeout(() => setDebouncedSearch(searchTerm), 300);
    return () => clearTimeout(timer);
  }, [searchTerm]);

  // Load data
  useEffect(() => {
    async function load() {
      setLoading(true);
      try {
        // Try Firestore first
        let data;
        if (window.DATA && Array.isArray(window.DATA) && window.DATA.length > 0) {
          data = window.DATA.slice();
        } else {
          data = await fetchAllRestaurants();
        }
        // Ensure all records have vetted grading fields
        setAllData(data.map(ensureVettedFields));
      } catch (err) {
        console.error('Data load failed:', err);
        // Fallback to embedded data
        if (window.DATA && Array.isArray(window.DATA) && window.DATA.length > 0) {
          setAllData(window.DATA.map(ensureVettedFields));
        } else {
          setError('Failed to load restaurant data. Please refresh.');
        }
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
  }, []);

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
    // Risk filter
    if (riskFilter === 'safe') data = data.filter(r => r.rs >= 90);
    else if (riskFilter === 'moderate') data = data.filter(r => r.rs >= 70 && r.rs < 90);
    else if (riskFilter === 'risk') data = data.filter(r => r.rs < 70);

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
      case 'score-asc':
        sorted.sort((a, b) => (a.rs || 0) - (b.rs || 0));
        break;
      case 'name-asc':
        sorted.sort((a, b) => (a.n || '').localeCompare(b.n || ''));
        break;
      default: // score-desc
        sorted.sort((a, b) => (b.rs || 0) - (a.rs || 0));
    }

    return sorted;
  }, [allData, cityFilter, metroFilter, riskFilter, gradeFilter, infractionFilter, debouncedSearch, sortBy]);

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

  // Loading state
  if (loading) {
    return null; // The HTML loading overlay handles this
  }

  return (
    <div className="min-h-screen bg-white dark:bg-slate-900 flex flex-col font-sans text-slate-900 dark:text-slate-100">
      {/* Header */}
      <header className="bg-white dark:bg-slate-800 border-b border-slate-200 dark:border-slate-700 shadow-sm z-50">
        <div className="max-w-7xl mx-auto px-4 py-3">
          <div className="flex items-center gap-3">
            {/* Logo */}
            <div className="flex items-center gap-2">
              <svg className="w-7 h-7" viewBox="0 0 32 32" fill="none">
                <path d="M16 2L4 8v8c0 7.18 5.12 13.88 12 16 6.88-2.12 12-8.82 12-16V8L16 2z" fill="#0891b2" opacity="0.15"/>
                <path d="M16 2L4 8v8c0 7.18 5.12 13.88 12 16 6.88-2.12 12-8.82 12-16V8L16 2z" stroke="#0891b2" strokeWidth="1.5" fill="none"/>
                <circle cx="20" cy="20" r="5" fill="#0891b2"/>
                <text x="20" y="22.5" textAnchor="middle" fill="white" fontSize="7" fontWeight="700" fontFamily="system-ui">A</text>
              </svg>
              <span className="text-xl font-black tracking-tight text-slate-900 dark:text-white">DineScores</span>
            </div>

            {/* Search */}
            <div className="relative flex-1 max-w-md ml-4">
              <svg className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" viewBox="0 0 20 20" fill="none">
                <circle cx="8.5" cy="8.5" r="5.5" stroke="currentColor" strokeWidth="1.5"/>
                <path d="M13 13l4 4" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"/>
              </svg>
              <input
                type="search"
                placeholder="Search restaurants..."
                className="w-full bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 text-slate-900 dark:text-white pl-10 pr-4 py-2 rounded-lg focus:ring-2 focus:ring-cyan-500 focus:border-transparent outline-none text-sm"
                value={searchTerm}
                onChange={e => setSearchTerm(e.target.value)}
              />
              {searchTerm && (
                <button
                  onClick={() => setSearchTerm('')}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-600"
                >
                  <svg viewBox="0 0 20 20" width="16" height="16" fill="none">
                    <path d="M6 6l8 8M14 6l-8 8" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"/>
                  </svg>
                </button>
              )}
            </div>

            {/* Count */}
            <span className="hidden sm:block text-xs font-semibold text-slate-500 dark:text-slate-400 whitespace-nowrap">
              {filtered.length.toLocaleString()} restaurants
            </span>
          </div>
        </div>
      </header>

      {/* Filter Bar */}
      <FilterBar
        cityFilter={cityFilter}
        setCityFilter={setCityFilter}
        metroFilter={metroFilter}
        setMetroFilter={setMetroFilter}
        riskFilter={riskFilter}
        setRiskFilter={setRiskFilter}
        gradeFilter={gradeFilter}
        toggleGrade={toggleGrade}
        infractionFilter={infractionFilter}
        toggleInfraction={toggleInfraction}
      />

      {/* Main content */}
      <main className="flex-1 flex flex-col md:flex-row relative">
        {/* Map */}
        <div className="h-[45vh] md:h-auto md:flex-1 relative z-0">
          <RestaurantMap
            restaurants={filtered}
            onMarkerClick={handleMarkerClick}
          />
        </div>

        {/* Desktop Sidebar */}
        <aside className="hidden md:flex flex-col w-[380px] border-l border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 overflow-hidden">
          <div className="px-4 py-2 border-b border-slate-100 dark:border-slate-700 flex items-center justify-between">
            <select
              value={sortBy}
              onChange={e => setSortBy(e.target.value)}
              className="text-xs font-medium bg-transparent text-slate-600 dark:text-slate-300 border-0 outline-none cursor-pointer"
            >
              <option value="score-desc">Score: High → Low</option>
              <option value="score-asc">Score: Low → High</option>
              <option value="name-asc">Name: A → Z</option>
            </select>
            <span className="text-xs text-slate-400">{filtered.length} results</span>
          </div>
          <div className="flex-1 overflow-y-auto">
            {filtered.slice(0, 100).map(r => (
              <RestaurantCard
                key={r.i}
                restaurant={r}
                formatDate={formatDate}
                onClick={() => setSelectedRestaurant(r)}
                selected={selectedRestaurant?.i === r.i}
              />
            ))}
            {filtered.length === 0 && !loading && (
              <div className="p-8 text-center text-slate-400">
                <p className="font-medium">No restaurants found</p>
                <p className="text-xs mt-1">Try adjusting your filters</p>
              </div>
            )}
          </div>
        </aside>

        {/* Mobile Bottom Sheet */}
        <div
          ref={sheetRef}
          className={`md:hidden bottom-sheet bg-white dark:bg-slate-800 shadow-2xl border-t border-slate-200 dark:border-slate-700 ${sheetState}`}
        >
          <button
            className="w-full py-3 flex justify-center"
            onClick={() => setSheetState(prev =>
              prev === 'collapsed' ? 'peek' : prev === 'peek' ? 'expanded' : 'collapsed'
            )}
          >
            <div className="w-10 h-1 bg-slate-300 dark:bg-slate-600 rounded-full" />
          </button>
          <div className="px-3 pb-1 flex items-center justify-between">
            <span className="text-xs font-semibold text-slate-500">{filtered.length} restaurants</span>
            <select
              value={sortBy}
              onChange={e => setSortBy(e.target.value)}
              className="text-xs bg-transparent text-slate-500 border-0 outline-none"
            >
              <option value="score-desc">Score ↓</option>
              <option value="score-asc">Score ↑</option>
              <option value="name-asc">Name A-Z</option>
            </select>
          </div>
          <div className="overflow-y-auto max-h-[calc(70vh-80px)]">
            {filtered.slice(0, 50).map(r => (
              <RestaurantCard
                key={r.i}
                restaurant={r}
                formatDate={formatDate}
                onClick={() => { setSelectedRestaurant(r); setSheetState('collapsed'); }}
                selected={false}
              />
            ))}
          </div>
        </div>
      </main>

      {/* Inspection Modal */}
      {selectedRestaurant && (
        <InspectionModal
          restaurant={selectedRestaurant}
          onClose={() => setSelectedRestaurant(null)}
          formatDate={formatDate}
        />
      )}

      {/* Error banner */}
      {error && (
        <div className="fixed bottom-4 left-4 right-4 md:left-auto md:right-4 md:w-96 bg-red-50 dark:bg-red-900/50 border border-red-200 dark:border-red-800 rounded-xl p-4 shadow-lg z-50 animate-slide-up">
          <p className="text-sm font-medium text-red-700 dark:text-red-300">{error}</p>
        </div>
      )}
    </div>
  );
}

function RestaurantCard({ restaurant: r, formatDate, onClick, selected }) {
  return (
    <button
      onClick={onClick}
      className={`w-full text-left px-4 py-3 border-b border-slate-100 dark:border-slate-700 hover:bg-slate-50 dark:hover:bg-slate-700/50 transition flex gap-3 ${
        selected ? 'bg-cyan-50 dark:bg-cyan-900/20 border-l-2 border-l-cyan-500' : ''
      }`}
    >
      <div className="flex-shrink-0">
        <GradeBadge grade={r.vg} score={r.ws || r.rs} size="sm" />
      </div>
      <div className="flex-1 min-w-0">
        <h3 className="font-semibold text-sm text-slate-900 dark:text-white truncate">{r.n}</h3>
        <p className="text-xs text-slate-500 dark:text-slate-400 truncate">{r.a}</p>
        {/* Infraction icons */}
        <div className="flex items-center gap-1.5 mt-1.5">
          {r.inf && r.inf.includes('pests') && <span className="text-[10px] px-1.5 py-0.5 bg-red-50 dark:bg-red-900/30 text-red-600 dark:text-red-400 rounded font-bold">Pests</span>}
          {r.inf && r.inf.includes('temp') && <span className="text-[10px] px-1.5 py-0.5 bg-blue-50 dark:bg-blue-900/30 text-blue-600 dark:text-blue-400 rounded font-bold">Temp</span>}
          {r.inf && r.inf.includes('hygiene') && <span className="text-[10px] px-1.5 py-0.5 bg-teal-50 dark:bg-teal-900/30 text-teal-600 dark:text-teal-400 rounded font-bold">Hygiene</span>}
          {(!r.inf || r.inf.length === 0) && <span className="text-[10px] px-1.5 py-0.5 bg-green-50 dark:bg-green-900/30 text-green-600 dark:text-green-400 rounded font-bold">Clean</span>}
        </div>
        <div className="flex items-center justify-between mt-1.5">
          <span className="text-[10px] font-medium text-slate-400 uppercase tracking-wider">{r.it || r.c}</span>
          <span className="text-[10px] text-slate-400">{formatDate(r.d)}</span>
        </div>
      </div>
    </button>
  );
}
