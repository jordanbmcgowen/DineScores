import React, { useEffect, useMemo, useRef, useState } from 'react';
import { gradeMeta } from './GradeBadge.jsx';
import { fetchSearchFromApi } from '../api.js';

const MAX_LOCAL = 6;
const MAX_API_EXTRA = 4;

/**
 * Search box with instant suggestions: area matches (city/metro filters),
 * loaded restaurants (instant), then full-database matches from the D1 API
 * (covers restaurants that aren't lazy-loaded yet). Arrow keys + Enter work;
 * plain typing still live-filters the list/map exactly as before.
 */
export default function SearchBar({
  value, onChange, count, allData, cityOptions,
  onPickRestaurant, onPickArea,
}) {
  const [open, setOpen] = useState(false);
  const [active, setActive] = useState(-1);
  const [apiItems, setApiItems] = useState([]);
  const boxRef = useRef(null);
  const inputRef = useRef(null);
  // Tracks a touch/pointer gesture so we can tell a tap from a scroll drag
  const dragRef = useRef({ x: 0, y: 0, moved: false });
  const q = value.trim().toLowerCase();

  // Instant local matches, prefix matches first
  const localItems = useMemo(() => {
    if (q.length < 2) return [];
    const starts = [];
    const contains = [];
    for (const r of allData) {
      const name = (r.n || '').toLowerCase();
      if (name.startsWith(q)) starts.push(r);
      else if (name.includes(q) || (r.a || '').toLowerCase().includes(q)) contains.push(r);
      if (starts.length >= MAX_LOCAL) break;
    }
    return [...starts, ...contains].slice(0, MAX_LOCAL);
  }, [q, allData]);

  const areaItems = useMemo(() => {
    if (q.length < 2) return [];
    return cityOptions
      .filter(o => o.city !== 'all' || o.metro)
      .filter(o => o.label.toLowerCase().startsWith(q))
      .slice(0, 2);
  }, [q, cityOptions]);

  // Full-database matches (debounced); drop ones already shown locally
  useEffect(() => {
    if (q.length < 2) { setApiItems([]); return; }
    let cancelled = false;
    const timer = setTimeout(() => {
      fetchSearchFromApi(q, 10).then(rows => {
        if (!cancelled) setApiItems(rows);
      });
    }, 300);
    return () => { cancelled = true; clearTimeout(timer); };
  }, [q]);

  const items = useMemo(() => {
    const localIds = new Set(localItems.map(r => r.i));
    const extra = apiItems.filter(r => r.i && !localIds.has(r.i)).slice(0, MAX_API_EXTRA);
    return [
      ...areaItems.map(o => ({ kind: 'area', key: `a:${o.label}`, area: o })),
      ...localItems.map(r => ({ kind: 'restaurant', key: r.i, r })),
      ...extra.map(r => ({ kind: 'restaurant', key: r.i, r })),
    ];
  }, [areaItems, localItems, apiItems]);

  const showDropdown = open && q.length >= 2 && items.length > 0;

  const pick = (item) => {
    setOpen(false);
    setActive(-1);
    if (item.kind === 'area') {
      onChange('');
      onPickArea(item.area);
    } else {
      onChange(item.r.n || '');
      onPickRestaurant(item.r);
    }
  };

  const onKeyDown = (e) => {
    if (!showDropdown) {
      if (e.key === 'Escape') onChange('');
      return;
    }
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      setActive(prev => (prev + 1) % items.length);
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      setActive(prev => (prev <= 0 ? items.length - 1 : prev - 1));
    } else if (e.key === 'Enter' && active >= 0) {
      e.preventDefault();
      pick(items[active]);
    } else if (e.key === 'Escape') {
      setOpen(false);
      setActive(-1);
    }
  };

  // Close when clicking anywhere outside
  useEffect(() => {
    const onDown = (e) => {
      if (boxRef.current && !boxRef.current.contains(e.target)) setOpen(false);
    };
    document.addEventListener('pointerdown', onDown);
    return () => document.removeEventListener('pointerdown', onDown);
  }, []);

  return (
    <div ref={boxRef} className="relative z-30 flex-1 max-w-xl">
      <svg className="absolute left-3.5 top-[22px] -translate-y-1/2 w-4 h-4 text-slate-400 pointer-events-none" viewBox="0 0 20 20" fill="none" aria-hidden="true">
        <circle cx="8.5" cy="8.5" r="5.5" stroke="currentColor" strokeWidth="1.8"/>
        <path d="M13 13l4 4" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round"/>
      </svg>
      <input
        ref={inputRef}
        type="search"
        placeholder="Search restaurants, addresses…"
        role="combobox"
        aria-expanded={showDropdown}
        aria-autocomplete="list"
        className="w-full h-11 bg-white/90 dark:bg-slate-800/90 backdrop-blur text-slate-900 dark:text-white pl-10 pr-20 rounded-2xl shadow-lg ring-1 ring-slate-900/5 dark:ring-white/10 focus:ring-2 focus:ring-brand-500 outline-none text-[15px] placeholder:text-slate-400"
        value={value}
        onChange={e => { onChange(e.target.value); setOpen(true); setActive(-1); }}
        onFocus={() => setOpen(true)}
        onKeyDown={onKeyDown}
      />
      {value ? (
        <button
          onClick={() => { onChange(''); setOpen(false); }}
          className="absolute right-3 top-[22px] -translate-y-1/2 p-1 text-slate-400 hover:text-slate-600 dark:hover:text-slate-200"
          aria-label="Clear search"
        >
          <svg viewBox="0 0 20 20" width="16" height="16" fill="none">
            <path d="M6 6l8 8M14 6l-8 8" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round"/>
          </svg>
        </button>
      ) : (
        <span className="absolute right-3.5 top-[22px] -translate-y-1/2 text-[11px] font-bold text-slate-400 tabular-nums whitespace-nowrap">
          {count.toLocaleString()}
        </span>
      )}

      {showDropdown && (
        <div
          className="absolute z-50 top-[50px] inset-x-0 rounded-2xl bg-white dark:bg-slate-900 shadow-2xl ring-1 ring-slate-900/10 dark:ring-white/10 overflow-y-auto overscroll-contain max-h-[60vh] animate-fade-in"
          role="listbox"
          // Scrolling the results on mobile drops the keyboard so the list
          // has room; the input keeps its value and reopens on the next focus.
          onScroll={() => inputRef.current?.blur()}
        >
          {items.map((item, idx) => (
            <button
              key={item.key}
              role="option"
              aria-selected={idx === active}
              // Track the gesture so a scroll drag doesn't count as a tap:
              // record the start point, flag any real movement, and only
              // commit the pick on release when the finger stayed put.
              onPointerDown={e => { dragRef.current = { x: e.clientX, y: e.clientY, moved: false }; }}
              onPointerMove={e => {
                const d = dragRef.current;
                if (Math.abs(e.clientX - d.x) > 8 || Math.abs(e.clientY - d.y) > 8) d.moved = true;
              }}
              onPointerUp={e => {
                if (dragRef.current.moved) return;
                e.preventDefault();
                pick(item);
              }}
              onMouseEnter={() => setActive(idx)}
              className={`w-full text-left px-3.5 py-2.5 flex items-center gap-3 transition-colors ${
                idx === active ? 'bg-slate-50 dark:bg-slate-800/70' : ''
              }`}
            >
              {item.kind === 'area' ? (
                <>
                  <span className="w-8 h-8 shrink-0 rounded-lg bg-slate-100 dark:bg-slate-800 flex items-center justify-center text-slate-500 dark:text-slate-400">
                    <svg className="w-4 h-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                      <path d="M20 10c0 6-8 12-8 12s-8-6-8-12a8 8 0 0 1 16 0Z"/><circle cx="12" cy="10" r="3"/>
                    </svg>
                  </span>
                  <span className="flex-1 min-w-0">
                    <span className="block font-bold text-[14px] truncate">{item.area.label}</span>
                    <span className="block text-[12px] text-slate-400">
                      {item.area.metro ? 'Metro area' : 'City'} — show all restaurants
                    </span>
                  </span>
                </>
              ) : (
                <>
                  <span className={`w-8 h-8 shrink-0 rounded-lg flex items-center justify-center font-black text-sm ${gradeMeta(item.r.vg).tile}`}>
                    {item.r.vg || '–'}
                  </span>
                  <span className="flex-1 min-w-0">
                    <span className="block font-bold text-[14px] truncate">{item.r.n}</span>
                    <span className="block text-[12px] text-slate-400 truncate">
                      {item.r.a}{item.r.c ? ` · ${item.r.c}` : ''}
                    </span>
                  </span>
                </>
              )}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
