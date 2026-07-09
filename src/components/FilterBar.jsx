import React from 'react';
import { gradeMeta } from './GradeBadge.jsx';

// Filterable issue categories. `docs` (paperwork violations) is deliberately
// not offered as a filter — nobody avoids a restaurant over paperwork — but
// it still shows on cards and in the detail modal.
const INFRACTION_META = {
  pests: { label: 'Pests' },
  temp: { label: 'Temperature' },
  hygiene: { label: 'Hygiene' },
  equipment: { label: 'Equipment' },
};

function Chip({ active, onClick, children, activeClass }) {
  return (
    <button
      onClick={onClick}
      className={`shrink-0 px-3.5 h-9 rounded-full text-[13px] font-semibold transition-colors whitespace-nowrap ${
        active
          ? activeClass || 'bg-slate-900 text-white dark:bg-white dark:text-slate-900'
          : 'bg-white/90 dark:bg-slate-800/90 text-slate-700 dark:text-slate-200 ring-1 ring-slate-200 dark:ring-slate-700 hover:bg-white dark:hover:bg-slate-800 shadow-sm backdrop-blur'
      }`}
    >
      {children}
    </button>
  );
}

/**
 * Filter rail floating over the map. The location picker is a single dropdown
 * (the city list grew past what a chip row can hold), which keeps the grade
 * and issue chips permanently visible beside it.
 */
export default function FilterBar({
  cityOptions,
  cityFilter, setCityFilter,
  metroFilter, setMetroFilter,
  gradeFilter, toggleGrade,
  infractionFilter, toggleInfraction,
}) {
  // Encode the two-field selection (city XOR metro) as one select value.
  const selectValue = metroFilter ? `m:${metroFilter}` : cityFilter !== 'all' ? `c:${cityFilter}` : 'all';
  const handleSelect = (value) => {
    if (value.startsWith('m:')) {
      setCityFilter('all');
      setMetroFilter(value.slice(2));
    } else {
      setCityFilter(value === 'all' ? 'all' : value.slice(2));
      setMetroFilter(null);
    }
  };

  const metros = cityOptions.filter(o => o.metro);
  const cities = cityOptions.filter(o => !o.metro && o.city !== 'all');
  const locationActive = selectValue !== 'all';

  return (
    <div className="flex items-center gap-2 overflow-x-auto no-scrollbar px-3 md:px-4 pb-1 pointer-events-auto">
      {/* Location dropdown */}
      <div className="relative shrink-0">
        <select
          value={selectValue}
          onChange={e => handleSelect(e.target.value)}
          aria-label="Filter by city or metro area"
          className={`appearance-none h-9 pl-3.5 pr-8 rounded-full text-[13px] font-semibold outline-none cursor-pointer transition-colors max-w-[46vw] md:max-w-none truncate ${
            locationActive
              ? 'bg-brand-600 text-white shadow-md'
              : 'bg-white/90 dark:bg-slate-800/90 text-slate-700 dark:text-slate-200 ring-1 ring-slate-200 dark:ring-slate-700 shadow-sm backdrop-blur'
          }`}
        >
          <option value="all">All cities</option>
          {metros.length > 0 && (
            <optgroup label="Metro areas">
              {metros.map(o => (
                <option key={`m:${o.metro}`} value={`m:${o.metro}`}>{o.label}</option>
              ))}
            </optgroup>
          )}
          <optgroup label="Cities">
            {cities.map(o => (
              <option key={`c:${o.city}`} value={`c:${o.city}`}>{o.label}</option>
            ))}
          </optgroup>
        </select>
        <svg
          className={`absolute right-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 pointer-events-none ${
            locationActive ? 'text-white' : 'text-slate-400'
          }`}
          viewBox="0 0 20 20" fill="none" aria-hidden="true"
        >
          <path d="M5 8l5 5 5-5" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"/>
        </svg>
      </div>

      <div className="shrink-0 w-px h-5 bg-slate-300/70 dark:bg-slate-600/70 mx-0.5" />

      {/* Grade chips (A / B+C / F) */}
      {[
        { grade: 'A', label: 'Safe' },
        { grade: 'B', label: 'Caution' },
        { grade: 'F', label: 'Avoid' },
      ].map(({ grade, label }) => {
        const meta = gradeMeta(grade);
        const active = gradeFilter.includes(grade);
        return (
          <Chip
            key={grade}
            active={active}
            onClick={() => toggleGrade(grade)}
            activeClass={`${meta.tile} shadow-md`}
          >
            <span className="inline-flex items-center gap-1.5">
              {!active && (
                <span className="w-2 h-2 rounded-full" style={{ background: meta.dot }} />
              )}
              {label}
            </span>
          </Chip>
        );
      })}

      <div className="shrink-0 w-px h-5 bg-slate-300/70 dark:bg-slate-600/70 mx-0.5" />

      {/* Issue chips */}
      {Object.entries(INFRACTION_META).map(([cat, meta]) => (
        <Chip
          key={cat}
          active={infractionFilter.includes(cat)}
          onClick={() => toggleInfraction(cat)}
        >
          {meta.label}
        </Chip>
      ))}
    </div>
  );
}
