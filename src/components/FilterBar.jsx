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

function Chip({ active, onClick, children, activeClass, label }) {
  return (
    <button
      onClick={onClick}
      aria-pressed={active}
      aria-label={label}
      title={label}
      className={`shrink-0 px-3.5 h-10 md:h-9 rounded-full text-[13px] font-bold transition-colors whitespace-nowrap ${
        active
          ? activeClass || 'bg-slate-900 text-white dark:bg-white dark:text-slate-900'
          : 'bg-white dark:bg-slate-800 text-slate-800 dark:text-slate-100 ring-1 ring-slate-900/5 dark:ring-white/10 hover:bg-slate-50 dark:hover:bg-slate-700 shadow-md shadow-slate-900/10'
      }`}
    >
      {children}
    </button>
  );
}

/**
 * Filter rail floating over the map, in the order people reach for it: low
 * scores near me, the four grades, then the location picker (one dropdown:
 * the city list grew past what a chip row can hold) and the issue chips.
 * On a phone the grade chips are just their letter on the grade's color, so
 * all four fit on screen next to "Low scores near me"; the dropdown used to
 * sit there and pushed every grade chip off the right edge. The rail still
 * scrolls sideways, and its right edge fades to say so.
 */
export default function FilterBar({
  cityOptions,
  cityFilter, setCityFilter,
  metroFilter, setMetroFilter,
  placeFilter, setPlaceFilter,
  gradeFilter, toggleGrade,
  infractionFilter, toggleInfraction,
  nearMe = false, toggleNearMe,
}) {
  // Encode the three-way selection (place XOR metro XOR city) as one select
  // value: 'p:<place id>', 'm:<metro>', 'c:<city>' or 'all'.
  const selectValue = placeFilter ? `p:${placeFilter}`
    : metroFilter ? `m:${metroFilter}`
    : cityFilter !== 'all' ? `c:${cityFilter}` : 'all';
  const handleSelect = (value) => {
    if (value.startsWith('p:')) {
      setCityFilter('all');
      setMetroFilter(null);
      setPlaceFilter?.(value.slice(2));
    } else if (value.startsWith('m:')) {
      setCityFilter('all');
      setMetroFilter(value.slice(2));
      setPlaceFilter?.(null);
    } else {
      setCityFilter(value === 'all' ? 'all' : value.slice(2));
      setMetroFilter(null);
      setPlaceFilter?.(null);
    }
  };

  const metros = cityOptions.filter(o => o.metro);
  const places = cityOptions.filter(o => o.place);
  const cities = cityOptions.filter(o => !o.metro && !o.place && o.city !== 'all');
  const locationActive = selectValue !== 'all';

  return (
    <div className="flex items-center gap-2 overflow-x-auto no-scrollbar rail-fade px-3 pr-8 md:px-4 pb-1 pointer-events-auto">
      {/* Near me: recent C/F grades within half a mile of the user */}
      {toggleNearMe && (
        <Chip active={nearMe} onClick={toggleNearMe} activeClass="bg-red-500 text-white shadow-md">
          <span className="inline-flex items-center gap-1.5">
            <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
              <path d="M12 21s-7-6.2-7-11.5A7 7 0 0 1 19 9.5C19 14.8 12 21 12 21z" />
              <circle cx="12" cy="9.5" r="2.5" />
            </svg>
            Low scores near me
          </span>
        </Chip>
      )}
      {/* Grade chips, one per letter */}
      {['A', 'B', 'C', 'F'].map(grade => {
        const meta = gradeMeta(grade);
        const active = gradeFilter.includes(grade);
        return (
          <Chip
            key={grade}
            active={active}
            onClick={() => toggleGrade(grade)}
            activeClass={`${meta.tile} shadow-md`}
            label={`Grade ${grade}: ${meta.label}`}
          >
            <span className="inline-flex items-center gap-1.5">
              <span
                className={`w-5 h-5 -ml-1.5 rounded-md flex items-center justify-center font-display font-extrabold text-[12px] ${meta.tile} ${
                  active ? 'ring-1 ring-black/10' : ''
                }`}
                aria-hidden="true"
              >
                {grade}
              </span>
              <span className="hidden md:inline">{meta.label}</span>
            </span>
          </Chip>
        );
      })}

      <div className="shrink-0 w-px h-5 bg-slate-300/70 dark:bg-slate-600/70 mx-0.5" />

      {/* Location dropdown */}
      <div className="relative shrink-0">
        <select
          value={selectValue}
          onChange={e => handleSelect(e.target.value)}
          aria-label="Filter by city, metro area or place"
          className={`appearance-none h-10 md:h-9 pl-3.5 pr-8 rounded-full text-[13px] font-bold outline-none cursor-pointer transition-colors max-w-[46vw] md:max-w-none truncate ${
            locationActive
              ? 'bg-slate-900 text-white dark:bg-white dark:text-slate-900 shadow-md'
              : 'bg-white dark:bg-slate-800 text-slate-800 dark:text-slate-100 ring-1 ring-slate-900/5 dark:ring-white/10 shadow-md shadow-slate-900/10'
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
          {/* Named areas that aren't a city in the data (beach towns on
              30A): matched by coordinates, offered even before any of
              their records have loaded. */}
          {places.length > 0 && (
            <optgroup label="Places">
              {places.map(o => (
                <option key={`p:${o.place}`} value={`p:${o.place}`}>{o.label}</option>
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
            locationActive ? 'text-white dark:text-slate-900' : 'text-slate-400'
          }`}
          viewBox="0 0 20 20" fill="none" aria-hidden="true"
        >
          <path d="M5 8l5 5 5-5" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"/>
        </svg>
      </div>

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
