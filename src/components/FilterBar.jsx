import React from 'react';
import { gradeMeta } from './GradeBadge.jsx';

const INFRACTION_META = {
  pests: { label: 'Pests' },
  temp: { label: 'Temperature' },
  hygiene: { label: 'Hygiene' },
  equipment: { label: 'Equipment' },
  docs: { label: 'Paperwork' },
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
 * Horizontally scrollable filter rail floating over the map:
 * city chips (derived from live data), grade segmented chips, issue chips.
 */
export default function FilterBar({
  cityOptions,
  cityFilter, setCityFilter,
  metroFilter, setMetroFilter,
  gradeFilter, toggleGrade,
  infractionFilter, toggleInfraction,
}) {
  function handleCity(city, metro = null) {
    setCityFilter(city);
    setMetroFilter(metro);
  }

  return (
    <div className="flex items-center gap-2 overflow-x-auto no-scrollbar px-3 md:px-4 pb-1 pointer-events-auto">
      {/* City chips */}
      {cityOptions.map(opt => {
        const active = opt.metro
          ? metroFilter === opt.metro
          : !metroFilter && cityFilter === opt.city;
        return (
          <Chip
            key={opt.label}
            active={active}
            onClick={() => handleCity(opt.city, opt.metro)}
            activeClass="bg-brand-600 text-white shadow-md"
          >
            {opt.label}
          </Chip>
        );
      })}

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
