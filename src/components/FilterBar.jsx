import React from 'react';

const INFRACTION_ICONS = {
  pests: { label: 'Pests', color: 'red' },
  temp: { label: 'Temp', color: 'blue' },
  hygiene: { label: 'Hygiene', color: 'teal' },
  equipment: { label: 'Equip', color: 'slate' },
  docs: { label: 'Docs', color: 'amber' },
};

export default function FilterBar({
  cityFilter, setCityFilter,
  metroFilter, setMetroFilter,
  riskFilter, setRiskFilter,
  gradeFilter, toggleGrade,
  infractionFilter, toggleInfraction,
}) {
  function handleCity(city, metro = null) {
    setCityFilter(city);
    setMetroFilter(metro);
  }

  return (
    <div className="bg-white dark:bg-slate-800 border-b border-slate-200 dark:border-slate-700 px-4 py-2 z-40 overflow-x-auto no-scrollbar">
      <div className="flex items-center gap-2 flex-wrap max-w-7xl mx-auto">
        {/* City chips */}
        <div className="flex items-center gap-1">
          {[
            { label: 'All Cities', city: 'all', metro: null },
            { label: 'Chicago', city: 'Chicago', metro: null },
            { label: 'DFW', city: 'all', metro: 'DFW' },
            { label: 'New York', city: 'New York', metro: null },
            { label: 'San Francisco', city: 'San Francisco', metro: null },
          ].map(opt => {
            const active = opt.metro
              ? metroFilter === opt.metro
              : !metroFilter && cityFilter === opt.city;
            return (
              <button
                key={opt.label}
                onClick={() => handleCity(opt.city, opt.metro)}
                className={`px-3 py-1.5 rounded-full text-xs font-semibold transition whitespace-nowrap ${
                  active
                    ? 'bg-cyan-600 text-white'
                    : 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300 hover:bg-slate-200 dark:hover:bg-slate-600'
                }`}
              >
                {opt.label}
              </button>
            );
          })}
        </div>

        <div className="w-px h-5 bg-slate-200 dark:bg-slate-600 mx-1" />

        {/* Risk chips */}
        <div className="flex items-center gap-1">
          {[
            { label: 'All Scores', value: 'all', activeClass: 'bg-slate-600 text-white' },
            { label: 'Safe 90+', value: 'safe', activeClass: 'bg-green-600 text-white' },
            { label: 'Moderate', value: 'moderate', activeClass: 'bg-yellow-500 text-white' },
            { label: 'At Risk <70', value: 'risk', activeClass: 'bg-red-600 text-white' },
          ].map(opt => (
            <button
              key={opt.value}
              onClick={() => setRiskFilter(opt.value)}
              className={`px-3 py-1.5 rounded-full text-xs font-semibold transition whitespace-nowrap ${
                riskFilter === opt.value
                  ? opt.activeClass
                  : 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300 hover:bg-slate-200'
              }`}
            >
              {opt.label}
            </button>
          ))}
        </div>

        <div className="w-px h-5 bg-slate-200 dark:bg-slate-600 mx-1" />

        {/* Grade filter (Safe/Evaluate/Avoid) */}
        <div className="flex items-center gap-1">
          <button
            onClick={() => toggleGrade('A')}
            className={`px-3 py-1.5 rounded-full text-xs font-semibold transition whitespace-nowrap ${
              gradeFilter.includes('A')
                ? 'bg-green-600 text-white'
                : 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300 hover:bg-slate-200'
            }`}
          >
            👍 Safe
          </button>
          <button
            onClick={() => toggleGrade('B')}
            className={`px-3 py-1.5 rounded-full text-xs font-semibold transition whitespace-nowrap ${
              gradeFilter.includes('B')
                ? 'bg-yellow-500 text-white'
                : 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300 hover:bg-slate-200'
            }`}
          >
            ✋ Evaluate
          </button>
          <button
            onClick={() => toggleGrade('F')}
            className={`px-3 py-1.5 rounded-full text-xs font-semibold transition whitespace-nowrap ${
              gradeFilter.includes('F')
                ? 'bg-red-600 text-white'
                : 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300 hover:bg-slate-200'
            }`}
          >
            👎 Avoid
          </button>
        </div>

        <div className="w-px h-5 bg-slate-200 dark:bg-slate-600 mx-1" />

        {/* Infraction filter icons */}
        <div className="flex items-center gap-1">
          {Object.entries(INFRACTION_ICONS).map(([cat, meta]) => {
            const active = infractionFilter.includes(cat);
            const colors = {
              red: active ? 'bg-red-100 dark:bg-red-900/40 text-red-600 border-red-300' : '',
              blue: active ? 'bg-blue-100 dark:bg-blue-900/40 text-blue-600 border-blue-300' : '',
              teal: active ? 'bg-teal-100 dark:bg-teal-900/40 text-teal-600 border-teal-300' : '',
              slate: active ? 'bg-slate-200 dark:bg-slate-600 text-slate-700 border-slate-400' : '',
              amber: active ? 'bg-amber-100 dark:bg-amber-900/40 text-amber-600 border-amber-300' : '',
            };
            return (
              <button
                key={cat}
                onClick={() => toggleInfraction(cat)}
                title={meta.label}
                className={`px-2 py-1.5 rounded-lg text-[10px] font-bold border transition ${
                  active
                    ? colors[meta.color]
                    : 'bg-slate-100 dark:bg-slate-700 text-slate-500 dark:text-slate-400 border-slate-200 dark:border-slate-600 hover:bg-slate-200'
                }`}
              >
                {meta.label}
              </button>
            );
          })}
        </div>
      </div>
    </div>
  );
}
