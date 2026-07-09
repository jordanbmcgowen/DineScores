import React from 'react';
import GradeBadge, { gradeMeta } from './GradeBadge.jsx';

const TAG_STYLE = {
  pests: 'bg-red-50 text-red-700 dark:bg-red-500/10 dark:text-red-400',
  temp: 'bg-sky-50 text-sky-700 dark:bg-sky-500/10 dark:text-sky-400',
  hygiene: 'bg-violet-50 text-violet-700 dark:bg-violet-500/10 dark:text-violet-400',
  equipment: 'bg-slate-100 text-slate-600 dark:bg-slate-500/10 dark:text-slate-400',
};
const TAG_LABEL = { pests: 'Pests', temp: 'Temp', hygiene: 'Hygiene', equipment: 'Equipment' };

function distanceMiles(userPos, r) {
  const dLat = (r.lt - userPos.lat) * 69.0;
  const dLng = (r.ln - userPos.lng) * 69.0 * Math.cos((userPos.lat * Math.PI) / 180);
  return Math.sqrt(dLat * dLat + dLng * dLng);
}

/**
 * Compact preview that floats over the live map when a marker (or list card,
 * or search suggestion) is selected — the map keeps working underneath.
 * The full report is one more tap away.
 */
export default function PreviewCard({ restaurant: r, userPos, formatDate, onClose, onFullReport }) {
  const meta = gradeMeta(r.vg);
  const dist = userPos && r.lt && r.ln ? distanceMiles(userPos, r) : null;
  const mapsUrl = `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(
    `${r.n} ${r.a} ${r.c}`)}`;

  return (
    <div className="absolute z-30 bottom-[110px] left-3 right-3 md:bottom-6 md:right-auto md:left-[calc(50%+210px)] md:-translate-x-1/2 md:w-[400px] animate-slide-up pb-safe">
      <div className="rounded-2xl bg-white/95 dark:bg-slate-900/95 backdrop-blur shadow-2xl ring-1 ring-slate-900/10 dark:ring-white/10 overflow-hidden">
        <div className="p-3.5 flex items-start gap-3">
          <GradeBadge grade={r.vg} size="sm" />
          <div className="flex-1 min-w-0">
            <div className="flex items-start justify-between gap-2">
              <h3 className="font-extrabold text-[15px] leading-snug truncate">{r.n}</h3>
              <button
                onClick={onClose}
                aria-label="Close preview"
                className="shrink-0 w-6 h-6 -mt-0.5 -mr-0.5 flex items-center justify-center rounded-full text-slate-400 hover:text-slate-600 hover:bg-slate-100 dark:hover:bg-slate-800 dark:hover:text-slate-200"
              >
                <svg viewBox="0 0 20 20" width="13" height="13" fill="none">
                  <path d="M6 6l8 8M14 6l-8 8" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round"/>
                </svg>
              </button>
            </div>
            <p className="text-[13px] text-slate-500 dark:text-slate-400 truncate mt-0.5">
              {r.a}{r.c ? ` · ${r.c}` : ''}
            </p>
            <div className="flex items-center gap-1.5 mt-1.5 flex-wrap">
              <span className={`text-[10px] px-1.5 py-0.5 rounded-md font-black uppercase tracking-wide ${meta.soft} ring-1`}>
                {meta.label}
              </span>
              {dist != null && (
                <span className="text-[11px] font-semibold text-slate-400 tabular-nums">
                  {dist < 10 ? dist.toFixed(1) : Math.round(dist)} mi
                </span>
              )}
              <span className="text-[11px] text-slate-400 tabular-nums">
                Inspected {formatDate(r.d)}
              </span>
              {(r.inf || []).filter(cat => TAG_STYLE[cat]).slice(0, 2).map(cat => (
                <span key={cat} className={`text-[10px] px-1.5 py-0.5 rounded-md font-bold ${TAG_STYLE[cat]}`}>
                  {TAG_LABEL[cat]}
                </span>
              ))}
            </div>
          </div>
        </div>
        <div className="px-3.5 pb-3.5 flex items-center gap-2">
          <button
            onClick={onFullReport}
            className="flex-1 h-9 rounded-xl bg-brand-600 hover:bg-brand-700 text-white text-[13px] font-bold transition-colors"
          >
            Full report
          </button>
          <a
            href={mapsUrl}
            target="_blank"
            rel="noopener noreferrer"
            aria-label={`Directions to ${r.n}`}
            className="h-9 px-3.5 rounded-xl ring-1 ring-slate-200 dark:ring-slate-700 text-[13px] font-bold text-slate-600 dark:text-slate-300 hover:bg-slate-50 dark:hover:bg-slate-800 flex items-center gap-1.5 transition-colors"
          >
            <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M9 18l6-6-6-6"/>
            </svg>
            Directions
          </a>
        </div>
      </div>
    </div>
  );
}
