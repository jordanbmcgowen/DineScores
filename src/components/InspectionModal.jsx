import React, { useState, useEffect } from 'react';
import { fetchInspectionHistory } from '../firebase.js';
import { fetchHistoryFromApi } from '../api.js';
import GradeBadge, { gradeMeta } from './GradeBadge.jsx';

const INFRACTION_META = {
  pests: { label: 'Pests detected', chip: 'bg-red-50 text-red-700 ring-red-600/20 dark:bg-red-500/10 dark:text-red-400 dark:ring-red-400/20' },
  temp: { label: 'Temperature issues', chip: 'bg-sky-50 text-sky-700 ring-sky-600/20 dark:bg-sky-500/10 dark:text-sky-400 dark:ring-sky-400/20' },
  hygiene: { label: 'Hygiene issues', chip: 'bg-violet-50 text-violet-700 ring-violet-600/20 dark:bg-violet-500/10 dark:text-violet-400 dark:ring-violet-400/20' },
  equipment: { label: 'Equipment issues', chip: 'bg-slate-100 text-slate-600 ring-slate-500/20 dark:bg-slate-500/10 dark:text-slate-400 dark:ring-slate-400/20' },
  docs: { label: 'Paperwork issues', chip: 'bg-amber-50 text-amber-700 ring-amber-600/20 dark:bg-amber-500/10 dark:text-amber-400 dark:ring-amber-400/20' },
};

const SEVERITY_STYLE = {
  CRITICAL: {
    card: 'bg-red-50/70 dark:bg-red-500/5 ring-red-200 dark:ring-red-500/20',
    label: 'text-red-600 dark:text-red-400',
  },
  WARNING: {
    card: 'bg-amber-50/70 dark:bg-amber-500/5 ring-amber-200 dark:ring-amber-500/20',
    label: 'text-amber-600 dark:text-amber-400',
  },
  INFO: {
    card: 'bg-slate-50 dark:bg-slate-800/60 ring-slate-200 dark:ring-slate-700',
    label: 'text-slate-500 dark:text-slate-400',
  },
};

export default function InspectionModal({ restaurant: r, onClose, formatDate }) {
  const [history, setHistory] = useState([]);
  const [loadingHistory, setLoadingHistory] = useState(false);
  const [expandedSummary, setExpandedSummary] = useState(null);

  // Inspection history sources, best first: the D1-backed API (full history
  // with violations), then Firestore (legacy), then the embedded score
  // history (r.h = [[date, score], ...]) shipped in data.js.
  useEffect(() => {
    if (!r) return;
    setExpandedSummary(null);
    setLoadingHistory(true);
    const embeddedHistory = () =>
      (Array.isArray(r.h) ? r.h : [])
        .filter(entry => Array.isArray(entry) && entry[0])
        .map(([date, rs]) => ({ id: `${r.i}_${date}`, date, rs: rs || 0, type: '' }));
    (async () => {
      let h = await fetchHistoryFromApi(r.i);
      if (!h || h.length === 0) {
        h = await fetchInspectionHistory(r.i).catch(() => []);
      }
      setHistory(h && h.length > 0 ? h : embeddedHistory());
      setLoadingHistory(false);
    })();
  }, [r]);

  // Close on Escape
  useEffect(() => {
    const onKey = e => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  if (!r) return null;

  const summaries = r.vs || [];
  const infractions = r.inf || [];
  const grade = r.vg;
  const meta = gradeMeta(grade);
  const score = r.ws ?? r.rs ?? 0;
  const mapsUrl = `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(
    `${r.n} ${r.a} ${r.c}`)}`;

  return (
    <div className="fixed inset-0 z-[9999] flex items-end md:items-center justify-center">
      {/* Backdrop */}
      <div
        className="absolute inset-0 bg-slate-950/50 backdrop-blur-sm animate-fade-in"
        onClick={onClose}
      />

      {/* Panel */}
      <div
        role="dialog"
        aria-modal="true"
        aria-label={`Inspection details for ${r.n}`}
        className="relative bg-white dark:bg-slate-900 w-full md:max-w-2xl max-h-[92dvh] md:max-h-[86vh] rounded-t-3xl md:rounded-3xl shadow-2xl flex flex-col overflow-hidden animate-slide-up md:animate-scale-in"
      >
        {/* Header */}
        <div className="px-5 pt-5 pb-4 md:px-6 flex items-start gap-4 shrink-0 border-b border-slate-100 dark:border-slate-800">
          <GradeBadge grade={grade} score={score} />
          <div className="flex-1 min-w-0 pr-8">
            <div className={`inline-flex items-center px-2 py-0.5 rounded-md text-[11px] font-bold uppercase tracking-wide ring-1 ${meta.soft}`}>
              {meta.label}
            </div>
            <h2 className="text-lg md:text-xl font-extrabold leading-tight tracking-tight mt-1 line-clamp-2">
              {r.n}
            </h2>
            <a
              href={mapsUrl}
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-1 mt-1 text-[13px] text-slate-500 dark:text-slate-400 hover:text-brand-600 dark:hover:text-brand-500 truncate max-w-full"
            >
              <svg className="w-3.5 h-3.5 shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M20 10c0 6-8 12-8 12s-8-6-8-12a8 8 0 0 1 16 0Z"/><circle cx="12" cy="10" r="3"/>
              </svg>
              <span className="truncate">{r.a}, {r.c}{r.z ? ` ${r.z}` : ''}</span>
            </a>
          </div>
          <button
            onClick={onClose}
            className="absolute top-4 right-4 w-8 h-8 flex items-center justify-center rounded-full bg-slate-100 dark:bg-slate-800 text-slate-500 dark:text-slate-400 hover:bg-slate-200 dark:hover:bg-slate-700 transition-colors"
            aria-label="Close"
          >
            <svg className="w-4 h-4" viewBox="0 0 20 20" fill="none">
              <path d="M6 6l8 8M14 6l-8 8" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round"/>
            </svg>
          </button>
        </div>

        {/* Body */}
        <div className="overflow-y-auto flex-1 px-5 md:px-6 py-5 space-y-6">
          {/* Stat tiles */}
          <div className="grid grid-cols-3 gap-2.5">
            <StatTile label="Last inspected" value={formatDate(r.d)} />
            <StatTile label="Weighted score" value={score} hint="60/30/10 of last 3" />
            <StatTile label="Inspections" value={r.ic || history.length || 1} />
          </div>

          {/* Infraction chips */}
          {infractions.length > 0 && (
            <div className="flex flex-wrap gap-2">
              {infractions.map(cat => {
                const m = INFRACTION_META[cat];
                if (!m) return null;
                return (
                  <span key={cat} className={`px-2.5 py-1 rounded-lg ring-1 text-xs font-bold ${m.chip}`}>
                    {m.label}
                  </span>
                );
              })}
            </div>
          )}

          {/* Violation summaries */}
          <section>
            <h3 className="text-sm font-extrabold uppercase tracking-wide text-slate-400 dark:text-slate-500 mb-2.5">
              Latest inspection findings
            </h3>
            <div className="space-y-2">
              {summaries.length > 0 ? (
                summaries.map((item, idx) => {
                  const style = SEVERITY_STYLE[item.severity] || SEVERITY_STYLE.INFO;
                  const isExpanded = expandedSummary === idx;
                  return (
                    <button
                      key={idx}
                      onClick={() => setExpandedSummary(prev => (prev === idx ? null : idx))}
                      className={`flex flex-col text-left rounded-2xl ring-1 transition-all w-full ${style.card}`}
                    >
                      <div className="flex items-start gap-3 p-3.5 w-full">
                        <div className="flex-1 min-w-0">
                          <span className={`block text-[10px] font-black uppercase tracking-wider mb-0.5 ${style.label}`}>
                            {item.category}
                          </span>
                          <p className="text-sm font-semibold leading-snug">{item.text}</p>
                        </div>
                        <svg className={`w-4 h-4 mt-1 shrink-0 text-slate-400 transition-transform ${isExpanded ? 'rotate-180' : ''}`} viewBox="0 0 20 20" fill="none">
                          <path d="M5 8l5 5 5-5" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round"/>
                        </svg>
                      </div>
                      {isExpanded && (
                        <div className="px-3.5 pb-3.5 pt-0 w-full animate-fade-in">
                          <div className="p-3 bg-white/60 dark:bg-black/20 rounded-xl text-xs font-mono text-slate-600 dark:text-slate-300 leading-relaxed break-words whitespace-pre-wrap">
                            <span className="font-bold text-slate-400 uppercase tracking-wider text-[10px] block mb-1">Official report text</span>
                            {item.verbatim}
                          </div>
                        </div>
                      )}
                    </button>
                  );
                })
              ) : (
                <div className="p-6 text-center rounded-2xl bg-slate-50 dark:bg-slate-800/60 ring-1 ring-slate-200 dark:ring-slate-700">
                  <p className="font-semibold text-sm text-slate-600 dark:text-slate-300">
                    No violation details on file for the latest inspection.
                  </p>
                  <p className="text-xs mt-1 text-slate-400">
                    Some health departments publish scores without itemized violations.
                  </p>
                </div>
              )}
            </div>
          </section>

          {/* Inspection history */}
          <section>
            <h3 className="text-sm font-extrabold uppercase tracking-wide text-slate-400 dark:text-slate-500 mb-2.5">
              Inspection history
            </h3>
            {loadingHistory && history.length === 0 && (
              <div className="flex items-center gap-2 text-slate-400 text-sm py-3">
                <div className="w-4 h-4 border-2 border-slate-300 border-t-brand-600 rounded-full animate-spin" />
                Loading history…
              </div>
            )}
            <div className="space-y-1.5">
              {history.map((insp, idx) => {
                const inspScore = insp.rs || 0;
                const g = inspScore >= 90 ? 'A' : inspScore >= 80 ? 'B' : inspScore >= 70 ? 'C' : 'F';
                const gm = gradeMeta(g);
                return (
                  <div
                    key={insp.id || idx}
                    className="flex items-center justify-between px-3.5 py-2.5 rounded-xl bg-slate-50 dark:bg-slate-800/60 ring-1 ring-slate-100 dark:ring-slate-800"
                  >
                    <div className="flex items-center gap-3 min-w-0">
                      <span className={`w-9 h-9 shrink-0 rounded-lg flex items-center justify-center text-xs font-black tabular-nums ${gm.tile}`}>
                        {inspScore}
                      </span>
                      <div className="min-w-0">
                        <div className="font-semibold text-sm tabular-nums">{formatDate(insp.date)}</div>
                        {insp.type && (
                          <div className="text-[11px] text-slate-400 truncate">{insp.type}</div>
                        )}
                      </div>
                    </div>
                    {idx === 0 && (
                      <span className="text-[10px] font-bold bg-slate-200 dark:bg-slate-700 text-slate-600 dark:text-slate-300 px-2 py-1 rounded-full shrink-0">
                        Latest
                      </span>
                    )}
                  </div>
                );
              })}
              {!loadingHistory && history.length === 0 && (
                <div className="text-center text-slate-400 py-3 text-sm">No history available.</div>
              )}
            </div>
          </section>
        </div>

        {/* Source footer */}
        <div className="px-5 md:px-6 py-3 border-t border-slate-100 dark:border-slate-800 flex items-center justify-between gap-3 shrink-0 pb-safe">
          <span className="text-[11px] text-slate-400 truncate">
            Data: {r.src || 'official health department records'}
          </span>
          {r.url && (
            <a
              href={r.url}
              target="_blank"
              rel="noopener noreferrer"
              className="text-[12px] font-bold text-brand-600 dark:text-brand-500 hover:underline whitespace-nowrap"
            >
              Official record ↗
            </a>
          )}
        </div>
      </div>
    </div>
  );
}

function StatTile({ label, value, hint }) {
  return (
    <div className="rounded-2xl bg-slate-50 dark:bg-slate-800/60 ring-1 ring-slate-100 dark:ring-slate-800 px-3 py-2.5 min-w-0">
      <div className="text-[10px] font-bold text-slate-400 uppercase tracking-wider truncate" title={hint}>
        {label}
      </div>
      <div className="font-extrabold text-[15px] mt-0.5 tabular-nums truncate">{value}</div>
    </div>
  );
}
