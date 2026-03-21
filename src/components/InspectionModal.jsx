import React, { useState, useEffect } from 'react';
import { fetchInspectionHistory } from '../firebase.js';
import GradeBadge from './GradeBadge.jsx';

const INFRACTION_META = {
  pests: { label: 'Pests Detected', color: 'text-red-600 bg-red-50 dark:bg-red-900/30 border-red-200 dark:border-red-800' },
  temp: { label: 'Temp Issues', color: 'text-blue-600 bg-blue-50 dark:bg-blue-900/30 border-blue-200 dark:border-blue-800' },
  hygiene: { label: 'Hygiene Issues', color: 'text-teal-600 bg-teal-50 dark:bg-teal-900/30 border-teal-200 dark:border-teal-800' },
  equipment: { label: 'Equipment Issues', color: 'text-slate-600 bg-slate-50 dark:bg-slate-700 border-slate-200 dark:border-slate-600' },
  docs: { label: 'Doc Issues', color: 'text-amber-600 bg-amber-50 dark:bg-amber-900/30 border-amber-200 dark:border-amber-800' },
};

const SEVERITY_STYLE = {
  CRITICAL: { card: 'bg-red-50 dark:bg-red-900/20 border-red-200 dark:border-red-800 text-red-900 dark:text-red-200', label: 'text-red-600 dark:text-red-400' },
  WARNING: { card: 'bg-orange-50 dark:bg-orange-900/20 border-orange-200 dark:border-orange-800 text-orange-900 dark:text-orange-200', label: 'text-orange-600 dark:text-orange-400' },
  INFO: { card: 'bg-slate-50 dark:bg-slate-700/50 border-slate-200 dark:border-slate-600 text-slate-700 dark:text-slate-300', label: 'text-slate-500 dark:text-slate-400' },
};

export default function InspectionModal({ restaurant: r, onClose, formatDate }) {
  const [history, setHistory] = useState([]);
  const [loadingHistory, setLoadingHistory] = useState(false);
  const [expandedSummary, setExpandedSummary] = useState(null);

  // Fetch inspection history from Firestore subcollection
  useEffect(() => {
    if (!r) return;
    setExpandedSummary(null);
    setLoadingHistory(true);
    fetchInspectionHistory(r.i).then(h => {
      setHistory(h);
      setLoadingHistory(false);
    }).catch(() => setLoadingHistory(false));
  }, [r]);

  if (!r) return null;

  const summaries = r.vs || [];
  const infractions = r.inf || [];
  const grade = r.vg || 'C';
  const score = r.ws || r.rs || 0;

  return (
    <div className="fixed inset-0 z-[9999] flex items-end md:items-center justify-center">
      {/* Backdrop */}
      <div
        className="absolute inset-0 bg-black/40 dark:bg-black/60 backdrop-blur-sm"
        onClick={onClose}
      />

      {/* Modal */}
      <div className="relative bg-white dark:bg-slate-800 w-full md:max-w-2xl h-[85vh] md:h-auto md:max-h-[90vh] md:rounded-2xl rounded-t-2xl shadow-2xl flex flex-col overflow-hidden animate-slide-up ring-1 ring-black/5">
        {/* Header */}
        <div className="bg-slate-50 dark:bg-slate-700/50 border-b border-slate-100 dark:border-slate-700 p-5 flex justify-between items-start shrink-0">
          <div className="flex-1 pr-10">
            <h2 className="text-xl md:text-2xl font-black text-slate-800 dark:text-white leading-tight tracking-tight line-clamp-2">
              {r.n}
            </h2>
            <div className="flex items-center gap-2 mt-2 text-slate-500 dark:text-slate-400 text-sm">
              <svg className="w-4 h-4 text-cyan-600 shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M20 10c0 6-8 12-8 12s-8-6-8-12a8 8 0 0 1 16 0Z"/><circle cx="12" cy="10" r="3"/>
              </svg>
              <span className="truncate">{r.a}, {r.c} {r.z}</span>
            </div>
          </div>
          <button
            onClick={onClose}
            className="absolute top-4 right-4 p-2 bg-white dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-full hover:bg-slate-100 dark:hover:bg-slate-600 transition shadow-sm text-slate-500"
          >
            <svg className="w-5 h-5" viewBox="0 0 20 20" fill="none">
              <path d="M6 6l8 8M14 6l-8 8" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"/>
            </svg>
          </button>
        </div>

        {/* Body */}
        <div className="overflow-y-auto flex-1 p-5 space-y-6 bg-white dark:bg-slate-800">
          {/* Grade card + details */}
          <div className="flex flex-col md:flex-row gap-5">
            {/* Grade */}
            <div className="flex-shrink-0 flex items-center justify-between md:flex-col md:items-center bg-gradient-to-br from-slate-50 to-white dark:from-slate-700/50 dark:to-slate-800 border border-slate-100 dark:border-slate-700 rounded-2xl p-4 md:p-6 shadow-sm min-w-[180px]">
              <div className="flex flex-col items-center">
                <div className="mb-2 text-xs font-bold text-slate-400 uppercase tracking-widest">Current Status</div>
                <GradeBadge grade={grade} score={score} />
              </div>
              <div className="md:mt-4 md:text-center ml-4 md:ml-0 md:pt-4 md:border-t border-slate-200 dark:border-slate-600 w-full">
                {grade === 'A' && <p className="text-green-700 dark:text-green-400 font-bold text-sm">Pass / Safe</p>}
                {(grade === 'B' || grade === 'C') && <p className="text-yellow-700 dark:text-yellow-400 font-bold text-sm">Issues Found</p>}
                {grade === 'F' && <p className="text-red-700 dark:text-red-400 font-bold text-sm">Failed / Unsafe</p>}
                <div className="mt-1 text-[10px] text-slate-400 font-medium">Weighted Metric</div>
              </div>
            </div>

            {/* Details grid */}
            <div className="flex-1 flex flex-col gap-3">
              <div className="grid grid-cols-2 gap-3">
                <div className="bg-slate-50 dark:bg-slate-700/50 p-3 rounded-xl border border-slate-100 dark:border-slate-700">
                  <div className="text-[10px] font-bold text-slate-400 uppercase tracking-wider mb-1">Date</div>
                  <div className="font-semibold text-slate-800 dark:text-white text-sm">{formatDate(r.d)}</div>
                </div>
                <div className="bg-slate-50 dark:bg-slate-700/50 p-3 rounded-xl border border-slate-100 dark:border-slate-700">
                  <div className="text-[10px] font-bold text-slate-400 uppercase tracking-wider mb-1">Type</div>
                  <div className="font-semibold text-slate-800 dark:text-white text-sm">{r.it || 'Routine'}</div>
                </div>
              </div>

              {/* Infraction tags */}
              {infractions.length > 0 && (
                <div className="bg-slate-50 dark:bg-slate-700/50 p-3 rounded-xl border border-slate-100 dark:border-slate-700">
                  <div className="text-[10px] font-bold text-slate-400 uppercase tracking-wider mb-2">Detected Risks</div>
                  <div className="flex flex-wrap gap-2">
                    {infractions.map(cat => {
                      const meta = INFRACTION_META[cat];
                      if (!meta) return null;
                      return (
                        <span key={cat} className={`flex items-center gap-1 px-2.5 py-1 rounded-lg border text-xs font-bold shadow-sm ${meta.color}`}>
                          {meta.label}
                        </span>
                      );
                    })}
                  </div>
                </div>
              )}
            </div>
          </div>

          {/* Violation summaries */}
          <div className="border-t border-slate-100 dark:border-slate-700 pt-5">
            <h3 className="text-lg font-bold text-slate-800 dark:text-white mb-3">Quick Summary</h3>

            <div className="space-y-2">
              {summaries.length > 0 ? (
                summaries.map((item, idx) => {
                  const style = SEVERITY_STYLE[item.severity] || SEVERITY_STYLE.INFO;
                  const isExpanded = expandedSummary === idx;

                  return (
                    <button
                      key={idx}
                      onClick={() => setExpandedSummary(prev => prev === idx ? null : idx)}
                      className={`flex flex-col text-left rounded-xl border transition-all w-full ${style.card} hover:shadow-sm`}
                    >
                      <div className="flex items-start gap-3 p-3 w-full">
                        <div className="flex-1">
                          <span className={`block text-[10px] font-black uppercase tracking-wider mb-0.5 ${style.label}`}>
                            {item.category}
                          </span>
                          <p className="text-sm font-bold leading-tight">{item.text}</p>
                        </div>
                        <svg className={`w-4 h-4 mt-1 text-slate-400 transition-transform ${isExpanded ? 'rotate-180' : ''}`} viewBox="0 0 20 20" fill="none">
                          <path d="M5 8l5 5 5-5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
                        </svg>
                      </div>

                      {isExpanded && (
                        <div className="px-3 pb-3 pt-0 animate-fade-in w-full">
                          <div className="p-3 bg-white/50 dark:bg-black/10 rounded-lg border border-black/5 text-xs font-mono text-slate-600 dark:text-slate-300 leading-relaxed break-words whitespace-pre-wrap">
                            <span className="font-bold text-slate-400 uppercase tracking-wider text-[10px] block mb-1">Official Report Text:</span>
                            {item.verbatim}
                          </div>
                        </div>
                      )}
                    </button>
                  );
                })
              ) : (
                <div className="p-6 text-center text-slate-500 flex flex-col items-center bg-slate-50 dark:bg-slate-700/50 rounded-2xl border border-slate-200 dark:border-slate-600 border-dashed">
                  <svg className="w-8 h-8 text-green-500 mb-2 opacity-80" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                    <path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/>
                  </svg>
                  <p className="font-medium text-sm">No notable violations extracted.</p>
                  <p className="text-xs mt-1">Restaurant appears to meet standards.</p>
                </div>
              )}
            </div>
            {summaries.length > 0 && (
              <p className="text-center text-[10px] text-slate-400 mt-2 font-medium">Click items to view official report text.</p>
            )}
          </div>

          {/* Inspection History */}
          <div className="border-t border-slate-100 dark:border-slate-700 pt-5">
            <h3 className="text-lg font-bold text-slate-800 dark:text-white mb-3 flex items-center gap-2">
              <svg className="w-5 h-5 text-slate-400" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/>
              </svg>
              Inspection History
            </h3>

            <div className="space-y-2">
              {loadingHistory && history.length === 0 && (
                <div className="flex items-center justify-center py-4 text-slate-400 gap-2">
                  <svg className="w-4 h-4 animate-spin" viewBox="0 0 24 24" fill="none">
                    <circle cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="2" opacity="0.3"/>
                    <path d="M12 2a10 10 0 0 1 10 10" stroke="currentColor" strokeWidth="2" strokeLinecap="round"/>
                  </svg>
                  <span className="text-sm">Loading history...</span>
                </div>
              )}

              {history.map((insp, idx) => {
                const inspScore = insp.rs || 0;
                const inspGrade = inspScore >= 90 ? 'A' : inspScore >= 80 ? 'B' : inspScore >= 70 ? 'C' : 'F';
                return (
                  <div
                    key={insp.id}
                    className="flex items-center justify-between p-3 rounded-xl border border-slate-100 dark:border-slate-700 bg-slate-50 dark:bg-slate-700/50"
                  >
                    <div className="flex items-center gap-3">
                      <GradeBadge grade={inspGrade} score={inspScore} size="sm" />
                      <div>
                        <div className="font-bold text-slate-800 dark:text-white text-sm">{formatDate(insp.date)}</div>
                        <div className="text-[10px] text-slate-400 uppercase tracking-wider">{insp.type}</div>
                      </div>
                    </div>
                    {idx === 0 && (
                      <span className="text-[10px] font-bold bg-slate-200 dark:bg-slate-600 text-slate-600 dark:text-slate-300 px-2 py-1 rounded-full">Latest</span>
                    )}
                  </div>
                );
              })}

              {!loadingHistory && history.length === 0 && (
                <div className="text-center text-slate-400 py-4 text-sm">No history available.</div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
