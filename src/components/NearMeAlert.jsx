import React from 'react';
import GradeBadge from './GradeBadge.jsx';

const SUMMARY_LIMIT = 2;

/**
 * Heads-up card when the user appears to be in (or beside) a restaurant with
 * a recent C or F. Worded as "may be" on purpose: GPS and geocoded pins are
 * both approximate, and the alert must never read as an accusation.
 */
export default function NearMeAlert({ alert, formatDate, onOpen, onDismiss }) {
  const { r, nextDoor, others } = alert;
  const failed = r.vg === 'F';
  const lead = nextDoor ? 'Next door' : 'You may be at';
  // The most serious findings first, plain wording only (lite records from
  // the API carry no summaries; the report has them).
  const items = (r.vs || [])
    .map(v => (typeof v === 'string' ? { text: v } : v))
    .filter(v => v && v.text && !v.text.endsWith('...'));
  const critical = items.filter(v => v.severity === 'CRITICAL');
  const summaries = (critical.length ? critical : items).slice(0, SUMMARY_LIMIT).map(v => v.text);

  return (
    <div
      role="alert"
      className="absolute z-40 left-3 right-3 md:left-auto md:right-4 md:w-96 top-[calc(env(safe-area-inset-top)_+_108px)] animate-slide-up"
    >
      <div className={`rounded-2xl backdrop-blur shadow-2xl ring-1 overflow-hidden ${
        failed
          ? 'bg-red-50/95 ring-red-200 dark:bg-red-950/90 dark:ring-red-900'
          : 'bg-amber-50/95 ring-amber-200 dark:bg-amber-950/90 dark:ring-amber-900'
      }`}>
        <div className="p-3.5 flex items-start gap-3">
          <GradeBadge grade={r.vg} size="sm" />
          <div className="flex-1 min-w-0">
            <p className={`text-[11px] font-black uppercase tracking-wide ${
              failed ? 'text-red-700 dark:text-red-300' : 'text-amber-700 dark:text-amber-300'
            }`}>
              Heads up · {lead}
            </p>
            <h3 className="font-extrabold text-[15px] leading-snug truncate">{r.n}</h3>
            <p className="text-[13px] text-slate-600 dark:text-slate-300 mt-0.5">
              Graded {r.vg} on {formatDate(r.d)}
            </p>
            {summaries.length > 0 && (
              <p className="text-[13px] font-semibold text-slate-700 dark:text-slate-200 mt-0.5">
                {summaries.join(' · ')}
              </p>
            )}
            {others > 0 && (
              <p className="text-[12px] text-slate-500 dark:text-slate-400 mt-0.5">
                {others} more low {others === 1 ? 'score' : 'scores'} right here
              </p>
            )}
          </div>
        </div>
        <div className="px-3.5 pb-3.5 flex items-center gap-2">
          <button
            onClick={onOpen}
            className="flex-1 h-9 rounded-xl bg-slate-900 hover:bg-slate-800 dark:bg-white dark:hover:bg-slate-100 text-white dark:text-slate-900 text-[13px] font-bold transition-colors"
          >
            See the report
          </button>
          <button
            onClick={onDismiss}
            className="h-9 px-3.5 rounded-xl ring-1 ring-slate-300 dark:ring-slate-600 text-[13px] font-bold text-slate-600 dark:text-slate-300 hover:bg-white/60 dark:hover:bg-slate-800 transition-colors"
          >
            Dismiss
          </button>
        </div>
      </div>
    </div>
  );
}
