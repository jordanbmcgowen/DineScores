import React, { useState, useEffect } from 'react';
import { fetchHistoryFromApi, fetchRestaurantDetail } from '../api.js';
import { gradeMeta } from './GradeBadge.jsx';
import { findingTitle, severityLabel } from '../findingText.js';
import { sourceLag } from '../sourceLag.js';
import { track, restaurantParams } from '../analytics.js';

// Issue-type tiles on the card, keyed by the record's `inf` categories.
const ISSUE_TILES = {
  pests: {
    label: 'Pests', hint: 'Signs of pests',
    tile: 'bg-red-50 dark:bg-red-500/10', ink: 'text-red-700 dark:text-red-300',
    icon: <><ellipse cx="12" cy="13" rx="4.5" ry="6" /><path d="M12 7V4M9 9 5 7M15 9l4-2M7.5 13H4M16.5 13H20M9 17l-3 3M15 17l3 3" /></>,
  },
  temp: {
    label: 'Temperature', hint: 'Held too warm or cold',
    tile: 'bg-sky-50 dark:bg-sky-500/10', ink: 'text-sky-700 dark:text-sky-300',
    icon: <path d="M14 14.76V3.5a2.5 2.5 0 0 0-5 0v11.26a4.5 4.5 0 1 0 5 0z" />,
  },
  hygiene: {
    label: 'Hygiene', hint: 'Handwashing, cleaning',
    tile: 'bg-violet-50 dark:bg-violet-500/10', ink: 'text-violet-700 dark:text-violet-300',
    icon: <path d="M12 2.7 6.3 8.4a8 8 0 1 0 11.4 0z" />,
  },
  equipment: {
    label: 'Equipment', hint: 'Repairs or upkeep',
    tile: 'bg-slate-100 dark:bg-slate-800', ink: 'text-slate-600 dark:text-slate-300',
    icon: <path d="M14.7 6.3a4 4 0 0 0-5.4 5.4L3 18l3 3 6.3-6.3a4 4 0 0 0 5.4-5.4l-2.5 2.5-2.5-.5-.5-2.5z" />,
  },
  docs: {
    label: 'Paperwork', hint: 'Permits, certificates',
    tile: 'bg-amber-50 dark:bg-amber-500/10', ink: 'text-amber-700 dark:text-amber-300',
    icon: <><rect x="5" y="3" width="14" height="18" rx="2" /><path d="M9 8h6M9 12h6M9 16h4" /></>,
  },
};

const SEVERITY_PILL = {
  CRITICAL: 'bg-red-50 text-red-700 dark:bg-red-500/15 dark:text-red-300',
  WARNING: 'bg-orange-50 text-orange-700 dark:bg-orange-500/15 dark:text-orange-300',
  INFO: 'bg-slate-100 text-slate-600 dark:bg-slate-800 dark:text-slate-300',
};

// Share/close float over the grade header and, once scrolled, over the
// panels, so they carry their own dark glass on every grade.
const FLOAT_BUTTON = 'bg-black/25 text-white hover:bg-black/40 backdrop-blur-md';

const gradeForScore = s => (s >= 90 ? 'A' : s >= 80 ? 'B' : s >= 70 ? 'C' : 'F');

/**
 * Turn the raw inspection history into a one-glance story: a chronological
 * strip of grade chips plus a plain-English verdict. Pure presentation —
 * grading itself is unchanged.
 */
function trendInfo(history) {
  const grades = history.map(h => gradeForScore(h.rs || 0)); // newest first
  const n = grades.length;
  const aCount = grades.filter(g => g === 'A').length;
  const rank = { A: 3, B: 2, C: 1, F: 0 };
  const latest = grades[0];
  const prior = grades.slice(1);

  if (grades.every(g => g === 'A')) {
    return { label: 'Consistently clean', tone: 'A', sentence: `Rated A on all ${n} inspections on file.` };
  }
  if (latest === 'F') {
    return { label: 'Failed latest inspection', tone: 'F', sentence: `${aCount} of ${n} inspections rated A.` };
  }
  if (latest === 'A' && prior.length > 0 && prior[0] !== 'A') {
    return { label: 'Improving', tone: 'A', sentence: 'Latest inspection passed after earlier issues.' };
  }
  if (prior.length > 0 && rank[latest] < rank[prior[0]]) {
    return { label: 'Declining', tone: latest, sentence: `Latest inspection rated ${latest}, down from ${prior[0]}.` };
  }
  if (latest === 'A') {
    return { label: 'Mostly clean', tone: 'A', sentence: `${aCount} of ${n} inspections rated A, including the latest.` };
  }
  return { label: 'Mixed record', tone: latest, sentence: `${aCount} of ${n} inspections rated A.` };
}

export default function InspectionModal({ restaurant: r, onClose, formatDate, onUpgrade, sourceAreas }) {
  const formatShort = d => {
    const t = d ? new Date(`${String(d).slice(0, 10)}T12:00:00`) : null;
    return t && !isNaN(t) ? t.toLocaleDateString('en-US', { month: 'short', year: '2-digit' }) : '';
  };
  const lag = sourceLag(r, sourceAreas);
  const [history, setHistory] = useState([]);
  const [loadingHistory, setLoadingHistory] = useState(false);
  const [expandedSummary, setExpandedSummary] = useState(null);
  const [detail, setDetail] = useState(null);
  const [loadingDetail, setLoadingDetail] = useState(false);
  const [copied, setCopied] = useState(false);

  // Lite records (bulk-loaded from the API) omit the `vs` key entirely.
  // Fetch the full record before showing findings, so we never display
  // "no violations on file" for a restaurant whose details simply haven't
  // been transferred yet.
  const isLite = r && r.vs === undefined;
  useEffect(() => {
    setDetail(null);
    if (!r || !isLite) return;
    setLoadingDetail(true);
    let cancelled = false;
    fetchRestaurantDetail(r.i).then(full => {
      if (cancelled) return;
      setLoadingDetail(false);
      if (full) {
        setDetail(full);
        if (onUpgrade) onUpgrade(full);
      }
    }).catch(() => { if (!cancelled) setLoadingDetail(false); });
    return () => { cancelled = true; };
  }, [r, isLite, onUpgrade]);

  // Inspection history sources, best first: the D1-backed API (full history
  // with violations), then the embedded score history
  // (r.h = [[date, score], ...]) shipped in data.js.
  useEffect(() => {
    if (!r) return;
    setExpandedSummary(null);
    setLoadingHistory(true);
    const embeddedHistory = () =>
      (Array.isArray(r.h) ? r.h : [])
        .filter(entry => Array.isArray(entry) && entry[0])
        .map(([date, rs]) => ({ id: `${r.i}_${date}`, date, rs: rs || 0, type: '' }));
    (async () => {
      const h = await fetchHistoryFromApi(r.i);
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

  const summaries = (detail ? detail.vs : r.vs) || [];
  const summariesReady = !isLite || !!detail;
  const infractions = (detail ? detail.inf : r.inf) || [];
  const grade = r.vg;
  const meta = gradeMeta(grade);
  const officialUrl = detail?.url ?? r.url;
  const mapsUrl = `https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(
    `${r.n} ${r.a} ${r.c}`)}`;

  const share = () => {
    const url = `${window.location.origin}/#r/${r.i}`;
    track('share', { ...restaurantParams(r), method: navigator.share ? 'share_sheet' : 'copy_link' });
    if (navigator.share) {
      navigator.share({ title: `${r.n} — DineScores`, url }).catch(() => {});
    } else if (navigator.clipboard) {
      navigator.clipboard.writeText(url).then(() => {
        setCopied(true);
        setTimeout(() => setCopied(false), 1800);
      }).catch(() => {});
    }
  };

  const serious = summaries.filter(v => v.severity === 'CRITICAL').length;
  const plural = (n, w) => `${n} ${w}${n === 1 ? '' : 's'}`;
  const headline = !summariesReady
    ? null
    : summaries.length === 0
      ? 'No violations listed'
      : serious > 0
        ? `Last visit: ${plural(serious, 'serious issue')}`
        : `Last visit: ${plural(summaries.length, 'issue')}, none serious`;
  const tiles = infractions.filter(cat => ISSUE_TILES[cat]);
  const inspections = r.ic || history.length || 1;

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
        className="relative bg-slate-100 dark:bg-slate-950 w-full md:max-w-xl max-h-[92dvh] md:max-h-[88vh] rounded-t-[28px] md:rounded-[28px] shadow-2xl flex flex-col overflow-hidden animate-slide-up md:animate-scale-in"
      >
        {/* Share / close float over the grade header */}
        <div className="absolute top-4 right-4 z-10 flex items-center gap-2">
          <button
            onClick={share}
            className={`h-9 px-3 flex items-center gap-1.5 rounded-full transition-colors text-[12px] font-bold ${FLOAT_BUTTON}`}
            aria-label={`Share a link to ${r.n}`}
          >
            <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <circle cx="18" cy="5" r="3"/><circle cx="6" cy="12" r="3"/><circle cx="18" cy="19" r="3"/>
              <path d="m8.6 13.5 6.8 4M15.4 6.5l-6.8 4"/>
            </svg>
            {copied ? 'Copied!' : 'Share'}
          </button>
          <button
            onClick={onClose}
            className={`w-9 h-9 flex items-center justify-center rounded-full transition-colors ${FLOAT_BUTTON}`}
            aria-label="Close"
          >
            <svg className="w-4 h-4" viewBox="0 0 20 20" fill="none">
              <path d="M6 6l8 8M14 6l-8 8" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round"/>
            </svg>
          </button>
        </div>

        <div className="overflow-y-auto flex-1 overscroll-contain">
          {/* Grade header: the grade owns the top of the card */}
          <header className={`relative overflow-hidden px-5 md:px-6 pt-5 pb-14 ${meta.hero}`}>
            <span
              aria-hidden="true"
              className="pointer-events-none select-none absolute -right-6 -top-10 font-display font-extrabold text-[220px] leading-none opacity-[0.16]"
            >
              {grade || '–'}
            </span>
            <div className="relative inline-flex items-center gap-2 rounded-full bg-white/25 dark:bg-black/15 pl-1 pr-3 py-1 text-[13px] font-bold">
              <span className="w-6 h-6 rounded-full bg-white text-slate-900 flex items-center justify-center font-display font-extrabold text-[13px]">
                {grade || '–'}
              </span>
              {meta.label}
            </div>
            <h2 className="relative font-display font-extrabold text-[30px] md:text-[34px] leading-[1.05] tracking-tight mt-3 pr-16 line-clamp-3">
              {r.n}
            </h2>
            <a
              href={mapsUrl}
              target="_blank"
              rel="noopener noreferrer"
              onClick={() => track('directions', { ...restaurantParams(r), via: 'address' })}
              className="relative block mt-2 text-[14px] font-medium opacity-90 hover:opacity-100 hover:underline truncate"
            >
              {r.a}, {r.c}{r.z ? ` ${r.z}` : ''}
            </a>
            <p className="relative mt-0.5 text-[13px] font-medium opacity-80 tabular-nums">
              Inspected {formatDate(r.d)} · {plural(inspections, 'inspection')} on file
            </p>
          </header>

          <div className="relative -mt-9 px-3 md:px-4 pb-4 space-y-3">
            {/* The health department posts late: say so, or an old date reads
                as "not inspected in months" */}
            {lag && (
              <p className="text-[12.5px] leading-snug text-amber-900 dark:text-amber-200 bg-amber-50 dark:bg-amber-500/15 ring-1 ring-amber-200 dark:ring-amber-500/25 rounded-2xl px-4 py-3 shadow-sm">
                This area's health department has published nothing newer than {formatDate(lag.newest)}, so a more recent visit may not show here yet.
              </p>
            )}

            {/* Summary of the last visit, issues grouped by type */}
            <Panel>
              {headline ? (
                <h3 className="font-display font-bold text-[19px] tracking-tight">{headline}</h3>
              ) : (
                <div className="flex items-center gap-2 text-slate-400 text-sm">
                  <div className="w-4 h-4 border-2 border-slate-300 border-t-slate-600 rounded-full animate-spin" />
                  Loading the last visit…
                </div>
              )}
              {tiles.length > 0 && (
                <div className="grid grid-cols-2 gap-2.5 mt-3">
                  {tiles.map(cat => {
                    const t = ISSUE_TILES[cat];
                    return (
                      <div key={cat} className={`rounded-2xl px-3 py-2.5 flex items-center gap-2.5 min-w-0 ${t.tile}`}>
                        <svg className={`w-5 h-5 shrink-0 ${t.ink}`} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
                          {t.icon}
                        </svg>
                        <div className="min-w-0">
                          <div className="font-bold text-[14px] leading-tight text-slate-900 dark:text-slate-100">{t.label}</div>
                          <div className={`text-[11.5px] font-medium leading-tight mt-0.5 ${t.ink}`}>{t.hint}</div>
                        </div>
                      </div>
                    );
                  })}
                </div>
              )}
            </Panel>

            {/* Track record */}
            {history.length >= 2 && <TrackRecord history={history} formatShort={formatShort} />}

            {/* What the inspector found */}
            <Panel>
              <h3 className="font-display font-bold text-[19px] tracking-tight">What they found</h3>
              <div className="mt-2 -mx-1">
                {summaries.length > 0 ? (
                  summaries.map((item, idx) => {
                    const sev = SEVERITY_PILL[item.severity] || SEVERITY_PILL.INFO;
                    const isExpanded = expandedSummary === idx;
                    return (
                      <button
                        key={idx}
                        onClick={() => {
                          if (expandedSummary !== idx) track('finding_expand', restaurantParams(r));
                          setExpandedSummary(prev => (prev === idx ? null : idx));
                        }}
                        aria-expanded={isExpanded}
                        className="w-full text-left px-1 py-2.5 border-t first:border-t-0 border-slate-100 dark:border-slate-800"
                      >
                        <div className="flex items-start gap-3">
                          <span className={`mt-0.5 shrink-0 text-[10.5px] font-bold uppercase tracking-wide px-2 py-1 rounded-lg ${sev}`}>
                            {severityLabel(item.severity)}
                          </span>
                          <p className="flex-1 min-w-0 text-[14.5px] font-semibold leading-snug">{findingTitle(item)}</p>
                          <svg className={`w-4 h-4 mt-1 shrink-0 text-slate-400 transition-transform ${isExpanded ? 'rotate-180' : ''}`} viewBox="0 0 20 20" fill="none">
                            <path d="M5 8l5 5 5-5" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round"/>
                          </svg>
                        </div>
                        {isExpanded && (
                          <div className="mt-2 p-3 bg-slate-50 dark:bg-slate-800/70 rounded-xl text-[12.5px] text-slate-600 dark:text-slate-300 leading-relaxed break-words whitespace-pre-wrap animate-fade-in">
                            <span className="font-bold text-slate-400 uppercase tracking-wider text-[10px] block mb-1">Inspector's report</span>
                            {item.verbatim || item.text}
                          </div>
                        )}
                      </button>
                    );
                  })
                ) : loadingDetail ? (
                  <div className="flex items-center gap-2 text-slate-400 text-sm py-3 px-1">
                    <div className="w-4 h-4 border-2 border-slate-300 border-t-slate-600 rounded-full animate-spin" />
                    Loading findings…
                  </div>
                ) : !summariesReady ? (
                  <div className="py-4 px-1">
                    <p className="font-semibold text-sm text-slate-600 dark:text-slate-300">
                      Couldn't load the inspection findings.
                    </p>
                    <p className="text-xs mt-1 text-slate-500 dark:text-slate-400">
                      Check your connection and reopen this restaurant.
                    </p>
                  </div>
                ) : (
                  <div className="py-4 px-1">
                    <p className="font-semibold text-sm text-slate-600 dark:text-slate-300">
                      No violation details on file for the latest inspection.
                    </p>
                    <p className="text-xs mt-1 text-slate-500 dark:text-slate-400">
                      Some health departments publish scores without itemized violations.
                    </p>
                  </div>
                )}
              </div>
            </Panel>

            {/* Inspection history */}
            <Panel>
              <h3 className="font-display font-bold text-[19px] tracking-tight">Every inspection</h3>
              {loadingHistory && history.length === 0 && (
                <div className="flex items-center gap-2 text-slate-400 text-sm py-3">
                  <div className="w-4 h-4 border-2 border-slate-300 border-t-slate-600 rounded-full animate-spin" />
                  Loading history…
                </div>
              )}
              <div className="mt-2 -mx-1">
                {history.map((insp, idx) => {
                  const g = gradeForScore(insp.rs || 0);
                  const gm = gradeMeta(g);
                  // Per-inspection official record when the API provides one;
                  // otherwise the restaurant-level source link still applies.
                  const recordUrl = insp.url || officialUrl;
                  return (
                    <div
                      key={insp.id || idx}
                      className="flex items-center justify-between gap-2 px-1 py-2 border-t first:border-t-0 border-slate-100 dark:border-slate-800"
                    >
                      <div className="flex items-center gap-3 min-w-0">
                        <span className={`w-9 h-9 shrink-0 rounded-xl flex items-center justify-center font-display font-extrabold text-base ${gm.tile}`}>
                          {g}
                        </span>
                        <div className="min-w-0">
                          <div className="font-semibold text-sm tabular-nums">
                            {formatDate(insp.date)}
                            {idx === 0 && (
                              <span className="ml-2 align-middle text-[10px] font-bold bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-300 px-2 py-0.5 rounded-full">
                                Latest
                              </span>
                            )}
                          </div>
                          {insp.type && (
                            <div className="text-[11.5px] text-slate-500 dark:text-slate-400 truncate">{insp.type}</div>
                          )}
                        </div>
                      </div>
                      {recordUrl && (
                        <a
                          href={recordUrl}
                          target="_blank"
                          rel="noopener noreferrer"
                          onClick={() => track('official_record', { ...restaurantParams(r), via: 'history' })}
                          aria-label={`Official record for the ${formatDate(insp.date)} inspection`}
                          className="w-9 h-9 shrink-0 flex items-center justify-center rounded-xl text-slate-400 hover:text-slate-700 hover:bg-slate-100 dark:hover:text-white dark:hover:bg-slate-800 transition-colors"
                        >
                          <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                            <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/>
                            <path d="M15 3h6v6"/><path d="M10 14 21 3"/>
                          </svg>
                        </a>
                      )}
                    </div>
                  );
                })}
                {!loadingHistory && history.length === 0 && (
                  <div className="text-slate-400 py-3 px-1 text-sm">No history available.</div>
                )}
              </div>
            </Panel>

            <p className="px-2 text-[11.5px] text-slate-500 dark:text-slate-400">
              Data: {(detail?.src ?? r.src) || 'official health department records'}
            </p>
          </div>
        </div>

        {/* Actions */}
        <div className="px-3 md:px-4 pt-3 pb-3 flex items-center gap-2.5 shrink-0 bg-slate-100/95 dark:bg-slate-950/95 border-t border-slate-200/70 dark:border-slate-800 pb-safe">
          {officialUrl && (
            <a
              href={officialUrl}
              target="_blank"
              rel="noopener noreferrer"
              onClick={() => track('official_record', { ...restaurantParams(r), via: 'button' })}
              className="flex-1 h-12 rounded-2xl bg-white dark:bg-slate-900 ring-1 ring-slate-200 dark:ring-slate-800 flex items-center justify-center text-[14px] font-bold text-slate-800 dark:text-slate-100 hover:bg-slate-50 dark:hover:bg-slate-800 transition-colors"
            >
              Official record ↗
            </a>
          )}
          <a
            href={mapsUrl}
            target="_blank"
            rel="noopener noreferrer"
            onClick={() => track('directions', { ...restaurantParams(r), via: 'button' })}
            className="flex-1 h-12 rounded-2xl bg-slate-900 dark:bg-white text-white dark:text-slate-900 flex items-center justify-center text-[14px] font-bold hover:bg-slate-800 dark:hover:bg-slate-100 transition-colors"
          >
            Directions
          </a>
        </div>
      </div>
    </div>
  );
}

function Panel({ children }) {
  return (
    <section className="rounded-3xl bg-white dark:bg-slate-900 ring-1 ring-slate-900/5 dark:ring-white/5 shadow-sm p-4 md:p-5">
      {children}
    </section>
  );
}

const BAR_HEIGHT = { A: 72, B: 56, C: 42, F: 28 };

function TrackRecord({ history, formatShort }) {
  const { label, sentence } = trendInfo(history);
  const chron = [...history].reverse().slice(-8); // oldest → newest
  return (
    <Panel>
      <div className="flex items-baseline justify-between gap-3">
        <h3 className="font-display font-bold text-[19px] tracking-tight">Track record</h3>
        <span className="text-[12px] font-bold text-slate-500 dark:text-slate-400">{label}</span>
      </div>
      <div className="flex items-end gap-2 mt-3 h-[96px]" aria-label="Grade history, oldest to newest">
        {chron.map((h, i) => {
          const g = gradeForScore(h.rs || 0);
          return (
            <div key={h.id || i} className="flex-1 min-w-0 flex flex-col items-center gap-1.5">
              <div
                className={`w-full rounded-xl flex items-start justify-center pt-1 font-display font-extrabold text-[14px] ${gradeMeta(g).tile}`}
                style={{ height: BAR_HEIGHT[g] }}
                title={h.date}
              >
                {g}
              </div>
              <span className="text-[10.5px] font-semibold text-slate-500 dark:text-slate-400 tabular-nums truncate max-w-full">
                {formatShort(h.date)}
              </span>
            </div>
          );
        })}
      </div>
      <p className="text-[12.5px] text-slate-500 dark:text-slate-400 mt-2">{sentence}</p>
    </Panel>
  );
}
