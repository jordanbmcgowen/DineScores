import React from 'react';

/**
 * Grade display vocabulary, used consistently across map, cards, and modal.
 * Grades are STATUS colors and are never shown by color alone — the letter
 * (and usually a label) always accompanies the color.
 */
export const GRADE_META = {
  A: {
    label: 'Safe',
    tile: 'bg-emerald-500 text-white',
    soft: 'bg-emerald-50 text-emerald-700 ring-emerald-600/20 dark:bg-emerald-500/10 dark:text-emerald-400 dark:ring-emerald-400/20',
    dot: '#10b981',
  },
  // B and C used to share one amber and one label, so an 89 looked exactly
  // like a 70. Each letter now has its own color and word. The yellow tile
  // carries dark text (white on yellow is unreadable).
  B: {
    label: 'Good',
    tile: 'bg-yellow-400 text-slate-900',
    soft: 'bg-yellow-50 text-yellow-800 ring-yellow-600/30 dark:bg-yellow-400/10 dark:text-yellow-300 dark:ring-yellow-300/20',
    dot: '#facc15',
    ink: '#1e293b',
  },
  C: {
    label: 'Caution',
    tile: 'bg-orange-500 text-white',
    soft: 'bg-orange-50 text-orange-700 ring-orange-600/20 dark:bg-orange-500/10 dark:text-orange-400 dark:ring-orange-400/20',
    dot: '#f97316',
  },
  F: {
    label: 'Avoid',
    tile: 'bg-red-500 text-white',
    soft: 'bg-red-50 text-red-700 ring-red-600/20 dark:bg-red-500/10 dark:text-red-400 dark:ring-red-400/20',
    dot: '#ef4444',
  },
};

const FALLBACK = {
  label: 'Unrated',
  tile: 'bg-slate-400 text-white',
  soft: 'bg-slate-100 text-slate-600 ring-slate-500/20 dark:bg-slate-500/10 dark:text-slate-400 dark:ring-slate-400/20',
  dot: '#94a3b8',
};

export function gradeMeta(grade) {
  return GRADE_META[grade] || FALLBACK;
}

/**
 * Square grade tile: just the letter. Numeric scores are deliberately not
 * shown anywhere in the UI — jurisdictions publish incompatible scales, so
 * the nationally-consistent letter is the only user-facing grade (numbers
 * still drive the grading behind the scenes).
 */
export default function GradeBadge({ grade, size = 'md' }) {
  const meta = gradeMeta(grade);
  const dims = size === 'sm'
    ? { box: 'w-11 h-11 rounded-xl', letter: 'text-xl' }
    : { box: 'w-16 h-16 rounded-2xl', letter: 'text-4xl' };

  return (
    <div className={`flex items-center justify-center shrink-0 ${dims.box} ${meta.tile} shadow-sm`}>
      <span className={`font-black leading-none ${dims.letter}`}>{grade || '–'}</span>
    </div>
  );
}
