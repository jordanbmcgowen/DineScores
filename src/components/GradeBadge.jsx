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
  B: {
    label: 'Caution',
    tile: 'bg-amber-500 text-white',
    soft: 'bg-amber-50 text-amber-700 ring-amber-600/20 dark:bg-amber-500/10 dark:text-amber-400 dark:ring-amber-400/20',
    dot: '#f59e0b',
  },
  C: {
    label: 'Caution',
    tile: 'bg-amber-500 text-white',
    soft: 'bg-amber-50 text-amber-700 ring-amber-600/20 dark:bg-amber-500/10 dark:text-amber-400 dark:ring-amber-400/20',
    dot: '#f59e0b',
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
 * Square grade tile: big letter, score underneath.
 */
export default function GradeBadge({ grade, score, size = 'md' }) {
  const meta = gradeMeta(grade);
  const dims = size === 'sm'
    ? { box: 'w-11 h-11 rounded-xl', letter: 'text-lg', score: 'text-[10px]' }
    : { box: 'w-16 h-16 rounded-2xl', letter: 'text-3xl', score: 'text-xs' };

  return (
    <div className={`flex flex-col items-center justify-center shrink-0 ${dims.box} ${meta.tile} shadow-sm`}>
      <span className={`font-black leading-none ${dims.letter}`}>{grade || '–'}</span>
      {score != null && (
        <span className={`font-bold leading-none mt-0.5 opacity-90 tabular-nums ${dims.score}`}>{score}</span>
      )}
    </div>
  );
}
