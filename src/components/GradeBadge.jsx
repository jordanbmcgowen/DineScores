import React from 'react';

/**
 * Grade badge with thumbs-up/hand/thumbs-down icon.
 * Matches Gemini edition GradeBadge component.
 */
export default function GradeBadge({ grade, score, size = 'md' }) {
  const config = {
    A: { bg: 'bg-green-100 dark:bg-green-900/30', text: 'text-green-700 dark:text-green-400', ring: 'ring-green-500/20', label: 'Safe', icon: 'up' },
    B: { bg: 'bg-yellow-100 dark:bg-yellow-900/30', text: 'text-yellow-700 dark:text-yellow-400', ring: 'ring-yellow-500/20', label: 'Evaluate', icon: 'hand' },
    C: { bg: 'bg-yellow-100 dark:bg-yellow-900/30', text: 'text-yellow-700 dark:text-yellow-400', ring: 'ring-yellow-500/20', label: 'Evaluate', icon: 'hand' },
    F: { bg: 'bg-red-100 dark:bg-red-900/30', text: 'text-red-700 dark:text-red-400', ring: 'ring-red-500/20', label: 'Avoid', icon: 'down' },
  }[grade] || { bg: 'bg-slate-100', text: 'text-slate-600', ring: 'ring-slate-500/20', label: '?', icon: 'hand' };

  const dim = size === 'sm' ? 'w-10 h-10' : 'w-14 h-14';
  const iconSize = size === 'sm' ? 'w-5 h-5' : 'w-7 h-7';

  return (
    <div className="flex flex-col items-center gap-0.5">
      <div className={`flex items-center justify-center ${dim} rounded-xl shadow-sm ring-1 ${config.ring} ${config.bg} ${config.text}`}>
        {config.icon === 'up' && (
          <svg className={iconSize} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
            <path d="M7 10v12m8-16.12L14 10h5.83a2 2 0 0 1 1.92 2.56l-2.33 8A2 2 0 0 1 17.5 22H4a2 2 0 0 1-2-2v-8a2 2 0 0 1 2-2h2.76a2 2 0 0 0 1.79-1.11L12 2h0a3.13 3.13 0 0 1 3 3.88Z"/>
          </svg>
        )}
        {config.icon === 'hand' && (
          <svg className={iconSize} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
            <path d="M18 11V6a2 2 0 0 0-2-2v0a2 2 0 0 0-2 2v0M14 10V4a2 2 0 0 0-2-2v0a2 2 0 0 0-2 2v2M10 10.5V6a2 2 0 0 0-2-2v0a2 2 0 0 0-2 2v8"/>
            <path d="M18 8a2 2 0 1 1 4 0v6a8 8 0 0 1-8 8h-2c-2.8 0-4.5-.86-5.99-2.34l-3.6-3.6a2 2 0 0 1 2.83-2.82L7 15"/>
          </svg>
        )}
        {config.icon === 'down' && (
          <svg className={iconSize} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
            <path d="M17 14V2M9 18.12L10 14H4.17a2 2 0 0 1-1.92-2.56l2.33-8A2 2 0 0 1 6.5 2H20a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2h-2.76a2 2 0 0 0-1.79 1.11L12 22h0a3.13 3.13 0 0 1-3-3.88Z"/>
          </svg>
        )}
      </div>
      <span className="text-[10px] font-bold text-slate-400 dark:text-slate-500 uppercase tracking-tight">Score</span>
      <span className="text-xs font-black text-slate-800 dark:text-slate-200 font-mono leading-none">{score}</span>
    </div>
  );
}
