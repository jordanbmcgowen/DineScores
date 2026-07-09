import React, { useRef, useState, useCallback, useEffect } from 'react';

const COLLAPSED_PEEK = 96; // px of sheet visible when collapsed

/**
 * Mobile results sheet: drag the header up/down to resize (snaps to
 * collapsed / half / full), tap it to cycle states. The list body scrolls
 * normally; only the header is a drag handle, so scrolling and dragging
 * never fight over the same gesture. Bumping `collapseKey` collapses the
 * sheet (used when a map preview card needs the space).
 */
export default function BottomSheet({ header, children, collapseKey }) {
  const [state, setState] = useState('collapsed'); // collapsed | half | full

  useEffect(() => {
    if (collapseKey) setState('collapsed');
  }, [collapseKey]);
  const sheetRef = useRef(null);
  const dragRef = useRef(null); // { startY, startOffset, height, moved }
  const suppressClickRef = useRef(false); // a drag's trailing click must not cycle

  const offsetFor = useCallback((s, height) => {
    if (s === 'full') return 0;
    if (s === 'half') return height * 0.5;
    return height - COLLAPSED_PEEK;
  }, []);

  const onTouchStart = useCallback(e => {
    const sheet = sheetRef.current;
    if (!sheet) return;
    const height = sheet.offsetHeight;
    dragRef.current = {
      startY: e.touches[0].clientY,
      startOffset: offsetFor(state, height),
      height,
      moved: false,
    };
    sheet.style.transition = 'none';
  }, [state, offsetFor]);

  const onTouchMove = useCallback(e => {
    const drag = dragRef.current;
    const sheet = sheetRef.current;
    if (!drag || !sheet) return;
    const dy = e.touches[0].clientY - drag.startY;
    if (Math.abs(dy) > 6) drag.moved = true;
    const offset = Math.min(Math.max(drag.startOffset + dy, 0), drag.height - COLLAPSED_PEEK);
    sheet.style.transform = `translateY(${offset}px)`;
  }, []);

  const onTouchEnd = useCallback(e => {
    const drag = dragRef.current;
    const sheet = sheetRef.current;
    dragRef.current = null;
    if (!drag || !sheet) return;
    sheet.style.transition = '';
    sheet.style.transform = '';
    if (!drag.moved) return; // plain tap — the header's onClick cycles states
    suppressClickRef.current = true;
    const endOffset = Math.min(
      Math.max(drag.startOffset + (e.changedTouches[0].clientY - drag.startY), 0),
      drag.height - COLLAPSED_PEEK,
    );
    // Snap to whichever state is nearest to where the finger let go
    const snaps = ['full', 'half', 'collapsed'].map(s => ({ s, y: offsetFor(s, drag.height) }));
    snaps.sort((a, b) => Math.abs(a.y - endOffset) - Math.abs(b.y - endOffset));
    setState(snaps[0].s);
  }, [offsetFor]);

  const cycle = useCallback(() => {
    if (suppressClickRef.current) { suppressClickRef.current = false; return; }
    setState(prev => (prev === 'collapsed' ? 'half' : prev === 'half' ? 'full' : 'collapsed'));
  }, []);

  return (
    <div
      ref={sheetRef}
      className={`md:hidden sheet bg-white dark:bg-slate-900 shadow-2xl ring-1 ring-slate-900/10 dark:ring-white/10 flex flex-col sheet-${state}`}
    >
      <div
        className="shrink-0 cursor-grab active:cursor-grabbing select-none"
        style={{ touchAction: 'none' }}
        onTouchStart={onTouchStart}
        onTouchMove={onTouchMove}
        onTouchEnd={onTouchEnd}
        onClick={cycle}
        role="button"
        tabIndex={0}
        aria-label="Drag or tap to resize the results list"
        onKeyDown={e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); cycle(); } }}
      >
        <div className="w-10 h-1.5 bg-slate-300 dark:bg-slate-600 rounded-full mx-auto mt-3 mb-2" />
        {header}
      </div>
      <div className="flex-1 overflow-y-auto pb-safe overscroll-contain" style={{ touchAction: 'pan-y' }}>
        {children}
      </div>
    </div>
  );
}
