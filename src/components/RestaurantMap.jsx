import React, { useEffect, useRef, useState } from 'react';
import maplibregl from 'maplibre-gl';
import { gradeMeta } from './GradeBadge.jsx';

function basemapStyle(dark) {
  const flavor = dark ? 'dark_all' : 'light_all';
  return {
    version: 8,
    sources: {
      basemap: {
        type: 'raster',
        tiles: [
          `https://a.basemaps.cartocdn.com/${flavor}/{z}/{x}/{y}@2x.png`,
          `https://b.basemaps.cartocdn.com/${flavor}/{z}/{x}/{y}@2x.png`,
          `https://c.basemaps.cartocdn.com/${flavor}/{z}/{x}/{y}@2x.png`,
        ],
        tileSize: 256,
        attribution: '&copy; OpenStreetMap &copy; CARTO',
      },
    },
    layers: [{ id: 'basemap', type: 'raster', source: 'basemap' }],
  };
}

// Cluster ring segments use the same status vocabulary as everything else:
// Safe (A) / Caution (B, C) / Avoid (F) / Unrated.
const DONUT_COLORS = { safe: '#10b981', caution: '#f59e0b', avoid: '#ef4444', unrated: '#94a3b8' };

/**
 * Grade markers are a hybrid: the colored disc + white ring is a MapLibre
 * circle layer (true vector — the edge is shader-smooth at every radius),
 * and only the grade LETTER is a pre-rendered bitmap glyph on top. Map-font
 * SDF glyphs degrade badly at small text sizes (an 8px SDF "A" rasterizes
 * as a triangle-ish blob), while a canvas-drawn letter survives scaling.
 * Rendering the disc in the bitmap too made its silhouette look rough when
 * minified — hence the split.
 */
const LETTER_BASE = 24; // logical px of the letter glyph box at icon-size 1
                        // (kept tight around the glyph — the box doubles as
                        // the letter's collision footprint)

function makeLetterIcon(letter) {
  const pr = 3;
  const s = LETTER_BASE * pr;
  const canvas = document.createElement('canvas');
  canvas.width = s;
  canvas.height = s;
  const ctx = canvas.getContext('2d');
  ctx.fillStyle = '#ffffff';
  ctx.font = `900 ${26 * pr}px system-ui, -apple-system, sans-serif`;
  ctx.textAlign = 'center';
  ctx.textBaseline = 'middle';
  ctx.fillText(letter, s / 2, s / 2 + pr);
  return { data: ctx.getImageData(0, 0, s, s), pixelRatio: pr };
}

const GRADE_LETTERS = ['A', 'B', 'C', 'F', '?'];

// Stacking hierarchy for markers that touch: the worse the grade, the more
// the user needs to see it. Higher priority paints on top (circle-sort-key)
// and wins the letter collision (symbol-sort-key, where LOWER places first).
const GRADE_PRIORITY = { F: 4, C: 3, B: 2, A: 1, '?': 0 };

// Marker radius by zoom, times a density factor — markers grow when few
// restaurants are in view and shrink (to a still-readable floor) when the
// viewport is packed. The letter's icon-size derives from the same numbers
// so the glyph always fills ~70% of its disc.
const RADIUS_STOPS = [[8, 9], [11, 12.5], [13.5, 16], [16, 19]];

function circleRadiusExpr(density) {
  return ['interpolate', ['linear'], ['zoom'],
    ...RADIUS_STOPS.flatMap(([z, r]) => [z, r * density])];
}

function letterSizeExpr(density) {
  // Glyph cap height is ~18 logical px at icon-size 1 (26px font, 900 weight)
  return ['interpolate', ['linear'], ['zoom'],
    ...RADIUS_STOPS.flatMap(([z, r]) => [z, (r * density * 1.25) / 18])];
}

function densityFactor(count) {
  if (count <= 12) return 1.5;
  if (count <= 30) return 1.3;
  if (count <= 80) return 1.12;
  if (count <= 200) return 1.0;
  if (count <= 500) return 0.85;
  return 0.72; // floor: ~14px marker at mid zoom, letter still readable
}

function donutSegment(start, end, r, r0, color) {
  if (end - start === 1) end -= 0.00001;
  const a0 = 2 * Math.PI * (start - 0.25);
  const a1 = 2 * Math.PI * (end - 0.25);
  const x0 = Math.cos(a0), y0 = Math.sin(a0);
  const x1 = Math.cos(a1), y1 = Math.sin(a1);
  const largeArc = end - start > 0.5 ? 1 : 0;
  return `<path d="M ${r + r0 * x0} ${r + r0 * y0} L ${r + r * x0} ${r + r * y0} ` +
    `A ${r} ${r} 0 ${largeArc} 1 ${r + r * x1} ${r + r * y1} ` +
    `L ${r + r0 * x1} ${r + r0 * y1} A ${r0} ${r0} 0 ${largeArc} 0 ${r + r0 * x0} ${r + r0 * y0}" ` +
    `fill="${color}"/>`;
}

/**
 * Build a donut-chart DOM element for a cluster: ring segments sized by the
 * cluster's grade distribution, total count in the middle. The ring makes a
 * cluster's health mix legible without zooming; exact numbers are in the
 * aria-label/tooltip.
 */
function createDonutChart(props, dark) {
  const counts = {
    safe: props.safe || 0,
    caution: props.caution || 0,
    avoid: props.avoid || 0,
  };
  const total = props.point_count;
  counts.unrated = Math.max(0, total - counts.safe - counts.caution - counts.avoid);

  const r = total >= 500 ? 32 : total >= 100 ? 26 : total >= 25 ? 21 : 16;
  const r0 = Math.round(r * 0.62);
  const w = r * 2;

  let html = `<svg width="${w}" height="${w}" viewBox="0 0 ${w} ${w}" ` +
    `style="display:block; filter: drop-shadow(0 1px 3px rgb(0 0 0 / 0.35));">`;
  let offset = 0;
  for (const key of ['safe', 'caution', 'avoid', 'unrated']) {
    if (!counts[key]) continue;
    html += donutSegment(offset / total, (offset + counts[key]) / total, r, r0, DONUT_COLORS[key]);
    offset += counts[key];
  }
  const centerBg = dark ? '#1e293b' : '#ffffff';
  const centerFg = dark ? '#f1f5f9' : '#0f172a';
  const label = total >= 10000 ? `${Math.round(total / 1000)}k`
    : total >= 1000 ? `${(total / 1000).toFixed(1)}k` : String(total);
  html += `<circle cx="${r}" cy="${r}" r="${r0}" fill="${centerBg}"/>` +
    `<text x="${r}" y="${r}" text-anchor="middle" dominant-baseline="central" ` +
    `style="font: 700 ${r >= 26 ? 12 : 11}px system-ui, sans-serif; fill:${centerFg};">${label}</text></svg>`;

  const el = document.createElement('div');
  el.innerHTML = html;
  el.style.cursor = 'pointer';
  el.setAttribute('role', 'button');
  el.setAttribute('aria-label',
    `${total} restaurants: ${counts.safe} safe, ${counts.caution} caution, ` +
    `${counts.avoid} avoid${counts.unrated ? `, ${counts.unrated} unrated` : ''}. Zoom in.`);
  el.title = el.getAttribute('aria-label');
  return el;
}

/**
 * Full-bleed MapLibre map. Markers are colored by vetted grade AND carry the
 * grade letter, so color is never the only encoding. Clusters render as
 * grade-distribution donuts. Clicking a marker opens the inspection panel
 * directly (no intermediate popup).
 *
 * Camera behavior is deliberately split from data:
 *   - the data effect updates markers whenever `restaurants` changes, and never
 *     moves the camera (so viewport-driven data loading can't fight the user);
 *   - the fit effect re-frames the map only when `fitSignal` changes (a city
 *     or metro change — an intentional move to a new area), while
 *     `narrowSignal` (grade/issue/search changes) at most zooms OUT to the
 *     nearest match and otherwise leaves the camera alone;
 *   - `flyTo` ({lng, lat, zoom, key}) centers the camera once per key — used
 *     to open on the user's own location. When it arrives before the first
 *     fit, it wins over that initial nationwide framing.
 * `onViewportChange({ zoom, bounds })` fires (debounced) after the user pans or
 * zooms, so the parent can lazy-load whatever is now in view.
 */
export default function RestaurantMap({ restaurants, onMarkerClick, onViewportChange, fitSignal, narrowSignal, flyTo }) {
  const mapContainer = useRef(null);
  const mapRef = useRef(null);
  const restaurantsRef = useRef(restaurants);
  const onViewportChangeRef = useRef(onViewportChange);
  const [mapLoaded, setMapLoaded] = useState(false);
  const donutsRef = useRef({ markers: {}, onScreen: {} });
  const densityRef = useRef(1.0);
  const gpsCenteredRef = useRef(false);
  const didInitialFitRef = useRef(false);
  const narrowInitRef = useRef(true);
  restaurantsRef.current = restaurants;
  onViewportChangeRef.current = onViewportChange;

  // Initialize map (once)
  useEffect(() => {
    if (mapRef.current) return;
    const dark = window.matchMedia('(prefers-color-scheme: dark)').matches;

    const map = new maplibregl.Map({
      container: mapContainer.current,
      style: basemapStyle(dark),
      center: [-96.9, 36.5],
      zoom: 4,
      maxZoom: 18,
      attributionControl: { compact: true },
    });

    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'bottom-right');
    map.addControl(new maplibregl.GeolocateControl({
      positionOptions: { enableHighAccuracy: true },
      trackUserLocation: false,
    }), 'bottom-right');

    map.on('load', () => setMapLoaded(true));

    // Debounced viewport reporting
    let moveTimer;
    map.on('moveend', () => {
      clearTimeout(moveTimer);
      moveTimer = setTimeout(() => {
        const cb = onViewportChangeRef.current;
        if (!cb) return;
        const b = map.getBounds();
        cb({
          zoom: map.getZoom(),
          bounds: { w: b.getWest(), s: b.getSouth(), e: b.getEast(), n: b.getNorth() },
        });
      }, 350);
    });

    mapRef.current = map;
    if (typeof window !== 'undefined') window.__dsMap = map; // support/debug handle
    return () => {
      clearTimeout(moveTimer);
      map.remove();
      mapRef.current = null;
    };
  }, []);

  // Data effect: update markers when the result set changes. Never fits.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapLoaded) return;

    const geojson = {
      type: 'FeatureCollection',
      features: restaurants
        .filter(r => r.lt && r.ln && r.lt !== 0 && r.ln !== 0)
        .map(r => ({
          type: 'Feature',
          geometry: { type: 'Point', coordinates: [r.ln, r.lt] },
          properties: {
            id: r.i,
            grade: GRADE_LETTERS.includes(r.vg) ? r.vg : '?',
            color: gradeMeta(r.vg).dot,
            priority: GRADE_PRIORITY[GRADE_LETTERS.includes(r.vg) ? r.vg : '?'],
          },
        })),
    };

    if (map.getSource('restaurants')) {
      // New data invalidates cluster ids — rebuild the donut marker cache.
      for (const m of Object.values(donutsRef.current.markers)) m.remove();
      donutsRef.current = { markers: {}, onScreen: {} };
      map.getSource('restaurants').setData(geojson);
      return;
    }

    for (const letter of GRADE_LETTERS) {
      const { data, pixelRatio } = makeLetterIcon(letter);
      map.addImage(`letter-${letter}`, data, { pixelRatio });
    }

    map.addSource('restaurants', {
      type: 'geojson',
      data: geojson,
      cluster: true,
      // Radius ≈ one marker diameter: restaurants that would physically
      // overlap (food courts, malls) stay grouped as a small donut instead
      // of stacking; genuinely separate ones show individually. Everything
      // unclusters at street level (16+).
      clusterMaxZoom: 15,
      clusterRadius: 40,
      // Aggregate the grade mix per cluster so donuts can render it.
      clusterProperties: {
        safe: ['+', ['case', ['==', ['get', 'grade'], 'A'], 1, 0]],
        caution: ['+', ['case', ['any', ['==', ['get', 'grade'], 'B'], ['==', ['get', 'grade'], 'C']], 1, 0]],
        avoid: ['+', ['case', ['==', ['get', 'grade'], 'F'], 1, 0]],
      },
    });

    // Anchor layer for clusters: effectively invisible, but being *rendered*
    // means queryRenderedFeatures returns exactly the clusters at the current
    // zoom — querySourceFeatures would leak stale clusters from cached tiles
    // of other zoom levels, leaving orphaned donuts behind after zooming.
    map.addLayer({
      id: 'clusters',
      type: 'circle',
      source: 'restaurants',
      filter: ['has', 'point_count'],
      paint: { 'circle-radius': 1, 'circle-opacity': 0.01 },
    });

    // Individual markers: vector disc (smooth at any radius) + bitmap letter.
    // When markers still touch (adjacent storefronts at street level), the
    // hierarchy keeps it readable: worse grades paint on top of better ones,
    // and letters collision-cull — the worst grade's letter always survives,
    // so text never overlaps text.
    map.addLayer({
      id: 'unclustered-point',
      type: 'circle',
      source: 'restaurants',
      filter: ['!', ['has', 'point_count']],
      layout: {
        'circle-sort-key': ['get', 'priority'],
      },
      paint: {
        'circle-color': ['get', 'color'],
        'circle-radius': circleRadiusExpr(densityRef.current),
        'circle-stroke-width': 2,
        'circle-stroke-color': '#ffffff',
      },
    });
    map.addLayer({
      id: 'unclustered-letter',
      type: 'symbol',
      source: 'restaurants',
      filter: ['!', ['has', 'point_count']],
      layout: {
        'icon-image': ['concat', 'letter-', ['get', 'grade']],
        'icon-size': letterSizeExpr(densityRef.current),
        // Lower sort key places first and wins the collision — invert
        // priority so F letters beat the A's they overlap.
        'symbol-sort-key': ['-', 4, ['get', 'priority']],
        'icon-padding': 0,
      },
    });

    // Clusters render as HTML donut markers (grade distribution + count),
    // synced to the current clustering.
    const darkScheme = window.matchMedia('(prefers-color-scheme: dark)').matches;
    const updateDonuts = () => {
      const cache = donutsRef.current;
      const seen = {};
      let feats = [];
      try {
        feats = map.queryRenderedFeatures({ layers: ['clusters'] });
      } catch (e) { return; }
      for (const f of feats) {
        const props = f.properties;
        if (!props.cluster) continue;
        const id = props.cluster_id;
        if (seen[id]) continue;
        let marker = cache.markers[id];
        if (!marker) {
          const el = createDonutChart(props, darkScheme);
          el.addEventListener('click', () => {
            // v4 returns a Promise (the callback form is gone)
            Promise.resolve(map.getSource('restaurants').getClusterExpansionZoom(id))
              .then(zoom => map.easeTo({ center: f.geometry.coordinates, zoom }))
              .catch(() => {});
          });
          marker = cache.markers[id] =
            new maplibregl.Marker({ element: el }).setLngLat(f.geometry.coordinates);
        }
        seen[id] = true;
        if (!cache.onScreen[id]) {
          marker.addTo(map);
          // addTo() stamps a generic "Map marker" aria-label — replace it
          // with the descriptive one (grade mix + count, mirrored in title).
          const el = marker.getElement();
          el.setAttribute('aria-label', el.title);
          cache.onScreen[id] = marker;
        }
      }
      for (const id of Object.keys(cache.onScreen)) {
        if (!seen[id]) {
          cache.onScreen[id].remove();
          delete cache.onScreen[id];
        }
      }
    };

    // Density-adaptive marker size: count the individual markers currently
    // rendered and rescale so sparse views get big, tappable markers and
    // packed views shrink them only as far as legibility allows.
    const updateDensity = () => {
      let feats = [];
      try {
        feats = map.queryRenderedFeatures({ layers: ['unclustered-point'] });
      } catch (e) { return; }
      const next = densityFactor(feats.length);
      if (next !== densityRef.current) {
        densityRef.current = next;
        map.setPaintProperty('unclustered-point', 'circle-radius', circleRadiusExpr(next));
        map.setLayoutProperty('unclustered-letter', 'icon-size', letterSizeExpr(next));
      }
    };

    // 'sourcedata' covers initial load and every setData; move/zoom events
    // cover cluster recomposition. ('render' alone misses source loads that
    // complete while the map is idle — it stops firing between frames.)
    const syncDonuts = () => {
      if (map.getSource('restaurants') && map.isSourceLoaded('restaurants')) {
        updateDonuts();
        updateDensity();
      }
    };
    map.on('sourcedata', e => {
      if (e.sourceId === 'restaurants' && e.isSourceLoaded) syncDonuts();
    });
    map.on('move', syncDonuts);
    map.on('moveend', syncDonuts);

    // Interactions (cluster donuts carry their own click handlers)
    map.on('click', 'unclustered-point', e => {
      const props = e.features[0].properties;
      const r = restaurantsRef.current.find(x => x.i === props.id);
      if (r && onMarkerClick) onMarkerClick(r);
    });
    map.on('mouseenter', 'unclustered-point', () => { map.getCanvas().style.cursor = 'pointer'; });
    map.on('mouseleave', 'unclustered-point', () => { map.getCanvas().style.cursor = ''; });
  }, [restaurants, mapLoaded, onMarkerClick]);

  // GPS effect: center on the user once per flyTo key. If it lands before
  // the initial nationwide fit, it takes precedence over that fit.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapLoaded || !flyTo || !flyTo.key) return;
    gpsCenteredRef.current = true;
    map.easeTo({ center: [flyTo.lng, flyTo.lat], zoom: flyTo.zoom || 12.5, duration: 900 });
  }, [flyTo, mapLoaded]);

  // Narrow effect: grade/issue/search filters must not throw the user back
  // to a nationwide view. If matches are already on screen, the camera stays
  // put; otherwise zoom out just far enough to bring the nearest match into
  // view (never zooming in — that would feel like the map acting on its own).
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapLoaded) return;
    if (narrowInitRef.current) { narrowInitRef.current = false; return; }
    const pts = restaurantsRef.current.filter(r => r.lt && r.ln && r.lt !== 0 && r.ln !== 0);
    if (pts.length === 0) return;
    const b = map.getBounds();
    const inView = pts.some(r =>
      r.ln >= b.getWest() && r.ln <= b.getEast() &&
      r.lt >= b.getSouth() && r.lt <= b.getNorth());
    if (inView) return;
    const c = map.getCenter();
    const cosLat = Math.cos((c.lat * Math.PI) / 180);
    let best = null, bestD = Infinity;
    for (const r of pts) {
      const d = (r.lt - c.lat) ** 2 + ((r.ln - c.lng) * cosLat) ** 2;
      if (d < bestD) { bestD = d; best = r; }
    }
    const bounds = new maplibregl.LngLatBounds([c.lng, c.lat], [c.lng, c.lat])
      .extend([best.ln, best.lt]);
    const desktop = window.innerWidth >= 768;
    map.fitBounds(bounds, {
      padding: desktop
        ? { top: 150, left: 460, right: 60, bottom: 60 }
        : { top: 140, left: 50, right: 50, bottom: 140 },
      maxZoom: map.getZoom(), // only ever zoom OUT to reveal the match
      duration: 600,
    });
  }, [narrowSignal, mapLoaded]);

  // Fit effect: re-frame only on an intentional new result set (fitSignal).
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapLoaded) return;
    // The very first fit is just the default framing — skip it if the map
    // already centered on the user's own location. Later fitSignal changes
    // are explicit filter/search actions and still win.
    const isInitialFit = !didInitialFitRef.current;
    didInitialFitRef.current = true;
    if (isInitialFit && gpsCenteredRef.current) return;
    const pts = restaurantsRef.current.filter(r => r.lt && r.ln && r.lt !== 0 && r.ln !== 0);
    if (pts.length === 0) return;

    // Frame the central 96% of points so a few far-flung records (e.g. mobile
    // vendors registered at out-of-state addresses) don't skew the camera —
    // every record still renders on the map.
    const lngs = pts.map(r => r.ln).sort((a, b) => a - b);
    const lats = pts.map(r => r.lt).sort((a, b) => a - b);
    const lo = arr => arr[Math.floor(arr.length * 0.02)];
    const hi = arr => arr[Math.ceil(arr.length * 0.98) - 1];
    const bounds = new maplibregl.LngLatBounds([lo(lngs), lo(lats)], [hi(lngs), hi(lats)]);
    const desktop = window.innerWidth >= 768;
    map.fitBounds(bounds, {
      padding: desktop
        ? { top: 150, left: 440, right: 40, bottom: 40 }
        : { top: 130, left: 30, right: 30, bottom: 130 },
      maxZoom: 13,
      duration: 600,
    });
  }, [fitSignal, mapLoaded]);

  return <div ref={mapContainer} className="w-full h-full" />;
}
