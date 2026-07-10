import React, { useEffect, useRef, useState } from 'react';
import maplibregl from 'maplibre-gl';
import { gradeMeta } from './GradeBadge.jsx';

function basemapStyle(dark) {
  const flavor = dark ? 'dark_all' : 'light_all';
  return {
    version: 8,
    // Needed for the restaurant-name labels (SDF text). The grade LETTERS
    // deliberately don't use this — see makeLetterIcon.
    glyphs: 'https://tiles.basemaps.cartocdn.com/fonts/{fontstack}/{range}.pbf',
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

// Clustering runs all the way to max zoom: restaurants stacked at one
// street address (casinos, food halls) stay a counted donut forever instead
// of collapsing into what looks like a single restaurant. The map's maxZoom
// stays fractionally below CLUSTER_MAX_ZOOM + 1 so no zoom level ever
// renders the raw splat.
const CLUSTER_MAX_ZOOM = 17;
const MAP_MAX_ZOOM = 17.9;

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

const NAME_TEXT_SIZE = 11.5;

// Soft brand-colored halo behind the selected restaurant's marker
function haloRadiusExpr(density) {
  return ['interpolate', ['linear'], ['zoom'],
    ...RADIUS_STOPS.flatMap(([z, r]) => [z, r * density + 7])];
}

// Name labels sit just outside the disc edge (radial offset is in ems)
function nameOffsetExpr(density) {
  return ['interpolate', ['linear'], ['zoom'],
    ...RADIUS_STOPS.flatMap(([z, r]) => [z, (r * density + 5) / NAME_TEXT_SIZE])];
}

// Marker radius in px at an exact zoom (the JS twin of circleRadiusExpr),
// used to size cluster donuts to at least match individual markers.
function radiusAtZoom(zoom, density) {
  const s = RADIUS_STOPS;
  let r = s[s.length - 1][1];
  if (zoom <= s[0][0]) r = s[0][1];
  else {
    for (let i = 1; i < s.length; i++) {
      if (zoom <= s[i][0]) {
        const [z0, r0] = s[i - 1];
        const [z1, r1] = s[i];
        r = r0 + ((r1 - r0) * (zoom - z0)) / (z1 - z0);
        break;
      }
    }
  }
  return r * density;
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
// A cluster stands in for SEVERAL restaurants, so it must never look smaller
// than the individual markers around it: its radius is the count tier or the
// current marker radius + a bit, whichever is larger.
function donutRadius(total, minR) {
  const tier = total >= 500 ? 32 : total >= 100 ? 26 : total >= 25 ? 21 : 16;
  return Math.max(tier, Math.round(minR));
}

function createDonutChart(props, dark, minR) {
  const counts = {
    safe: props.safe || 0,
    caution: props.caution || 0,
    avoid: props.avoid || 0,
  };
  const total = props.point_count;
  counts.unrated = Math.max(0, total - counts.safe - counts.caution - counts.avoid);

  const r = donutRadius(total, minR);
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
    `style="font: 700 ${r >= 26 ? 13 : 12}px system-ui, sans-serif; fill:${centerFg};">${label}</text></svg>`;

  const el = document.createElement('div');
  el.innerHTML = html;
  el._dsRadius = r; // so a zoom/density change can tell the donut is stale
  el.style.cursor = 'pointer';
  el.setAttribute('role', 'button');
  el.setAttribute('aria-label',
    `${total} restaurants: ${counts.safe} safe, ${counts.caution} caution, ` +
    `${counts.avoid} avoid${counts.unrated ? `, ${counts.unrated} unrated` : ''}. ` +
    'Tap to zoom in or list them.');
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
export default function RestaurantMap({
  restaurants, onMarkerClick, onBackgroundClick, onStackClick, onViewportChange,
  fitSignal, narrowSignal, flyTo, selectedId,
}) {
  const mapContainer = useRef(null);
  const mapRef = useRef(null);
  const restaurantsRef = useRef(restaurants);
  const onViewportChangeRef = useRef(onViewportChange);
  const onBackgroundClickRef = useRef(onBackgroundClick);
  onBackgroundClickRef.current = onBackgroundClick;
  const onStackClickRef = useRef(onStackClick);
  onStackClickRef.current = onStackClick;
  const [mapLoaded, setMapLoaded] = useState(false);
  const donutsRef = useRef({ byPos: {} });
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
      maxZoom: MAP_MAX_ZOOM,
      attributionControl: { compact: true },
    });

    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'bottom-right');
    map.addControl(new maplibregl.GeolocateControl({
      positionOptions: { enableHighAccuracy: true },
      trackUserLocation: false,
    }), 'bottom-right');

    map.on('load', () => setMapLoaded(true));

    // Debounced viewport reporting. Bounds cover only the part of the map
    // the user can actually SEE — the strips hidden under the sidebar, the
    // header rail, and the mobile sheet are excluded, so the results list
    // mirrors the visible map exactly.
    const visibleBounds = () => {
      const el = map.getContainer();
      const w = el.clientWidth;
      const h = el.clientHeight;
      const desktop = window.innerWidth >= 768;
      const inset = desktop
        ? { left: 432, top: 112, right: 8, bottom: 8 }
        : { left: 0, top: 148, right: 0, bottom: 104 };
      if (inset.left + inset.right > w * 0.8 || inset.top + inset.bottom > h * 0.8) {
        const b = map.getBounds();
        return { w: b.getWest(), s: b.getSouth(), e: b.getEast(), n: b.getNorth() };
      }
      const p1 = map.unproject([inset.left, h - inset.bottom]);
      const p2 = map.unproject([w - inset.right, inset.top]);
      return {
        w: Math.min(p1.lng, p2.lng), s: Math.min(p1.lat, p2.lat),
        e: Math.max(p1.lng, p2.lng), n: Math.max(p1.lat, p2.lat),
      };
    };
    let moveTimer;
    map.on('moveend', () => {
      clearTimeout(moveTimer);
      moveTimer = setTimeout(() => {
        const cb = onViewportChangeRef.current;
        if (!cb) return;
        cb({ zoom: map.getZoom(), bounds: visibleBounds() });
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
            name: r.n || '',
            grade: GRADE_LETTERS.includes(r.vg) ? r.vg : '?',
            color: gradeMeta(r.vg).dot,
            priority: GRADE_PRIORITY[GRADE_LETTERS.includes(r.vg) ? r.vg : '?'],
          },
        })),
    };

    if (map.getSource('restaurants')) {
      // Donut markers persist across setData: they are cached by map
      // position (not cluster_id), so the next sourcedata pass morphs
      // their contents in place instead of blinking them out and back.
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
      // overlap stay grouped as a donut instead of stacking. Clustering
      // never switches off (see CLUSTER_MAX_ZOOM) — a same-address stack
      // remains a counted donut at max zoom, and clicking it lists the
      // individual restaurants in the sidebar.
      clusterMaxZoom: CLUSTER_MAX_ZOOM,
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

    // Halo behind the currently-selected marker (filter swaps per selection)
    map.addLayer({
      id: 'selected-halo',
      type: 'circle',
      source: 'restaurants',
      filter: ['==', ['get', 'id'], '___none'],
      paint: {
        'circle-radius': haloRadiusExpr(densityRef.current),
        'circle-color': '#0d9488',
        'circle-opacity': 0.30,
        'circle-stroke-color': '#0d9488',
        'circle-stroke-width': 2,
        'circle-stroke-opacity': 0.65,
      },
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
    const darkScheme = window.matchMedia('(prefers-color-scheme: dark)').matches;

    // One symbol per restaurant: the grade letter (mandatory icon) plus the
    // restaurant name beside the disc (text-optional — it drops out where
    // space is tight, the letter never does). Names appear from z13 up.
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
        'text-field': ['step', ['zoom'], '', 13, ['get', 'name']],
        'text-font': ['Montserrat Bold'],
        'text-size': NAME_TEXT_SIZE,
        'text-variable-anchor': ['left', 'right', 'top', 'bottom'],
        'text-radial-offset': nameOffsetExpr(densityRef.current),
        'text-justify': 'auto',
        'text-max-width': 9,
        'text-optional': true,
      },
      paint: {
        'text-color': darkScheme ? '#e2e8f0' : '#334155',
        'text-halo-color': darkScheme ? '#0f172a' : '#ffffff',
        'text-halo-width': 1.6,
      },
    });

    // Clusters render as HTML donut markers (grade distribution + count),
    // synced to the current clustering and sized to the current markers.
    // The cache is keyed by QUANTIZED MAP POSITION, not cluster_id: every
    // setData (each background-sync merge) reassigns all cluster_ids, so
    // id-keyed donuts blink out and back on every merge. A position key
    // survives re-clustering, and the donut at that spot just has its
    // contents redrawn in place as its count converges.
    const donutPosKey = (coords, zb, used) => {
      // ~64px grid cells at the current integer zoom (world is 256*2^z px)
      const scale = Math.pow(2, zb) * 4;
      const x = ((coords[0] + 180) / 360) * scale;
      const latRad = (coords[1] * Math.PI) / 180;
      const y = (0.5 - Math.log(Math.tan(Math.PI / 4 + latRad / 2)) / (2 * Math.PI)) * scale;
      let key = `${zb}:${Math.round(x)}:${Math.round(y)}`;
      while (used[key]) key += '+'; // two clusters in one cell: distinct keys
      return key;
    };

    const updateDonuts = () => {
      const cache = donutsRef.current.byPos;
      const seen = {};
      const seenClusters = {};
      let feats = [];
      try {
        feats = map.queryRenderedFeatures({ layers: ['clusters'] });
      } catch (e) { return; }
      const zb = Math.round(map.getZoom());
      // Donuts must read as ≥ the individual markers beside them
      const minR = radiusAtZoom(map.getZoom(), densityRef.current) + 3;
      for (const f of feats) {
        const props = f.properties;
        if (!props.cluster || seenClusters[props.cluster_id]) continue;
        seenClusters[props.cluster_id] = true;
        const coords = f.geometry.coordinates;
        const key = donutPosKey(coords, zb, seen);
        const sig = `${props.point_count}|${props.safe || 0}|${props.caution || 0}|` +
          `${props.avoid || 0}|${donutRadius(props.point_count, minR)}`;
        let entry = cache[key];
        if (entry) {
          // Same spot: keep the marker, refresh position and contents in
          // place (no remove+add — that's the blink this cache avoids).
          entry.marker.setLngLat(coords);
          if (entry.sig !== sig) {
            const el = entry.marker.getElement();
            const next = createDonutChart(props, darkScheme, minR);
            el.replaceChildren(...next.children);
            el._dsRadius = next._dsRadius;
            el.title = next.title;
            el.setAttribute('aria-label', next.title);
            entry.sig = sig;
          }
        } else {
          const el = createDonutChart(props, darkScheme, minR);
          entry = cache[key] = {
            marker: new maplibregl.Marker({ element: el }).setLngLat(coords),
            sig,
          };
          const ent = entry; // click reads LIVE cluster id/coords from the entry
          el.addEventListener('click', () => {
            const src = map.getSource('restaurants');
            const id = ent.clusterId;
            // v4 returns Promises (the callback forms are gone)
            Promise.resolve(src.getClusterExpansionZoom(id)).then(zoom => {
              if (zoom <= CLUSTER_MAX_ZOOM) {
                map.easeTo({ center: ent.coords, zoom });
                return;
              }
              // Terminal stack: these restaurants share one location and no
              // amount of zoom separates them — center on it and hand the
              // member list to the sidebar instead.
              map.easeTo({
                center: ent.coords,
                zoom: Math.max(map.getZoom(), 16.5),
              });
              Promise.resolve(src.getClusterLeaves(id, 1000, 0)).then(leaves => {
                const ids = new Set(leaves.map(l => l.properties.id));
                const records = restaurantsRef.current.filter(r => ids.has(r.i));
                if (records.length && onStackClickRef.current) onStackClickRef.current(records);
              }).catch(() => {});
            }).catch(() => {});
          });
          entry.marker.addTo(map);
          // addTo() stamps a generic "Map marker" aria-label — replace it
          // with the descriptive one (grade mix + count, mirrored in title).
          el.setAttribute('aria-label', el.title);
        }
        entry.clusterId = props.cluster_id;
        entry.coords = coords;
        seen[key] = true;
      }
      for (const key of Object.keys(cache)) {
        if (!seen[key]) {
          cache[key].marker.remove();
          delete cache[key];
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
        map.setLayoutProperty('unclustered-letter', 'text-radial-offset', nameOffsetExpr(next));
        map.setPaintProperty('selected-halo', 'circle-radius', haloRadiusExpr(next));
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
    // 'idle' is the settle point after a zoom: during the transition the
    // renderer briefly shows BOTH the old and new zoom's tiles, so a pass
    // triggered mid-transition caches donuts for clusters that are about
    // to be evicted — and no sourcedata/move event fires on the eviction
    // itself. The idle pass sees only the final clusters and sweeps any
    // stale donuts (their counts otherwise linger from the previous zoom).
    map.on('idle', syncDonuts);

    // Interactions (cluster donuts carry their own click handlers)
    map.on('click', 'unclustered-point', e => {
      const props = e.features[0].properties;
      const r = restaurantsRef.current.find(x => x.i === props.id);
      if (r && onMarkerClick) onMarkerClick(r);
    });
    // A click on empty map (no marker under the pointer) dismisses the
    // preview card — same dismissal gesture as Google Maps.
    map.on('click', e => {
      let hits = [];
      try {
        hits = map.queryRenderedFeatures(e.point, { layers: ['unclustered-point'] });
      } catch { /* layer not ready */ }
      if (hits.length === 0 && onBackgroundClickRef.current) onBackgroundClickRef.current();
    });
    map.on('mouseenter', 'unclustered-point', () => { map.getCanvas().style.cursor = 'pointer'; });
    map.on('mouseleave', 'unclustered-point', () => { map.getCanvas().style.cursor = ''; });
  }, [restaurants, mapLoaded, onMarkerClick]);

  // Selection follows whichever restaurant is previewed/open: halo behind
  // its disc, and its letter jumps to top collision priority so the marker
  // the user just clicked never shows as a letterless disc.
  useEffect(() => {
    const map = mapRef.current;
    if (!map || !mapLoaded || !map.getLayer('selected-halo')) return;
    const id = selectedId || '___none';
    map.setFilter('selected-halo', ['==', ['get', 'id'], id]);
    map.setLayoutProperty('unclustered-letter', 'symbol-sort-key',
      ['case', ['==', ['get', 'id'], id], -1, ['-', 4, ['get', 'priority']]]);
  }, [selectedId, mapLoaded, restaurants]);

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
