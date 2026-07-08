import React, { useEffect, useRef, useState } from 'react';
import maplibregl from 'maplibre-gl';
import { gradeMeta } from './GradeBadge.jsx';

const GLYPHS_URL = 'https://tiles.basemaps.cartocdn.com/fonts/{fontstack}/{range}.pbf';

function basemapStyle(dark) {
  const flavor = dark ? 'dark_all' : 'light_all';
  return {
    version: 8,
    glyphs: GLYPHS_URL,
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
 *   - the fit effect re-frames the map only when `fitSignal` changes (i.e. a
 *     filter, city, or search change — an intentional new result set);
 *   - `flyTo` ({lng, lat, zoom, key}) centers the camera once per key — used
 *     to open on the user's own location. When it arrives before the first
 *     fit, it wins over that initial nationwide framing.
 * `onViewportChange({ zoom, bounds })` fires (debounced) after the user pans or
 * zooms, so the parent can lazy-load whatever is now in view.
 */
export default function RestaurantMap({ restaurants, onMarkerClick, onViewportChange, fitSignal, flyTo }) {
  const mapContainer = useRef(null);
  const mapRef = useRef(null);
  const restaurantsRef = useRef(restaurants);
  const onViewportChangeRef = useRef(onViewportChange);
  const [mapLoaded, setMapLoaded] = useState(false);
  const donutsRef = useRef({ markers: {}, onScreen: {} });
  const gpsCenteredRef = useRef(false);
  const didInitialFitRef = useRef(false);
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
            grade: r.vg || '?',
            color: gradeMeta(r.vg).dot,
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

    map.addSource('restaurants', {
      type: 'geojson',
      data: geojson,
      cluster: true,
      clusterMaxZoom: 14,
      clusterRadius: 46,
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
    // 'sourcedata' covers initial load and every setData; move/zoom events
    // cover cluster recomposition. ('render' alone misses source loads that
    // complete while the map is idle — it stops firing between frames.)
    const syncDonuts = () => {
      if (map.getSource('restaurants') && map.isSourceLoaded('restaurants')) updateDonuts();
    };
    map.on('sourcedata', e => {
      if (e.sourceId === 'restaurants' && e.isSourceLoaded) syncDonuts();
    });
    map.on('move', syncDonuts);
    map.on('moveend', syncDonuts);

    // Individual markers: grade color + grade letter
    map.addLayer({
      id: 'unclustered-point',
      type: 'circle',
      source: 'restaurants',
      filter: ['!', ['has', 'point_count']],
      paint: {
        'circle-color': ['get', 'color'],
        'circle-radius': ['interpolate', ['linear'], ['zoom'], 10, 7, 14, 11],
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
        'text-field': '{grade}',
        'text-size': ['interpolate', ['linear'], ['zoom'], 10, 8, 14, 12],
        'text-font': ['Montserrat Bold'],
        'text-allow-overlap': true,
      },
      paint: { 'text-color': '#ffffff' },
    });

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
