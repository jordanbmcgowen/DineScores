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

/**
 * Full-bleed MapLibre map. Markers are colored by vetted grade AND carry the
 * grade letter, so color is never the only encoding. Clicking a marker opens
 * the inspection panel directly (no intermediate popup).
 */
export default function RestaurantMap({ restaurants, onMarkerClick }) {
  const mapContainer = useRef(null);
  const mapRef = useRef(null);
  const restaurantsRef = useRef(restaurants);
  const [mapLoaded, setMapLoaded] = useState(false);
  restaurantsRef.current = restaurants;

  // Initialize map
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
    map.on('error', e => console.error('MAP ERROR:', e.error?.message || e));
    if (import.meta.env.DEV) window.__map = map;
    mapRef.current = map;

    return () => {
      map.remove();
      mapRef.current = null;
    };
  }, []);

  // Update data source when restaurants change
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
      map.getSource('restaurants').setData(geojson);
    } else {
      map.addSource('restaurants', {
        type: 'geojson',
        data: geojson,
        cluster: true,
        clusterMaxZoom: 14,
        clusterRadius: 46,
      });

      // Cluster bubbles — neutral, count-scaled (lighter on the dark basemap)
      const darkScheme = window.matchMedia('(prefers-color-scheme: dark)').matches;
      map.addLayer({
        id: 'clusters',
        type: 'circle',
        source: 'restaurants',
        filter: ['has', 'point_count'],
        paint: {
          'circle-color': darkScheme ? '#64748b' : '#334155',
          'circle-opacity': 0.92,
          'circle-radius': ['step', ['get', 'point_count'], 16, 25, 21, 100, 26, 500, 32],
          'circle-stroke-width': 2.5,
          'circle-stroke-color': '#ffffff',
        },
      });
      map.addLayer({
        id: 'cluster-count',
        type: 'symbol',
        source: 'restaurants',
        filter: ['has', 'point_count'],
        layout: {
          'text-field': '{point_count_abbreviated}',
          'text-size': 12,
          'text-font': ['Montserrat Bold'],
        },
        paint: { 'text-color': '#ffffff' },
      });

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

      // Interactions
      map.on('click', 'clusters', e => {
        const features = map.queryRenderedFeatures(e.point, { layers: ['clusters'] });
        const clusterId = features[0].properties.cluster_id;
        map.getSource('restaurants').getClusterExpansionZoom(clusterId, (err, zoom) => {
          if (err) return;
          map.easeTo({ center: features[0].geometry.coordinates, zoom });
        });
      });
      map.on('click', 'unclustered-point', e => {
        const props = e.features[0].properties;
        const r = restaurantsRef.current.find(x => x.i === props.id);
        if (r && onMarkerClick) onMarkerClick(r);
      });
      for (const layer of ['clusters', 'unclustered-point']) {
        map.on('mouseenter', layer, () => { map.getCanvas().style.cursor = 'pointer'; });
        map.on('mouseleave', layer, () => { map.getCanvas().style.cursor = ''; });
      }
    }

    // Fit to the current result set, keeping clear of floating UI. Bounds use
    // the central 96% of points so a few far-flung records (e.g. mobile
    // vendors registered at out-of-state addresses) don't skew the camera —
    // every record still renders on the map.
    if (geojson.features.length > 0) {
      const lngs = geojson.features.map(f => f.geometry.coordinates[0]).sort((a, b) => a - b);
      const lats = geojson.features.map(f => f.geometry.coordinates[1]).sort((a, b) => a - b);
      const lo = arr => arr[Math.floor(arr.length * 0.02)];
      const hi = arr => arr[Math.ceil(arr.length * 0.98) - 1];
      const bounds = new maplibregl.LngLatBounds(
        [lo(lngs), lo(lats)], [hi(lngs), hi(lats)]);
      const desktop = window.innerWidth >= 768;
      map.fitBounds(bounds, {
        padding: desktop
          ? { top: 150, left: 440, right: 40, bottom: 40 }
          : { top: 130, left: 30, right: 30, bottom: 130 },
        maxZoom: 13,
        duration: 600,
      });
    }
  }, [restaurants, mapLoaded, onMarkerClick]);

  return <div ref={mapContainer} className="w-full h-full" />;
}
