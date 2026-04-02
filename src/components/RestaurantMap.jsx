import React, { useEffect, useRef, useState } from 'react';
import maplibregl from 'maplibre-gl';

/**
 * MapLibre GL JS map with color-coded markers based on vetted_grade.
 * Keeps the Firebase edition's clustering and CARTO basemap.
 */
export default function RestaurantMap({ restaurants, onMarkerClick }) {
  const mapContainer = useRef(null);
  const mapRef = useRef(null);
  const [mapLoaded, setMapLoaded] = useState(false);

  // Marker color by vetted grade
  function gradeColor(grade) {
    switch (grade) {
      case 'A': return '#22c55e'; // green
      case 'B': return '#84cc16'; // lime
      case 'C': return '#eab308'; // yellow
      case 'F': return '#ef4444'; // red
      default: return '#94a3b8';  // gray
    }
  }

  // Initialize map
  useEffect(() => {
    if (mapRef.current) return;

    const map = new maplibregl.Map({
      container: mapContainer.current,
      style: {
        version: 8,
        sources: {
          'carto-light': {
            type: 'raster',
            tiles: [
              'https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}@2x.png',
              'https://b.basemaps.cartocdn.com/light_all/{z}/{x}/{y}@2x.png',
            ],
            tileSize: 256,
            attribution: '&copy; OpenStreetMap &copy; CARTO',
          },
        },
        layers: [{ id: 'carto-light', type: 'raster', source: 'carto-light' }],
      },
      center: [-95.5, 37.5], // Center of US
      zoom: 4,
      maxZoom: 18,
    });

    // Dark mode tiles
    if (window.matchMedia('(prefers-color-scheme: dark)').matches) {
      map.setStyle({
        version: 8,
        sources: {
          'carto-dark': {
            type: 'raster',
            tiles: [
              'https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png',
              'https://b.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png',
            ],
            tileSize: 256,
          },
        },
        layers: [{ id: 'carto-dark', type: 'raster', source: 'carto-dark' }],
      });
    }

    map.addControl(new maplibregl.NavigationControl(), 'top-right');

    map.on('load', () => {
      setMapLoaded(true);
    });

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
            name: r.n,
            grade: r.vg || 'C',
            score: r.ws || r.rs || 0,
            address: r.a,
            city: r.c,
            color: gradeColor(r.vg),
          },
        })),
    };

    // Remove existing layers/source
    if (map.getLayer('clusters')) map.removeLayer('clusters');
    if (map.getLayer('cluster-count')) map.removeLayer('cluster-count');
    if (map.getLayer('unclustered-point')) map.removeLayer('unclustered-point');
    if (map.getSource('restaurants')) map.removeSource('restaurants');

    map.addSource('restaurants', {
      type: 'geojson',
      data: geojson,
      cluster: true,
      clusterMaxZoom: 14,
      clusterRadius: 50,
    });

    // Cluster circles
    map.addLayer({
      id: 'clusters',
      type: 'circle',
      source: 'restaurants',
      filter: ['has', 'point_count'],
      paint: {
        'circle-color': [
          'step', ['get', 'point_count'],
          '#0891b2', 10,
          '#0e7490', 50,
          '#155e75',
        ],
        'circle-radius': ['step', ['get', 'point_count'], 18, 10, 24, 50, 32],
        'circle-stroke-width': 2,
        'circle-stroke-color': '#fff',
      },
    });

    // Cluster count labels
    map.addLayer({
      id: 'cluster-count',
      type: 'symbol',
      source: 'restaurants',
      filter: ['has', 'point_count'],
      layout: {
        'text-field': '{point_count_abbreviated}',
        'text-size': 12,
        'text-font': ['Open Sans Bold', 'Arial Unicode MS Bold'],
      },
      paint: { 'text-color': '#ffffff' },
    });

    // Individual markers
    map.addLayer({
      id: 'unclustered-point',
      type: 'circle',
      source: 'restaurants',
      filter: ['!', ['has', 'point_count']],
      paint: {
        'circle-color': ['get', 'color'],
        'circle-radius': 7,
        'circle-stroke-width': 2,
        'circle-stroke-color': '#fff',
      },
    });

    // Click handlers
    map.on('click', 'clusters', e => {
      const features = map.queryRenderedFeatures(e.point, { layers: ['clusters'] });
      const clusterId = features[0].properties.cluster_id;
      map.getSource('restaurants').getClusterExpansionZoom(clusterId, (err, zoom) => {
        if (err) return;
        map.easeTo({ center: features[0].geometry.coordinates, zoom: zoom });
      });
    });

    map.on('click', 'unclustered-point', e => {
      const props = e.features[0].properties;
      // Show popup
      const popup = new maplibregl.Popup({ offset: 15, maxWidth: '260px' })
        .setLngLat(e.features[0].geometry.coordinates)
        .setHTML(`
          <div style="padding:12px;font-family:Inter,system-ui,sans-serif">
            <div style="font-weight:700;font-size:14px;margin-bottom:4px">${props.name}</div>
            <div style="font-size:12px;color:#64748b;margin-bottom:6px">${props.address}</div>
            <div style="display:flex;align-items:center;gap:6px">
              <span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:${props.color}"></span>
              <span style="font-weight:600;font-size:13px">Grade ${props.grade} · Score ${props.score}</span>
            </div>
          </div>
        `)
        .addTo(map);

      // Find the restaurant and trigger click
      const r = restaurants.find(r => r.i === props.id);
      if (r && onMarkerClick) onMarkerClick(r);
    });

    map.on('mouseenter', 'clusters', () => { map.getCanvas().style.cursor = 'pointer'; });
    map.on('mouseleave', 'clusters', () => { map.getCanvas().style.cursor = ''; });
    map.on('mouseenter', 'unclustered-point', () => { map.getCanvas().style.cursor = 'pointer'; });
    map.on('mouseleave', 'unclustered-point', () => { map.getCanvas().style.cursor = ''; });

    // Fit bounds if we have data
    if (geojson.features.length > 0) {
      const bounds = new maplibregl.LngLatBounds();
      geojson.features.forEach(f => bounds.extend(f.geometry.coordinates));
      map.fitBounds(bounds, { padding: 50, maxZoom: 13, duration: 500 });
    }
  }, [restaurants, mapLoaded, onMarkerClick]);

  return (
    <div ref={mapContainer} className="w-full h-full" style={{ minHeight: '200px' }} />
  );
}
