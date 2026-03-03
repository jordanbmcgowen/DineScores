/* ===== DineScores App — Firebase Edition ===== */
/* Fetches restaurant data from Firestore; falls back to window.DATA if present. */

import { firebaseConfig } from './firebase-config.js';
import { initializeApp } from 'https://www.gstatic.com/firebasejs/12.10.0/firebase-app.js';
import {
  getFirestore,
  collection,
  getDocs,
  query,
  where,
  orderBy,
  limit,
  doc,
  getDoc,
  startAfter,
  enableIndexedDbPersistence
} from 'https://www.gstatic.com/firebasejs/12.10.0/firebase-firestore.js';

/* ---------- FIREBASE INIT ---------- */
const firebaseApp = initializeApp(firebaseConfig);
const db = getFirestore(firebaseApp);

// Enable offline persistence for fast subsequent loads
// Wrapping in try/catch: fails silently in Safari private mode or when
// multiple tabs are open without multi-tab persistence enabled.
try {
  enableIndexedDbPersistence(db).catch(function (err) {
    if (err.code === 'failed-precondition') {
      // Multiple tabs open — persistence can only be enabled in one tab at a time.
      console.warn('Firestore persistence unavailable (multiple tabs open).');
    } else if (err.code === 'unimplemented') {
      // Browser does not support all required features.
      console.warn('Firestore persistence not supported in this browser.');
    }
  });
} catch (e) {
  console.warn('Firestore persistence setup error:', e);
}

/* ---------- FIRESTORE DATA FETCH ---------- */

/**
 * Map a Firestore document snapshot to the compact key format the app uses internally.
 * Firestore field names → compact keys:
 *   name → n, address → a, city → c, state → s, zip → z
 *   latitude → lt, longitude → ln, inspection_date → d
 *   original_score → os, risk_score → rs
 *   priority_violations → pv, priority_foundation_violations → pfv
 *   core_violations → cv, total_violations → tv
 *   source → src, violations → v  (each violation: [cat, sev, desc])
 *   source_url → url, id → i
 */
function mapDocToRecord(docSnap) {
  const d = docSnap.data();

  // Map violations array: Firestore stores them as objects or arrays.
  // Support both { category, severity, description } objects and raw [cat, sev, desc] arrays.
  let violations = [];
  if (Array.isArray(d.violations)) {
    violations = d.violations.map(function (v) {
      if (Array.isArray(v)) return v; // already compact
      return [v.category || 'unclassified', v.severity || 'core', v.description || ''];
    });
  }

  return {
    i:   d.id        !== undefined ? d.id        : docSnap.id,
    n:   d.name      || '',
    a:   d.address   || '',
    c:   d.city      || '',
    s:   d.state     || '',
    z:   d.zip       || '',
    lt:  d.latitude  || 0,
    ln:  d.longitude || 0,
    d:   d.inspection_date || '',
    os:  d.original_score  || 0,
    rs:  d.risk_score      || 0,
    pv:  d.priority_violations            || 0,
    pfv: d.priority_foundation_violations || 0,
    cv:  d.core_violations                || 0,
    tv:  d.total_violations               || 0,
    src: d.source     || '',
    url: d.source_url || '',
    ic:  d.inspection_count || 1,
    v:   violations,
    m:   d.metro || ''
  };
}

async function fetchAllRestaurants() {
  const snapshot = await getDocs(collection(db, 'restaurants'));
  return snapshot.docs.map(mapDocToRecord);
}

/**
 * Fetch inspection history for a specific restaurant from the subcollection.
 * Returns array of inspection records sorted by date descending.
 */
async function fetchInspectionHistory(restaurantId) {
  try {
    const snap = await getDocs(
      query(
        collection(db, 'restaurants', restaurantId, 'inspections'),
        orderBy('inspection_date', 'desc')
      )
    );
    return snap.docs.map(function (d) {
      const data = d.data();
      return {
        id:   d.id,
        date: data.inspection_date || '',
        rs:   data.risk_score || 0,
        os:   data.original_score,
        pv:   data.priority_violations || 0,
        pfv:  data.priority_foundation_violations || 0,
        cv:   data.core_violations || 0,
        tv:   data.total_violations || 0,
        type: data.inspection_type || '',
        result: data.results || '',
        v:    Array.isArray(data.violations) ? data.violations : [],
      };
    });
  } catch (e) {
    console.warn('Failed to fetch inspection history:', e);
    return [];
  }
}

/* ---------- CONSTANTS ---------- */
const CATEGORY_META = {
  temperature_control: { label: 'Temperature', icon: '🌡️' },
  food_handling:       { label: 'Food Handling', icon: '🍽️' },
  personal_hygiene:    { label: 'Hygiene', icon: '🧼' },
  cross_contamination: { label: 'Cross-Contamination', icon: '⚠️' },
  equipment_utensils:  { label: 'Equipment', icon: '🔧' },
  water_sewage:        { label: 'Water & Plumbing', icon: '💧' },
  facility_design:     { label: 'Facility', icon: '🏗️' },
  general_cleanliness: { label: 'Cleanliness', icon: '✨' },
  pest_control:        { label: 'Pests', icon: '🐛' },
  maintenance:         { label: 'Maintenance', icon: '🔨' },
  waste_management:    { label: 'Waste', icon: '🗑️' },
  employee_training:   { label: 'Training', icon: '📋' },
  food_source:         { label: 'Food Source', icon: '📦' },
  unclassified:        { label: 'Other', icon: '📎' }
};

const SEVERITY_LABELS = {
  priority:            'Priority',
  priority_foundation: 'Priority Foundation',
  core:                'Core'
};

const DEBOUNCE_MS = 300;

/* ---------- HELPERS ---------- */
function getTier(score) {
  if (score >= 90) return { tier: 'very-safe', label: 'Very Safe', grade: 'A' };
  if (score >= 80) return { tier: 'safe',      label: 'Safe',      grade: 'B' };
  if (score >= 70) return { tier: 'moderate',  label: 'Moderate',  grade: 'C' };
  if (score >= 60) return { tier: 'elevated',  label: 'Elevated',  grade: 'D' };
  return              { tier: 'risk',      label: 'High Risk', grade: 'F' };
}

function tierColor(score) {
  if (score >= 90) return '#22c55e';
  if (score >= 80) return '#84cc16';
  if (score >= 70) return '#eab308';
  if (score >= 60) return '#f97316';
  return '#ef4444';
}

function tierClass(score) {
  return 'tier-' + getTier(score).tier;
}

function formatDate(d) {
  if (!d) return 'N/A';
  const parts = d.split('-');
  if (parts.length < 3) return d;
  const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  return months[parseInt(parts[1], 10) - 1] + ' ' + parseInt(parts[2], 10) + ', ' + parts[0];
}

function debounce(fn, ms) {
  let t;
  return function (...args) { clearTimeout(t); t = setTimeout(() => fn.apply(this, args), ms); };
}

function escapeHtml(s) {
  if (!s) return '';
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}

/* ---------- LOADING STATE ---------- */
function showLoading() {
  const overlay = document.getElementById('loading-overlay');
  if (overlay) overlay.hidden = false;
}

function hideLoading() {
  const overlay = document.getElementById('loading-overlay');
  if (!overlay) return;
  overlay.classList.add('loading-fade-out');

  function removeOverlay() {
    overlay.hidden = true;
    overlay.style.display = 'none';
    overlay.classList.remove('loading-fade-out');
    // Fully remove from DOM so nothing can block interaction
    if (overlay.parentNode) overlay.parentNode.removeChild(overlay);
  }

  // Try to wait for the CSS fade-out transition
  overlay.addEventListener('transitionend', function handler() {
    overlay.removeEventListener('transitionend', handler);
    removeOverlay();
  });

  // Aggressive fallback: force-remove after 600ms regardless
  setTimeout(removeOverlay, 600);
}

function setLastUpdated() {
  const el = document.getElementById('last-updated');
  if (!el) return;
  const now = new Date();
  const timeStr = now.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  el.textContent = 'Updated ' + timeStr;
  el.hidden = false;
}

/* ---------- STATE ---------- */
let allData      = [];
let filtered     = [];
let currentSort  = 'score-desc';
let currentCity  = 'all';
let currentMetro = null;  // null or 'DFW'
let currentRisk  = 'all';
let searchQuery  = '';
let selectedId   = null;
let map          = null;
let isMobile     = window.innerWidth < 768;
let sheetState   = 'collapsed';

/* ---------- INIT ---------- */
async function init() {
  showLoading();

  try {
    // Backward-compat: use embedded DATA if already on the page
    if (window.DATA && Array.isArray(window.DATA) && window.DATA.length > 0) {
      allData = window.DATA.slice();
    } else {
      allData = await fetchAllRestaurants();
    }
  } catch (err) {
    console.error('Failed to fetch restaurant data:', err);
    // Surface a user-friendly error inside the loading overlay
    const loadingText = document.querySelector('.loading-text');
    if (loadingText) {
      loadingText.textContent = 'Failed to load data. Please refresh.';
      loadingText.style.color = 'var(--color-risk)';
    }
    return; // Leave loading overlay up so the user sees the error
  }

  hideLoading();
  setLastUpdated();

  filtered = allData.slice();
  sortData();
  initMap();
  bindEvents();
  renderList();
  updateCount();
}

/* ---------- MAP ---------- */
function initMap() {
  try {
    map = new maplibregl.Map({
      container: 'map',
      style: {
        version: 8,
        sources: {
          'carto-light': {
            type: 'raster',
            tiles: [
              'https://a.basemaps.cartocdn.com/light_all/{z}/{x}/{y}@2x.png',
              'https://b.basemaps.cartocdn.com/light_all/{z}/{x}/{y}@2x.png',
              'https://c.basemaps.cartocdn.com/light_all/{z}/{x}/{y}@2x.png'
            ],
            tileSize: 256,
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright" target="_blank">OpenStreetMap</a> &copy; <a href="https://carto.com/" target="_blank">CARTO</a>'
          }
        },
        layers: [{
          id: 'carto-light-layer',
          type: 'raster',
          source: 'carto-light',
          minzoom: 0,
          maxzoom: 20
        }]
      },
      center: [-96, 37.5],
      zoom: 3.5,
      attributionControl: true
    });

    map.on('load', addMapData);

    map.on('error', function (e) {
      console.warn('Map error:', e);
    });
  } catch (err) {
    console.error('Map init failed:', err);
    document.getElementById('map').innerHTML =
      '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:#94a3b8;font-size:14px;">Map loading...</div>';
  }
}

function addMapData() {
  const geojson = makeGeoJSON(allData);

  map.addSource('restaurants', {
    type: 'geojson',
    data: geojson,
    cluster: true,
    clusterMaxZoom: 14,
    clusterRadius: 50
  });

  // Cluster circles
  map.addLayer({
    id: 'clusters',
    type: 'circle',
    source: 'restaurants',
    filter: ['has', 'point_count'],
    paint: {
      'circle-color': '#0891b2',
      'circle-radius': ['step', ['get', 'point_count'], 20, 20, 26, 100, 34, 500, 42],
      'circle-stroke-width': 3,
      'circle-stroke-color': '#ffffff'
    }
  });

  // Cluster count text
  map.addLayer({
    id: 'cluster-count',
    type: 'symbol',
    source: 'restaurants',
    filter: ['has', 'point_count'],
    layout: {
      'text-field': '{point_count_abbreviated}',
      'text-size': 13
    },
    paint: { 'text-color': '#ffffff' }
  });

  // Individual points
  map.addLayer({
    id: 'unclustered-point',
    type: 'circle',
    source: 'restaurants',
    filter: ['!', ['has', 'point_count']],
    paint: {
      'circle-color': ['get', 'color'],
      'circle-radius': 8,
      'circle-stroke-width': 2.5,
      'circle-stroke-color': '#ffffff'
    }
  });

  // Click cluster → zoom
  map.on('click', 'clusters', function (e) {
    const features  = map.queryRenderedFeatures(e.point, { layers: ['clusters'] });
    const clusterId = features[0].properties.cluster_id;
    map.getSource('restaurants').getClusterExpansionZoom(clusterId, function (err, zoom) {
      if (err) return;
      map.easeTo({ center: features[0].geometry.coordinates, zoom: zoom });
    });
  });

  // Click point → show card
  map.on('click', 'unclustered-point', function (e) {
    const props = e.features[0].properties;
    selectRestaurant(props.id, true);
  });

  // Hover cursors
  map.on('mouseenter', 'clusters',          function () { map.getCanvas().style.cursor = 'pointer'; });
  map.on('mouseleave', 'clusters',          function () { map.getCanvas().style.cursor = ''; });
  map.on('mouseenter', 'unclustered-point', function () { map.getCanvas().style.cursor = 'pointer'; });
  map.on('mouseleave', 'unclustered-point', function () { map.getCanvas().style.cursor = ''; });

  // Desktop hover popup
  let hoverPopup = null;
  if (!isMobile) {
    map.on('mouseenter', 'unclustered-point', function (e) {
      const p = e.features[0].properties;
      const t = getTier(p.score);
      hoverPopup = new maplibregl.Popup({ closeButton: false, closeOnClick: false, offset: 12 })
        .setLngLat(e.features[0].geometry.coordinates)
        .setHTML(
          '<div style="font-weight:600;font-size:14px;margin-bottom:2px">' + escapeHtml(p.name) + '</div>' +
          '<div style="color:#64748b;font-size:12px;margin-bottom:4px">' + escapeHtml(p.address) + ', ' + escapeHtml(p.city) + '</div>' +
          '<div style="font-weight:700;font-size:14px;color:' + p.color + '">' + p.score + ' &mdash; ' + t.grade + ' (' + t.label + ')</div>'
        )
        .addTo(map);
    });
    map.on('mouseleave', 'unclustered-point', function () {
      if (hoverPopup) { hoverPopup.remove(); hoverPopup = null; }
    });
  }

  // Click on map background to dismiss
  map.on('click', function (e) {
    const features = map.queryRenderedFeatures(e.point, { layers: ['unclustered-point', 'clusters'] });
    if (features.length === 0 && isMobile) {
      hideMarkerCard();
    }
  });

  fitBoundsToData();
}

function makeGeoJSON(records) {
  return {
    type: 'FeatureCollection',
    features: records
      .filter(function (r) { return r.lt && r.ln && (r.lt !== 0 || r.ln !== 0); })
      .map(function (r) {
      return {
        type: 'Feature',
        geometry: { type: 'Point', coordinates: [r.ln, r.lt] },
        properties: {
          id:      r.i,
          name:    r.n,
          score:   r.rs,
          address: r.a,
          city:    r.c,
          color:   tierColor(r.rs)
        }
      };
    })
  };
}

function fitBoundsToData() {
  if (!map || filtered.length === 0) return;
  const bounds = new maplibregl.LngLatBounds();
  filtered.forEach(function (r) { bounds.extend([r.ln, r.lt]); });
  const padding = isMobile
    ? { top: 20, bottom: 100, left: 20, right: 20 }
    : { top: 40, bottom: 40, left: 40, right: 40 };
  map.fitBounds(bounds, { padding: padding, maxZoom: 15, duration: 600 });
}

function updateMapData() {
  if (!map || !map.getSource('restaurants')) return;
  const ids = {};
  filtered.forEach(function (r) { ids[r.i] = true; });
  const visible = allData.filter(function (r) { return ids[r.i]; });
  map.getSource('restaurants').setData(makeGeoJSON(visible));
}

/* ---------- FILTERING & SORTING ---------- */
function applyFilters() {
  const q = searchQuery.toLowerCase().trim();
  filtered = allData.filter(function (r) {
    // Metro/city filter
    if (currentMetro) {
      if (r.m !== currentMetro) return false;
      if (currentCity !== 'all' && r.c !== currentCity) return false;
    } else if (currentCity !== 'all') {
      if (r.c !== currentCity) return false;
    }
    if (currentRisk === 'safe'     && r.rs < 90) return false;
    if (currentRisk === 'moderate' && (r.rs < 70 || r.rs >= 90)) return false;
    if (currentRisk === 'risk'     && r.rs >= 70) return false;
    if (q && r.n.toLowerCase().indexOf(q) === -1 && r.a.toLowerCase().indexOf(q) === -1) return false;
    return true;
  });
  sortData();
  renderList();
  updateCount();
  updateMapData();
}

function sortData() {
  filtered.sort(function (a, b) {
    if (currentSort === 'score-desc') return b.rs - a.rs;
    if (currentSort === 'score-asc')  return a.rs - b.rs;
    return a.n.localeCompare(b.n);
  });
}

function updateCount() {
  const text = filtered.length + ' restaurant' + (filtered.length !== 1 ? 's' : '');
  const rc = document.getElementById('result-count');
  const sc = document.getElementById('sheet-count');
  if (rc) rc.textContent = text;
  if (sc) sc.textContent = text;
}

/* ---------- LIST RENDERING ---------- */
function createListItemHTML(r) {
  const t = getTier(r.rs);
  const violText = r.tv > 0 ? r.tv + ' violation' + (r.tv !== 1 ? 's' : '') : 'No violations';
  return '<div class="rest-item' + (selectedId === r.i ? ' highlighted' : '') + '" data-id="' + r.i + '" role="listitem">' +
    '<div class="rest-item-score">' +
      '<div class="score-badge ' + tierClass(r.rs) + '">' +
        '<span>' + r.rs + '</span>' +
        '<span class="grade">' + t.grade + '</span>' +
      '</div>' +
    '</div>' +
    '<div class="rest-item-info">' +
      '<div class="rest-item-name">' + escapeHtml(r.n) + '</div>' +
      '<div class="rest-item-meta">' + escapeHtml(r.a) + ', ' + escapeHtml(r.c) + '</div>' +
      '<div class="rest-item-violations">' + violText + '</div>' +
    '</div>' +
    '<div class="rest-item-arrow">' +
      '<svg viewBox="0 0 20 20" width="16" height="16" fill="none"><path d="M7 4l6 6-6 6" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg>' +
    '</div>' +
  '</div>';
}

function renderList() {
  let html = '';
  if (filtered.length === 0) {
    html = '<div class="list-empty"><div class="list-empty-icon">🔍</div><div class="list-empty-text">No restaurants found</div></div>';
  } else {
    for (let i = 0; i < filtered.length; i++) {
      html += createListItemHTML(filtered[i]);
    }
  }
  const desktopList = document.getElementById('restaurant-list');
  const mobileList  = document.getElementById('mobile-restaurant-list');
  if (desktopList) desktopList.innerHTML = html;
  if (mobileList)  mobileList.innerHTML  = html;
}

/* ---------- DETAIL RENDERING ---------- */
function renderDetail(r) {
  const t           = getTier(r.rs);
  const color       = tierColor(r.rs);
  const circumference = 2 * Math.PI * 48;
  const offset      = circumference - (r.rs / 100) * circumference;

  // Group violations by category
  const groups = {};
  (r.v || []).forEach(function (v) {
    const cat = v[0] || 'unclassified';
    if (!groups[cat]) groups[cat] = [];
    groups[cat].push({ severity: v[1], description: v[2] });
  });

  const severityOrder = { priority: 0, priority_foundation: 1, core: 2 };
  let violationsHTML = '';
  const catKeys = Object.keys(groups);

  if (catKeys.length === 0) {
    violationsHTML = '<div class="no-violations"><div class="no-violations-icon">✅</div><div>No violations found</div></div>';
  } else {
    catKeys.sort(function (a, b) {
      let aSev = 3, bSev = 3;
      groups[a].forEach(function (v) { const o = severityOrder[v.severity]; if (o !== undefined && o < aSev) aSev = o; });
      groups[b].forEach(function (v) { const o = severityOrder[v.severity]; if (o !== undefined && o < bSev) bSev = o; });
      return aSev - bSev;
    });

    catKeys.forEach(function (cat) {
      const meta  = CATEGORY_META[cat] || CATEGORY_META.unclassified;
      const items = groups[cat].sort(function (a, b) {
        return (severityOrder[a.severity] || 3) - (severityOrder[b.severity] || 3);
      });
      let itemsHTML = '';
      items.forEach(function (v) {
        const sevLabel = SEVERITY_LABELS[v.severity] || v.severity;
        itemsHTML +=
          '<div class="violation-item severity-' + v.severity + '">' +
            '<div class="violation-severity-tag">' + escapeHtml(sevLabel) + '</div>' +
            '<div>' + escapeHtml(v.description) + '</div>' +
          '</div>';
      });
      violationsHTML +=
        '<div class="violation-group">' +
          '<div class="violation-group-header">' +
            '<span class="vg-icon">' + meta.icon + '</span>' +
            '<span>' + meta.label + '</span>' +
            '<span style="color:var(--color-text-muted);font-weight:400;font-size:var(--text-xs)">(' + items.length + ')</span>' +
          '</div>' +
          itemsHTML +
        '</div>';
    });
  }

  const urlLink = r.url
    ? '<a href="' + escapeHtml(r.url) + '" target="_blank" rel="noopener noreferrer" class="detail-source-link">View Original Inspection Report →</a>'
    : '';

  // Inspection history section (shown if ic > 1; loads dynamically)
  const historySection = (r.ic && r.ic > 1)
    ? '<div class="inspection-history-section" id="history-section-' + r.i + '">' +
        '<div class="history-section-header">' +
          '<span class="history-icon">📊</span>' +
          '<span>Inspection History</span>' +
          '<span class="history-count-badge">' + r.ic + ' inspections</span>' +
        '</div>' +
        '<div class="history-loading" id="history-list-' + r.i + '">Loading history...</div>' +
      '</div>'
    : '';

  return (
    '<div class="score-ring-container">' +
      '<div class="score-ring">' +
        '<svg viewBox="0 0 120 120">' +
          '<circle cx="60" cy="60" r="48" fill="none" stroke="var(--color-border-light)" stroke-width="8"/>' +
          '<circle cx="60" cy="60" r="48" fill="none" stroke="' + color + '" stroke-width="8"' +
          ' stroke-dasharray="' + circumference + '" stroke-dashoffset="' + offset + '"' +
          ' stroke-linecap="round" transform="rotate(-90 60 60)"' +
          ' style="transition:stroke-dashoffset 0.8s cubic-bezier(0.16,1,0.3,1)"/>' +
        '</svg>' +
        '<div class="score-ring-label">' +
          '<div class="score-ring-number" style="color:' + color + '">' + r.rs + '</div>' +
          '<div class="score-ring-grade"  style="color:' + color + '">' + t.grade + '</div>' +
        '</div>' +
      '</div>' +
      '<div class="score-ring-tier">' + t.label + '</div>' +
    '</div>' +
    '<h2 class="detail-title">' + escapeHtml(r.n) + '</h2>' +
    '<div class="detail-meta">' +
      '<div class="detail-meta-row"><span class="detail-meta-icon">📍</span><div><div class="detail-meta-label">Address</div><div class="detail-meta-value">' + escapeHtml(r.a) + ', ' + escapeHtml(r.c) + ', ' + escapeHtml(r.s) + ' ' + escapeHtml(r.z) + '</div></div></div>' +
      '<div class="detail-meta-row"><span class="detail-meta-icon">📅</span><div><div class="detail-meta-label">Inspection Date</div><div class="detail-meta-value">' + formatDate(r.d) + '</div></div></div>' +
      '<div class="detail-meta-row"><span class="detail-meta-icon">🏷️</span><div><div class="detail-meta-label">Source</div><div class="detail-meta-value">' + escapeHtml(r.src || 'N/A') + '</div></div></div>' +
    '</div>' +
    '<div class="violation-stats">' +
      '<div class="vstat"><div class="vstat-num priority">'   + r.pv  + '</div><div class="vstat-label">Priority</div></div>' +
      '<div class="vstat"><div class="vstat-num foundation">' + r.pfv + '</div><div class="vstat-label">Foundation</div></div>' +
      '<div class="vstat"><div class="vstat-num core">'       + r.cv  + '</div><div class="vstat-label">Core</div></div>' +
      '<div class="vstat"><div class="vstat-num total">'      + r.tv  + '</div><div class="vstat-label">Total</div></div>' +
    '</div>' +
    '<div class="violation-section">' +
      '<div class="violation-section-title">Violations</div>' +
      violationsHTML +
    '</div>' +
    urlLink +
    historySection
  );
}

/* ---------- INSPECTION HISTORY ---------- */
async function loadInspectionHistory(restaurantId) {
  const listEl = document.getElementById('history-list-' + restaurantId);
  if (!listEl) return;

  const history = await fetchInspectionHistory(restaurantId);

  if (!history || history.length === 0) {
    listEl.innerHTML = '<div class="history-empty">No additional inspection records available.</div>';
    return;
  }

  let html = '<div class="history-timeline">';
  history.forEach(function (insp, idx) {
    const color = tierColor(insp.rs || 0);
    const t     = getTier(insp.rs || 0);
    const isLatest = idx === 0;
    html +=
      '<div class="history-entry' + (isLatest ? ' history-entry-latest' : '') + '">' +
        '<div class="history-entry-dot" style="background:' + color + '"></div>' +
        '<div class="history-entry-body">' +
          '<div class="history-entry-date">' +
            formatDate(insp.date) +
            (isLatest ? ' <span class="history-latest-tag">Latest</span>' : '') +
          '</div>' +
          '<div class="history-entry-score" style="color:' + color + '">' +
            (insp.rs || 0) + ' — ' + t.grade + ' (' + t.label + ')' +
          '</div>' +
          (insp.tv > 0
            ? '<div class="history-entry-violations">' +
                insp.pv + ' priority · ' + insp.pfv + ' foundation · ' + insp.cv + ' core' +
              '</div>'
            : '<div class="history-entry-violations">No violations</div>') +
          (insp.type ? '<div class="history-entry-type">' + escapeHtml(insp.type) + '</div>' : '') +
        '</div>' +
      '</div>';
  });
  html += '</div>';
  listEl.innerHTML = html;
}

/* ---------- MARKER CARD (Mobile) ---------- */
function showMarkerCard(r) {
  const t        = getTier(r.rs);
  const violText = r.tv > 0 ? r.tv + ' violation' + (r.tv !== 1 ? 's' : '') : 'No violations';

  document.getElementById('marker-card-content').innerHTML =
    '<div class="mc-header">' +
      '<div class="score-badge ' + tierClass(r.rs) + '" style="width:52px;height:52px;font-size:var(--text-xl)">' +
        '<span>' + r.rs + '</span><span class="grade">' + t.grade + '</span>' +
      '</div>' +
      '<div class="mc-info">' +
        '<div class="mc-name">'    + escapeHtml(r.n) + '</div>' +
        '<div class="mc-address">' + escapeHtml(r.a) + ', ' + escapeHtml(r.c) + '</div>' +
      '</div>' +
    '</div>' +
    '<div class="mc-violations">' + violText + '</div>' +
    '<button class="mc-detail-btn" data-id="' + r.i + '">View Details</button>';

  document.getElementById('marker-card').hidden = false;
  setSheetState('collapsed');
}

function hideMarkerCard() {
  document.getElementById('marker-card').hidden = true;
}

/* ---------- DETAIL VIEWS ---------- */
function showDetailMobile(r) {
  document.getElementById('detail-overlay-content').innerHTML = renderDetail(r);
  document.getElementById('detail-overlay').hidden = false;
  hideMarkerCard();
}

function hideDetailMobile() {
  document.getElementById('detail-overlay').hidden = true;
}

function showDetailDesktop(r) {
  document.getElementById('sidebar-detail-content').innerHTML = renderDetail(r);
  document.getElementById('sidebar-list-view').classList.remove('active');
  document.getElementById('sidebar-detail-view').classList.add('active');
}

function hideDetailDesktop() {
  document.getElementById('sidebar-detail-view').classList.remove('active');
  document.getElementById('sidebar-list-view').classList.add('active');
}

/* ---------- SELECT RESTAURANT ---------- */
function selectRestaurant(id, fromMap) {
  let r = null;
  for (let i = 0; i < allData.length; i++) {
    if (allData[i].i === id) { r = allData[i]; break; }
  }
  if (!r) return;
  selectedId = id;

  if (isMobile) {
    if (fromMap) {
      showMarkerCard(r);
      map.easeTo({ center: [r.ln, r.lt], duration: 400 });
    } else {
      showDetailMobile(r);
      if (r.ic && r.ic > 1) loadInspectionHistory(r.i);
    }
  } else {
    showDetailDesktop(r);
    map.flyTo({ center: [r.ln, r.lt], zoom: 15, duration: 800 });
    if (r.ic && r.ic > 1) loadInspectionHistory(r.i);
  }
}

/* ---------- BOTTOM SHEET ---------- */
function setSheetState(state) {
  const sheet = document.getElementById('bottom-sheet');
  sheet.dataset.state = state;
  sheet.className     = 'bottom-sheet ' + state;
  sheetState          = state;
}

function initSheetDrag() {
  const sheet  = document.getElementById('bottom-sheet');
  const handle = document.getElementById('sheet-handle');
  let startY        = 0;
  let startSheetTop = 0;
  let isDragging    = false;

  function onStart(e) {
    isDragging    = true;
    startY        = e.touches ? e.touches[0].clientY : e.clientY;
    startSheetTop = sheet.getBoundingClientRect().top;
    sheet.style.transition = 'none';
  }

  function onMove(e) {
    if (!isDragging) return;
    const y      = e.touches ? e.touches[0].clientY : e.clientY;
    const delta  = y - startY;
    let newTop   = startSheetTop + delta;
    const minTop = 104; // topbar + filter
    const maxTop = window.innerHeight - 80;
    newTop = Math.max(minTop, Math.min(maxTop, newTop));
    const translateY = newTop - sheet.offsetTop;
    sheet.style.transform = 'translateY(' + translateY + 'px)';
  }

  function onEnd() {
    if (!isDragging) return;
    isDragging = false;
    sheet.style.transition = '';
    sheet.style.transform  = '';

    const rect        = sheet.getBoundingClientRect();
    const viewH       = window.innerHeight;
    const sheetVisible = viewH - rect.top;
    const maxH        = viewH - 104;

    if (sheetVisible < maxH * 0.25) {
      setSheetState('collapsed');
    } else if (sheetVisible < maxH * 0.65) {
      setSheetState('half');
    } else {
      setSheetState('expanded');
    }
  }

  handle.addEventListener('touchstart', onStart, { passive: true });
  document.addEventListener('touchmove',  onMove,  { passive: true });
  document.addEventListener('touchend',   onEnd);
  handle.addEventListener('mousedown',    onStart);
  document.addEventListener('mousemove',  onMove);
  document.addEventListener('mouseup',    onEnd);

  // Tap handle to cycle states
  handle.addEventListener('click', function () {
    if      (sheetState === 'collapsed') setSheetState('half');
    else if (sheetState === 'half')      setSheetState('expanded');
    else                                 setSheetState('collapsed');
  });
}

/* ---------- EVENTS ---------- */
function bindEvents() {
  // Search
  const searchInput   = document.getElementById('search-input');
  const searchClear   = document.getElementById('search-clear');
  const debouncedSearch = debounce(function () {
    searchQuery        = searchInput.value;
    searchClear.hidden = !searchQuery;
    applyFilters();
  }, DEBOUNCE_MS);
  searchInput.addEventListener('input', debouncedSearch);
  searchClear.addEventListener('click', function () {
    searchInput.value = '';
    searchQuery       = '';
    searchClear.hidden = true;
    applyFilters();
  });

  // City / Metro chips
  document.querySelectorAll('.city-chips .chip').forEach(function (chip) {
    chip.addEventListener('click', function () {
      document.querySelectorAll('.city-chips .chip').forEach(function (c) { c.classList.remove('active'); });
      chip.classList.add('active');

      var dfwSubChips = document.getElementById('dfw-sub-chips');

      if (chip.dataset.metro === 'DFW') {
        // DFW metro chip — show all DFW cities, reveal sub-chips
        currentMetro = 'DFW';
        currentCity = 'all';
        buildDfwSubChips();
        if (dfwSubChips) dfwSubChips.hidden = false;
      } else {
        // Regular city chip or "All Cities"
        currentMetro = null;
        currentCity = chip.dataset.city;
        if (dfwSubChips) dfwSubChips.hidden = true;
      }

      applyFilters();
      if (filtered.length > 0) fitBoundsToData();
    });
  });

  function buildDfwSubChips() {
    var container = document.getElementById('dfw-sub-chips');
    if (!container) return;
    var dfwCities = [];
    var seen = {};
    allData.forEach(function (r) {
      if (r.m === 'DFW' && !seen[r.c]) {
        seen[r.c] = true;
        dfwCities.push(r.c);
      }
    });
    dfwCities.sort();

    var html = '<button class="chip active" data-dfw-city="all">All DFW</button>';
    dfwCities.forEach(function (c) {
      html += '<button class="chip" data-dfw-city="' + c + '">' + c + '</button>';
    });
    container.innerHTML = html;

    container.querySelectorAll('.chip').forEach(function (sub) {
      sub.addEventListener('click', function () {
        container.querySelectorAll('.chip').forEach(function (s) { s.classList.remove('active'); });
        sub.classList.add('active');
        currentCity = sub.dataset.dfwCity === 'all' ? 'all' : sub.dataset.dfwCity;
        applyFilters();
        if (filtered.length > 0) fitBoundsToData();
      });
    });
  }

  // Risk chips
  document.querySelectorAll('.risk-chips .chip').forEach(function (chip) {
    chip.addEventListener('click', function () {
      document.querySelectorAll('.risk-chips .chip').forEach(function (c) { c.classList.remove('active'); });
      chip.classList.add('active');
      currentRisk = chip.dataset.risk;
      applyFilters();
    });
  });

  // Desktop sort
  document.getElementById('sort-select').addEventListener('change', function (e) {
    currentSort = e.target.value;
    sortData();
    renderList();
  });

  // Mobile sort
  document.getElementById('sheet-sort-btn').addEventListener('click', function () {
    const popup = document.getElementById('sort-popup');
    popup.hidden = !popup.hidden;
  });

  document.querySelectorAll('.sort-option').forEach(function (opt) {
    opt.addEventListener('click', function () {
      document.querySelectorAll('.sort-option').forEach(function (o) { o.classList.remove('active'); });
      opt.classList.add('active');
      currentSort = opt.dataset.sort;
      sortData();
      renderList();
      document.getElementById('sort-popup').hidden = true;
    });
  });

  // List item clicks (delegation)
  document.addEventListener('click', function (e) {
    const item = e.target.closest('.rest-item');
    if (item) {
      selectRestaurant(item.dataset.id, false);
      return;
    }
    const detailBtn = e.target.closest('.mc-detail-btn');
    if (detailBtn) {
      for (let i = 0; i < allData.length; i++) {
        if (allData[i].i === detailBtn.dataset.id) { showDetailMobile(allData[i]); break; }
      }
      return;
    }
  });

  // Marker card close
  document.getElementById('marker-card-close').addEventListener('click', function () {
    hideMarkerCard();
    selectedId = null;
  });

  // Mobile detail back
  document.getElementById('detail-back-btn').addEventListener('click', function () {
    hideDetailMobile();
    selectedId = null;
  });

  // Desktop detail back
  document.getElementById('sidebar-back-btn').addEventListener('click', function () {
    hideDetailDesktop();
    selectedId = null;
  });

  // Locate button
  document.getElementById('locate-btn').addEventListener('click', function () {
    if ('geolocation' in navigator) {
      navigator.geolocation.getCurrentPosition(
        function (pos) {
          map.flyTo({ center: [pos.coords.longitude, pos.coords.latitude], zoom: 14 });
        },
        function () {},
        { enableHighAccuracy: true, timeout: 5000 }
      );
    }
  });

  // Bottom sheet (mobile only)
  if (isMobile) {
    initSheetDrag();
  }

  // Resize
  window.addEventListener('resize', debounce(function () {
    const wasMobile = isMobile;
    isMobile = window.innerWidth < 768;
    if (wasMobile !== isMobile) {
      selectedId = null;
      hideDetailMobile();
      hideDetailDesktop();
      hideMarkerCard();
      renderList();
      if (isMobile) initSheetDrag();
    }
  }, 200));
}

/* ---------- BOOT ---------- */
// ES modules are deferred by default; DOMContentLoaded will already have fired
// if the script tag is at the bottom of <body>, so guard both cases.
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', init);
} else {
  init();
}
