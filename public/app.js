/* ===== DineScores App ===== */
(function () {
  'use strict';

  /* ---------- CONSTANTS ---------- */
  const CATEGORY_META = {
    temperature_control: { label: 'Temperature', icon: '🌡️' },
    food_handling: { label: 'Food Handling', icon: '🍽️' },
    personal_hygiene: { label: 'Hygiene', icon: '🧼' },
    cross_contamination: { label: 'Cross-Contamination', icon: '⚠️' },
    equipment_utensils: { label: 'Equipment', icon: '🔧' },
    water_sewage: { label: 'Water & Plumbing', icon: '💧' },
    facility_design: { label: 'Facility', icon: '🏗️' },
    general_cleanliness: { label: 'Cleanliness', icon: '✨' },
    pest_control: { label: 'Pests', icon: '🐛' },
    maintenance: { label: 'Maintenance', icon: '🔨' },
    waste_management: { label: 'Waste', icon: '🗑️' },
    employee_training: { label: 'Training', icon: '📋' },
    food_source: { label: 'Food Source', icon: '📦' },
    unclassified: { label: 'Other', icon: '📎' }
  };

  const SEVERITY_LABELS = {
    priority: 'Priority',
    priority_foundation: 'Priority Foundation',
    core: 'Core'
  };

  const DEBOUNCE_MS = 300;

  /* ---------- HELPERS ---------- */
  function getTier(score) {
    if (score >= 90) return { tier: 'very-safe', label: 'Very Safe', grade: 'A' };
    if (score >= 80) return { tier: 'safe', label: 'Safe', grade: 'B' };
    if (score >= 70) return { tier: 'moderate', label: 'Moderate', grade: 'C' };
    if (score >= 60) return { tier: 'elevated', label: 'Elevated', grade: 'D' };
    return { tier: 'risk', label: 'High Risk', grade: 'F' };
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
    return months[parseInt(parts[1],10)-1] + ' ' + parseInt(parts[2],10) + ', ' + parts[0];
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

  /* ---------- STATE ---------- */
  let allData = [];
  let filtered = [];
  let currentSort = 'score-desc';
  let currentCity = 'all';
  let currentRisk = 'all';
  let searchQuery = '';
  let selectedId = null;
  let map = null;
  let isMobile = window.innerWidth < 768;
  let sheetState = 'collapsed';

  /* ---------- INIT ---------- */
  function init() {
    allData = DATA.slice();
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

      map.on('error', function(e) {
        console.warn('Map error:', e);
      });
    } catch (err) {
      console.error('Map init failed:', err);
      document.getElementById('map').innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100%;color:#94a3b8;font-size:14px;">Map loading...</div>';
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
    map.on('click', 'clusters', function(e) {
      var features = map.queryRenderedFeatures(e.point, { layers: ['clusters'] });
      var clusterId = features[0].properties.cluster_id;
      map.getSource('restaurants').getClusterExpansionZoom(clusterId, function(err, zoom) {
        if (err) return;
        map.easeTo({ center: features[0].geometry.coordinates, zoom: zoom });
      });
    });

    // Click point → show card
    map.on('click', 'unclustered-point', function(e) {
      var props = e.features[0].properties;
      selectRestaurant(props.id, true);
    });

    // Hover cursors
    map.on('mouseenter', 'clusters', function() { map.getCanvas().style.cursor = 'pointer'; });
    map.on('mouseleave', 'clusters', function() { map.getCanvas().style.cursor = ''; });
    map.on('mouseenter', 'unclustered-point', function() { map.getCanvas().style.cursor = 'pointer'; });
    map.on('mouseleave', 'unclustered-point', function() { map.getCanvas().style.cursor = ''; });

    // Desktop hover popup
    var hoverPopup = null;
    if (!isMobile) {
      map.on('mouseenter', 'unclustered-point', function(e) {
        var p = e.features[0].properties;
        var t = getTier(p.score);
        hoverPopup = new maplibregl.Popup({ closeButton: false, closeOnClick: false, offset: 12 })
          .setLngLat(e.features[0].geometry.coordinates)
          .setHTML(
            '<div style="font-weight:600;font-size:14px;margin-bottom:2px">' + escapeHtml(p.name) + '</div>' +
            '<div style="color:#64748b;font-size:12px;margin-bottom:4px">' + escapeHtml(p.address) + ', ' + escapeHtml(p.city) + '</div>' +
            '<div style="font-weight:700;font-size:14px;color:' + p.color + '">' + p.score + ' &mdash; ' + t.grade + ' (' + t.label + ')</div>'
          )
          .addTo(map);
      });
      map.on('mouseleave', 'unclustered-point', function() {
        if (hoverPopup) { hoverPopup.remove(); hoverPopup = null; }
      });
    }

    // Click on map background to dismiss
    map.on('click', function(e) {
      var features = map.queryRenderedFeatures(e.point, { layers: ['unclustered-point', 'clusters'] });
      if (features.length === 0 && isMobile) {
        hideMarkerCard();
      }
    });

    fitBoundsToData();
  }

  function makeGeoJSON(records) {
    return {
      type: 'FeatureCollection',
      features: records.map(function(r) {
        return {
          type: 'Feature',
          geometry: { type: 'Point', coordinates: [r.ln, r.lt] },
          properties: {
            id: r.i,
            name: r.n,
            score: r.rs,
            address: r.a,
            city: r.c,
            color: tierColor(r.rs)
          }
        };
      })
    };
  }

  function fitBoundsToData() {
    if (!map || filtered.length === 0) return;
    var bounds = new maplibregl.LngLatBounds();
    filtered.forEach(function(r) { bounds.extend([r.ln, r.lt]); });
    var padding = isMobile
      ? { top: 20, bottom: 100, left: 20, right: 20 }
      : { top: 40, bottom: 40, left: 40, right: 40 };
    map.fitBounds(bounds, { padding: padding, maxZoom: 15, duration: 600 });
  }

  function updateMapData() {
    if (!map || !map.getSource('restaurants')) return;
    var ids = {};
    filtered.forEach(function(r) { ids[r.i] = true; });
    var visible = allData.filter(function(r) { return ids[r.i]; });
    map.getSource('restaurants').setData(makeGeoJSON(visible));
  }

  /* ---------- FILTERING & SORTING ---------- */
  function applyFilters() {
    var q = searchQuery.toLowerCase().trim();
    filtered = allData.filter(function(r) {
      if (currentCity !== 'all' && r.c !== currentCity) return false;
      if (currentRisk === 'safe' && r.rs < 90) return false;
      if (currentRisk === 'moderate' && (r.rs < 70 || r.rs >= 90)) return false;
      if (currentRisk === 'risk' && r.rs >= 70) return false;
      if (q && r.n.toLowerCase().indexOf(q) === -1 && r.a.toLowerCase().indexOf(q) === -1) return false;
      return true;
    });
    sortData();
    renderList();
    updateCount();
    updateMapData();
  }

  function sortData() {
    filtered.sort(function(a, b) {
      if (currentSort === 'score-desc') return b.rs - a.rs;
      if (currentSort === 'score-asc') return a.rs - b.rs;
      return a.n.localeCompare(b.n);
    });
  }

  function updateCount() {
    var text = filtered.length + ' restaurant' + (filtered.length !== 1 ? 's' : '');
    var rc = document.getElementById('result-count');
    var sc = document.getElementById('sheet-count');
    if (rc) rc.textContent = text;
    if (sc) sc.textContent = text;
  }

  /* ---------- LIST RENDERING (no virtual list - native scroll) ---------- */
  function createListItemHTML(r) {
    var t = getTier(r.rs);
    var violText = r.tv > 0 ? r.tv + ' violation' + (r.tv !== 1 ? 's' : '') : 'No violations';
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
    var html = '';
    if (filtered.length === 0) {
      html = '<div class="list-empty"><div class="list-empty-icon">🔍</div><div class="list-empty-text">No restaurants found</div></div>';
    } else {
      for (var i = 0; i < filtered.length; i++) {
        html += createListItemHTML(filtered[i]);
      }
    }
    // Render to both containers
    var desktopList = document.getElementById('restaurant-list');
    var mobileList = document.getElementById('mobile-restaurant-list');
    if (desktopList) desktopList.innerHTML = html;
    if (mobileList) mobileList.innerHTML = html;
  }

  /* ---------- DETAIL RENDERING ---------- */
  function renderDetail(r) {
    var t = getTier(r.rs);
    var color = tierColor(r.rs);
    var circumference = 2 * Math.PI * 48;
    var offset = circumference - (r.rs / 100) * circumference;

    // Group violations by category
    var groups = {};
    (r.v || []).forEach(function(v) {
      var cat = v[0] || 'unclassified';
      if (!groups[cat]) groups[cat] = [];
      groups[cat].push({ severity: v[1], description: v[2] });
    });

    var violationsHTML = '';
    var catKeys = Object.keys(groups);
    if (catKeys.length === 0) {
      violationsHTML = '<div class="no-violations"><div class="no-violations-icon">✅</div><div>No violations found</div></div>';
    } else {
      var severityOrder = { priority: 0, priority_foundation: 1, core: 2 };
      catKeys.sort(function(a, b) {
        var aSev = 3, bSev = 3;
        groups[a].forEach(function(v) { var o = severityOrder[v.severity]; if (o !== undefined && o < aSev) aSev = o; });
        groups[b].forEach(function(v) { var o = severityOrder[v.severity]; if (o !== undefined && o < bSev) bSev = o; });
        return aSev - bSev;
      });

      catKeys.forEach(function(cat) {
        var meta = CATEGORY_META[cat] || CATEGORY_META.unclassified;
        var items = groups[cat].sort(function(a, b) {
          return (severityOrder[a.severity] || 3) - (severityOrder[b.severity] || 3);
        });
        var itemsHTML = '';
        items.forEach(function(v) {
          var sevLabel = SEVERITY_LABELS[v.severity] || v.severity;
          itemsHTML += '<div class="violation-item severity-' + v.severity + '">' +
            '<div class="violation-severity-tag">' + escapeHtml(sevLabel) + '</div>' +
            '<div>' + escapeHtml(v.description) + '</div>' +
          '</div>';
        });
        violationsHTML += '<div class="violation-group">' +
          '<div class="violation-group-header">' +
            '<span class="vg-icon">' + meta.icon + '</span>' +
            '<span>' + meta.label + '</span>' +
            '<span style="color:var(--color-text-muted);font-weight:400;font-size:var(--text-xs)">(' + items.length + ')</span>' +
          '</div>' +
          itemsHTML +
        '</div>';
      });
    }

    var urlLink = r.url
      ? '<a href="' + escapeHtml(r.url) + '" target="_blank" rel="noopener noreferrer" class="detail-source-link">View Original Inspection Report →</a>'
      : '';

    return '<div class="score-ring-container">' +
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
          '<div class="score-ring-grade" style="color:' + color + '">' + t.grade + '</div>' +
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
      '<div class="vstat"><div class="vstat-num priority">' + r.pv + '</div><div class="vstat-label">Priority</div></div>' +
      '<div class="vstat"><div class="vstat-num foundation">' + r.pfv + '</div><div class="vstat-label">Foundation</div></div>' +
      '<div class="vstat"><div class="vstat-num core">' + r.cv + '</div><div class="vstat-label">Core</div></div>' +
      '<div class="vstat"><div class="vstat-num total">' + r.tv + '</div><div class="vstat-label">Total</div></div>' +
    '</div>' +
    '<div class="violation-section">' +
      '<div class="violation-section-title">Violations</div>' +
      violationsHTML +
    '</div>' +
    urlLink;
  }

  /* ---------- MARKER CARD (Mobile) ---------- */
  function showMarkerCard(r) {
    var t = getTier(r.rs);
    var violText = r.tv > 0 ? r.tv + ' violation' + (r.tv !== 1 ? 's' : '') : 'No violations';

    document.getElementById('marker-card-content').innerHTML =
      '<div class="mc-header">' +
        '<div class="score-badge ' + tierClass(r.rs) + '" style="width:52px;height:52px;font-size:var(--text-xl)">' +
          '<span>' + r.rs + '</span><span class="grade">' + t.grade + '</span>' +
        '</div>' +
        '<div class="mc-info">' +
          '<div class="mc-name">' + escapeHtml(r.n) + '</div>' +
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
    var r = null;
    for (var i = 0; i < allData.length; i++) {
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
      }
    } else {
      showDetailDesktop(r);
      map.flyTo({ center: [r.ln, r.lt], zoom: 15, duration: 800 });
    }
  }

  /* ---------- BOTTOM SHEET ---------- */
  function setSheetState(state) {
    var sheet = document.getElementById('bottom-sheet');
    sheet.dataset.state = state;
    sheet.className = 'bottom-sheet ' + state;
    sheetState = state;
  }

  function initSheetDrag() {
    var sheet = document.getElementById('bottom-sheet');
    var handle = document.getElementById('sheet-handle');
    var startY = 0;
    var startSheetTop = 0;
    var isDragging = false;

    function onStart(e) {
      isDragging = true;
      startY = e.touches ? e.touches[0].clientY : e.clientY;
      startSheetTop = sheet.getBoundingClientRect().top;
      sheet.style.transition = 'none';
    }

    function onMove(e) {
      if (!isDragging) return;
      var y = e.touches ? e.touches[0].clientY : e.clientY;
      var delta = y - startY;
      var newTop = startSheetTop + delta;
      var minTop = 104; // topbar + filter
      var maxTop = window.innerHeight - 80;
      newTop = Math.max(minTop, Math.min(maxTop, newTop));
      var translateY = newTop - sheet.offsetTop;
      sheet.style.transform = 'translateY(' + translateY + 'px)';
    }

    function onEnd() {
      if (!isDragging) return;
      isDragging = false;
      sheet.style.transition = '';
      sheet.style.transform = '';

      var rect = sheet.getBoundingClientRect();
      var viewH = window.innerHeight;
      var sheetVisible = viewH - rect.top;
      var maxH = viewH - 104;

      if (sheetVisible < maxH * 0.25) {
        setSheetState('collapsed');
      } else if (sheetVisible < maxH * 0.65) {
        setSheetState('half');
      } else {
        setSheetState('expanded');
      }
    }

    handle.addEventListener('touchstart', onStart, { passive: true });
    document.addEventListener('touchmove', onMove, { passive: true });
    document.addEventListener('touchend', onEnd);
    handle.addEventListener('mousedown', onStart);
    document.addEventListener('mousemove', onMove);
    document.addEventListener('mouseup', onEnd);

    // Tap handle to cycle states
    handle.addEventListener('click', function() {
      if (sheetState === 'collapsed') setSheetState('half');
      else if (sheetState === 'half') setSheetState('expanded');
      else setSheetState('collapsed');
    });
  }

  /* ---------- EVENTS ---------- */
  function bindEvents() {
    // Search
    var searchInput = document.getElementById('search-input');
    var searchClear = document.getElementById('search-clear');
    var debouncedSearch = debounce(function() {
      searchQuery = searchInput.value;
      searchClear.hidden = !searchQuery;
      applyFilters();
    }, DEBOUNCE_MS);
    searchInput.addEventListener('input', debouncedSearch);
    searchClear.addEventListener('click', function() {
      searchInput.value = '';
      searchQuery = '';
      searchClear.hidden = true;
      applyFilters();
    });

    // City chips
    document.querySelectorAll('.city-chips .chip').forEach(function(chip) {
      chip.addEventListener('click', function() {
        document.querySelectorAll('.city-chips .chip').forEach(function(c) { c.classList.remove('active'); });
        chip.classList.add('active');
        currentCity = chip.dataset.city;
        applyFilters();
        if (filtered.length > 0) fitBoundsToData();
      });
    });

    // Risk chips
    document.querySelectorAll('.risk-chips .chip').forEach(function(chip) {
      chip.addEventListener('click', function() {
        document.querySelectorAll('.risk-chips .chip').forEach(function(c) { c.classList.remove('active'); });
        chip.classList.add('active');
        currentRisk = chip.dataset.risk;
        applyFilters();
      });
    });

    // Desktop sort
    document.getElementById('sort-select').addEventListener('change', function(e) {
      currentSort = e.target.value;
      sortData();
      renderList();
    });

    // Mobile sort
    document.getElementById('sheet-sort-btn').addEventListener('click', function() {
      var popup = document.getElementById('sort-popup');
      popup.hidden = !popup.hidden;
    });

    document.querySelectorAll('.sort-option').forEach(function(opt) {
      opt.addEventListener('click', function() {
        document.querySelectorAll('.sort-option').forEach(function(o) { o.classList.remove('active'); });
        opt.classList.add('active');
        currentSort = opt.dataset.sort;
        sortData();
        renderList();
        document.getElementById('sort-popup').hidden = true;
      });
    });

    // List item clicks (delegation)
    document.addEventListener('click', function(e) {
      var item = e.target.closest('.rest-item');
      if (item) {
        selectRestaurant(item.dataset.id, false);
        return;
      }
      var detailBtn = e.target.closest('.mc-detail-btn');
      if (detailBtn) {
        for (var i = 0; i < allData.length; i++) {
          if (allData[i].i === detailBtn.dataset.id) { showDetailMobile(allData[i]); break; }
        }
        return;
      }
    });

    // Marker card close
    document.getElementById('marker-card-close').addEventListener('click', function() {
      hideMarkerCard();
      selectedId = null;
    });

    // Mobile detail back
    document.getElementById('detail-back-btn').addEventListener('click', function() {
      hideDetailMobile();
      selectedId = null;
    });

    // Desktop detail back
    document.getElementById('sidebar-back-btn').addEventListener('click', function() {
      hideDetailDesktop();
      selectedId = null;
    });

    // Locate button
    document.getElementById('locate-btn').addEventListener('click', function() {
      if ('geolocation' in navigator) {
        navigator.geolocation.getCurrentPosition(
          function(pos) {
            map.flyTo({ center: [pos.coords.longitude, pos.coords.latitude], zoom: 14 });
          },
          function() {},
          { enableHighAccuracy: true, timeout: 5000 }
        );
      }
    });

    // Bottom sheet
    if (isMobile) {
      initSheetDrag();
    }

    // Resize
    window.addEventListener('resize', debounce(function() {
      var wasMobile = isMobile;
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
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
