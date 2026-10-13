/**
 * The map's "show my location" button (a MapLibre IControl).
 *
 * It replaces maplibregl.GeolocateControl, which disables its button for
 * good after a single PERMISSION_DENIED: a dismissed prompt, iOS Location
 * Services switched off, or "Don't Allow" on the on-load prompt left it
 * greyed out ("Location not available") until a reload, even though the
 * browser would happily ask again. This button is never disabled. Every tap
 * asks the browser again (which re-prompts wherever the browser allows it),
 * zooms to the user, and keeps the camera on them live as fixes arrive,
 * until they pan or zoom the map themselves. A tap after that re-centers.
 *
 * Reuses the stock control's class names, so the stock icon, its blue
 * following/background states and its spinner apply unchanged.
 *
 * Options:
 *   onFix({ lat, lng, accuracy })  every fix from the watch
 *   onError(kind)                  'denied' | 'unavailable' | 'unsupported',
 *                                  only for a tap that found no location
 *   getOffset()                    [x, y] px to shift the center by, so the
 *                                  user lands in the part of the map not
 *                                  hidden by the sidebar, header or sheet
 */

// Street level, about 800m across a phone screen: the restaurants around
// the user show as individual grade markers rather than clusters.
const LOCATE_ZOOM = 15;
// Never zoom out past this for a coarse (IP or cell tower) fix.
const LOCATE_MIN_ZOOM = 10;

const BUTTON_CLASS = 'maplibregl-ctrl-geolocate';
const STATE_CLASSES = {
  off: [],
  waiting: ['maplibregl-ctrl-geolocate-waiting'],
  follow: ['maplibregl-ctrl-geolocate-active'],
  background: ['maplibregl-ctrl-geolocate-background'],
};

export default class LocateControl {
  constructor({ onFix, onError, getOffset } = {}) {
    this._onFix = onFix;
    this._onErrorCb = onError;
    this._getOffset = getOffset;
    this._state = 'off'; // off | waiting | follow | background
    this._watchId = null;
    this._last = null; // latest GeolocationPosition
    this._recenter = false; // the next fix answers a tap
  }

  onAdd(map) {
    this._map = map;
    this._container = document.createElement('div');
    this._container.className = 'maplibregl-ctrl maplibregl-ctrl-group';
    const button = document.createElement('button');
    button.type = 'button';
    button.className = BUTTON_CLASS;
    const icon = document.createElement('span');
    icon.className = 'maplibregl-ctrl-icon';
    icon.setAttribute('aria-hidden', 'true');
    button.appendChild(icon);
    button.addEventListener('click', this._onClick);
    this._button = button;
    this._container.appendChild(button);
    this._setState('off');
    map.on('movestart', this._onMoveStart);
    return this._container;
  }

  onRemove() {
    this._clearWatch();
    this._map.off('movestart', this._onMoveStart);
    this._container.remove();
    this._map = null;
  }

  _onClick = () => {
    if (!navigator.geolocation) {
      this._report('unsupported');
      return;
    }
    if (this._state === 'waiting') return; // already asking
    if (this._watchId !== null && this._last) {
      // Already watching: jump straight to the latest fix and follow again
      this._setState('follow');
      this._moveCamera(this._last, true);
      return;
    }
    this._recenter = true;
    this._setState('waiting');
    this._watchId = navigator.geolocation.watchPosition(this._onPosition, this._onPositionError, {
      enableHighAccuracy: true,
      maximumAge: 5000,
      timeout: 20000,
    });
  };

  _onPosition = pos => {
    if (!this._map) return;
    this._last = pos;
    if (this._state === 'waiting') this._setState('follow');
    if (this._state === 'follow') {
      this._moveCamera(pos, this._recenter);
      this._recenter = false;
    }
    if (this._onFix) {
      this._onFix({ lat: pos.coords.latitude, lng: pos.coords.longitude, accuracy: pos.coords.accuracy });
    }
  };

  _onPositionError = err => {
    if (!this._map) return;
    const denied = err && err.code === 1;
    // A slow or missed reading while already tracking is not worth an
    // alarm: keep the watch and the last fix.
    if (!denied && this._last) return;
    this._clearWatch();
    this._last = null;
    this._setState('off');
    this._report(denied ? 'denied' : 'unavailable');
  };

  // Any camera move this control did not make (a drag, a pinch, picking a
  // city) hands the camera back to the user: the location keeps updating,
  // the camera stops following. map.resize() (a phone's address bar sliding
  // away) also fires movestart, untagged, and its 'resize' event follows in
  // the same tick, so wait one microtask to tell the two apart.
  _onMoveStart = e => {
    if (e.geolocateSource || this._state !== 'follow') return;
    const map = this._map;
    let resized = false;
    const onResize = () => { resized = true; };
    map.once('resize', onResize);
    queueMicrotask(() => {
      map.off('resize', onResize);
      if (!resized && this._state === 'follow') this._setState('background');
    });
  };

  _moveCamera(pos, recenter) {
    const map = this._map;
    const { latitude, longitude, accuracy } = pos.coords;
    const fit = accuracyZoom(map, latitude, accuracy);
    const zoom = map.getZoom();
    // A tap goes to street level (or stays closer if already there), but
    // never closer than the fix's accuracy supports. Follow updates only
    // ever zoom IN, as accuracy improves, so walking never jolts the zoom.
    const target = recenter
      ? Math.min(Math.max(zoom, LOCATE_ZOOM), fit)
      : Math.max(zoom, Math.min(LOCATE_ZOOM, fit));
    map.easeTo({
      center: [longitude, latitude],
      zoom: Math.min(Math.max(target, LOCATE_MIN_ZOOM), map.getMaxZoom()),
      offset: this._getOffset ? this._getOffset() : [0, 0],
      duration: recenter ? 900 : 600,
    }, { geolocateSource: true });
  }

  _setState(state) {
    this._state = state;
    const b = this._button;
    for (const list of Object.values(STATE_CLASSES)) b.classList.remove(...list);
    b.classList.add(...STATE_CLASSES[state]);
    const following = state === 'follow';
    const title = following ? 'Following your location'
      : state === 'waiting' ? 'Finding your location…'
        : 'Show my location';
    b.title = title;
    b.setAttribute('aria-label', title);
    b.setAttribute('aria-pressed', following ? 'true' : 'false');
  }

  _clearWatch() {
    if (this._watchId !== null) navigator.geolocation.clearWatch(this._watchId);
    this._watchId = null;
  }

  _report(kind) {
    if (this._onErrorCb) this._onErrorCb(kind);
  }
}

// The zoom at which the fix's accuracy circle spans about half the map's
// shorter side (512px tiles, Web Mercator). A precise phone fix allows far
// more than LOCATE_ZOOM; a coarse fix shows the whole area it could be in.
function accuracyZoom(map, lat, accuracy) {
  const el = map.getContainer();
  const side = Math.max(Math.min(el.clientWidth, el.clientHeight), 200);
  const radius = Math.max(accuracy || 0, 10);
  const metersAcross = 40075016.686 * Math.cos((lat * Math.PI) / 180) * side / 512;
  return Math.log2(metersAcross / (4 * radius));
}
