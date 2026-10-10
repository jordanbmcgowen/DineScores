/**
 * Named places that are NOT a city in the data.
 *
 * Health departments file a restaurant under its postal city, so the
 * unincorporated beach towns along Florida's Scenic Highway 30A (Seaside,
 * WaterColor, Grayton Beach, Alys Beach, Rosemary Beach, ...) are stored as
 * "Santa Rosa Beach", "Inlet Beach" or whatever the mailing address says.
 * A place is therefore defined GEOGRAPHICALLY — a bounding box in degrees —
 * and matched on a record's coordinates, never on its city string.
 *
 * Shape:
 *   id       stable slug, used as the filter value ('p:<id>' in the picker)
 *   label    what the UI shows
 *   state    two-letter state, for display and future disambiguation
 *   bbox     { w, s, e, n } ENVELOPE of the place — what the map fits to and
 *            what the D1 eager-load requests (/api/restaurants?bbox=)
 *   boxes    optional list of { w, s, e, n }; when present, MEMBERSHIP is
 *            "inside any of these" instead of "inside bbox". Needed where a
 *            single rectangle misdescribes the place: WaterColor wraps around
 *            Seaside on the west and north, so its envelope would swallow
 *            Seaside's entire 30A frontage.
 *   aliases  extra names the search box should match (prefix match)
 *
 * Boundaries were derived 2026-10 from OpenStreetMap Nominatim (Destin's
 * city limits and Seaside's admin polygon are real boundaries; the other
 * places are point features) and then adjusted against the geocoded
 * restaurant coordinates in data.js and the live D1 roster, so that every
 * known storefront lands in the community it belongs to. Longitude bands
 * along 30A are contiguous west→east: each sub-place hands off to the next.
 */

const FL = 'FL';

export const PLACES = [
  {
    id: 'destin',
    label: 'Destin',
    state: FL,
    // OSM city limits (30.3786–30.4178, -86.5159 to -86.3972) padded a hair.
    // The Okaloosa/Walton line at -86.397 separates Destin from Miramar
    // Beach; records filed under a Destin mailing city but located east of
    // it (Scenic Gulf Dr, Cannery Ln) belong to Miramar Beach / Sandestin.
    bbox: { w: -86.5200, s: 30.3740, e: -86.3970, n: 30.4200 },
    aliases: ['Destin FL', 'Destin Harbor', 'HarborWalk', 'Crystal Beach', 'Henderson Beach'],
  },
  {
    id: 'miramar-beach',
    label: 'Miramar Beach / Sandestin',
    state: FL,
    // Walton County line west, Sandestin's east gate (~-86.30, where Santa
    // Rosa Beach's US-98 strip begins) east, Gulf to the bay north-south.
    bbox: { w: -86.3970, s: 30.3600, e: -86.3000, n: 30.4050 },
    aliases: ['Miramar Beach', 'Sandestin', 'Grand Boulevard', 'Baytowne Wharf', 'Silver Sands', 'Scenic Gulf Drive', 'Seascape'],
  },
  {
    id: '30a',
    label: '30A (Walton County beaches)',
    state: FL,
    // The whole scenic highway from its US-98 west junction near Dune Allen
    // to Inlet Beach at the Bay County line. Three boxes trace the corridor
    // so the US-98 strip of Santa Rosa Beach (north of ~30.371) and Point
    // Washington stay out while everything south of 98 along 30A is in.
    bbox: { w: -86.2740, s: 30.2650, e: -85.9950, n: 30.3700 },
    boxes: [
      { w: -86.2740, s: 30.3000, e: -86.1900, n: 30.3700 }, // Dune Allen → Blue Mountain
      { w: -86.1900, s: 30.3000, e: -86.0850, n: 30.3450 }, // Grayton → Seagrove / Eastern Lake
      { w: -86.0850, s: 30.2650, e: -85.9950, n: 30.3300 }, // Watersound → Inlet Beach
    ],
    aliases: ['30A', 'Highway 30A', 'Scenic Highway 30A', 'South Walton', 'Walton County beaches', 'Beaches of South Walton', 'Emerald Coast beaches'],
  },
  // ---- 30A sub-places, west → east -------------------------------------
  {
    id: 'dune-allen-beach',
    label: 'Dune Allen Beach',
    state: FL,
    // 30A's western end: from the US-98 junction (Coastal Coffee, 7525 W
    // 30A) past Stinky's and the Santa Rosa Golf Club to Oyster Lake.
    bbox: { w: -86.2740, s: 30.3350, e: -86.2380, n: 30.3700 },
    aliases: ['Dune Allen', 'Dune Lakes', 'Oyster Lake'],
  },
  {
    id: 'gulf-place',
    label: 'Gulf Place (Santa Rosa Beach)',
    state: FL,
    // The 30A / CR-393 town center: Town Center Loop, Spires Ln, Ed Walline
    // beach access, Shunk Gulley.
    bbox: { w: -86.2380, s: 30.3400, e: -86.2245, n: 30.3560 },
    aliases: ['Gulf Place', 'Ed Walline', 'Shunk Gulley', 'Town Center Loop'],
  },
  {
    id: 'blue-mountain-beach',
    label: 'Blue Mountain Beach',
    state: FL,
    // Gulfview Heights east to Big Redfish Lake, including the CR-83 shops
    // (Village Blvd) and Red Fish Taco / Cowgirl Kitchen at the east end.
    bbox: { w: -86.2245, s: 30.3300, e: -86.1900, n: 30.3560 },
    aliases: ['Blue Mountain', 'Blue Mtn Beach', 'Redfish Lake'],
  },
  {
    id: 'grayton-beach',
    label: 'Grayton Beach',
    state: FL,
    // Grayton Beach State Park and the old town (Hotz Ave, Logan Ln shops)
    // up CR-283 to ~30.345; Western Lake (-86.150) hands off to WaterColor.
    bbox: { w: -86.1900, s: 30.3200, e: -86.1500, n: 30.3450 },
    aliases: ['Grayton', 'Grayton Beach State Park', 'Western Lake', 'The Red Bar'],
  },
  {
    id: 'watercolor',
    label: 'WaterColor',
    state: FL,
    // WaterColor wraps around Seaside: the Inn / Town Center / Boathouse
    // (Goldenrod Cir, Watercolor Blvd S, 1777 E 30A) sit on the beach
    // immediately WEST of Seaside's line at -86.1425, the residential
    // phases run NORTH of Seaside and Seagrove above ~30.3235, and
    // WaterColor Crossings (Watercolor Way at CR-395, Publix) is at the
    // north-east corner. Two boxes keep Seaside's and Seagrove's 30A
    // frontage out of it.
    bbox: { w: -86.1500, s: 30.3150, e: -86.1200, n: 30.3360 },
    boxes: [
      { w: -86.1500, s: 30.3150, e: -86.1425, n: 30.3360 }, // beachside, west of Seaside
      { w: -86.1425, s: 30.3235, e: -86.1200, n: 30.3360 }, // inland phases + Crossings
    ],
    aliases: ['Watercolor', 'Water Color', 'WaterColor Inn', 'WaterColor Crossings', 'Watercolor Blvd', 'Watercolor Way'],
  },
  {
    id: 'seaside',
    label: 'Seaside',
    state: FL,
    // OSM admin polygon (30.3175–30.3227, -86.1421 to -86.1336) padded to
    // the beach on the south; WaterColor takes over west of -86.1425 and
    // north of 30.3235, Seagrove east of -86.1333.
    bbox: { w: -86.1425, s: 30.3150, e: -86.1333, n: 30.3235 },
    aliases: ['Seaside FL', 'Seaside Florida', 'Seaside 30A', 'Central Square', 'Ruskin Place', 'Airstream Row'],
  },
  {
    id: 'seagrove-beach',
    label: 'Seagrove Beach',
    state: FL,
    // From Seaside's east line past CR-395 (Seagrove Village Market) to
    // Eastern Lake (Old Florida Fish House). West of -86.120 only the 30A
    // frontage counts (WaterColor owns the inland block); east of it the
    // neighbourhood runs north to ~30.330.
    bbox: { w: -86.1333, s: 30.3000, e: -86.0850, n: 30.3300 },
    boxes: [
      { w: -86.1333, s: 30.3000, e: -86.1200, n: 30.3235 },
      { w: -86.1200, s: 30.3000, e: -86.0850, n: 30.3300 },
    ],
    aliases: ['Seagrove', 'Eastern Lake', 'Cafe Thirty-A'],
  },
  {
    id: 'watersound',
    label: 'WaterSound',
    state: FL,
    // Watersound Beach and Deer Lake on 30A (Bruno's, The Hub), plus Camp
    // Creek and Watersound Town Center north of 30A up to the Origins.
    bbox: { w: -86.0850, s: 30.2850, e: -86.0300, n: 30.3100 },
    boxes: [
      { w: -86.0850, s: 30.2850, e: -86.0400, n: 30.3100 }, // Watersound Beach / The Hub
      { w: -86.0400, s: 30.2900, e: -86.0300, n: 30.3100 }, // Camp Creek / Town Center (north of Alys)
    ],
    aliases: ['Watersound', 'Water Sound', 'Watersound Beach', 'Watersound Origins', 'Watersound Town Center', 'The Hub 30A', 'Camp Creek', 'Deer Lake'],
  },
  {
    id: 'alys-beach',
    label: 'Alys Beach',
    state: FL,
    // The white town between Watersound and Seacrest; its US-98 frontage
    // east of -86.030 (the 11285 US-98 shops) is included.
    bbox: { w: -86.0400, s: 30.2750, e: -86.0225, n: 30.2975 },
    boxes: [
      { w: -86.0400, s: 30.2750, e: -86.0300, n: 30.2900 },
      { w: -86.0300, s: 30.2750, e: -86.0225, n: 30.2975 },
    ],
    aliases: ['Alys', 'Alys Beach FL', 'Fonville Press', 'Caliza'],
  },
  {
    id: 'seacrest-beach',
    label: 'Seacrest Beach',
    state: FL,
    // The short stretch between Alys and Rosemary (10343 E 30A plaza).
    bbox: { w: -86.0225, s: 30.2750, e: -86.0185, n: 30.2975 },
    aliases: ['Seacrest', 'Peddlers Pavilion'],
  },
  {
    id: 'rosemary-beach',
    label: 'Rosemary Beach',
    state: FL,
    // Main St and Barrett Square; 30Avenue at -86.0115 belongs to Inlet Beach.
    bbox: { w: -86.0185, s: 30.2700, e: -86.0125, n: 30.2975 },
    aliases: ['Rosemary', 'Barrett Square', 'Rosemary Beach FL'],
  },
  {
    id: 'inlet-beach',
    label: 'Inlet Beach',
    state: FL,
    // East end of 30A: 30Avenue, Shades, the US-98 strip to Phillips Inlet.
    bbox: { w: -86.0125, s: 30.2650, e: -85.9950, n: 30.2975 },
    aliases: ['Inlet', '30Avenue', 'Phillips Inlet', 'Camp Helen'],
  },
];

export const PLACE_BY_ID = Object.fromEntries(PLACES.map(p => [p.id, p]));

/** True when (lat, lng) is inside one box. */
export function inBox(box, lat, lng) {
  return lng >= box.w && lng <= box.e && lat >= box.s && lat <= box.n;
}

/** True when (lat, lng) belongs to the place (any of its boxes, else its bbox). */
export function placeContains(place, lat, lng) {
  if (!place || lat == null || lng == null) return false;
  if (!inBox(place.bbox, lat, lng)) return false; // envelope rejects fast
  const boxes = place.boxes;
  if (!boxes || boxes.length === 0) return true;
  return boxes.some(b => inBox(b, lat, lng));
}

/** Envelope of every place — a cheap pre-check before testing each one. */
export const PLACES_ENVELOPE = PLACES.reduce((env, p) => ({
  w: Math.min(env.w, p.bbox.w), s: Math.min(env.s, p.bbox.s),
  e: Math.max(env.e, p.bbox.e), n: Math.max(env.n, p.bbox.n),
}), { w: Infinity, s: Infinity, e: -Infinity, n: -Infinity });

/** Ids of every place containing (lat, lng); [] when none. */
export function placesAt(lat, lng) {
  if (lat == null || lng == null || !inBox(PLACES_ENVELOPE, lat, lng)) return [];
  const ids = [];
  for (const p of PLACES) if (placeContains(p, lat, lng)) ids.push(p.id);
  return ids;
}

/**
 * Does a search query match this place? Prefix on the label, on any alias,
 * or on any word of either ("water" → WaterColor, WaterSound; "30a" → the
 * corridor; "crossings" → WaterColor). Returns a rank (0 best) or -1.
 */
export function placeMatchRank(place, q) {
  if (!q) return -1;
  const needle = q.toLowerCase();
  const label = place.label.toLowerCase();
  if (label.startsWith(needle)) return 0;
  const aliases = (place.aliases || []).map(a => a.toLowerCase());
  if (aliases.some(a => a.startsWith(needle))) return 1;
  const words = [label, ...aliases].flatMap(s => s.split(/[\s(),/-]+/)).filter(Boolean);
  if (words.some(w => w.startsWith(needle))) return 2;
  return -1;
}
