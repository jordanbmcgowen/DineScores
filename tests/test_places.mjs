import assert from 'node:assert/strict';
import { PLACE_BY_ID, placeContains, placeMatchRank, placesAt } from '../src/places.js';

const southWalton = PLACE_BY_ID['south-walton'];
// Near WaterColor: both the 30A waterfront and US-98 / Point Washington
// discovery area are reachable through the broader regional selector.
assert(placeContains(southWalton, 30.320, -86.145));
assert(placeContains(southWalton, 30.390, -86.150));
assert(!placeContains(PLACE_BY_ID['30a'], 30.390, -86.150));
assert.equal(placeMatchRank(southWalton, 'South Walton'), 0);
assert.equal(placeMatchRank(PLACE_BY_ID['30a'], 'South Walton'), -1);
assert(placeContains(southWalton, 30.385, -86.35)); // Miramar / Sandestin
assert(!placeContains(southWalton, 30.32, -86.48)); // Destin
assert(placesAt(30.22, -85.88).includes('panama-city-beach'));
assert(!placesAt(30.17, -85.66).includes('panama-city-beach')); // Panama City mainland
// Preserve the existing WaterColor / Seaside distinction.
assert(placesAt(30.320, -86.145).includes('watercolor'));
assert(!placesAt(30.320, -86.138).includes('watercolor'));
assert(placesAt(30.320, -86.138).includes('seaside'));
assert(!placesAt(null, null).length);
console.log('Place selection checks passed');
