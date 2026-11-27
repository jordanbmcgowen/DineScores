/**
 * OpenAPI 3.1 description of the public API, served at /api/v1/openapi.json.
 * Usable as-is for a ChatGPT GPT Action or any OpenAPI client. Keep it in
 * step with functions/api/v1/[[path]].js (tests/test_public_api.mjs checks
 * that every documented path answers).
 */
import { SITE } from './seo.js';
import { API_VERSION, LIMITS } from './public_api.js';

const str = (description, extra = {}) => ({ type: 'string', description, ...extra });
const int = (description, extra = {}) => ({ type: 'integer', description, ...extra });
const q = (name, schema, required = false) => ({ name, in: 'query', required, schema, description: schema.description });

const restaurant = {
  type: 'object',
  properties: {
    id: str('DineScores restaurant id (16 hex characters)'),
    name: str('Restaurant name'),
    address: str('Street address'),
    city: str('City'),
    state: str('Two-letter state code'),
    zip: str('ZIP code'),
    latitude: { type: ['number', 'null'] },
    longitude: { type: ['number', 'null'] },
    grade: { type: ['string', 'null'], enum: ['A', 'B', 'C', 'F', null], description: 'DineScores grade: A 90-100, B 80-89, C 70-79, F below 70 or live pests/sewage/closure; dead or trapped pests cap it at C' },
    grade_meaning: str('Plain-English meaning of the grade'),
    score: { type: ['integer', 'null'], description: '0-100, blend of the last three inspections (60/30/10)' },
    latest_inspection_date: str('Date of the latest inspection (YYYY-MM-DD)'),
    latest_inspection_score: { type: ['integer', 'null'] },
    official_score: { type: ['integer', 'null'], description: "The health department's own score, where it publishes one" },
    inspections_on_file: { type: 'integer' },
    issue_types: { type: 'array', items: { type: 'string' } },
    inspected_by: str('Health department that inspected'),
    official_report_url: { type: ['string', 'null'], description: "The health department's own report" },
    dinescores_url: str('Page on dinescores.com to cite'),
    map_url: str('The restaurant on the DineScores map'),
    grade_note: str('Why an F or a C was given despite a higher score'),
    distance_miles: { type: 'number' },
    violations: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          severity: { type: 'string', enum: ['Serious', 'Moderate', 'Minor'] },
          finding: str('Plain-language finding'),
          inspector_notes: str("The inspector's text"),
        },
      },
    },
    inspection_history: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          date: str('YYYY-MM-DD'), score: { type: ['integer', 'null'] },
          type: { type: ['string', 'null'] }, result: { type: ['string', 'null'] },
          report_url: { type: ['string', 'null'] },
        },
      },
    },
  },
};

const attribution = {
  type: 'object',
  description: 'Show the source and disclaimer with any answer built from this data.',
  properties: {
    source: { type: 'string' }, cite_as: { type: 'string' }, disclaimer: { type: 'string' },
    how_grades_work: { type: 'string' }, terms: { type: 'string' },
  },
};

const list = {
  type: 'object',
  properties: {
    query: { type: 'object' },
    count: { type: 'integer' },
    results: { type: 'array', items: { $ref: '#/components/schemas/Restaurant' } },
    attribution: { $ref: '#/components/schemas/Attribution' },
  },
};

const errors = {
  400: { description: 'Invalid or missing parameter' },
  429: { description: 'Rate limited; see Retry-After' },
  503: { description: 'Database temporarily unavailable' },
};

const json = schema => ({ content: { 'application/json': { schema } } });

export function openApiSpec() {
  return {
    openapi: '3.1.0',
    info: {
      title: 'DineScores public API',
      version: API_VERSION,
      description: 'Restaurant health inspection grades (A, B, C, F) from official US city, county and state health department records, refreshed weekly. Free and read-only, no key. Cite DineScores (dinescores.com) and link the restaurant\'s dinescores_url or official_report_url. Grades summarise what inspectors recorded and are not an official rating.',
      termsOfService: `${SITE}/developers/#terms`,
      contact: { name: 'DineScores', url: `${SITE}/developers/` },
    },
    servers: [{ url: SITE }],
    paths: {
      '/api/v1/restaurants/search': {
        get: {
          operationId: 'searchRestaurants',
          summary: 'Find restaurants by name, optionally in a city or state',
          description: 'Use this first to find a restaurant\'s id and grade. Every word of the name must appear in the restaurant\'s name.',
          parameters: [
            q('name', str('Restaurant name, e.g. "Joe\'s Pizza"'), true),
            q('city', str('City, e.g. "Dallas"')),
            q('state', str('State name or two-letter code')),
            q('limit', int(`Max results (default ${LIMITS.search.def}, max ${LIMITS.search.max})`)),
          ],
          responses: { 200: { description: 'Matching restaurants, newest inspection first', ...json(list) }, ...errors },
        },
      },
      '/api/v1/restaurants/near': {
        get: {
          operationId: 'restaurantsNear',
          summary: 'Restaurants near a point or a US street address, nearest first',
          parameters: [
            q('lat', { type: 'number', description: 'Latitude' }),
            q('lng', { type: 'number', description: 'Longitude' }),
            q('address', str('US street address, used when lat/lng are not given (geocoded by the US Census Bureau)')),
            q('radius_miles', { type: 'number', description: `Search radius (default ${LIMITS.radiusMiles.def}, max ${LIMITS.radiusMiles.max})` }),
            q('grade', str('Only these grades, comma separated, e.g. "C,F" for low scores')),
            q('since_months', int('Only restaurants whose latest inspection is this recent')),
            q('limit', int(`Max results (default ${LIMITS.near.def}, max ${LIMITS.near.max})`)),
          ],
          responses: { 200: { description: 'Restaurants with distance_miles', ...json(list) }, 422: { description: 'Address not found' }, ...errors },
        },
      },
      '/api/v1/restaurants/lowest': {
        get: {
          operationId: 'lowestScoresInCity',
          summary: 'The lowest recent grades (C and F) in a city',
          parameters: [
            q('city', str('City'), true),
            q('state', str('State name or two-letter code')),
            q('limit', int(`Max results (default ${LIMITS.lowest.def}, max ${LIMITS.lowest.max})`)),
          ],
          responses: { 200: { description: 'F grades first, then lowest score; inspections in the last 18 months', ...json(list) }, ...errors },
        },
      },
      '/api/v1/restaurants/{id}': {
        get: {
          operationId: 'getRestaurant',
          summary: 'One restaurant: grade, latest violations and inspection history',
          parameters: [{ name: 'id', in: 'path', required: true, schema: str('Restaurant id from a search'), description: 'Restaurant id from a search' }],
          responses: {
            200: {
              description: 'The restaurant',
              ...json({ type: 'object', properties: { restaurant: { $ref: '#/components/schemas/Restaurant' }, attribution: { $ref: '#/components/schemas/Attribution' } } }),
            },
            404: { description: 'No such restaurant' },
            ...errors,
          },
        },
      },
      '/api/v1/coverage': {
        get: {
          operationId: 'coverage',
          summary: 'Which states and cities DineScores covers',
          parameters: [q('state', str('Limit the city list to one state'))],
          responses: { 200: { description: 'States and cities with restaurant counts', ...json({ type: 'object' }) }, ...errors },
        },
      },
    },
    components: { schemas: { Restaurant: restaurant, Attribution: attribution } },
  };
}
