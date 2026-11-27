// The frontend mirror of the pipeline's grade rules (src/grading.js).
import assert from 'node:assert/strict';
import { calculateGrade, textCap, pestFindingIsMinor } from '../src/grading.js';

const MINOR = [
  '38: 6-501.112 OBSERVED MULTIPLE DEAD COCKROACHES. SHALL CLEAN',
  'OBSERVED A FEW ROACHES IN STICKY TRAP UNDER SERVING LINE',
  'Small dead mouse noted on a glue board deep in the basement',
  '38: 6-501.111 OBSERVED ANTS BY 3 COMP SINK. SHALL CONTROL PEST',
  'Observed one dead cockroach on floor of pantry. Saw no other signs of pest activity.',
];
const SERIOUS = [
  "Evidence of mice or live mice in establishment's food or non-food areas.",
  'Pests and animals: Observed rodent droppings throughout facility.',
  'Observed two live cockroaches in dishwashing area and under shelving in glue trap',
  'sewage backs up from the floor drain',
  'OSBERVED DEAD AND LIIVE ROACHES',
  'Observed dead roaches on shelving. Observed rodent activity below shelves.',
];
assert.equal(pestFindingIsMinor('Septic system is failing in driveway'), false);
for (const line of MINOR) {
  assert.equal(pestFindingIsMinor(line), true, line);
  assert.equal(textCap(line), 'C', line);
  assert.equal(calculateGrade(99, line), 'C', line);
  assert.equal(calculateGrade(60, line), 'F', line);
}
for (const line of SERIOUS) {
  assert.equal(textCap(line), 'F', line);
  assert.equal(calculateGrade(99, line), 'F', line);
}
assert.equal(textCap([MINOR[0], 'Floors dirty', SERIOUS[1]].join('|||')), 'F');
assert.equal(textCap('Floors dirty'), null);
assert.deepEqual([95, 85, 75, 65].map(s => calculateGrade(s)), ['A', 'B', 'C', 'F']);
console.log('grading.js: ok');
