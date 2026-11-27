// node tests/test_finding_text.mjs
import assert from 'node:assert/strict';
import { findingTitle, severityLabel } from '../src/findingText.js';

const cases = [
  [{ text: 'Food not kept cold enough', verbatim: '22: 3717-1-03.4(F)(1)(b)(Repeat) TCS foods ...' }, 'Food not kept cold enough'],
  [{ text: '15: 3717-1-03.2(J) Food in contact with an un...', verbatim: '15: 3717-1-03.2(J) Food in contact with an unclean surface or linens. **Observed cut broccoli stored in the original produce container.' }, 'Food in contact with an unclean surface or linens'],
  [{ text: '66: 3701-21-25(K)(Repeat) FSO/RFE did not hav...', verbatim: '66: 3701-21-25(K)(Repeat) FSO/RFE did not have a person in charge per shift with the person in charge certification in food protection. **Observed no employee' }, 'FSO/RFE did not have a person in charge per shift with the person in charge…'],
  [{ text: '2: 228.31(d) Observed expired food handler of...', verbatim: '2: 228.31(d) Observed expired food handler certificate for 1 employee.' }, 'Observed expired food handler certificate for 1 employee'],
  [{ text: '5-205.11 Hand sink blocked by sheet pans', verbatim: '' }, 'Hand sink blocked by sheet pans'],
  [{ text: 'Dented/Damaged cans', verbatim: 'Dented/Damaged cans' }, 'Dented/Damaged cans'],
  [{ text: '23: Proper date marking and disposition . Thi...', verbatim: '23: Proper date marking and disposition . This is a Priority Foundation item 3-501.17.' }, 'Proper date marking and disposition'],
  [{ text: 'Food separated and protected (OUT): 1. Observ...', verbatim: 'Food separated and protected (OUT): 1. Observed raw eggs above lettuce.' }, 'Food separated and protected'],
  [{ text: '1: 2-102.11 (A),(B), and (C)(1),(4)-(16) Pers...', verbatim: '1: 2-102.11 (A),(B), and (C)(1),(4)-(16) Person-in-charge is not assuring oversight. **Observed' }, 'Person-in-charge is not assuring oversight'],
  [{ text: 'Item 51: MFC 6-501.16 Mop was stored with mop...', verbatim: 'Item 51: MFC 6-501.16 Mop was stored with mop head in mop bucket.' }, 'Mop was stored with mop head in mop bucket'],
  [{ text: '2: 2-102.12(A) Corrective Actions: 2-102.12: ...', verbatim: '2: 2-102.12(A) Corrective Actions: 2-102.12: At least one employee that has supervisory duty.' }, 'At least one employee that has supervisory duty'],
  [{ text: '6: 2-401.11 Eating while prepping food Correc...', verbatim: '6: 2-401.11 Eating while prepping food Corrective Actions: 2-401.11: An employee shall eat elsewhere.' }, 'Eating while prepping food'],
  [{ text: 'ALL FOOD EMPLOYEES HAVE FOOD HANDLER TRAINING...', verbatim: 'ALL FOOD EMPLOYEES HAVE FOOD HANDLER TRAINING: 2-102.13: AT TIME OF INSPECTION' }, 'All food employees have food handler training'],
  [{ text: '', verbatim: '' }, ''],
];
for (const [item, want] of cases) assert.equal(findingTitle(item), want, JSON.stringify(item));
assert.equal(severityLabel('CRITICAL'), 'Serious');
assert.equal(severityLabel('WARNING'), 'Moderate');
assert.equal(severityLabel(undefined), 'Minor');
console.log('finding text: ok');
