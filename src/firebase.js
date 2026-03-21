import { initializeApp } from 'firebase/app';
import { getFirestore, collection, getDocs, query, orderBy } from 'firebase/firestore';

const firebaseConfig = {
  apiKey: "AIzaSyCgIgPOh4WtXhfdRc_YI9nc0AgfmFqjntc",
  authDomain: "healthinspections.firebaseapp.com",
  projectId: "healthinspections",
  storageBucket: "healthinspections.firebasestorage.app",
  messagingSenderId: "1036976274526",
  appId: "1:1036976274526:web:abfd378fbae6d2ca9108cf",
  measurementId: "G-1VB8672S14"
};

const app = initializeApp(firebaseConfig);
const db = getFirestore(app);

/**
 * Map a Firestore doc to a normalized restaurant object.
 * Supports both the new vetted fields and legacy risk_score-only docs.
 */
function mapDoc(docSnap) {
  const d = docSnap.data();

  // Map violations array: support both object and array format
  let violations = [];
  if (Array.isArray(d.violations)) {
    violations = d.violations.map(v => {
      if (Array.isArray(v)) return v;
      return [v.category || 'unclassified', v.severity || 'core', v.description || ''];
    });
  }

  return {
    i:   d.id !== undefined ? d.id : docSnap.id,
    n:   d.name || '',
    a:   d.address || '',
    c:   d.city || '',
    s:   d.state || '',
    z:   d.zip || '',
    lt:  d.latitude || 0,
    ln:  d.longitude || 0,
    d:   d.inspection_date || '',
    os:  d.original_score || 0,
    rs:  d.risk_score || 0,
    pv:  d.priority_violations || 0,
    pfv: d.priority_foundation_violations || 0,
    cv:  d.core_violations || 0,
    tv:  d.total_violations || 0,
    src: d.source || '',
    url: d.source_url || '',
    ic:  d.inspection_count || 1,
    v:   violations,
    m:   d.metro || '',
    // New vetted grading fields
    ws:  d.weighted_score ?? d.risk_score ?? 0,
    vg:  d.vetted_grade || null,
    inf: d.infractions || [],
    vs:  d.violation_summaries || [],
    it:  d.inspection_type || '',
  };
}

export async function fetchAllRestaurants() {
  const snapshot = await getDocs(collection(db, 'restaurants'));
  return snapshot.docs.map(mapDoc);
}

export async function fetchInspectionHistory(restaurantId) {
  try {
    const snap = await getDocs(
      query(
        collection(db, 'restaurants', restaurantId, 'inspections'),
        orderBy('inspection_date', 'desc')
      )
    );
    return snap.docs.map(d => {
      const data = d.data();
      return {
        id: d.id,
        date: data.inspection_date || '',
        rs: data.risk_score || 0,
        os: data.original_score,
        pv: data.priority_violations || 0,
        pfv: data.priority_foundation_violations || 0,
        cv: data.core_violations || 0,
        tv: data.total_violations || 0,
        type: data.inspection_type || '',
        result: data.results || '',
        v: Array.isArray(data.violations) ? data.violations : [],
      };
    });
  } catch (e) {
    console.warn('Failed to fetch inspection history:', e);
    return [];
  }
}

export { db };
