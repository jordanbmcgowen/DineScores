# DineScores — Restaurant Health Inspection Transparency

Multi-city restaurant health inspection scores with proprietary safety grading.
Shows weighted scores, grade badges (Safe/Evaluate/Avoid), infraction detection,
and violation summaries for Chicago, NYC, San Francisco, and DFW metro.

---

## Architecture

```
┌─────────────────────────────────────────────────┐
│  Python Pipeline (dinescores_pipeline.py)        │
│  • Fetches data from Chicago, NYC, SF, DFW APIs  │
│  • Classifies violations (14 categories)          │
│  • Computes risk_score, weighted_score, grades    │
│  • Detects infractions & summarizes violations    │
│  • Uploads to Firestore + generates data.js       │
└─────────────┬───────────────────────┬────────────┘
              │                       │
     Firestore DB              public/data.js
     (primary)                  (fallback)
              │                       │
┌─────────────┴───────────────────────┴────────────┐
│  React Frontend (Vite build → public/)            │
│  • MapLibre GL JS map with clustering             │
│  • Grade badges (thumbs up/hand/thumbs down)      │
│  • Infraction filter icons                        │
│  • Inspection detail modal with summaries         │
│  • City/risk/grade filter chips                   │
│  • Dark mode, responsive (mobile bottom sheet)    │
└──────────────────────────────────────────────────┘
              │
       Firebase Hosting
```

## Tech Stack

- **Backend**: Python pipeline → Firestore + `data.js` fallback
- **Frontend**: React (JSX) + Tailwind CSS (CDN) + MapLibre GL JS
- **Build**: Vite → outputs to `public/` for Firebase Hosting
- **Hosting**: Firebase Hosting
- **CI**: GitHub Actions weekly refresh (Chicago, NYC, SF)

---

## Quick Start

### 1. Install Dependencies

```bash
# Frontend
npm install

# Pipeline
pip install requests firebase-admin
```

### 2. Development

```bash
npm run dev     # Vite dev server on port 3000
```

### 3. Production Build

```bash
npm run build   # Outputs to public/ for Firebase Hosting
```

### 4. Deploy

```bash
firebase deploy  # Deploys hosting + Firestore rules/indexes
```

---

## Data Pipeline

```bash
# Full load (all cities, 2024+)
python dinescores_pipeline.py --mode full --cities chicago nyc sf dfw \
  --creds serviceAccount.json --output-data-js public/data.js

# Weekly refresh
python dinescores_pipeline.py --mode weekly --cities chicago nyc sf \
  --creds serviceAccount.json --output-data-js public/data.js

# Test run (25 records per city, no Firestore upload)
python dinescores_pipeline.py --mode test --dry-run
```

### Pipeline Fields

The pipeline computes these fields for each restaurant:

| Field | Description |
|-------|-------------|
| `risk_score` | 0-100 score based on violation severity |
| `weighted_score` | Weighted average: 60% latest + 30% 2nd + 10% 3rd inspection |
| `vetted_grade` | A (90+, no bad words), B (80-89), C (70-79), F (<70 or bad words) |
| `infractions` | Array of detected categories: pests, temp, hygiene, equipment, docs |
| `violation_summaries` | Top 5 violations with category, severity, paraphrased text, verbatim |

---

## Firestore Schema

```
restaurants/{id}
  ├── name, address, city, state, zip, latitude, longitude
  ├── risk_score, weighted_score, vetted_grade
  ├── infractions[]           (pests, temp, hygiene, equipment, docs)
  ├── violation_summaries[]   (text, verbatim, severity, category)
  ├── violations[]            (category, severity, description)
  ├── inspection_date, inspection_type, inspection_count
  └── inspections/{id}        (subcollection: full history)
```

---

## File Reference

| File | Purpose |
|------|---------|
| `dinescores_pipeline.py` | Multi-city data pipeline (fetch + grade + upload) |
| `src/` | React frontend source (JSX components) |
| `src/App.jsx` | Main app: map, sidebar, filters, bottom sheet |
| `src/components/GradeBadge.jsx` | Grade badge with thumbs icons |
| `src/components/RestaurantMap.jsx` | MapLibre GL JS map with clustering |
| `src/components/InspectionModal.jsx` | Detail modal with summaries + history |
| `src/components/FilterBar.jsx` | City/risk/grade/infraction filters |
| `src/firebase.js` | Firebase SDK init + Firestore queries |
| `src/grading.js` | Client-side grading for fallback data |
| `vite.config.js` | Vite build config (outputs to public/) |
| `firebase.json` | Firebase Hosting + Firestore config |
| `firestore.rules` | Security rules (public read, admin write) |
| `firestore.indexes.json` | Composite indexes for queries |
| `.github/workflows/refresh-data.yml` | Weekly automated refresh |
| `public/data.js` | Auto-generated fallback data file |
