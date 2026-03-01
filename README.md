# DineScores

**Know Before You Go** — Restaurant food safety scores and inspection data, visualized on an interactive map.

DineScores standardizes health inspection data across multiple US cities into a consistent food safety risk score based on FDA Food Code violation categories.

## Features

- Interactive map with clustered markers (MapLibre GL)
- Mobile-first design with bottom sheet navigation
- City and risk-level filtering (Dallas, Chicago, NYC, San Francisco)
- Detailed violation breakdown by 13 FDA Food Code categories
- Standardized risk scoring: `100 - (priority × 5) - (priority_foundation × 2) - (core × 1)`
- Dark mode support

## Architecture

```
public/           # Static frontend (HTML/CSS/JS)
  index.html      # App shell
  style.css       # Design system + responsive layout
  app.js          # Map, filtering, detail views
firebase/         # Firebase config and cloud functions
  firestore.rules # Security rules
  firestore.indexes.json
scripts/          # Data collection pipeline
```

## Tech Stack

- **Frontend**: Vanilla JS, MapLibre GL JS, CARTO raster tiles
- **Backend**: Firebase (Firestore)
- **Data Sources**: Chicago API, NYC API, Dallas (collected), San Francisco API
- **Hosting**: GitHub Pages (frontend), Firebase (data API)

## Risk Score Formula

Each restaurant starts at 100. Deductions based on violation severity:
- **Priority violations** (e.g., temperature abuse, contamination): −5 each
- **Priority Foundation violations** (e.g., missing certifications): −2 each
- **Core violations** (e.g., cleanliness issues): −1 each

## Data Coverage

| City | Restaurants | Source |
|------|------------|--------|
| Chicago | 499 | City of Chicago Open Data API |
| San Francisco | 495 | SF Open Data API |
| New York | 163 | NYC Open Data API |
| Dallas | 97 | Dallas City Hall |

## License

Private — All rights reserved.
