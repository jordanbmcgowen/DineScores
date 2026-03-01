# DineScores — Firebase Infrastructure

Firebase Firestore backend for the DineScores restaurant health inspection map.
Migrates 1,254+ restaurants (Chicago, Dallas, NYC, San Francisco) from embedded
`data.js` to a scalable Firestore database.

---

## Setup Guide

### 1. Create a Firebase Project

1. Go to the [Firebase Console](https://console.firebase.google.com/)
2. Click **Add project** → name it (e.g. `dinescores-prod`)
3. Disable Google Analytics if not needed, then click **Create project**

### 2. Enable Firestore

1. In the left sidebar, go to **Build → Firestore Database**
2. Click **Create database**
3. Select **Production mode** (security rules lock it down by default)
4. Choose region: **`us-central1`** (recommended for lowest latency in the US)
5. Click **Enable**

### 3. Generate a Service Account Key

1. Go to **Project Settings** (gear icon) → **Service accounts**
2. Click **Generate new private key**
3. Save the downloaded file as `serviceAccount.json` in this directory

> **Never commit `serviceAccount.json` to version control.**
> Add it to `.gitignore` immediately:
> ```
> echo "serviceAccount.json" >> .gitignore
> ```

### 4. Install Python Dependencies

```bash
pip install firebase-admin python-geohash
```

### 5. Update `.firebaserc`

Open `.firebaserc` and replace the placeholder with your real project ID:

```json
{
  "projects": {
    "default": "your-actual-project-id"
  }
}
```

Your project ID is visible in Firebase Console → Project Settings → General.

### 6. Run the Initial Data Upload

```bash
# From this directory (dinescores-firebase/)
python upload_data.py serviceAccount.json

# If data.js is in a non-default location:
python upload_data.py serviceAccount.json --data-file /path/to/data.js

# Preview without writing (dry run):
python upload_data.py serviceAccount.json --dry-run
```

Expected output:
```
[parse] Reading ../dinescores/data.js …
[parse] Parsed 1,254 records.
[transform] Expanding fields and computing geohashes …
[transform] 1,254 documents ready.
[cities] Building city summary documents …
[cities] 4 cities found: Chicago, IL, Dallas, TX, New York, NY, San Francisco, CA
[upload] Uploading 1,254 restaurants …
  [restaurants] Batch 1–500 / 1,254 … done
  [restaurants] Batch 501–1,000 / 1,254 … done
  [restaurants] Batch 1,001–1,254 / 1,254 … done
[upload] 1,254 restaurant documents written in 4.2s
[upload] Uploading 4 city summaries …
[upload] Writing platform config …
✓ Upload complete.
  Restaurants : 1,254
  Cities      : 4
  Meta docs   : 1  (meta/platform_config)
```

### 7. Deploy Firestore Rules and Indexes

```bash
# Install Firebase CLI if not already installed:
npm install -g firebase-tools

# Log in:
firebase login

# Deploy rules + indexes only (does not touch hosting):
firebase deploy --only firestore

# Deploy everything including hosting:
firebase deploy
```

---

## Ongoing City Updates

When fresh inspection data arrives for a city, use `update_city.py`:

```bash
# Upsert all Chicago restaurants from a new JSON export:
python update_city.py serviceAccount.json Chicago chicago_2026-03.json

# Dry run first to see add/update counts:
python update_city.py serviceAccount.json Dallas dallas_new.json --dry-run
```

The JSON file must be an array of restaurant objects in the same compact format
as `data.js` entries. The script:
- Updates records that already exist (preserves `created_at`)
- Creates records that are new
- Refreshes the city summary document
- Patches `meta/platform_config` with updated totals

---

## Setting the Admin Custom Claim

The Firestore rules allow writes only for users with `request.auth.token.admin == true`.
Set this claim via the Admin SDK (Node.js example):

```js
const admin = require('firebase-admin');
admin.auth().setCustomUserClaims(uid, { admin: true });
```

Or use the provided Python scripts directly — they use the server-side Admin SDK
which bypasses client security rules entirely.

---

## Firestore Schema

```
firestore/
├── restaurants/             Collection
│   └── {docId}              Document  (docId = original "i" field)
│       ├── id                string
│       ├── name              string
│       ├── address           string
│       ├── city              string
│       ├── state             string    (2-letter code)
│       ├── zip               string
│       ├── latitude          number
│       ├── longitude         number
│       ├── geohash           string    (precision-7, ~150m × 150m cells)
│       ├── inspection_date   string    (YYYY-MM-DD)
│       ├── original_score    number | null
│       ├── risk_score        number    (0–100)
│       ├── priority_violations            number
│       ├── priority_foundation_violations number
│       ├── core_violations               number
│       ├── total_violations              number
│       ├── source            string    ("chicago_api", "dallas_api", …)
│       ├── source_url        string
│       ├── violations        array
│       │   └── {item}
│       │       ├── category     string  (e.g. "temperature_control")
│       │       ├── severity     string  ("priority" | "priority_foundation" | "core")
│       │       └── description  string
│       ├── created_at        timestamp
│       └── updated_at        timestamp
│
├── cities/                  Collection
│   └── {city_state}         Document  (e.g. "chicago_il")
│       ├── city              string
│       ├── state             string
│       ├── restaurant_count  number
│       ├── avg_score         number
│       └── last_updated      timestamp
│
└── meta/                    Collection
    └── platform_config      Document
        ├── version                string
        ├── scoring_formula        string
        ├── tier_definitions       array
        │   └── {item}
        │       ├── tier     string  ("A"–"F")
        │       ├── label    string  ("Excellent"–"Failing")
        │       ├── min_score number
        │       ├── max_score number
        │       └── color    string  (hex)
        ├── violation_categories   array<string>
        ├── total_restaurants      number
        ├── cities                 array<string>  (city_id keys)
        └── last_updated           timestamp
```

---

## Composite Indexes

Defined in `firestore.indexes.json` and deployed with `firebase deploy --only firestore`:

| Collection    | Fields                                    | Use case                          |
|---------------|-------------------------------------------|-----------------------------------|
| restaurants   | city ASC + risk_score DESC                | City filter, sorted by score      |
| restaurants   | city ASC + inspection_date DESC           | City filter, sorted by date       |
| restaurants   | city ASC + risk_score DESC + date DESC    | City filter, score + date sort    |
| restaurants   | city ASC + state ASC + risk_score DESC    | State → city drill-down           |
| restaurants   | risk_score DESC                           | Global top/bottom restaurants     |
| restaurants   | inspection_date DESC                      | Most recently inspected           |
| restaurants   | geohash ASC                               | Geo proximity queries (future)    |
| restaurants   | city ASC + geohash ASC                    | City-scoped geo queries (future)  |

---

## File Reference

| File                    | Purpose                                                      |
|-------------------------|--------------------------------------------------------------|
| `firestore.rules`       | Security rules — public read, admin-only write               |
| `firestore.indexes.json`| Composite index definitions                                  |
| `firebase.json`         | Firebase project config (rules, indexes, hosting)            |
| `.firebaserc`           | Project ID alias                                             |
| `upload_data.py`        | One-time migration from data.js to Firestore                 |
| `update_city.py`        | Ongoing upsert script for refreshing a single city           |
| `serviceAccount.json`   | **(not committed)** Your Firebase service account key        |

---

## Scaling Notes

- **Reads**: Firestore scales horizontally with no configuration. The public-read
  rules mean the client app can query directly without a backend.
- **Indexes**: All query patterns used by the app are covered by explicit composite
  indexes. Avoid adding new `where()` + `orderBy()` combinations without a
  matching index entry.
- **Geohash queries**: The `geohash` field enables efficient bounding-box and
  proximity queries. Use the [geofire-common](https://github.com/firebase/geofire-js)
  library to generate query ranges client-side.
- **Write throughput**: Each city update runs through batched writes (500 ops/batch).
  For very large cities (10k+ restaurants) add a short `time.sleep(0.1)` between
  batches to avoid hitting the 1-write/second/document sustained limit on hot spots.
