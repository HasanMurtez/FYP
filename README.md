# Football Club Management Platform

**AI-Powered Injury Risk Prediction & Player Scouting**

A full-stack web application that uses machine learning to predict injury risk for Premier League players and identify statistically similar replacements using KNN-based scouting. Built as a Final Year Project.

🔗 **Live App (Frontend):** [football-club-frontend.onrender.com](https://football-club-frontend.onrender.com)
🔗 **Backend:** [fcm-backend-lvcp.onrender.com](https://fcm-backend-lvcp.onrender.com)
🎬 **Screencast:** [Watch Demo](https://go.screenpal.com/watch/cOfO2hnOkTc)

> ⚠️ **Important:** If the app is not loading, the backend must be deployed first. Deploy the backend web service and wait for it to go live, then deploy the frontend static site. The frontend will not work without the backend running.
---

## Features

- **Injury Risk Prediction** — Random Forest classifier trained on 20 engineered features predicts player injury risk as Low, Medium, or High with confidence scores
- **Player Scouting** — KNN-based similarity search finds the top 5 statistically similar players based on 13 performance metrics
- **FIFA-Style Formation View** — Interactive 4-3-3 pitch layout displaying the starting XI with player photos from the Premier League CDN
- **AI Manager Assistant** — Integrated chatbot powered by Claude API that provides tactical recommendations, squad analysis, and player advice using live data
- **Real-Time Data** — Synchronises with the Fantasy Premier League API to pull stats for 800+ players across all 20 teams
- **Player Stats Modal** — Detailed breakdown of performance, injury and workload metrics, and discipline data for each player
- **Smart Replacement Finder** — Suggests available, low-risk alternatives when a player is injured or flagged as high risk

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| **Frontend** | React 18, React Router, Axios, CSS3 |
| **Backend** | Python 3.12, Flask, Flask-CORS, Gunicorn |
| **Database** | SQLAlchemy ORM, SQLite (dev), PostgreSQL (prod) |
| **ML** | scikit-learn (Random Forest, KNN), SMOTE (imbalanced-learn), joblib |
| **AI Chatbot** | Anthropic Claude API (Sonnet 4.6) |
| **Data Source** | Fantasy Premier League API |
| **Deployment** | Render (backend + frontend static site) |
| **Dev Environment** | GitHub Codespaces |

---

## Project Structure

```
FYP/
├── backend/
│   ├── app/
│   │   ├── __init__.py              # Flask app factory with CORS
│   │   ├── models.py                # SQLAlchemy models (Team, Player)
│   │   └── routes/
│   │       ├── players.py           # Team and player CRUD endpoints
│   │       ├── predictions.py       # ML injury risk prediction pipeline
│   │       ├── scouting.py          # KNN similarity search and replacements
│   │       ├── sync.py              # FPL API data synchronisation
│   │       └── chat.py              # AI chatbot endpoint (Claude API)
│   ├── models/
│   │   ├── injury_risk_model.pkl    # Trained Random Forest model
│   │   └── injury_scaler.pkl        # Feature scaler
│   ├── data/
│   │   ├── raw/                     # Raw FPL API data (CSV)
│   │   └── processed/               # Engineered feature dataset
│   ├── train_injury_model.py        # Model training script
│   ├── fetch_basic_player_data.py   # Data collection pipeline
│   ├── config.py                    # Dev/prod configuration
│   ├── run.py                       # Flask entry point
│   └── requirements.txt             # Python dependencies
├── frontend/
│   ├── src/
│   │   ├── pages/
│   │   │   ├── HomePage.jsx         # Team selection grid
│   │   │   └── DashboardPage.jsx    # FIFA-style formation view
│   │   ├── components/
│   │   │   ├── PlayerStatsModal.jsx  # Detailed player stats popup
│   │   │   ├── ScoutingModal.jsx     # KNN replacement finder
│   │   │   └── ChatBot.jsx          # AI Manager Assistant
│   │   ├── services/
│   │   │   └── api.js               # Axios API service layer
│   │   └── App.js                   # Root component with routing
│   └── package.json
└── README.md
```

---

## Machine Learning Pipeline

### Injury Risk Prediction

1. **Data Collection** — Fetches player stats, match-by-match history, and fixture data from the FPL API
2. **Feature Engineering** — 20 features including:
   - Workload intensity, recent minutes (last 5 games), total minutes (last 10)
   - Availability score, consecutive unavailability, injury prone flag
   - Physical demand (cards), position risk, fixture congestion
   - ICT Index (Influence, Creativity, Threat)
3. **Class Balancing** — SMOTE applied to handle imbalanced risk classes
4. **Model Training** — Random Forest classifier with stratified cross-validation
5. **Prediction** — Each player assigned Low, Medium, or High risk with confidence percentage

### Player Scouting (KNN)

- 13 features: goals, assists, minutes, starts, ICT metrics, cards, recent form, availability
- StandardScaler normalisation before distance calculation
- Euclidean distance with similarity score (0-100%)
- Filters by position, team, availability, and max risk level

---

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/teams` | GET | All 20 Premier League teams |
| `/api/teams/:id/players` | GET | Players for a specific team |
| `/api/players` | GET | All players with optional filters |
| `/api/players/search?q=` | GET | Search players by name |
| `/api/stats/summary` | GET | League-wide statistics |
| `/api/sync` | POST | Sync data from FPL API |
| `/api/predict` | POST | Run injury risk predictions |
| `/api/predict/high-risk` | GET | All high-risk players |
| `/api/scouting/similar?player_id=` | GET | Find similar players (KNN) |
| `/api/scouting/replacements?player_id=` | GET | Find replacement players |
| `/api/chat` | POST | AI chatbot query |

---

## Local Development Setup

### Prerequisites

- Python 3.12+
- Node.js 18+
- Git

### Option 1: GitHub Codespaces (Recommended)

1. Go to [github.com/HasanMurtez/FYP](https://github.com/HasanMurtez/FYP)
2. Click the green **Code** button -> **Codespaces** -> **Create codespace on main**
3. Wait for the environment to load

**Start the backend (Terminal 1):**

```bash
cd /workspaces/FYP/backend
pip install -r requirements.txt
python -c "from app import create_app, db; app = create_app(); app.app_context().push(); db.create_all(); print('done')"
python run.py
```

**Populate the database (Terminal 2):**

```bash
curl -X POST http://localhost:5000/api/sync
curl -X POST http://localhost:5000/api/predict
```

**Start the frontend (Terminal 3):**

```bash
cd /workspaces/FYP/frontend
npm install
npm start
```

**Important Codespaces setup:**

4. Go to the **PORTS** tab at the bottom of the Codespaces window
5. Set **both port 3000 and port 5000** visibility to **Public** (right-click → Port Visibility → Public)
6. Copy the **Forwarded Address** for port 5000 (e.g. `https://your-codespace-name-5000.app.github.dev`)
7. Update `frontend/src/services/api.js` — replace the `BASE_URL` with your port 5000 forwarded address
8. Update `frontend/src/App.js` — replace the `API_BASE_URL` with the same port 5000 address
9. Open the **Forwarded Address** for port 3000 in your browser to view the app

**Note:** The sync process fetches data for ~800 players from the FPL API and takes 2-3 minutes to complete. The predict step takes a few seconds.

### Option 2: Local Machine

**Backend:**

```bash
cd backend
pip install -r requirements.txt
python -c "from app import create_app, db; app = create_app(); app.app_context().push(); db.create_all(); print('done')"
python run.py
```

**Populate the database:**

```bash
curl -X POST http://localhost:5000/api/sync
curl -X POST http://localhost:5000/api/predict
```

**Frontend:**

```bash
cd frontend
npm install
npm start
```

The app will be available at `http://localhost:3000`.

### AI Chatbot Setup

The AI Manager Assistant requires an Anthropic API key:

1. Create an account at [console.anthropic.com](https://console.anthropic.com)
2. Generate an API key
3. In the app, click the ⚽ button (bottom-right corner)
4. Click the ⚙ gear icon and paste your API key
5. The key is saved in your browser only and is never stored on the server

---

## Deployment

The application is deployed on Render:

- **Backend:** Web Service running Gunicorn with `--timeout 300` to handle the FPL API sync
- **Frontend:** Static Site serving the React production build
- **Database:** PostgreSQL (Render managed)

To deploy your own instance:

1. Create a PostgreSQL database on Render
2. Create a Web Service pointing to the `backend/` directory
3. Set `DATABASE_URL` environment variable to your PostgreSQL connection string
4. Set `FLASK_ENV=production`
5. Build command: `pip install -r requirements.txt && python init_db.py`
6. Start command: `gunicorn --timeout 300 run:app`
7. Create a Static Site pointing to the `frontend/` directory
8. Set `REACT_APP_API_URL` to your backend URL
9. Build command: `npm install && npm run build`
10. Publish directory: `build`

---

## Author

**Hasan Murtaza**
B.Sc. (Hons) Software Development
Atlantic Technological University, Galway
Student ID: G00419888
Supervisor: Gerard Harrison
