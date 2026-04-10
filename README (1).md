# OddsIQ - FastAPI + Vanilla JS

## Local development

```bash
pip install fastapi uvicorn pandas kalshi-python-sync python-dotenv pytz
cp .env.example .env
# Edit .env with your Kalshi credentials
uvicorn main:app --reload --port 8000
# Open http://localhost:8000
```

## Deploy on Railway

1. Push to GitHub
2. Go to railway.app → New Project → Deploy from GitHub
3. Set environment variables:
   - `KALSHI_API_KEY_ID`
   - `KALSHI_PRIVATE_KEY`
4. Start command: `uvicorn main:app --host 0.0.0.0 --port $PORT`

## Deploy on Render

1. Push to GitHub
2. New Web Service → connect repo
3. Build: `pip install -r requirements.txt`
4. Start: `uvicorn main:app --host 0.0.0.0 --port $PORT`
5. Set env vars in dashboard

## Features
- True infinite scroll (IntersectionObserver)
- Sticky sports nav with expand/collapse
- Date filtering
- Search
- 30-min data cache
- All Kalshi sports/categories
