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

## Keep-warm (prevents cold starts)

Railway parks idle containers after ~10 minutes of inactivity, which
makes the first visit after a quiet period slow (10–40 s). Ping
`/healthz` every few minutes to keep the container warm. Pick one:

### Option 1: UptimeRobot (recommended, free)
1. Create a free account at uptimerobot.com
2. Add a new HTTP monitor
3. URL: `https://YOUR-DOMAIN/healthz`
4. Interval: 5 minutes
5. Bonus: you get uptime alerts via email/SMS for free

### Option 2: Railway Cron (if you stay on Railway)
Add a cron service to your Railway project:
```
schedule: "*/5 * * * *"
command: curl -s https://YOUR-DOMAIN/healthz > /dev/null
```

### Option 3: GitHub Actions (free, uses your repo)
Create `.github/workflows/keep-warm.yml`:
```yaml
on:
  schedule: [{ cron: '*/5 * * * *' }]
jobs:
  ping:
    runs-on: ubuntu-latest
    steps:
      - run: curl -s https://YOUR-DOMAIN/healthz
```

The `/healthz` endpoint is intentionally cheap (no DB, no Kalshi
calls) so the keep-warm traffic has near-zero cost.

## Environment variables

Required:
- `KALSHI_API_KEY_ID` — Kalshi API key ID
- `KALSHI_PRIVATE_KEY` — Kalshi API private key (PEM content, multi-line)

Optional:
- `DATABASE_URL` — Postgres URL (Railway injects automatically with the plugin)
- `SENTRY_DSN` — enables Sentry error tracking
- `SENTRY_TRACES_SAMPLE_RATE` — `0.0`–`1.0`, Sentry performance sampling (default `0.0`)
- `SENTRY_ENVIRONMENT` — Sentry env tag (default `production`)
- `ANALYTICS_DOMAIN` — Plausible domain (e.g. `oddsiq.com`) to enable analytics
- `ANALYTICS_SCRIPT_URL` — override Plausible script URL for self-hosted instances
- `SLOW_REQUEST_MS` — log requests slower than this many ms (default `1000`)

## Launch checklist

Before you share the URL:

1. **Custom domain** — Railway → Settings → Domains. Point your DNS (CNAME to the Railway-provided host). SSL is automatic.
2. **Keep-warm pinger** — see section above. 5-minute HTTP monitor on `/healthz`.
3. **Sentry** — [sentry.io](https://sentry.io) free tier. Create a project, copy the DSN, set `SENTRY_DSN` in Railway env. Python errors appear with full stack traces + request context.
4. **Analytics** — [plausible.io](https://plausible.io) ($9/mo) or self-host. Add your domain, set `ANALYTICS_DOMAIN` in Railway env. Events track automatically.
5. **Smoke-test** — visit your domain, click through sports categories, open a card, refresh, use browser back. Nothing should 500.
6. **Watch logs** — Railway logs will show `SLOW …` warnings for any request > 1s; investigate anything that recurs.

## Features
- True infinite scroll (IntersectionObserver)
- Sticky sports nav with expand/collapse
- Date filtering
- Search
- Stale-while-revalidate cache (30-min TTL, 2-hour hard expiry)
- Gzip compression + ETag on hot endpoints
- Optional Sentry + Plausible integrations
- All Kalshi sports/categories
