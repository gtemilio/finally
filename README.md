# FinAlly — AI Trading Workstation

A visually stunning AI-powered trading workstation that streams live market data, lets you trade a simulated portfolio, and integrates an LLM chat assistant that can analyze positions and execute trades on your behalf. Built as a capstone for an agentic AI coding course.

## What It Does

- **Live price streaming** — prices flash green/red on every tick via SSE
- **Simulated portfolio** — start with $10,000 virtual cash, buy/sell with market orders
- **Portfolio heatmap** — treemap sized by position weight, colored by P&L
- **AI chat assistant** — ask questions, get analysis, have the AI execute trades for you
- **Sparkline mini-charts** — per-ticker price history accumulated since page load

## Quick Start

```bash
# Copy and fill in your API key
cp .env.example .env

# macOS/Linux
./scripts/start_mac.sh

# Windows
./scripts/start_windows.ps1
```

Then open [http://localhost:8000](http://localhost:8000).

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `OPENROUTER_API_KEY` | Yes | Powers the AI chat assistant |
| `MASSIVE_API_KEY` | No | Real market data (uses simulator if omitted) |
| `LLM_MOCK` | No | Set `true` for deterministic mock responses (testing) |

## Stack

| Layer | Technology |
|---|---|
| Frontend | Next.js + TypeScript (static export) |
| Backend | FastAPI + Python (managed by `uv`) |
| Database | SQLite (auto-initialized on first run) |
| Real-time | Server-Sent Events (SSE) |
| AI | LiteLLM → OpenRouter (Cerebras inference) |
| Deployment | Single Docker container, port 8000 |

## Architecture

```
Docker Container (port 8000)
├── FastAPI
│   ├── /api/*          REST endpoints
│   ├── /api/stream/*   SSE price stream
│   └── /*              Next.js static export
└── SQLite db (volume-mounted at /app/db)
```

Market data defaults to a built-in geometric Brownian motion simulator. Set `MASSIVE_API_KEY` to switch to live data from the Massive (Polygon.io) API.
