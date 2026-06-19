# WikiPulse — Real-Time Wikipedia Edit Analytics

A production-grade streaming data pipeline that ingests every Wikipedia edit in real time, processes it through Apache Kafka and PySpark Structured Streaming, stores results in PostgreSQL, and serves a live analytics dashboard.

![Dashboard](https://img.shields.io/badge/stack-Kafka%20%7C%20PySpark%20%7C%20PostgreSQL%20%7C%20FastAPI%20%7C%20Next.js-blue)
![Docker](https://img.shields.io/badge/orchestration-Docker%20Compose-2496ED)

## Architecture

```
Wikipedia SSE Stream
        │
        ▼
   Kafka (Topic: wiki-edits)
        │
        ▼
  PySpark Structured Streaming
  ├── edits table (raw)
  ├── edit_stats_1min (time-series aggregation)
  ├── top_articles (ranked by edit count)
  └── spikes (breaking news detection)
        │
        ▼
    PostgreSQL
        │
        ▼
    FastAPI REST API
        │
        ▼
  Next.js Dashboard (live, polls every 5s)
```

## Features

- **Live Edit Feed** — every Wikipedia article edit streamed in real time (500+ edits/min)
- **Edit Velocity Chart** — 30-minute rolling window line chart (total / human / bot)
- **Bot vs Human Detection** — real-time classification using Wikipedia's bot flag
- **Breaking News Spikes** — articles with >10 edits in a single micro-batch surface as alerts
- **Top Articles Heatmap** — most-edited articles in the last 15 minutes
- **Language Breakdown** — bar chart of edits by Wikipedia language edition (en, de, fr, ja, …)
- **New Page Detection** — highlights when a brand-new article is created

## Tech Stack

| Layer | Technology |
|---|---|
| **Event Source** | Wikipedia SSE EventStream (`stream.wikimedia.org`) |
| **Message Queue** | Apache Kafka (Confluent 7.5.0) |
| **Stream Processing** | PySpark Structured Streaming 3.5.1 |
| **Database** | PostgreSQL 15 |
| **Backend API** | FastAPI + psycopg2 |
| **Frontend** | Next.js 15, Recharts, Tailwind CSS |
| **Orchestration** | Docker Compose |

## Quick Start

```bash
git clone https://github.com/Riiyansh/wiki-pulse.git
cd wiki-pulse
docker compose up --build -d
```

Wait ~60 seconds for all services to initialize, then open **http://localhost:3010**.

### Prerequisites
- Docker + Docker Compose
- 4GB+ RAM (Spark needs headroom)

## Services

| Service | Port | Description |
|---|---|---|
| Zookeeper | — | Kafka coordination |
| Kafka | 9092 | Message broker |
| PostgreSQL | 5433 | Analytics database |
| Producer | — | Wikipedia SSE → Kafka |
| Spark | — | Stream processing job |
| FastAPI | 8000 | REST API |
| Next.js | 3010 | Dashboard |

## API Endpoints

```
GET /api/live-feed?limit=30       — latest N edits
GET /api/stats?minutes=30         — edit velocity time series
GET /api/top-articles?minutes=15  — most-edited articles
GET /api/bot-vs-human?minutes=30  — bot/human/new-page counts
GET /api/spikes                   — active breaking-news spikes
GET /api/languages?minutes=30     — edit count by language
```

## Data Pipeline Details

**Producer** (`producer/producer.py`) — connects to the Wikimedia SSE endpoint, filters for article namespace (ns=0), and publishes JSON events to the `wiki-edits` Kafka topic with gzip compression.

**Spark Job** (`spark/job.py`) — reads from Kafka in 10-second micro-batches and writes to four PostgreSQL tables using `foreachBatch`. Spike detection fires when any article accumulates >10 edits in a single batch.

**Database Schema** (`init.sql`) — four tables: `edits` (raw), `edit_stats_1min` (UPSERT on time window), `top_articles` (UPSERT on title), `spikes` (breaking news log).

## Why This Project

Wikipedia's edit stream is structurally identical to:
- E-commerce clickstreams (page views, add-to-cart events)
- Financial trade ticks
- Social media activity feeds

This pipeline demonstrates the complete DE skillset: **ingestion → queueing → distributed processing → analytical storage → API serving → visualization** — the same pattern used at Netflix, Uber, and LinkedIn at scale.

## Author

Riyansh Chouhan — [GitHub](https://github.com/Riiyansh) · [LinkedIn](https://linkedin.com/in/riyansh-chouhan)
