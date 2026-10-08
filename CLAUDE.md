# News Bot: Project Context

This document describes the repository as it exists today. Read it before making changes.

## Overview

A Telegram bot that collects articles from configured RSS feeds, ranks candidates in batches with an OpenRouter model, and posts selected stories with summaries and source links. Readers can vote on posts; votes update a local recommendation profile.

## Current behavior

- `bot.py` is a single-file, synchronous application built with `requests`, `feedparser`, and `beautifulsoup4`.
- The bot checks configured feeds every `CHECK_INTERVAL` seconds (default: 600).
- On the first run for each source, it records existing entries without posting them.
- New entries are queued and their article text is extracted when available. During `ACTIVE_HOURS`, queued candidates are ranked in batches, newest first.
- From each batch the model marks up to `AI_MAX_SEND_PER_BATCH` items that meet `AI_MIN_SCORE` as *ready*, with an English summary. Ranking and sending are separate steps: ready articles wait in a pool, and the best one (highest score) is posted whenever a send slot opens.
- The bot sends at most `DAILY_NEWS_LIMIT` posts per calendar day in `BOT_TIMEZONE`. With `PACE_POSTS=1` (default) the limit is spread evenly across `ACTIVE_HOURS`: one slot opens at the window start and the rest at equal intervals, so posts don't arrive in a burst. Nothing is ranked or posted outside `ACTIVE_HOURS`.
- Telegram posts include the original article title, an English summary, a source label, a link, and 👍 / 👎 buttons.
- A vote can be toggled by pressing the same button again. `ALLOWED_VOTERS` can restrict who may vote.
- The recommendation profile is bootstrapped from existing votes. Once a day it is updated with every vote made since the previous update (state key `profile_votes_until`), regardless of when the voted article was sent. Model output is validated by `sanitize_profile`: only the expected fields are kept, and lists and strings are capped so the profile cannot grow without bound.
- The bot uses Telegram long polling in a single loop; do not configure a webhook for its token.

## Data and reliability

SQLite stores article metadata, evaluation results, votes, and bot state. The profile is stored as JSON next to the database by default. Unvoted articles older than `KEEP_DAYS` are removed; stale queued candidates and unsent ready articles expire after `CANDIDATE_MAX_AGE_HOURS`.

Telegram errors are logged without exception details that could expose the bot token. Preserve this behavior. If posting fails, the article stays in the ready pool with its summary and the send is retried after 5 minutes without another model request; after `SEND_MAX_FAILURES` failures the article is dropped.

Model request handling (`openrouter_json` returns `(result, error)`):

- `transport` errors (network, HTTP, quota/rate limits) back off exponentially, from 5 minutes up to 2 hours; the batch is kept unchanged.
- `output` errors (truncated or invalid JSON, no usable items) also back off, and each one halves the next batch size (10 → 5 → 2 → 1). If a single-article batch still fails, that article is skipped so it cannot block the queue.
- If the model returns only some of the ids, the scored items are saved and the rest stay queued.
- `AI_DAILY_REQUEST_LIMIT` caps ranking requests per day; the daily profile update is always allowed.

Article text and feed content are untrusted input. Model prompts must instruct the model to ignore instructions embedded in source material. Do not send private data to the model provider.

## Configuration

See `.env.example` for the configuration template. Keep credentials in an untracked environment file; never commit API keys or bot tokens.

Important settings include:

- `BOT_TOKEN`, `CHAT_ID`, and `OPENROUTER_API_KEY`
- `OPENROUTER_MODEL` and the profile/ranking token limits
- `CHECK_INTERVAL`, `BOT_TIMEZONE`, and `DAILY_NEWS_LIMIT`
- `ACTIVE_HOURS` and `PACE_POSTS`
- `AI_BATCH_SIZE`, `AI_BATCH_MAX_ITEMS`, `AI_BATCH_MAX_WAIT`, `AI_MAX_SEND_PER_BATCH`, `AI_MIN_SCORE`, and `AI_DAILY_REQUEST_LIMIT`
- `ARTICLE_MAX_CHARS`, `CANDIDATE_MAX_AGE_HOURS`, and `ALLOWED_VOTERS`

The default model is `nvidia/nemotron-3.5-lightning:free`. OpenRouter limits and model availability can change; check current provider documentation before relying on a quota.

## Deployment

The `newsbot.service` unit runs the bot as the dedicated `newsbot` user from `/opt/newsbot`. Install dependencies from `requirements.txt`, provide the environment file, then enable the systemd service. Typical operational commands:

```bash
python3 -m venv venv
venv/bin/pip install -r requirements.txt
sudo systemctl enable --now newsbot
sudo journalctl -u newsbot -f
```

## Development notes

- Keep the dependency footprint small unless a feature requires more.
- Preserve batch requests; avoid one model request per article.
- If a model request fails or returns invalid data, retain candidates for retry rather than crashing or silently discarding them.
- Keep this document focused on repository behavior and technical decisions. Do not add private chat history or personal details.
