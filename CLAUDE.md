# News Bot: project context

This file holds the full context of the project, so a new Claude Code session can
continue without the original chat. Read it before changing anything.

## What this is

A Telegram bot that posts news from three Russian sites to a chat, with the
source name under each post. It was built for a friend of the owner and runs on
the owner's server. The owner doesn't read Russian; the friend does.

The owner builds bots with AI help. The friend wants **filtering** and a
**recommendation system** in the future, possibly an AI agent later.

## Current state (v2)

- `bot.py` is a single-file bot. It is synchronous, with no framework, and uses only `requests` + `feedparser`.
- Every `CHECK_INTERVAL` seconds (default 600), it reads the RSS feeds and posts new articles.
- On the **first run for each site**, it only stores existing articles and posts nothing, to avoid flooding the chat.
- Every post has 👍 / 👎 inline buttons. Pressing one stores a vote; pressing the same button again removes it. The button shows ✓ on the chosen vote.
- **Recommendations (simple keyword model, no AI yet):**
  - Title words are lowercased, kept if 4+ letters and not stopwords, then cut to 6 chars as a crude Russian stem ("смартфона" → "смартф").
  - Word weight = sum of votes on titles containing that word. Site bias = 0.2 × site's vote sum, capped to ±2.
  - Article score = sum of its word weights + site bias.
  - `score >= STAR_SCORE` → post marked ⭐. `score < MIN_SCORE` → skipped (only if `MIN_SCORE` is set; off by default).
  - The model is rebuilt from the DB before every feed check.
- Between feed checks, the bot long-polls `getUpdates` (only `callback_query`) to receive button presses. A single loop, no threads.
- SQLite file `news.db`:
  - `articles(id, item_id UNIQUE, source, title, link, added, sent, score, vote)`
  - `state(key, value)`, which stores the Telegram update `offset`.
  - Unvoted articles older than 30 days are deleted. Voted ones are kept as training data.
- If sending fails, the article row is deleted so the next check retries it.
- Telegram errors are logged **without** the exception text, because that text contains the URL with the bot token. Keep it that way.
- `ALLOWED_VOTERS` limits who can vote (important if posting to a public channel).

### Files
- `bot.py`: the bot
- `requirements.txt`: `requests`, `feedparser`
- `.env.example`: config template (the real `.env` is gitignored and never committed)
- `newsbot.service`: systemd unit (runs as user `newsbot` from `/opt/newsbot`, `MemoryMax=200M`)

### Feeds
| Site | What it is | RSS |
|---|---|---|
| Habr | Russian IT community, articles on programming/DevOps | `https://habr.com/ru/rss/articles/?fl=ru` |
| Rozetked | Gadget/tech news and reviews | `https://rozetked.me/rss` (**not verified**, check it) |
| Meduza | Independent general news outlet based in Latvia (politics, war, Russia) | `https://meduza.io/rss/all` |

## Server and constraints

- 1 GB RAM, 1 CPU core, 10 GB disk. The bot uses ~30–50 MB. A 1 GB swap file is recommended.
- No local AI models: they don't fit in 1 GB. Any AI must go through an API.
- Planned AI provider: **OpenRouter free models, ~30 requests/day** (per the owner; limits may change). So:
  - **Batch** articles: send many titles in one request, never one request per article.
  - Do cheap keyword filtering before calling AI.
  - If the AI call fails or the quota runs out, **fall back** to sending unfiltered (or queue) rather than crash or drop news.
  - Free models may log prompts. That's fine for public news titles; never send anything private.

## Important background and decisions (from the original chat)

1. **Resources:** the server is more than enough for this bot.
2. **Telegram is blocked in Russia** (fully since April 2026, after restrictions from 2025). A bot on a Russian server can't reach Telegram without a proxy or bypass.
3. The friend asked to rent the server **in Russia** "for speed" and sent instructions for a bypass tool. The advice given:
   - Prefer a server **outside Russia** (Finland, Netherlands, Germany). Telegram then works directly, and latency doesn't matter for a bot that checks every 10 minutes.
   - Don't run unknown bypass install scripts as root without reading them.
   - If a Russian server is used anyway, `requests` honors `HTTPS_PROXY` from `.env`.
   - **The server location decision was still open at the time.**
4. **Meduza legal note:** Russia blocked Meduza and designated it an "undesirable organization" (2023). Distributing its content can bring fines for people in Russia, and repeated involvement can lead to criminal charges. Outside Russia, it's an ordinary news site. The owner was told to decide this consciously.
5. "Bot vs agent": for plain RSS → Telegram, a bot is enough. AI/agent features make sense for summaries, translation (e.g. into English for the owner), topic filtering, and Q&A.
6. Deliberately simple stack (no aiogram/async). Keep it light unless there's a real reason.

## Roadmap (agreed order)

1. ✅ 👍/👎 buttons + vote storage + simple keyword recommendations (this version)
2. Keyword filters (blocklist/allowlist, e.g. skip ads/promos), configurable without code changes
3. Batched AI filtering via OpenRouter (respect ~30 req/day, fallback on failure)
4. Better recommendations: put liked/disliked titles into the AI prompt so it learns the friend's taste
5. Maybe later: summaries / translation, commands like `/top` or `/stats`

## Deploy (Ubuntu/Debian)

```bash
# check connectivity first
curl -I https://api.telegram.org
curl -I https://rozetked.me/rss

apt update && apt install -y python3 python3-venv
useradd -r -m -d /opt/newsbot newsbot
cd /opt/newsbot               # put the repo files here (git clone or scp)
python3 -m venv venv
venv/bin/pip install -r requirements.txt
cp .env.example .env && nano .env
chmod 600 .env && chown -R newsbot:newsbot /opt/newsbot
cp newsbot.service /etc/systemd/system/
systemctl daemon-reload && systemctl enable --now newsbot
journalctl -u newsbot -f      # logs
```

- **CHAT_ID:** for a channel, add the bot as admin and use `@channelname`. For a private chat, message the bot and read `chat.id` from `https://api.telegram.org/bot<TOKEN>/getUpdates`.
- The bot uses long polling, so a webhook must **not** be set on this token.
- **Upgrading from v1:** v1 used `seen.db`. v2 uses `news.db`, starts fresh, and does a silent first run, so there's no flood. `seen.db` can be deleted.

## Open questions

- Is the Rozetked RSS URL correct?
- Where will the server be (Russia vs abroad)? This decides whether a proxy is needed.
- Keep Meduza or not?
- Which topics does the friend want to filter for?
