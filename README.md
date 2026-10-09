# Bot-news

Telegram news bot with batch AI ranking, a per-day delivery limit, article
summaries, vote collection, and an adaptive recommendation profile.

## AI provider

AI requests use Polza AI's OpenAI-compatible Chat Completions API at
`https://polza.ai/api/v1`. Set `POLZA_API_KEY` in the bot's environment file;
do not commit or share the key. `POLZA_MODEL` defaults to
`openai/gpt-6-luna`.

The bot asks the model for JSON in the prompt and accepts plain JSON, fenced
JSON, or an object surrounded by extra text. GPT-6 Luna requests set
`reasoning_effort=none` for these focused ranking and profile tasks. Separate
output limits are used: `OPENROUTER_RANKING_MAX_TOKENS=8192` for a batch and
`OPENROUTER_PROFILE_MAX_TOKENS=2048` for a profile update. These existing
setting names are retained for compatibility and can be adjusted in the
environment file.

Copy `.env.example` to your environment file and set at least `BOT_TOKEN`,
`CHAT_ID`, and `POLZA_API_KEY`. Install the existing requirements with
`pip install -r requirements.txt`; no additional packages are needed for the
Polza AI integration.

## Persistent delivery queue

AI evaluation and Telegram delivery are separate. Every approved candidate enters
SQLite; there is no three-item batch cap. Relevance must meet `AI_MIN_SCORE`, and
summaries must pass the Russian prose gate. Empty or English summaries are
regenerated in later batches; an RSS excerpt is never used as a fallback. Source
headlines may retain their original language; the summary and `Источник:` label
are Russian. Latin company/technology names are allowed.

A local publisher wakes every five seconds and sends at most one eligible story.
Normal posts require **90 minutes since the previous delivery** by default
(`MIN_SEND_INTERVAL=5400`, seconds). This is a ceiling on frequency, not a schedule
that forces a post. No worthy stories means no messages. Midnight never resets
the interval or releases a backlog. The overall limit is ten per local calendar
day in `BOT_TIMEZONE`; actual Unix timestamps survive restart, timezone changes
and DST. The first qualifying article can be sent immediately when no earlier
publication restricts it.

Within the eligible queue, relevance is adjusted for freshness and waiting time:
`score - 1.5 * age_hours + min(4, 0.25 * waiting_hours)`, with earlier arrivals
breaking ties. Age starts at the earlier RSS publication/arrival timestamp.
Pending, queued and retrying articles older than `CANDIDATE_MAX_AGE_HOURS=24`
expire. Low scores cannot become eligible merely by waiting.

Urgent bypass is disabled until `URGENT_TRUSTED_SOURCES` lists trusted feed names
(case-sensitive, comma-separated, e.g. `Meduza,Habr`). Enabling it still requires
score >=95, known RSS publication within three hours, an explicit AI flag, an
allowed emergency category, and a verbatim evidence quote with a matching local
emergency marker. Categories are active exploitation of a vulnerability,
immediate danger/evacuation and an ongoing major infrastructure outage. Routine
product launches or interesting news do not qualify. These gates rely on source
claims and AI classification; they do not independently confirm an emergency.
An urgent article can shorten spacing to 30 minutes after any previous post,
at most once per local day by default. It consumes the same ten-post daily quota
and restarts the normal cooldown. Further urgent articles use ordinary spacing.

Configuration added:

| Variable | Default | Meaning |
| --- | --- | --- |
| `AI_DAILY_REQUEST_LIMIT` | `10` | Ranking requests/day, including failures; 0 disables budget |
| `MIN_SEND_INTERVAL` | `5400` | Minimum ordinary spacing, seconds |
| `SEND_RETRY_SECONDS` | `300` | Base delay for definitely failed delivery; grows to two hours |
| `URGENT_TRUSTED_SOURCES` | empty | Allowed feed names; empty disables urgent bypass |
| `URGENT_MIN_SCORE` | `95` | Urgent score threshold, permitted range 95–100 |
| `URGENT_MAX_AGE_HOURS` | `3` | Maximum age of urgent source publication |
| `URGENT_MIN_INTERVAL` | `1800` | Urgent spacing after any delivery, seconds |
| `URGENT_DAILY_LIMIT` | `1` | Maximum interval bypasses per local calendar day |

`AI_MAX_SEND_PER_BATCH` is obsolete and ignored. Existing environments can leave
it in place. Existing provider, profile, batch and quota settings remain compatible.
The new defaults require no edits to the production environment. Decide explicitly
which sources, if any, should enable urgent bypass.

## Persistence and delivery failures

States are `pending`, `rejected`, `queued`, `sending`, `published`, `retry`,
`expired`, and `delivery_unknown`. The additive transactional migration retains
historical ids, votes, scores, summaries and state. It recovers unsent high-score
legacy candidates stranded by the batch cap; old English summaries return for
batch regeneration. First-run snapshots remain excluded. A file lock prevents
two instances from publishing from the same database. Each execution thread has
its own SQLite connection; slow RSS/AI calls do not block Telegram voting.

A durable `sending` reservation is committed before network I/O. `sendMessage`
is attempted once. Explicit Bot API 4xx rejections (including 429) and connection
establishment timeouts permit a local retry, with the approved summary retained. A persisted queue-wide cooldown for 429 and
connection establishment failures prevents retrying the entire queue against an
unavailable API.
Read timeouts, resets, 5xx and malformed responses may follow a successful send.
These become `delivery_unknown` and **are never retried automatically**. A crash
while sending is handled the same way. Uncertain attempts reserve daily capacity
and spacing; requests crossing midnight reserve both potentially affected days.
Telegram cannot guarantee exactly-once sends or query a message by an application
idempotency key. Automatic resend would risk duplicates; quarantine preserves the
article for reconciliation instead of dropping it.

A click on a delivered article's vote button confirms its message id and actual
date even if the original response was lost. Otherwise inspect the target chat
and resolve manually. Find unresolved records (adjust DB path if needed):

```bash
sudo -u newsbot /opt/newsbot/venv/bin/python - <<'PY'
import sqlite3
with sqlite3.connect('/var/lib/newsbot/news.db') as db:
    for row in db.execute("SELECT id,title,attempted_at,send_error FROM articles WHERE status='delivery_unknown'"):
        print(row)
PY
```

Stop the service before using the reconciliation CLI so the process lock is free.
Use `systemd-run` to load the existing environment without printing secrets:

```bash
sudo systemctl stop newsbot
# Confirm a message that is present in the chat. Replace numeric examples with
# article id, Telegram message id and ACTUAL delivery time (UTC Unix seconds).
sudo systemd-run --wait --pipe --collect \
  -p User=newsbot -p Group=newsbot -p WorkingDirectory=/opt/newsbot \
  -p EnvironmentFile=/etc/newsbot.env \
  /opt/newsbot/venv/bin/python /opt/newsbot/bot.py \
  --resolve-delivery 123 --message-id 456 --sent-at 1791547200
sudo systemctl start newsbot
```

Only after verifying that the message was NOT delivered, use
`--resolve-delivery 123 --confirm-not-delivered` instead of the last line of CLI
arguments. This allows a retry with the same summary and normal scheduling;
a stale article still expires. Never infer non-delivery from a timeout alone.

Article ids and all rating/vote history are retained to prevent repeated RSS
publication. Cleanup clears only old bodies/excerpts after 60 days. Metadata
therefore grows gradually with incoming volume; no heavy queue service is needed.

## API cost and testing

Scheduling, urgent checks and Telegram retries use **no AI calls**. Ranking stays
batched (up to ten candidates per request); bootstrap/profile updates keep their
existing behavior. Transport failures back off; truncated/malformed output retries
with smaller batches. Invalid summaries retry with per-article backoff. All
approved articles now get summaries, so output-token consumption may rise versus
the old three-item cap. Ranking also continues when the posting quota is full;
total request count can rise on busy days, depending on incoming candidates. There
is no per-post translation or urgency request. A persistent
`AI_DAILY_REQUEST_LIMIT=10` bounds ranking calls per local day, including failed
attempts; bootstrap/profile requests are separate. Once the budget is spent, raw
candidates remain pending for the next day, subject to the same freshness limit.
Changing this budget trades evaluation throughput against API cost. The budget
also means a newly arrived urgent candidate may wait if ranking is exhausted.

Tests require no real keys and no additional testing dependencies:

```bash
python3 -m venv venv
venv/bin/pip install -r requirements.txt
venv/bin/python -m unittest discover -s tests -v
venv/bin/python -m py_compile bot.py tests/test_bot.py
```

## Safe VPS update (operator-run; after PR merge)

Use the paths configured in the existing unit/environment. The following examples
match `newsbot.service` and `.env.example`. Do not copy `.env.example` over
`/etc/newsbot.env`, replace the profile, or delete the database. Keep the existing
`BOT_TIMEZONE` unless a deliberate timezone change is wanted.

```bash
cd /opt/newsbot
git status --short
# Proceed with a clean working tree; preserve any local modifications first.
git fetch origin
git switch main
git pull --ff-only origin main
venv/bin/pip install -r requirements.txt
venv/bin/python -m unittest discover -s tests -v
venv/bin/python -m py_compile bot.py tests/test_bot.py

sudo systemctl stop newsbot
# SQLite backup includes committed WAL data; copying news.db alone may omit it.
# Adjust these two paths to the actual DB_PATH and PROFILE_PATH on the server.
sudo -u newsbot /opt/newsbot/venv/bin/python - <<'PY'
from datetime import datetime, timezone
from pathlib import Path
import shutil
import sqlite3
suffix = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
path = Path('/var/lib/newsbot/news.db')
with sqlite3.connect(str(path)) as source, sqlite3.connect(str(path) + '.backup-' + suffix) as target:
    source.backup(target)
profile = Path('/var/lib/newsbot/recommendation_profile.json')
if profile.exists():
    shutil.copy2(profile, str(profile) + '.backup-' + suffix)
PY
# First start performs the additive migration automatically.
sudo systemctl start newsbot
sudo systemctl status newsbot --no-pager
sudo journalctl -u newsbot -n 100 --no-pager
```

Retain the database/profile backup until the new queue and voting behavior have
been checked. To roll back, stop the service, restore BOTH the old code and the
pre-migration database/profile backup, and start again. Running the old code on
a migrated queue is unsafe because it ignores queue statuses. Do not restore an
old backup after new publications without reconciling those messages: otherwise
history can roll back and allow duplicate sends. The unit and dependency list
are unchanged; no daemon-reload is needed for this code-only update.
