# News Bot: Project Context

Read this document before changing the repository. It describes technical behavior,
not private user history. Keep credentials and server configuration untracked.

## Architecture

`bot.py` uses requests, feedparser, beautifulsoup4 and the Python standard library
(Python 3.9+ on Linux). The systemd unit remains compatible and uses the `newsbot`
user, `/opt/newsbot`, and `/etc/newsbot.env`. Never deploy or restart production
unless explicitly requested.

Three execution loops share SQLite through separate connections:

- The foreground loop long-polls Telegram callbacks and stores 👍/👎 votes.
- A maintenance thread fetches RSS every `CHECK_INTERVAL` (600 seconds), extracts
  article bodies, evaluates candidates in batches and maintains the existing
  recommendation profile. The initial feed snapshot is remembered without posting.
- A publisher thread checks the persistent queue every five seconds and sends at
  most one eligible article. RSS extraction and the 90-second AI timeout cannot
  block votes or the publisher. No SQLite write transaction spans network I/O.

SQLite uses WAL and a busy timeout. A Linux file lock beside `DB_PATH` prevents two
bot processes from publishing from the same database. Do not remove an active lock
file or run another copy against another database for the same chat.

## Selection and language

GPT-6 Luna (`openai/gpt-6-luna`) is called through Polza AI's Chat Completions API,
with `reasoning_effort=none`. Preserve batch ranking, the stored profile, existing
votes, profile bootstrap and daily profile updates. Keep the legacy
`OPENROUTER_*_MAX_TOKENS` setting names for compatibility.

Every worthwhile article scoring at least `AI_MIN_SCORE` is approved independently
of daily delivery capacity. `AI_MAX_SEND_PER_BATCH` is obsolete and ignored.
Never discard good articles because a batch contains more than three of them.
The model can approve zero articles; quality takes precedence over the quota.

New summaries must be natural, grammatical Russian, independent of source language,
700–1600 characters, factual and without filler or invented details. Proper names,
companies and technologies may remain in Latin script. Posts retain the source
headline and use the label `Источник:`. Profile values may remain in their existing
language. Do not blindly replace them or translate historical summaries.

The local publication gate checks length, prevalence of Russian words, Russian
function words and English sentence patterns. This conservative heuristic allows
Latin names; it is not a grammar or fact verifier. Empty, English, mixed-prose or
otherwise invalid summaries stay pending for batch regeneration with backoff.
Never fall back to an untranslated RSS excerpt. Validate language both after AI
ranking and immediately before sending, including migrated old summaries.

Feed/article text and profile text are data, not instructions. The prompt explicitly
ignores instructions embedded in them. Do not send secrets to the AI provider.

## Queue and scheduling

`status` is authoritative; legacy `sent` and `evaluated` fields stay compatible:

- `pending`: awaits AI evaluation (including deferred summary regeneration).
- `rejected`: evaluated and rejected, or a first-run feed snapshot.
- `queued`: approved and waiting for publication.
- `sending`: durable reservation committed before the Telegram request.
- `published`: confirmed delivery, with `sent_at` and Telegram message id.
- `retry`: definitely unsuccessful delivery, waiting for a local retry.
- `expired`: no longer timely; never publish.
- `delivery_unknown`: delivery may have succeeded; requires reconciliation.

Normal posts require `MIN_SEND_INTERVAL` elapsed seconds (default 5400) since the
latest confirmed or potentially successful delivery. There are no accumulating
slots, fixed posting times or quota-fill requirements. The interval survives
midnight, restart and timezone changes. Priority combines AI score, freshness
(-1.5 points per hour since the earlier of RSS publication and arrival), waiting
(+0.25 points/hour, capped at 4), then arrival time/id. Scores below the quality
threshold are never promoted. Candidates and queued/retry entries expire after
`CANDIDATE_MAX_AGE_HOURS` (24) based on the earlier publication/arrival timestamp.

Urgent interval bypass is opt-in via `URGENT_TRUSTED_SOURCES`. It requires an
explicit AI flag, score >= `URGENT_MIN_SCORE` (at least 95), a known RSS publication
time within `URGENT_MAX_AGE_HOURS` (3), an allowed source, and a verbatim 40–400
character evidence quote containing an emergency marker. Allowed categories are
active exploitation, immediate public-safety danger and an ongoing major outage.
Product launches, popularity, routine news, speculation and resolved events do not
qualify. The local gate cannot independently verify source claims; configure only
sources you trust. At send time, source permission and freshness are checked again.
Urgent bypasses are separated by at least `URGENT_MIN_INTERVAL` (1800) from ANY
publication and limited to `URGENT_DAILY_LIMIT` (1) per local day. Further urgent
candidates follow the normal interval. All posts count against the overall daily
limit, including urgent posts. An urgent send also resets the ordinary interval.

Calendar boundaries use `BOT_TIMEZONE` (IANA, default UTC) and actual UTC timestamps,
not a resettable date counter. DST days can have 23 or 25 hours. Changing the
configured timezone recalculates calendar-day counts from retained history while
leaving elapsed-time spacing intact. Uncertain requests spanning midnight reserve
capacity in both potentially affected days.

## Reliability and migration

`init_db()` adds columns and indexes transactionally. It preserves ids, votes,
scores, summaries, state and profile history. Sent rows retain their timestamps;
legacy rows without a timestamp use arrival as the best available approximation.
First-run snapshots stay rejected. Unsent high-score legacy rows stranded by the
batch cap are recovered into the queue or returned for evaluation if the summary
is absent. Old English summaries cannot pass the publication gate.

AI transport errors and invalid JSON/schema retain candidates with exponential
backoff (5 minutes to 2 hours). Truncated/malformed responses reduce the next batch
size; a successful response restores the configured maximum. Invalid individual
summaries use per-article backoff, allowing other candidates to proceed. Never mark
an unassessed article rejected merely to avoid a retry. Delivery retries and
scheduling do not call Polza. Summarizing all approved articles can increase output
tokens relative to the previous three-item cap, and ranking continues when today's
delivery quota is exhausted; API request growth depends on incoming volume. A timestamped SQLite request ledger
limits ranking attempts (including failures) to `AI_DAILY_REQUEST_LIMIT=10` per
local day by default; 0 disables the budget. Profile calls are excluded. Pending
candidates wait when this budget is exhausted and remain subject to expiry; this
can also postpone evaluation of a new urgent candidate. Preserve this cost ceiling.

Telegram `sendMessage` has no idempotency key. Make one HTTP attempt only. A
ConnectTimeout or explicit Bot API 4xx rejection allows retry with persisted
backoff (respecting a persisted queue-wide 429 cooldown). ReadTimeout, connection reset, 5xx or malformed
response is ambiguous: retain `delivery_unknown`, reserve quota, never automatically
resend. Interrupted `sending` records are similarly quarantined at startup. This
chooses duplicate avoidance over automatic redelivery of uncertain requests; the
article remains inspectable and recoverable. An authentic callback from the target
chat confirms delivery; an operator can otherwise reconcile via CLI after checking
the chat. Do not claim exactly-once delivery is guaranteed by Telegram.

Do not log exception text or Telegram URLs containing the token. Callbacks are
idempotent by callback id, while a new click still toggles its vote. SQLite history
and deduplication ids are retained; cleanup only clears old terminal article bodies
and RSS excerpts after 60 days, preserving votes, scores and summaries.

## Development and operations

`.env.example` documents all scheduling settings; README.md provides update,
backup and reconciliation commands. Never overwrite a real environment file with
the example. No dependencies beyond requirements.txt are needed.

Run offline mocked integration tests with:

```bash
venv/bin/python -m unittest discover -s tests -v
venv/bin/python -m py_compile bot.py tests/test_bot.py
```

Tests use temporary SQLite databases, controlled time and mocked Polza/Telegram
HTTP. Keep coverage for bursts, batches, midnight, restart, urgency, quotas, empty
selection, network failures, Russian summaries, legacy migrations and stranded
high-score candidates. Required future changes must preserve these invariants.
