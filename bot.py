"""
Telegram news bot with simple recommendations.

- Checks RSS feeds every CHECK_INTERVAL seconds and posts new articles + source name.
- Every post has 👍 / 👎 buttons. Votes are stored in SQLite.
- Votes train a simple keyword model: words from liked titles get +1, disliked -1.
  Each new article gets a score from that model.
  * score >= STAR_SCORE  -> post is marked with ⭐ (recommended)
  * score <  MIN_SCORE   -> post is skipped (only if MIN_SCORE is set)

Settings (environment variables, see .env.example):
  BOT_TOKEN        token from @BotFather (required)
  CHAT_ID          your user id, a group id, or @channelname (required)
  CHECK_INTERVAL   seconds between feed checks (default 600)
  DB_PATH          SQLite file (default news.db)
  ALLOWED_VOTERS   comma-separated Telegram user ids allowed to vote (empty = anyone)
  STAR_SCORE       score needed for ⭐ (default 3)
  MIN_SCORE        skip articles below this score (empty = never skip)
"""
import html
import logging
import os
import re
import sqlite3
import time
from collections import defaultdict

import feedparser
import requests

# ---------- config ----------

BOT_TOKEN = os.environ["BOT_TOKEN"]
CHAT_ID = os.environ["CHAT_ID"]
INTERVAL = int(os.environ.get("CHECK_INTERVAL", "600"))
DB_PATH = os.environ.get("DB_PATH", "news.db")
ALLOWED_VOTERS = {
    int(x) for x in os.environ.get("ALLOWED_VOTERS", "").replace(" ", "").split(",") if x
}
STAR_SCORE = float(os.environ.get("STAR_SCORE", "3"))
_min = os.environ.get("MIN_SCORE", "").strip()
MIN_SCORE = float(_min) if _min else None

# Site name -> RSS feed. Add or remove sites here.
FEEDS = {
    "Habr": "https://habr.com/ru/rss/articles/?fl=ru",
    "Rozetked": "https://rozetked.me/rss.xml",
    "Meduza": "https://meduza.io/rss/all",
}

SOURCE_LABEL = "Источник"  # "Source" in Russian
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; NewsBot/2.0)"}
API = f"https://api.telegram.org/bot{BOT_TOKEN}"
KEEP_DAYS = 30  # unvoted articles older than this are deleted; voted ones are kept

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("newsbot")


# ---------- database ----------

def init_db():
    db = sqlite3.connect(DB_PATH)
    db.executescript(
        """
        CREATE TABLE IF NOT EXISTS articles (
            id      INTEGER PRIMARY KEY AUTOINCREMENT,
            item_id TEXT UNIQUE NOT NULL,   -- RSS guid or link
            source  TEXT NOT NULL,
            title   TEXT,
            link    TEXT,
            added   INTEGER NOT NULL,
            sent    INTEGER NOT NULL DEFAULT 0,  -- 1 = posted, 0 = remembered/skipped
            score   REAL,
            vote    INTEGER                      -- 1, -1 or NULL
        );
        CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT);
        """
    )
    db.commit()
    return db


def get_state(db, key, default):
    row = db.execute("SELECT value FROM state WHERE key = ?", (key,)).fetchone()
    return row[0] if row else default


def set_state(db, key, value):
    db.execute("INSERT OR REPLACE INTO state VALUES (?, ?)", (key, value))
    db.commit()


def is_seen(db, item_id):
    return db.execute("SELECT 1 FROM articles WHERE item_id = ?", (item_id,)).fetchone() is not None


def source_known(db, source):
    return db.execute("SELECT 1 FROM articles WHERE source = ? LIMIT 1", (source,)).fetchone() is not None


def add_article(db, item_id, source, title, link, sent, score):
    cur = db.execute(
        "INSERT OR IGNORE INTO articles (item_id, source, title, link, added, sent, score) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (item_id, source, title, link, int(time.time()), sent, score),
    )
    db.commit()
    return cur.lastrowid


def cleanup(db):
    db.execute(
        "DELETE FROM articles WHERE vote IS NULL AND added < ?",
        (int(time.time()) - KEEP_DAYS * 86400,),
    )
    db.commit()


# ---------- recommendation model ----------

WORD_RE = re.compile(r"[a-zа-яё0-9]+")
STOPWORDS = {
    # Russian
    "этот", "этого", "чтобы", "который", "которые", "когда", "после", "почему",
    "также", "может", "будет", "были", "более", "очень", "своих", "свои",
    # English
    "this", "that", "with", "from", "what", "your", "have", "will", "about",
}


def keywords(text):
    """Lowercase words, 4+ letters, cut to 6 chars as a crude stem.
    Cutting helps Russian: 'смартфон', 'смартфона', 'смартфоны' -> 'смартф'."""
    words = set()
    for w in WORD_RE.findall((text or "").lower()):
        if len(w) >= 4 and w not in STOPWORDS:
            words.add(w[:6])
    return words


def build_model(db):
    word_w = defaultdict(float)
    src_w = defaultdict(float)
    for source, title, vote in db.execute(
        "SELECT source, title, vote FROM articles WHERE vote IS NOT NULL"
    ):
        src_w[source] += vote
        for k in keywords(title):
            word_w[k] += vote
    return word_w, src_w


def score_article(model, source, title):
    word_w, src_w = model
    words = sum(word_w.get(k, 0.0) for k in keywords(title))
    src = max(-2.0, min(2.0, src_w.get(source, 0.0) * 0.2))  # small, capped site bias
    return words + src


# ---------- Telegram ----------

def tg(method, **params):
    """Call a Telegram Bot API method. Returns the result or None.
    Never logs exception text, because it would contain the URL with the token."""
    for _ in range(3):
        try:
            r = requests.post(
                f"{API}/{method}", json=params, timeout=params.get("timeout", 0) + 20
            )
            data = r.json()
        except (requests.RequestException, ValueError) as e:
            log.warning("Telegram %s failed (%s), retrying", method, type(e).__name__)
            time.sleep(5)
            continue
        if data.get("ok"):
            return data["result"]
        if r.status_code == 429:
            time.sleep(data.get("parameters", {}).get("retry_after", 5))
            continue
        log.error("Telegram %s error %s: %s", method, r.status_code, data.get("description"))
        return None
    return None


def keyboard(article_id, vote=None):
    up = "👍" + (" ✓" if vote == 1 else "")
    down = "👎" + (" ✓" if vote == -1 else "")
    return {
        "inline_keyboard": [[
            {"text": up, "callback_data": f"v:{article_id}:1"},
            {"text": down, "callback_data": f"v:{article_id}:-1"},
        ]]
    }


def send_article(article_id, source, title, link, score):
    star = "⭐ " if score >= STAR_SCORE else ""
    text = (
        f"{star}<b>{html.escape(title)}</b>\n"
        f"{html.escape(link)}\n\n"
        f"{SOURCE_LABEL}: {source}"
    )
    result = tg(
        "sendMessage",
        chat_id=CHAT_ID,
        text=text,
        parse_mode="HTML",
        reply_markup=keyboard(article_id),
    )
    return result is not None


def handle_vote(db, cq):
    def answer(text=None):
        params = {"callback_query_id": cq["id"]}
        if text:
            params["text"] = text
        tg("answerCallbackQuery", **params)

    if ALLOWED_VOTERS and cq["from"]["id"] not in ALLOWED_VOTERS:
        answer("Voting is not available for you")
        return

    try:
        prefix, art_id, vote = cq.get("data", "").split(":")
        art_id, vote = int(art_id), int(vote)
    except ValueError:
        answer()
        return
    if prefix != "v" or vote not in (1, -1):
        answer()
        return

    row = db.execute("SELECT vote FROM articles WHERE id = ?", (art_id,)).fetchone()
    if row is None:
        answer("This article is too old")
        return

    new_vote = None if row[0] == vote else vote  # tapping the same button again removes the vote
    db.execute("UPDATE articles SET vote = ? WHERE id = ?", (new_vote, art_id))
    db.commit()

    msg = cq.get("message")
    if msg:
        tg(
            "editMessageReplyMarkup",
            chat_id=msg["chat"]["id"],
            message_id=msg["message_id"],
            reply_markup=keyboard(art_id, new_vote),
        )
    answer({1: "Saved 👍", -1: "Saved 👎", None: "Vote removed"}[new_vote])
    log.info("Vote %s on article %s by user %s", new_vote, art_id, cq["from"]["id"])


def handle_updates(db, wait):
    offset = int(get_state(db, "offset", "0"))
    updates = tg("getUpdates", offset=offset, timeout=wait, allowed_updates=["callback_query"])
    if updates is None:
        time.sleep(5)  # API error: don't spin in a tight loop
        return
    for u in updates:
        set_state(db, "offset", str(u["update_id"] + 1))
        if "callback_query" in u:
            handle_vote(db, u["callback_query"])


# ---------- feeds ----------

def fetch_entries(url):
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return feedparser.parse(r.content).entries


def check_source(db, model, source, url):
    try:
        entries = fetch_entries(url)
    except Exception as e:
        log.warning("%s: could not load feed: %s", source, e)
        return

    first_run = not source_known(db, source)
    new = []
    for entry in entries:
        item_id = entry.get("id") or entry.get("link")
        if item_id and not is_seen(db, item_id):
            new.append((item_id, entry))

    # Feeds list newest first; handle oldest first so the chat reads in order.
    for item_id, entry in reversed(new):
        title = entry.get("title", "(no title)")
        link = entry.get("link", "")

        if first_run:  # don't flood the chat with old news the first time
            add_article(db, item_id, source, title, link, sent=0, score=None)
            continue

        score = score_article(model, source, title)
        if MIN_SCORE is not None and score < MIN_SCORE:
            add_article(db, item_id, source, title, link, sent=0, score=score)
            log.info("%s: skipped (score %.1f) '%s'", source, score, title)
            continue

        art_id = add_article(db, item_id, source, title, link, sent=1, score=score)
        if send_article(art_id, source, title, link, score):
            log.info("%s: sent (score %.1f) '%s'", source, score, title)
        else:
            # Forget it so the next check retries
            db.execute("DELETE FROM articles WHERE id = ?", (art_id,))
            db.commit()
        time.sleep(1)  # be gentle with Telegram limits

    if first_run:
        log.info("%s: first run, remembered %d existing items", source, len(new))


# ---------- main loop ----------

def main():
    db = init_db()
    log.info("Bot started: %d sites, check every %d s", len(FEEDS), INTERVAL)
    next_check = 0.0
    while True:
        if time.time() >= next_check:
            model = build_model(db)
            for source, url in FEEDS.items():
                check_source(db, model, source, url)
            cleanup(db)
            next_check = time.time() + INTERVAL
        # Between feed checks, wait for button presses (long polling).
        wait = max(1, min(25, int(next_check - time.time())))
        handle_updates(db, wait)


if __name__ == "__main__":
    main()
