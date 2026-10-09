"""Offline integration tests: temporary SQLite, controlled clock, mocked HTTP APIs."""
import importlib
import json
import os
import sqlite3
import tempfile
import threading
import unittest
from datetime import date, datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

with patch.dict(os.environ, {"BOT_TOKEN": "test-token", "CHAT_ID": "42", "POLZA_API_KEY": "test-key"}):
    bot = importlib.import_module("bot")

RU = (
    "Компания OpenAI представила обновление технологии, которое меняет способ обработки запросов. "
    "В статье приведены результаты испытаний и описаны ограничения новой версии. Для пользователей "
    "это означает, что при выборе решения нужно учитывать не только скорость, но и точность ответа. "
    "Разработчики объяснили, как работает система и почему часть функций пока доступна лишь участникам "
    "тестирования. При этом сведения о сроках общего запуска не опубликованы, поэтому делать выводы "
    "о доступности для всех ещё рано. В материале также указано, что прежняя версия продолжит работать "
    "в течение переходного периода. Эти данные важны для команд, которые уже используют API и планируют "
    "изменения в своих приложениях. Авторы рекомендуют сначала проверить совместимость на собственных "
    "задачах и оценить результаты, прежде чем переводить рабочие процессы на новое решение."
)
EVIDENCE = "Объявлена немедленная эвакуация жителей района из-за непосредственной угрозы жизни."


class BotTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.now = int(datetime(2026, 10, 9, 12, tzinfo=timezone.utc).timestamp())
        settings = dict(DB_PATH=str(Path(self.tmp.name) / "news.db"),
                        PROFILE_PATH=str(Path(self.tmp.name) / "profile.json"),
                        POLZA_API_KEY="fake", AI_DAILY_REQUEST_LIMIT=10, TZ=ZoneInfo("UTC"), DAILY_NEWS_LIMIT=10,
                        MIN_SEND_INTERVAL=5400, URGENT_MIN_INTERVAL=1800, URGENT_DAILY_LIMIT=1,
                        URGENT_MIN_SCORE=95, URGENT_MAX_AGE_HOURS=3,
                        URGENT_TRUSTED_SOURCES={"Meduza"}, AI_BATCH_SIZE=5,
                        AI_BATCH_MAX_ITEMS=10, AI_BATCH_MAX_WAIT=3600, CANDIDATE_MAX_AGE_HOURS=24,
                        ALLOWED_VOTERS=set(), CHAT_ID="42")
        for key, val in settings.items():
            p = patch.object(bot, key, val)
            p.start()
            self.addCleanup(p.stop)
        clock = patch.object(bot.time, "time", side_effect=lambda: self.now)
        clock.start()
        self.addCleanup(clock.stop)
        self.overrides = {}
        self.ai_calls = []
        self.sent = []
        self.telegram_error = None
        self.ai_error = None
        self.http = patch.object(bot.requests, "post", side_effect=self.api)
        self.http_mock = self.http.start()
        self.addCleanup(self.http.stop)
        # A GET here would be an unintended real network request.
        guard = patch.object(bot.requests, "get", side_effect=AssertionError("Unexpected network GET"))
        guard.start()
        self.addCleanup(guard.stop)
        self.db = bot.init_db()
        self.addCleanup(lambda: self.db.close())

    def response(self, data, status=200):
        response = Mock(status_code=status)
        response.json.return_value = data
        response.raise_for_status.return_value = None
        return response

    def api(self, url, **kwargs):
        if "polza.ai" in url:
            self.ai_calls.append(kwargs["json"])
            if self.ai_error:
                raise self.ai_error
            prompt = kwargs["json"]["messages"][0]["content"]
            rows, _ = json.JSONDecoder().raw_decode(prompt.split("Кандидаты: ", 1)[1])
            items = []
            for r in rows:
                item = dict(id=r["id"], relevance_score=85, send=True, summary=RU,
                            reason="Содержательная статья", urgent=False)
                item.update(self.overrides.get(r["id"], {}))
                items.append(item)
            return self.response({"choices": [{"finish_reason": "stop", "message": {"content": json.dumps({"items": items})}}]})
        if url.endswith("/sendMessage"):
            if self.telegram_error:
                if isinstance(self.telegram_error, Exception):
                    raise self.telegram_error
                return self.telegram_error
            self.sent.append(kwargs["json"])
            return self.response({"ok": True, "result": {"message_id": len(self.sent), "date": self.now}})
        if url.endswith("/getUpdates"):
            return self.response({"ok": True, "result": []})
        return self.response({"ok": True, "result": True})

    def add(self, n=1, published_at=None, content="Исходный текст", evaluated=0):
        ids = []
        first = self.db.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
        for i in range(n):
            key = f"article-{first+i}"
            ids.append(bot.add_article(self.db, key, "Meduza", "Заголовок", f"https://example.org/{key}",
                                       content=content, evaluated=evaluated, published_at=published_at))
        return ids

    def row(self, article_id):
        return self.db.execute("SELECT * FROM articles WHERE id = ?", (article_id,)).fetchone()

    def rank(self):
        bot.process_candidate_batch(self.db, force=True)

    def publish(self):
        bot.publish_next(self.db)

    def urgent(self):
        article_id = self.add(published_at=self.now, content=EVIDENCE)[0]
        self.overrides[article_id] = dict(relevance_score=98, urgent=True,
                                          urgent_category="public_safety", urgent_evidence=EVIDENCE)
        self.rank()
        return article_id

    def test_30_articles_three_ai_calls_only_one_publication(self):
        self.add(30)
        for _ in range(3):
            self.rank()
            self.publish()
        for _ in range(10):
            self.publish()
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(len(self.ai_calls), 3)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM articles WHERE status='queued'").fetchone()[0], 29)

    def test_batches_share_minimum_interval(self):
        self.add(5)
        self.rank()
        self.publish()
        self.now += 600
        self.add(5)
        self.rank()
        self.publish()
        self.assertEqual(len(self.sent), 1)
        self.now += 4799
        self.publish()
        self.assertEqual(len(self.sent), 1)
        self.now += 1
        self.publish()
        self.assertEqual(len(self.sent), 2)

    def test_midnight_does_not_reset_interval(self):
        self.now = int(datetime(2026, 10, 9, 23, 50, tzinfo=timezone.utc).timestamp())
        self.add(10)
        self.rank()
        self.publish()
        self.now += 20 * 60
        for _ in range(10):
            self.publish()
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(bot.sent_today(self.db), 0)
        self.now += 70 * 60
        self.publish()
        self.assertEqual(len(self.sent), 2)

    def test_restart_preserves_queue_last_send_and_quota(self):
        self.add(10)
        self.rank()
        self.publish()
        self.db.close()
        self.now += 300
        self.db = bot.init_db()
        bot.recover_interrupted_deliveries(self.db)
        self.publish()
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(bot.sent_today(self.db), 1)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM articles WHERE status='queued'").fetchone()[0], 9)

    def test_genuine_urgent_can_bypass_normal_interval(self):
        self.add(5)
        self.rank()
        self.publish()
        self.now += 1800
        article_id = self.urgent()
        self.publish()
        self.assertEqual(len(self.sent), 2)
        self.assertEqual(self.row(article_id)["status"], "published")
        self.assertEqual(bot.sent_today(self.db, urgent_only=True), 1)
        self.now += 1800
        second = self.urgent()
        self.publish()
        self.assertEqual(self.row(second)["status"], "queued")
        self.assertEqual(len(self.sent), 2)

    def test_urgent_does_not_exceed_daily_limit(self):
        self.now = int(datetime(2026, 10, 9, 0, tzinfo=timezone.utc).timestamp())
        self.add(10)
        self.rank()
        for _ in range(10):
            self.publish()
            self.now += 5400
        self.assertEqual(bot.sent_today(self.db), 10)
        urgent_id = self.urgent()
        self.publish()
        self.assertEqual(len(self.sent), 10)
        self.assertEqual(self.row(urgent_id)["status"], "queued")

    def test_urgency_requires_trusted_source_freshness_score_and_exact_evidence(self):
        for change in ({"published_at": None}, {"published_at": self.now - 10801},
                       {"source": "Untrusted"}, {"ai_score": 94}, {"evidence": "Fabricated"}):
            article_id = self.add(published_at=self.now, content=EVIDENCE)[0]
            info = dict(score=98, urgent=True, urgent_category="public_safety", urgent_evidence=EVIDENCE)
            for key, value in change.items():
                if key in ("published_at", "source"):
                    self.db.execute(f"UPDATE articles SET {key}=? WHERE id=?", (value, article_id))
                    self.db.commit()
                elif key == "ai_score":
                    info["score"] = value
                else:
                    info["urgent_evidence"] = value
            self.assertFalse(bot.qualifies_urgent(self.row(article_id), info, self.now))

    def test_no_quality_no_posts(self):
        ids = self.add(5)
        self.overrides = {i: dict(send=False, relevance_score=30, summary="") for i in ids}
        self.rank()
        self.publish()
        self.assertFalse(self.sent)
        self.assertTrue(all(self.row(i)["status"] == "rejected" for i in ids))

    def test_explicit_telegram_429_retries_without_ai_or_news_loss(self):
        article_id = self.add()[0]
        self.rank()
        self.telegram_error = self.response({"ok": False, "error_code": 429, "parameters": {"retry_after": 600}}, 429)
        self.publish()
        self.assertEqual(self.row(article_id)["status"], "retry")
        self.assertEqual(bot.sent_today(self.db), 0)
        self.now += 599
        self.telegram_error = None
        self.publish()
        self.assertFalse(self.sent)
        self.now += 1
        self.publish()
        self.assertEqual(self.row(article_id)["status"], "published")
        self.assertEqual(len(self.ai_calls), 1)

    def test_connect_timeout_safe_retry(self):
        article_id = self.add()[0]
        self.rank()
        self.telegram_error = bot.requests.ConnectTimeout()
        self.publish()
        self.assertEqual(self.row(article_id)["status"], "retry")
        self.now += 300
        self.telegram_error = None
        self.publish()
        self.assertEqual(len(self.sent), 1)

    def test_read_timeout_never_automatically_retries_even_after_restart(self):
        article_id = self.add()[0]
        self.rank()
        self.telegram_error = bot.requests.ReadTimeout("Secret URL should never be logged")
        self.publish()
        self.assertEqual(self.row(article_id)["status"], "delivery_unknown")
        self.assertEqual(bot.sent_today(self.db), 1)
        self.now += 6000
        self.db.close()
        self.db = bot.init_db()
        bot.recover_interrupted_deliveries(self.db)
        self.telegram_error = None
        self.publish()
        self.assertFalse(self.sent)
        self.assertEqual(len(self.ai_calls), 1)
        self.assertEqual(self.http_mock.call_count, 2)

    def test_uncertain_send_crossing_midnight_reserves_both_days(self):
        self.now = int(datetime(2026, 10, 9, 23, 59, 59, tzinfo=timezone.utc).timestamp())
        article_id = self.add()[0]
        self.rank()
        def timeout(*args, **kwargs):
            self.now += 5
            raise bot.requests.ReadTimeout()
        with patch.object(bot.requests, "post", side_effect=timeout):
            self.publish()
        self.assertEqual(bot.sent_today(self.db), 1)
        self.assertEqual(bot.sent_today(self.db, self.now - 10), 1)
        self.assertEqual(bot.last_delivery(self.db), self.now)
        self.assertEqual(self.row(article_id)["status"], "delivery_unknown")

    def test_vote_reconciles_unknown_delivery_and_persists(self):
        article_id = self.add()[0]
        self.rank()
        self.telegram_error = bot.requests.ReadTimeout()
        self.publish()
        self.telegram_error = None
        cq = dict(id="cq", data=f"v:{article_id}:1", **{"from": {"id": 1}},
                  message={"chat": {"id": 42}, "message_id": 77, "date": self.now})
        bot.handle_vote(self.db, cq)
        row = self.row(article_id)
        self.assertEqual((row["vote"], row["status"], row["telegram_message_id"]), (1, "published", 77))
        bot.handle_vote(self.db, cq)
        self.assertEqual(self.row(article_id)["vote"], 1)  # Replayed callback must not toggle.
        cq["id"] = "cq-next"
        bot.handle_vote(self.db, cq)
        self.assertIsNone(self.row(article_id)["vote"])

    def test_operator_confirms_failure_or_delivery(self):
        for delivered in (True, False):
            article_id = self.add()[0]
            self.rank()
            self.telegram_error = bot.requests.ReadTimeout()
            self.publish()
            bot.resolve_delivery(self.db, article_id, 99 if delivered else None,
                                 self.now if delivered else None, not_delivered=not delivered)
            self.assertEqual(self.row(article_id)["status"], "published" if delivered else "retry")
            self.now += 5400

    def test_interrupted_sending_is_quarantined_not_requeued(self):
        article_id = self.add()[0]
        self.db.execute("UPDATE articles SET status='sending', attempted_at=? WHERE id=?", (self.now, article_id))
        self.db.commit()
        self.now += 5
        bot.recover_interrupted_deliveries(self.db)
        self.assertEqual(self.row(article_id)["status"], "delivery_unknown")
        self.assertEqual(bot.sent_today(self.db), 1)
        self.publish()
        self.assertFalse(self.sent)

    def test_ai_error_preserves_candidates_with_backoff(self):
        ids = self.add(10)
        self.ai_error = bot.requests.HTTPError()
        self.rank()
        self.assertTrue(all(self.row(i)["status"] == "pending" for i in ids))
        self.rank()
        self.assertEqual(len(self.ai_calls), 1)
        self.now += 300
        self.ai_error = None
        self.rank()
        self.assertTrue(all(self.row(i)["status"] == "queued" for i in ids))
        self.assertEqual(len(self.ai_calls), 2)

    def test_empty_english_or_mixed_summary_not_posted_or_replaced_by_excerpt(self):
        en = ("The company has announced a new product and the users will be able to use it. " * 13).strip()
        mixed = RU + " The company has announced a new product and the users will be able to use it."
        for summary in ("", None, en, mixed):
            article_id = self.add(content=en)[0]
            self.overrides[article_id] = dict(summary=summary)
            self.rank()
            self.publish()
            self.assertEqual(self.row(article_id)["status"], "pending")
        self.assertFalse(self.sent)

    def test_russian_prompt_summary_latin_names_and_source_label(self):
        self.assertTrue(bot.valid_russian_summary(RU))
        self.add()
        self.rank()
        self.publish()
        prompt = self.ai_calls[0]["messages"][0]["content"]
        self.assertIn("НА РУССКОМ", prompt)
        self.assertIn("700–1600", prompt)
        self.assertNotIn("English summary", prompt)
        self.assertIn("Источник:", self.sent[0]["text"])
        self.assertIn("OpenAI", self.sent[0]["text"])
        self.assertEqual(self.ai_calls[0]["model"], "openai/gpt-6-luna")
        self.assertEqual(self.ai_calls[0]["reasoning_effort"], "none")

    def test_all_high_quality_items_queued_even_when_quota_full(self):
        ids = self.add(10)
        self.db.execute("INSERT INTO articles(item_id, source, added, sent, sent_at, status) VALUES('old', 'Meduza', ?, 1, ?, 'published')", (self.now, self.now))
        self.db.commit()
        with patch.object(bot, "DAILY_NEWS_LIMIT", 1):
            self.rank()
            self.publish()
        self.assertTrue(all(self.row(i)["status"] == "queued" for i in ids))
        self.assertFalse(self.sent)

    def test_stale_queued_and_unprocessed_items_expire(self):
        ids = self.add(5)
        self.rank()
        unprocessed = self.add()[0]
        self.now += 86401
        self.publish()
        self.assertTrue(all(self.row(i)["status"] == "expired" for i in ids + [unprocessed]))
        self.assertFalse(self.sent)

    def test_old_source_timestamp_expires_even_if_newly_discovered(self):
        article_id = self.add(published_at=self.now-90000)[0]
        self.rank()
        self.assertEqual(self.row(article_id)["status"], "expired")
        self.assertFalse(self.ai_calls)

    def test_score_freshness_and_arrival_priority(self):
        low, high = self.add(2)
        self.overrides[high] = dict(relevance_score=98)
        self.rank()
        self.publish()
        self.assertEqual(self.row(high)["status"], "published")
        self.assertEqual(self.row(low)["status"], "queued")

    def test_language_recheck_before_send_catches_legacy_english(self):
        article_id = self.add()[0]
        self.db.execute("UPDATE articles SET status='queued', ai_score=90, ai_summary=? WHERE id=?", ("The article is in English. " * 40, article_id))
        self.db.commit()
        self.publish()
        self.assertEqual(self.row(article_id)["status"], "pending")
        self.assertFalse(self.sent)

    def test_invalid_schema_duplicate_ids_and_nan_leave_whole_batch_pending(self):
        ids = self.add(5)
        for response in ({"items": []}, {"items": [{"id": ids[0], "send": "false", "relevance_score": 90}]},
                         {"items": [{"id": i, "send": True, "relevance_score": float("nan")} for i in ids]},
                         {"items": [{"id": i, "send": True, "relevance_score": 90} for i in ids + [ids[0]]]}):
            with patch.object(bot, "polza_json", return_value=response):
                self.rank()
            self.assertTrue(all(self.row(i)["status"] == "pending" for i in ids))
            self.now += 7200

    def test_dst_days_and_timezone_changes_recalculate_quota(self):
        with patch.object(bot, "TZ", ZoneInfo("Europe/Berlin")):
            start, end = bot.day_bounds(date(2026, 3, 29))
            self.assertEqual(end-start, 23*3600)
            start, end = bot.day_bounds(date(2026, 10, 25))
            self.assertEqual(end-start, 25*3600)
        self.now = int(datetime(2026, 10, 10, 0, 10, tzinfo=timezone.utc).timestamp())
        article_id = self.add()[0]
        self.db.execute("UPDATE articles SET sent=1, status='published', sent_at=? WHERE id=?", (self.now-1200, article_id))
        self.db.commit()
        self.assertEqual(bot.sent_today(self.db), 0)
        with patch.object(bot, "TZ", ZoneInfo("Asia/Novosibirsk")):
            self.assertEqual(bot.sent_today(self.db), 1)
        self.assertEqual(bot.last_delivery(self.db), self.now-1200)

    def test_cleanup_preserves_votes_scores_and_dedup_ids(self):
        article_id = self.add()[0]
        self.rank()
        self.publish()
        self.now += 61*86400
        bot.cleanup(self.db)
        row = self.row(article_id)
        self.assertEqual(row["ai_score"], 85)
        self.assertEqual(row["ai_summary"], RU)
        self.assertIsNone(row["content"])
        self.assertTrue(bot.is_seen(self.db, row["item_id"]))

    def test_migrate_oldest_database_preserves_ids_history_votes_and_state(self):
        legacy = str(Path(self.tmp.name) / "legacy.db")
        with sqlite3.connect(legacy) as db:
            db.executescript("""CREATE TABLE articles(id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_id TEXT UNIQUE NOT NULL, source TEXT NOT NULL, title TEXT, link TEXT,
                added INTEGER NOT NULL, sent INTEGER NOT NULL DEFAULT 0, score REAL, vote INTEGER);
                CREATE TABLE state(key TEXT PRIMARY KEY, value TEXT);
                INSERT INTO articles VALUES(7,'historic','Habr','Title','url',100,1,88,1);
                INSERT INTO articles VALUES(8,'snapshot','Habr','Title','url',101,0,12,NULL);
                INSERT INTO state VALUES('offset','123');""")
        with patch.object(bot, "DB_PATH", legacy):
            db = bot.init_db()
            row = db.execute("SELECT * FROM articles WHERE id=7").fetchone()
            self.assertEqual((row["vote"], row["score"], row["sent_at"], row["status"]), (1,88,100,"published"))
            self.assertEqual(bot.get_state(db, "offset"), "123")
            self.assertEqual(db.execute("SELECT status FROM articles WHERE id=8").fetchone()[0], "rejected")
            db.close()
            db = bot.init_db()
            self.assertEqual(db.execute("SELECT COUNT(*) FROM articles").fetchone()[0], 2)
            db.close()

    def test_migrate_current_schema_recovers_high_scores_not_first_run_snapshots(self):
        legacy = str(Path(self.tmp.name) / "current.db")
        with sqlite3.connect(legacy) as db:
            db.executescript("""CREATE TABLE articles(id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_id TEXT UNIQUE NOT NULL, source TEXT NOT NULL, title TEXT, link TEXT,
                added INTEGER NOT NULL, sent INTEGER NOT NULL DEFAULT 0, score REAL, vote INTEGER,
                evaluated INTEGER DEFAULT 0, ai_score REAL, ai_summary TEXT, sent_at INTEGER);
                CREATE TABLE state(key TEXT PRIMARY KEY, value TEXT);""")
            db.execute("INSERT INTO articles(item_id,source,added,evaluated,ai_score,ai_summary) VALUES('high','Habr',?,1,95,?)", (self.now,RU))
            db.execute("INSERT INTO articles(item_id,source,added,evaluated) VALUES('snapshot','Habr',?,1)", (self.now,))
            db.execute("INSERT INTO articles(item_id,source,added,evaluated) VALUES('pending','Habr',?,0)", (self.now,))
            db.execute("INSERT INTO articles(item_id,source,added,evaluated,ai_score) VALUES('high-without-summary','Habr',?,1,92)", (self.now,))
        with patch.object(bot, "DB_PATH", legacy):
            db = bot.init_db()
            states = dict(db.execute("SELECT item_id, status FROM articles").fetchall())
            self.assertEqual(states, {"high": "queued", "snapshot": "rejected", "pending": "pending", "high-without-summary": "pending"})
            db.close()

    def test_single_process_lock(self):
        lock = bot.acquire_process_lock()
        try:
            with self.assertRaises(SystemExit):
                bot.acquire_process_lock()
        finally:
            lock.close()

    def test_blocked_ai_does_not_block_votes_or_publisher(self):
        article_id = self.add()[0]
        self.rank()
        self.publish()
        self.add(5)
        entered, release = threading.Event(), threading.Event()
        errors = []
        def slow_ai(*args, **kwargs):
            entered.set()
            release.wait(5)
            return None
        def worker():
            db = bot.connect_db()
            try:
                bot.process_candidate_batch(db, force=True)
            except Exception as e:
                errors.append(e)
            finally:
                db.close()
        with patch.object(bot, "polza_json", side_effect=slow_ai):
            thread = threading.Thread(target=worker)
            thread.start()
            try:
                self.assertTrue(entered.wait(2))
                bot.handle_vote(self.db, dict(id="cq", data=f"v:{article_id}:1", **{"from": {"id": 1}}))
                self.publish()
                self.assertEqual(self.row(article_id)["vote"], 1)
            finally:
                release.set()
                thread.join(3)
        self.assertFalse(thread.is_alive())
        self.assertFalse(errors)


    def test_truncated_ai_response_retries_smaller_batch_without_losing_candidates(self):
        ids = self.add(10)
        truncated = self.response({"choices": [{"finish_reason": "length", "message": {"content": "{}"}}]})
        with patch.object(bot.requests, "post", return_value=truncated):
            self.rank()
        self.assertEqual(bot.get_state(self.db, "ai_batch_limit"), "5")
        self.assertTrue(all(self.row(i)["status"] == "pending" for i in ids))
        self.now += 300
        self.rank()
        self.rank()
        self.assertTrue(all(self.row(i)["status"] == "queued" for i in ids))
        self.assertEqual(len(self.ai_calls), 2)

    def test_telegram_5xx_and_malformed_response_are_ambiguous(self):
        for response in (self.response({"ok": False, "error_code": 500}, 500),
                         self.response(["malformed"], 200)):
            self.add()
            self.rank()
            self.telegram_error = response
            self.publish()
            self.now += 5400
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM articles WHERE status='delivery_unknown'").fetchone()[0], 2)
        self.assertEqual(bot.sent_today(self.db), 2)
        self.assertFalse(self.sent)

    def test_freshness_and_waiting_order_break_priority_ties(self):
        old = self.add()[0]
        self.overrides[old] = dict(relevance_score=99)
        self.rank()
        self.now += 10*3600
        fresh1, fresh2 = self.add(2)
        self.overrides[fresh1] = dict(relevance_score=90)
        self.overrides[fresh2] = dict(relevance_score=90)
        self.rank()
        self.publish()
        self.assertEqual(self.row(old)["status"], "queued")
        self.assertEqual(self.row(fresh1)["status"], "published")
        self.assertEqual(self.row(fresh2)["status"], "queued")

    def test_feed_first_run_snapshot_and_published_timestamp(self):
        import time
        entries = [{"id": "rss-1", "title": "Title", "link": "url", "summary": "Text",
                    "published_parsed": time.gmtime(self.now)}]
        with patch.object(bot, "fetch_entries", return_value=entries):
            bot.collect_source(self.db, "Habr", "feed")
        self.assertEqual(self.db.execute("SELECT status FROM articles WHERE item_id='rss-1'").fetchone()[0], "rejected")
        entries[0]["id"] = "rss-2"
        with patch.object(bot, "fetch_entries", return_value=entries), patch.object(bot, "fetch_article_text", return_value=""):
            bot.collect_source(self.db, "Habr", "feed")
        row = self.db.execute("SELECT * FROM articles WHERE item_id='rss-2'").fetchone()
        self.assertEqual((row["status"], row["published_at"]), ("pending", self.now))

    def test_profile_and_earlier_votes_are_used_without_replacing_profile(self):
        profile = dict(bot.default_profile(), liked_topics=["Python"], summary="Практические статьи")
        bot.save_profile(profile)
        self.add(5)
        self.rank()
        self.assertIn('"liked_topics": ["Python"]', self.ai_calls[0]["messages"][0]["content"])
        self.assertEqual(bot.load_profile()["summary"], "Практические статьи")

    def test_telegram_rate_limit_cooldown_applies_to_entire_queue(self):
        self.add(5)
        self.rank()
        self.telegram_error = self.response({"ok": False, "error_code": 429, "parameters": {"retry_after": 600}}, 429)
        self.publish()
        calls = self.http_mock.call_count
        self.now += 5
        self.publish()
        self.assertEqual(self.http_mock.call_count, calls)
        self.assertEqual(bot.get_state(self.db, "telegram_retry_after"), str(self.now + 595))

    def test_duplicate_rss_item_id_does_not_create_another_article(self):
        article_id = self.add()[0]
        duplicate = bot.add_article(self.db, 'article-0', 'Meduza', 'Title', 'url')
        self.assertIsNone(duplicate)
        self.rank()
        self.publish()
        self.now += 5400
        self.publish()
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(self.row(article_id)["status"], "published")

    def test_ai_daily_budget_survives_restart_and_resets_on_local_day(self):
        ids = self.add(20)
        with patch.object(bot, "AI_DAILY_REQUEST_LIMIT", 1):
            self.rank()
            self.db.close()
            self.db = bot.init_db()
            self.rank()
            self.assertEqual(len(self.ai_calls), 1)
            self.assertTrue(all(self.row(i)["status"] == "pending" for i in ids[10:]))
            self.now += 13*3600
            self.rank()
            self.assertEqual(len(self.ai_calls), 2)
            self.assertTrue(all(self.row(i)["status"] == "queued" for i in ids))

if __name__ == "__main__":
    unittest.main()
