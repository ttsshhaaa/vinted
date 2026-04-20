import os
import sqlite3
import threading
import time
import logging
from pathlib import Path
from urllib.parse import quote

import requests
from flask import Flask, redirect, render_template, request, send_file, url_for

from vinted_parser import GEO_DOMAINS, expand_geos, get_item_age_minutes, parse_extra_params, run_search


BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
DB_PATH = BASE_DIR / "app.db"
WATCHER_POLL_SECONDS = 60

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)


def get_db_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_PATH, timeout=30)
    connection.row_factory = sqlite3.Row
    return connection


def safe_list_watchers() -> list[sqlite3.Row]:
    try:
        return list_watchers()
    except Exception:
        logger.exception("Failed to load watchers")
        return []


def init_db() -> None:
    with get_db_connection() as connection:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS watchers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                query TEXT NOT NULL,
                geos TEXT NOT NULL,
                pages INTEGER NOT NULL DEFAULT 1,
                price_from INTEGER,
                price_to INTEGER,
                order_name TEXT NOT NULL DEFAULT 'newest_first',
                extra_params TEXT DEFAULT '',
                discord_webhook_url TEXT NOT NULL DEFAULT '',
                interval_minutes INTEGER NOT NULL DEFAULT 5,
                enabled INTEGER NOT NULL DEFAULT 1,
                last_run_at TEXT,
                last_error TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        watcher_columns = {
            row["name"] for row in connection.execute("PRAGMA table_info(watchers)").fetchall()
        }
        if "telegram_bot_token" in watcher_columns or "telegram_chat_id" in watcher_columns:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS watchers_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    query TEXT NOT NULL,
                    geos TEXT NOT NULL,
                    pages INTEGER NOT NULL DEFAULT 1,
                    price_from INTEGER,
                    price_to INTEGER,
                    order_name TEXT NOT NULL DEFAULT 'newest_first',
                    extra_params TEXT DEFAULT '',
                    discord_webhook_url TEXT NOT NULL DEFAULT '',
                    interval_minutes INTEGER NOT NULL DEFAULT 5,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    last_run_at TEXT,
                    last_error TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            connection.execute(
                """
                INSERT INTO watchers_new (
                    id, name, query, geos, pages, price_from, price_to, order_name,
                    extra_params, discord_webhook_url, interval_minutes, enabled,
                    last_run_at, last_error, created_at
                )
                SELECT
                    id, name, query, geos, pages, price_from, price_to, order_name,
                    extra_params,
                    CASE
                        WHEN 'discord_webhook_url' IN (
                            SELECT name FROM pragma_table_info('watchers')
                        ) THEN COALESCE(discord_webhook_url, '')
                        ELSE ''
                    END,
                    interval_minutes, enabled, last_run_at, last_error, created_at
                FROM watchers
                """
            )
            connection.execute("DROP TABLE watchers")
            connection.execute("ALTER TABLE watchers_new RENAME TO watchers")
        else:
            watcher_columns = {
                row["name"] for row in connection.execute("PRAGMA table_info(watchers)").fetchall()
            }
            if "discord_webhook_url" not in watcher_columns:
                connection.execute(
                    "ALTER TABLE watchers ADD COLUMN discord_webhook_url TEXT NOT NULL DEFAULT ''"
                )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS watcher_seen_items (
                watcher_id INTEGER NOT NULL,
                item_url TEXT NOT NULL,
                first_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (watcher_id, item_url),
                FOREIGN KEY (watcher_id) REFERENCES watchers(id) ON DELETE CASCADE
            )
            """
        )
        connection.commit()


def safe_int(raw_value: str | None) -> int | None:
    if raw_value is None or not str(raw_value).strip():
        return None
    return int(raw_value)


def list_watchers() -> list[sqlite3.Row]:
    with get_db_connection() as connection:
        return connection.execute(
            """
            SELECT id, name, query, geos, pages, price_from, price_to, order_name,
                   extra_params, discord_webhook_url, interval_minutes, enabled,
                   last_run_at, last_error, created_at
            FROM watchers
            ORDER BY enabled DESC, id DESC
            """
        ).fetchall()


def create_watcher(form: dict[str, str], selected_geos: list[str]) -> None:
    with get_db_connection() as connection:
        cursor = connection.execute(
            """
            INSERT INTO watchers (
                name, query, geos, pages, price_from, price_to, order_name,
                extra_params, discord_webhook_url, interval_minutes, enabled
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """,
            (
                form["watcher_name"],
                form["query"],
                ",".join(selected_geos),
                int(form["pages"]),
                safe_int(form["price_from"]),
                safe_int(form["price_to"]),
                form["order"],
                form["extra_params"],
                form["discord_webhook_url"],
                int(form["interval_minutes"]),
            ),
        )
        watcher_id = cursor.lastrowid
        connection.commit()
    if watcher_id:
        prime_watcher_seen_items(watcher_id)


def prime_watcher_seen_items(watcher_id: int) -> int:
    with get_db_connection() as connection:
        watcher = connection.execute("SELECT * FROM watchers WHERE id = ?", (watcher_id,)).fetchone()

    if not watcher:
        return 0

    result = run_search(
        query=watcher["query"],
        geos=expand_geos(watcher["geos"]),
        pages=watcher["pages"],
        delay=0,
        order=watcher["order_name"],
        price_from=watcher["price_from"],
        price_to=watcher["price_to"],
        extra_params=parse_extra_params(
            [line.strip() for line in watcher["extra_params"].splitlines() if line.strip()]
        ),
        output_dir=OUTPUT_DIR,
    )

    inserted = 0
    with get_db_connection() as connection:
        for item in result["items"]:
            exists = connection.execute(
                "SELECT 1 FROM watcher_seen_items WHERE watcher_id = ? AND item_url = ?",
                (watcher_id, item.item_url),
            ).fetchone()
            if exists:
                continue
            connection.execute(
                "INSERT INTO watcher_seen_items (watcher_id, item_url) VALUES (?, ?)",
                (watcher_id, item.item_url),
            )
            inserted += 1
        connection.execute(
            "UPDATE watchers SET last_run_at = CURRENT_TIMESTAMP, last_error = NULL WHERE id = ?",
            (watcher_id,),
        )
        connection.commit()
    return inserted


def set_watcher_enabled(watcher_id: int, enabled: bool) -> None:
    with get_db_connection() as connection:
        connection.execute(
            "UPDATE watchers SET enabled = ?, last_error = CASE WHEN ? = 1 THEN last_error ELSE NULL END WHERE id = ?",
            (1 if enabled else 0, 1 if enabled else 0, watcher_id),
        )
        connection.commit()


def delete_watcher(watcher_id: int) -> None:
    with get_db_connection() as connection:
        connection.execute("DELETE FROM watcher_seen_items WHERE watcher_id = ?", (watcher_id,))
        connection.execute("DELETE FROM watchers WHERE id = ?", (watcher_id,))
        connection.commit()


def is_watcher_due(watcher: sqlite3.Row) -> bool:
    if not watcher["last_run_at"]:
        return True
    with get_db_connection() as connection:
        row = connection.execute(
            """
            SELECT CASE
                WHEN datetime(last_run_at, '+' || interval_minutes || ' minutes') <= datetime('now')
                THEN 1 ELSE 0 END AS due
            FROM watchers
            WHERE id = ?
            """,
            (watcher["id"],),
        ).fetchone()
        return bool(row["due"]) if row else False


def send_discord_message(webhook_url: str, text: str) -> None:
    response = requests.post(
        webhook_url,
        json={"content": text},
        timeout=30,
    )
    response.raise_for_status()


def record_watcher_run(watcher_id: int, error: str | None = None) -> None:
    with get_db_connection() as connection:
        connection.execute(
            "UPDATE watchers SET last_run_at = CURRENT_TIMESTAMP, last_error = ? WHERE id = ?",
            (error, watcher_id),
        )
        connection.commit()


def run_single_watcher(watcher_id: int) -> tuple[int, int]:
    with get_db_connection() as connection:
        watcher = connection.execute("SELECT * FROM watchers WHERE id = ?", (watcher_id,)).fetchone()

    if not watcher or not watcher["enabled"]:
        return 0, 0

    try:
        result = run_search(
            query=watcher["query"],
            geos=expand_geos(watcher["geos"]),
            pages=watcher["pages"],
            delay=0,
            order=watcher["order_name"],
            price_from=watcher["price_from"],
            price_to=watcher["price_to"],
            extra_params=parse_extra_params(
                [line.strip() for line in watcher["extra_params"].splitlines() if line.strip()]
            ),
            output_dir=OUTPUT_DIR,
        )

        new_items: list[dict] = []
        with get_db_connection() as connection:
            for item in result["items"]:
                exists = connection.execute(
                    "SELECT 1 FROM watcher_seen_items WHERE watcher_id = ? AND item_url = ?",
                    (watcher["id"], item.item_url),
                ).fetchone()
                if exists:
                    continue
                connection.execute(
                    "INSERT INTO watcher_seen_items (watcher_id, item_url) VALUES (?, ?)",
                    (watcher["id"], item.item_url),
                )
                new_items.append(
                    {
                        "title": item.title,
                        "price": item.price,
                        "geo": item.geo.upper(),
                        "item_url": item.item_url,
                    }
                )
            connection.commit()

        if new_items:
            session = requests.Session()
            max_age_minutes = max(int(watcher["interval_minutes"]) + 2, 10)
            fresh_items: list[dict] = []
            stale_items = 0
            unknown_age_items = 0
            for item in new_items:
                age_minutes = get_item_age_minutes(session, item["item_url"], timeout=30)
                if age_minutes is None:
                    unknown_age_items += 1
                    continue
                if age_minutes <= max_age_minutes:
                    item["age_minutes"] = age_minutes
                    fresh_items.append(item)
                else:
                    stale_items += 1

            if fresh_items:
                lines = [f"{watcher['name']}: {len(fresh_items)} new item(s)"]
                for item in fresh_items[:10]:
                    lines.append(
                        f"{item['geo']} | {item['price']} | {item['title']} | {item['age_minutes']} min ago"
                    )
                    lines.append(item["item_url"])
                if len(fresh_items) > 10:
                    lines.append(f"...and {len(fresh_items) - 10} more")
                send_discord_message(watcher["discord_webhook_url"], "\n".join(lines))

            if stale_items or unknown_age_items:
                extra_notes: list[str] = []
                if stale_items:
                    extra_notes.append(f"stale skipped: {stale_items}")
                if unknown_age_items:
                    extra_notes.append(f"unknown age skipped: {unknown_age_items}")
                record_watcher_run(watcher["id"], ", ".join(extra_notes))
            else:
                record_watcher_run(watcher["id"], None)
            return len(fresh_items), result["unique_count"]
        record_watcher_run(watcher["id"], None)
        return 0, result["unique_count"]
    except Exception as exc:
        record_watcher_run(watcher["id"], str(exc))
        raise


def watcher_worker() -> None:
    while True:
        try:
            watchers = [watcher for watcher in list_watchers() if watcher["enabled"]]
            for watcher in watchers:
                if is_watcher_due(watcher):
                    try:
                        run_single_watcher(watcher["id"])
                    except Exception:
                        pass
        except Exception:
            pass
        time.sleep(WATCHER_POLL_SECONDS)


@app.route("/", methods=["GET", "POST"])
def index():
    result = None
    error = None
    info = None
    watchers = safe_list_watchers()

    defaults = {
        "query": "",
        "pages": 1,
        "price_from": "",
        "price_to": "",
        "order": "newest_first",
        "delay": 0.5,
        "selected_geos": ["fr", "de", "it"],
        "extra_params": "",
        "watcher_name": "",
        "discord_webhook_url": "",
        "interval_minutes": 5,
    }

    if request.method == "POST":
        form = request.form
        action = form.get("action", "search").strip()
        defaults = {
            "query": form.get("query", "").strip(),
            "pages": form.get("pages", "1"),
            "price_from": form.get("price_from", "").strip(),
            "price_to": form.get("price_to", "").strip(),
            "order": form.get("order", "newest_first").strip() or "newest_first",
            "delay": form.get("delay", "0.5").strip() or "0.5",
            "selected_geos": form.getlist("geo") or ["fr", "de", "it"],
            "extra_params": form.get("extra_params", "").strip(),
            "watcher_name": form.get("watcher_name", "").strip(),
            "discord_webhook_url": form.get("discord_webhook_url", "").strip(),
            "interval_minutes": form.get("interval_minutes", "5").strip() or "5",
        }

        try:
            if action == "create_watcher":
                if not defaults["watcher_name"]:
                    raise ValueError("Watcher name is required.")
                if not defaults["query"]:
                    raise ValueError("Query is required for a watcher.")
                if not defaults["discord_webhook_url"]:
                    raise ValueError("Discord webhook URL is required.")
                selected_geos = expand_geos(defaults["selected_geos"])
                create_watcher(defaults, selected_geos)
                info = (
                    "Watcher created. Current listings were saved as baseline, "
                    "so alerts will now fire only for brand-new listings."
                )
            elif action == "toggle_watcher":
                watcher_id = int(form.get("watcher_id", "0"))
                enabled = form.get("enabled", "0") == "1"
                set_watcher_enabled(watcher_id, enabled)
                return redirect(url_for("index"))
            elif action == "delete_watcher":
                watcher_id = int(form.get("watcher_id", "0"))
                delete_watcher(watcher_id)
                return redirect(url_for("index"))
            elif action == "run_watcher":
                watcher_id = int(form.get("watcher_id", "0"))
                new_count, total_count = run_single_watcher(watcher_id)
                info = f"Watcher checked successfully. New items: {new_count}. Total unique in current scan: {total_count}."
            else:
                geos = expand_geos(defaults["selected_geos"] or ["fr"])
                raw_extra = [line.strip() for line in defaults["extra_params"].splitlines() if line.strip()]
                result = run_search(
                    query=defaults["query"],
                    geos=geos,
                    pages=int(defaults["pages"]),
                    delay=float(defaults["delay"]),
                    order=defaults["order"],
                    price_from=safe_int(defaults["price_from"]),
                    price_to=safe_int(defaults["price_to"]),
                    extra_params=parse_extra_params(raw_extra),
                    output_dir=OUTPUT_DIR,
                )
        except Exception as exc:
            logger.exception("Request handling failed")
            error = str(exc)

    try:
        return render_template(
            "index.html",
            geo_options=GEO_DOMAINS,
            defaults=defaults,
            watchers=watchers,
            result=result,
            error=error,
            info=info,
        )
    except Exception as exc:
        logger.exception("Template render failed")
        return f"Internal server error: {exc}", 500


@app.get("/download/<fmt>/<path:filename>")
def download(fmt: str, filename: str):
    if fmt not in {"json", "csv"}:
        return "Unsupported format", 400

    file_path = (OUTPUT_DIR / filename).resolve()
    if OUTPUT_DIR.resolve() not in file_path.parents:
        return "Invalid file path", 400
    if not file_path.exists():
        return "File not found", 404

    mimetype = "application/json" if fmt == "json" else "text/csv"
    return send_file(file_path, as_attachment=True, mimetype=mimetype)


@app.template_filter("basename")
def basename_filter(path: Path) -> str:
    return Path(path).name


@app.template_filter("urlquote")
def urlquote_filter(value: str) -> str:
    return quote(value)


init_db()
threading.Thread(target=watcher_worker, daemon=True).start()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)
