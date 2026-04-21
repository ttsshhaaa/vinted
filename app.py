
import logging
import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from functools import wraps
from pathlib import Path
from urllib.parse import quote

import requests
from flask import (
    Flask,
    abort,
    flash,
    g,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)
from werkzeug.security import check_password_hash, generate_password_hash

from vinted_parser import GEO_DOMAINS, expand_geos, get_item_age_minutes, parse_extra_params, run_search


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = (
    Path(os.environ["DATA_DIR"]).resolve()
    if os.environ.get("DATA_DIR")
    else (BASE_DIR / "data")
)
OUTPUT_DIR = DATA_DIR / "output"
DB_PATH = DATA_DIR / "app.db"
WATCHER_POLL_SECONDS = 60
DEFAULT_ADMIN_USERNAME = "kon1337"
DEFAULT_ADMIN_PASSWORD = "thklty13"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("FLASK_SECRET_KEY", "change-me-on-railway")

_watcher_thread_started = False


def ensure_storage() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def get_db_connection() -> sqlite3.Connection:
    ensure_storage()
    connection = sqlite3.connect(DB_PATH, timeout=30)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    row = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def column_names(connection: sqlite3.Connection, table_name: str) -> set[str]:
    if not table_exists(connection, table_name):
        return set()
    return {row["name"] for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()}


def safe_int(raw_value: str | None) -> int | None:
    if raw_value is None or not str(raw_value).strip():
        return None
    return int(str(raw_value).strip())


def safe_float(raw_value: str | None, default: float | None = None) -> float | None:
    if raw_value is None:
        return default
    normalized = str(raw_value).strip().replace(",", ".")
    if not normalized:
        return default
    return float(normalized)


def create_base_tables(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL DEFAULT 'user',
            active INTEGER NOT NULL DEFAULT 1,
            access_expires_at TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS watchers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
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
            fresh_minutes INTEGER NOT NULL DEFAULT 10,
            enabled INTEGER NOT NULL DEFAULT 1,
            last_started_at TEXT,
            last_run_at TEXT,
            last_success_at TEXT,
            last_notification_at TEXT,
            last_notification_count INTEGER NOT NULL DEFAULT 0,
            last_scan_count INTEGER NOT NULL DEFAULT 0,
            status_message TEXT DEFAULT '',
            last_error TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )
        """
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
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS favorites (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            item_url TEXT NOT NULL,
            item_id TEXT,
            title TEXT NOT NULL,
            subtitle TEXT,
            brand TEXT,
            size TEXT,
            condition TEXT,
            price TEXT,
            total_price TEXT,
            currency TEXT,
            image_url TEXT,
            search_url TEXT,
            geo TEXT,
            seller_country TEXT,
            seller_city TEXT,
            seller_last_online TEXT,
            listing_age_minutes INTEGER,
            listing_age_label TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (user_id, item_url),
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        )
        """
    )


def ensure_admin_user(connection: sqlite3.Connection) -> int:
    row = connection.execute(
        "SELECT id FROM users WHERE username = ?",
        (DEFAULT_ADMIN_USERNAME,),
    ).fetchone()
    if row:
        return int(row["id"])

    cursor = connection.execute(
        """
        INSERT INTO users (username, password_hash, role, active)
        VALUES (?, ?, 'admin', 1)
        """,
        (DEFAULT_ADMIN_USERNAME, generate_password_hash(DEFAULT_ADMIN_PASSWORD)),
    )
    logger.info("Seeded default admin account '%s'", DEFAULT_ADMIN_USERNAME)
    return int(cursor.lastrowid)


def migrate_legacy_users(connection: sqlite3.Connection) -> None:
    existing_columns = column_names(connection, "users")
    if "access_expires_at" not in existing_columns:
        connection.execute("ALTER TABLE users ADD COLUMN access_expires_at TEXT")


def migrate_legacy_watchers(connection: sqlite3.Connection, admin_user_id: int) -> None:
    existing_columns = column_names(connection, "watchers")
    if not existing_columns:
        return

    if "user_id" not in existing_columns:
        connection.execute("ALTER TABLE watchers ADD COLUMN user_id INTEGER")
        connection.execute("UPDATE watchers SET user_id = ? WHERE user_id IS NULL", (admin_user_id,))

    if "discord_webhook_url" not in existing_columns:
        connection.execute(
            "ALTER TABLE watchers ADD COLUMN discord_webhook_url TEXT NOT NULL DEFAULT ''"
        )

    if "enabled" not in existing_columns:
        connection.execute("ALTER TABLE watchers ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1")
    if "fresh_minutes" not in existing_columns:
        connection.execute("ALTER TABLE watchers ADD COLUMN fresh_minutes INTEGER NOT NULL DEFAULT 10")
    if "last_started_at" not in existing_columns:
        connection.execute("ALTER TABLE watchers ADD COLUMN last_started_at TEXT")
    if "last_success_at" not in existing_columns:
        connection.execute("ALTER TABLE watchers ADD COLUMN last_success_at TEXT")
    if "last_notification_at" not in existing_columns:
        connection.execute("ALTER TABLE watchers ADD COLUMN last_notification_at TEXT")
    if "last_notification_count" not in existing_columns:
        connection.execute(
            "ALTER TABLE watchers ADD COLUMN last_notification_count INTEGER NOT NULL DEFAULT 0"
        )
    if "last_scan_count" not in existing_columns:
        connection.execute("ALTER TABLE watchers ADD COLUMN last_scan_count INTEGER NOT NULL DEFAULT 0")
    if "status_message" not in existing_columns:
        connection.execute("ALTER TABLE watchers ADD COLUMN status_message TEXT DEFAULT ''")


def migrate_legacy_favorites(connection: sqlite3.Connection, admin_user_id: int) -> None:
    if not table_exists(connection, "favorites"):
        return

    existing_columns = column_names(connection, "favorites")
    if "user_id" in existing_columns and "id" in existing_columns:
        connection.execute(
            "UPDATE favorites SET user_id = ? WHERE user_id IS NULL",
            (admin_user_id,),
        )
        return

    connection.execute("ALTER TABLE favorites RENAME TO favorites_legacy")
    create_base_tables(connection)
    connection.execute(
        """
        INSERT OR IGNORE INTO favorites (
            user_id, item_url, item_id, title, subtitle, brand, size, condition,
            price, total_price, currency, image_url, search_url, geo,
            seller_country, seller_city, seller_last_online,
            listing_age_minutes, listing_age_label, created_at
        )
        SELECT
            ?, item_url, item_id, title, subtitle, brand, size, condition,
            price, total_price, currency, image_url, search_url, geo,
            seller_country, seller_city, seller_last_online,
            listing_age_minutes, listing_age_label,
            COALESCE(created_at, CURRENT_TIMESTAMP)
        FROM favorites_legacy
        """,
        (admin_user_id,),
    )
    connection.execute("DROP TABLE favorites_legacy")


def init_db() -> None:
    ensure_storage()
    with get_db_connection() as connection:
        create_base_tables(connection)
        migrate_legacy_users(connection)
        admin_user_id = ensure_admin_user(connection)
        migrate_legacy_watchers(connection, admin_user_id)
        migrate_legacy_favorites(connection, admin_user_id)
        create_base_tables(connection)
        connection.commit()


def get_user_by_id(user_id: int | None) -> sqlite3.Row | None:
    if not user_id:
        return None
    with get_db_connection() as connection:
        return connection.execute(
            "SELECT id, username, role, active, access_expires_at, created_at FROM users WHERE id = ?",
            (user_id,),
        ).fetchone()


def get_user_by_username(username: str) -> sqlite3.Row | None:
    with get_db_connection() as connection:
        return connection.execute(
            "SELECT * FROM users WHERE username = ?",
            (username,),
        ).fetchone()


def create_user(username: str, password: str, role: str = "user") -> int:
    return create_user_with_access(username=username, password=password, role=role, access_expires_at=None)


def create_user_with_access(
    username: str,
    password: str,
    role: str = "user",
    access_expires_at: str | None = None,
) -> int:
    with get_db_connection() as connection:
        cursor = connection.execute(
            """
            INSERT INTO users (username, password_hash, role, active, access_expires_at)
            VALUES (?, ?, ?, 1, ?)
            """,
            (username, generate_password_hash(password), role, access_expires_at),
        )
        connection.commit()
        return int(cursor.lastrowid)


def parse_db_datetime(raw_value: str | None) -> datetime | None:
    if not raw_value:
        return None
    for pattern in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S"):
        try:
            parsed = datetime.strptime(raw_value, pattern)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def is_user_access_valid(user: sqlite3.Row | None) -> bool:
    if not user or not user["active"]:
        return False
    expires_at = parse_db_datetime(user["access_expires_at"])
    return not expires_at or expires_at > datetime.now(timezone.utc)


def is_access_expiry_valid(expires_at: str | None) -> bool:
    parsed = parse_db_datetime(expires_at)
    return not parsed or parsed > datetime.now(timezone.utc)


def describe_access_window(expires_at: str | None) -> str:
    parsed = parse_db_datetime(expires_at)
    if not parsed:
        return "permanent"
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def compute_access_expiry(duration: str) -> str | None:
    now = datetime.now(timezone.utc)
    if duration == "1w":
        return (now + timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    if duration == "1m":
        return (now + timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
    if duration == "forever":
        return None
    raise ValueError("Unsupported access duration.")


def authenticate_user(username: str, password: str) -> sqlite3.Row | None:
    user = get_user_by_username(username)
    if not is_user_access_valid(user):
        return None
    if not check_password_hash(user["password_hash"], password):
        return None
    return user


@app.before_request
def load_current_user() -> None:
    g.current_user = get_user_by_id(session.get("user_id"))
    if g.current_user and not is_user_access_valid(g.current_user):
        session.clear()
        g.current_user = None


@app.context_processor
def inject_current_user() -> dict:
    return {"current_user": getattr(g, "current_user", None)}


def login_required(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if g.current_user is None:
            return redirect(url_for("login"))
        return view(*args, **kwargs)

    return wrapped


def admin_required(view):
    @wraps(view)
    @login_required
    def wrapped(*args, **kwargs):
        if g.current_user["role"] != "admin":
            abort(403)
        return view(*args, **kwargs)

    return wrapped

def list_favorites(user_id: int) -> list[sqlite3.Row]:
    with get_db_connection() as connection:
        return connection.execute(
            """
            SELECT
                id, item_id, title, subtitle, brand, size, condition, price, total_price, currency,
                image_url, item_url, search_url, geo, seller_country, seller_city,
                seller_last_online, listing_age_minutes, listing_age_label, created_at
            FROM favorites
            WHERE user_id = ?
            ORDER BY created_at DESC, item_url DESC
            """,
            (user_id,),
        ).fetchall()


def toggle_favorite(user_id: int, form: dict[str, str]) -> bool:
    item_url = form.get("item_url", "").strip()
    if not item_url:
        raise ValueError("Favorite item URL is required.")

    with get_db_connection() as connection:
        exists = connection.execute(
            "SELECT 1 FROM favorites WHERE user_id = ? AND item_url = ?",
            (user_id, item_url),
        ).fetchone()
        if exists:
            connection.execute(
                "DELETE FROM favorites WHERE user_id = ? AND item_url = ?",
                (user_id, item_url),
            )
            connection.commit()
            return False

        connection.execute(
            """
            INSERT INTO favorites (
                user_id, item_id, title, subtitle, brand, size, condition, price, total_price,
                currency, image_url, item_url, search_url, geo, seller_country, seller_city,
                seller_last_online, listing_age_minutes, listing_age_label
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                form.get("item_id", "").strip(),
                form.get("title", "").strip(),
                form.get("subtitle", "").strip(),
                form.get("brand", "").strip(),
                form.get("size", "").strip(),
                form.get("condition", "").strip(),
                form.get("price", "").strip(),
                form.get("total_price", "").strip(),
                form.get("currency", "").strip(),
                form.get("image_url", "").strip(),
                item_url,
                form.get("search_url", "").strip(),
                form.get("item_geo", form.get("geo", "")).strip(),
                form.get("seller_country", "").strip(),
                form.get("seller_city", "").strip(),
                form.get("seller_last_online", "").strip(),
                safe_int(form.get("listing_age_minutes", "").strip()),
                form.get("listing_age_label", "").strip(),
            ),
        )
        connection.commit()
    return True


def list_watchers(user_id: int | None = None) -> list[sqlite3.Row]:
    query = """
        SELECT
            watchers.id, watchers.user_id, users.username, watchers.name, watchers.query,
            watchers.geos, watchers.pages, watchers.price_from, watchers.price_to,
            watchers.order_name, watchers.extra_params, watchers.discord_webhook_url,
            watchers.interval_minutes, watchers.fresh_minutes, watchers.enabled,
            watchers.last_started_at, watchers.last_run_at, watchers.last_success_at,
            watchers.last_notification_at, watchers.last_notification_count,
            watchers.last_scan_count, watchers.status_message, watchers.last_error,
            watchers.created_at
        FROM watchers
        JOIN users ON users.id = watchers.user_id
    """
    params: tuple = ()
    if user_id is not None:
        query += " WHERE watchers.user_id = ?"
        params = (user_id,)
    query += " ORDER BY watchers.enabled DESC, watchers.id DESC"

    with get_db_connection() as connection:
        return connection.execute(query, params).fetchall()


def get_watcher_for_user(watcher_id: int, user_id: int, is_admin: bool) -> sqlite3.Row | None:
    sql = """
        SELECT watchers.*, users.active AS user_active
        FROM watchers
        JOIN users ON users.id = watchers.user_id
        WHERE watchers.id = ?
    """
    params: tuple = (watcher_id,)
    if not is_admin:
        sql += " AND watchers.user_id = ?"
        params = (watcher_id, user_id)
    with get_db_connection() as connection:
        return connection.execute(sql, params).fetchone()


def create_watcher(user_id: int, form: dict[str, str], selected_geos: list[str]) -> int:
    with get_db_connection() as connection:
        cursor = connection.execute(
            """
            INSERT INTO watchers (
                user_id, name, query, geos, pages, price_from, price_to,
                order_name, extra_params, discord_webhook_url, interval_minutes, fresh_minutes, enabled
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """,
            (
                user_id,
                form["watcher_name"],
                form["watcher_query"],
                ",".join(selected_geos),
                safe_int(form["watcher_pages"]) or 1,
                safe_int(form["watcher_price_from"]),
                safe_int(form["watcher_price_to"]),
                form["watcher_order"],
                form["watcher_extra_params"],
                form["discord_webhook_url"],
                int(form["interval_minutes"]),
                max(safe_int(form["watcher_fresh_minutes"]) or 10, 1),
            ),
        )
        watcher_id = int(cursor.lastrowid)
        connection.commit()
    prime_watcher_seen_items(watcher_id)
    try:
        send_discord_message(
            form["discord_webhook_url"],
            "\n".join(
                [
                    f"Watcher started: {form['watcher_name']}",
                    f"Query: {form['watcher_query']}",
                    f"Geos: {', '.join(selected_geos).upper()}",
                    f"Fresh window: {max(safe_int(form['watcher_fresh_minutes']) or 10, 1)} min",
                    "Baseline saved. Next Discord alerts will include only newly found fresh listings.",
                ]
            ),
        )
    except Exception:
        logger.exception("Failed to send watcher startup ping for watcher %s", watcher_id)
    return watcher_id


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
            """
            UPDATE watchers
            SET last_run_at = CURRENT_TIMESTAMP,
                last_success_at = CURRENT_TIMESTAMP,
                last_error = NULL,
                last_scan_count = ?,
                status_message = ?
            WHERE id = ?
            """,
            (inserted, f"Baseline saved with {inserted} listing(s).", watcher_id),
        )
        connection.commit()
    return inserted


def set_watcher_enabled(watcher_id: int, enabled: bool) -> None:
    with get_db_connection() as connection:
        connection.execute(
            """
            UPDATE watchers
            SET enabled = ?, last_error = CASE WHEN ? = 1 THEN last_error ELSE NULL END
            WHERE id = ?
            """,
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


def mark_watcher_started(watcher_id: int) -> None:
    with get_db_connection() as connection:
        connection.execute(
            """
            UPDATE watchers
            SET last_started_at = CURRENT_TIMESTAMP,
                status_message = 'Checking for fresh listings...'
            WHERE id = ?
            """,
            (watcher_id,),
        )
        connection.commit()


def record_watcher_run(
    watcher_id: int,
    *,
    error: str | None = None,
    scan_count: int = 0,
    notified_count: int = 0,
    status_message: str = "",
) -> None:
    with get_db_connection() as connection:
        if error:
            connection.execute(
                """
                UPDATE watchers
                SET last_run_at = CURRENT_TIMESTAMP,
                    last_error = ?,
                    last_scan_count = ?,
                    status_message = ?
                WHERE id = ?
                """,
                (error, scan_count, status_message or error, watcher_id),
            )
        else:
            connection.execute(
                """
                UPDATE watchers
                SET last_run_at = CURRENT_TIMESTAMP,
                    last_success_at = CURRENT_TIMESTAMP,
                    last_error = NULL,
                    last_scan_count = ?,
                    last_notification_count = ?,
                    last_notification_at = CASE WHEN ? > 0 THEN CURRENT_TIMESTAMP ELSE last_notification_at END,
                    status_message = ?
                WHERE id = ?
                """,
                (scan_count, notified_count, notified_count, status_message, watcher_id),
            )
        connection.commit()


def parse_sqlite_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def decorate_watcher_statuses(watchers: list[sqlite3.Row]) -> list[dict]:
    now = datetime.utcnow()
    decorated: list[dict] = []
    for watcher in watchers:
        item = dict(watcher)
        state = "paused"
        if watcher["enabled"]:
            state = "working"
            if watcher["last_error"]:
                state = "warning"
            last_run = parse_sqlite_timestamp(watcher["last_run_at"])
            grace_minutes = max(int(watcher["interval_minutes"] or 5) * 2, 5)
            if last_run and now - last_run > timedelta(minutes=grace_minutes):
                state = "idle"
        item["status_state"] = state
        decorated.append(item)
    return decorated


def run_single_watcher(watcher_id: int) -> tuple[int, int]:
    with get_db_connection() as connection:
        watcher = connection.execute(
            """
            SELECT watchers.*, users.active AS user_active, users.access_expires_at AS user_access_expires_at
            FROM watchers
            JOIN users ON users.id = watchers.user_id
            WHERE watchers.id = ?
            """,
            (watcher_id,),
        ).fetchone()

    if (
        not watcher
        or not watcher["enabled"]
        or not watcher["user_active"]
        or not is_access_expiry_valid(watcher["user_access_expires_at"])
    ):
        return 0, 0

    try:
        mark_watcher_started(watcher["id"])
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
        fresh_window_minutes = max(int(watcher["fresh_minutes"] or 10), 1)
        with get_db_connection() as connection:
            for item in result["items"]:
                exists = connection.execute(
                    "SELECT 1 FROM watcher_seen_items WHERE watcher_id = ? AND item_url = ?",
                    (watcher["id"], item.item_url),
                ).fetchone()
                if exists:
                    continue
                new_items.append(
                    {
                        "title": item.title,
                        "price": item.price,
                        "geo": item.geo.upper(),
                        "item_url": item.item_url,
                        "age_minutes": item.listing_age_minutes,
                    }
                )

        if new_items and watcher["discord_webhook_url"]:
            session_http = requests.Session()
            fresh_items: list[dict] = []
            stale_items = 0
            unknown_age_items = 0
            seen_urls: list[str] = []
            for item in new_items:
                age_minutes = item.get("age_minutes")
                if age_minutes is None:
                    age_minutes = get_item_age_minutes(session_http, item["item_url"], timeout=30)
                if age_minutes is None:
                    unknown_age_items += 1
                    continue
                if age_minutes <= fresh_window_minutes:
                    item["age_minutes"] = age_minutes
                    fresh_items.append(item)
                else:
                    stale_items += 1
                    seen_urls.append(item["item_url"])

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
                seen_urls.extend(item["item_url"] for item in fresh_items)

            if seen_urls:
                with get_db_connection() as connection:
                    for item_url in seen_urls:
                        connection.execute(
                            "INSERT OR IGNORE INTO watcher_seen_items (watcher_id, item_url) VALUES (?, ?)",
                            (watcher["id"], item_url),
                        )
                    connection.commit()

            notes: list[str] = []
            if fresh_items:
                notes.append(f"Sent {len(fresh_items)} fresh item(s).")
            else:
                notes.append("Checked successfully, no fresh items found.")
            if stale_items:
                notes.append(f"old skipped: {stale_items}")
            if unknown_age_items:
                notes.append(f"age unknown will retry: {unknown_age_items}")
            record_watcher_run(
                watcher["id"],
                scan_count=result["unique_count"],
                notified_count=len(fresh_items),
                status_message=" ".join(notes),
            )
            return len(fresh_items), result["unique_count"]

        if new_items:
            with get_db_connection() as connection:
                for item in new_items:
                    connection.execute(
                        "INSERT OR IGNORE INTO watcher_seen_items (watcher_id, item_url) VALUES (?, ?)",
                        (watcher["id"], item["item_url"]),
                    )
                connection.commit()

        record_watcher_run(
            watcher["id"],
            scan_count=result["unique_count"],
            notified_count=0,
            status_message="Checked successfully, but no Discord webhook is configured." if not watcher["discord_webhook_url"] else "Checked successfully, no fresh items found.",
        )
        return 0, result["unique_count"]
    except Exception as exc:
        record_watcher_run(watcher["id"], error=str(exc), status_message=f"Watcher failed: {exc}")
        raise


def watcher_worker() -> None:
    while True:
        try:
            watchers = [watcher for watcher in list_watchers() if watcher["enabled"]]
            for watcher in watchers:
                if is_watcher_due(watcher):
                    try:
                        run_single_watcher(int(watcher["id"]))
                    except Exception:
                        logger.exception("Watcher %s failed", watcher["id"])
        except Exception:
            logger.exception("Watcher worker loop failed")
        time.sleep(WATCHER_POLL_SECONDS)


def start_watcher_thread() -> None:
    global _watcher_thread_started
    if _watcher_thread_started:
        return
    _watcher_thread_started = True
    threading.Thread(target=watcher_worker, daemon=True, name="watcher-worker").start()


def normalize_dashboard_defaults(form_data: dict | None = None) -> dict:
    source = form_data or {}
    return {
        "query": str(source.get("query", "")).strip(),
        "pages": str(source.get("pages", "1")).strip() or "1",
        "price_from": str(source.get("price_from", "")).strip(),
        "price_to": str(source.get("price_to", "")).strip(),
        "order": str(source.get("order", "newest_first")).strip() or "newest_first",
        "delay": str(source.get("delay", "0.5")).strip() or "0.5",
        "selected_geos": source.getlist("geo") if hasattr(source, "getlist") else source.get("geo", ["fr", "de", "it"]),
        "extra_params": str(source.get("extra_params", "")).strip(),
        "watcher_name": str(source.get("watcher_name", "")).strip(),
        "discord_webhook_url": str(source.get("discord_webhook_url", "")).strip(),
        "interval_minutes": str(source.get("interval_minutes", "5")).strip() or "5",
        "watcher_query": str(source.get("watcher_query", "")).strip(),
        "watcher_pages": str(source.get("watcher_pages", "1")).strip() or "1",
        "watcher_price_from": str(source.get("watcher_price_from", "")).strip(),
        "watcher_price_to": str(source.get("watcher_price_to", "")).strip(),
        "watcher_order": str(source.get("watcher_order", "newest_first")).strip() or "newest_first",
        "watcher_fresh_minutes": str(source.get("watcher_fresh_minutes", "10")).strip() or "10",
        "watcher_selected_geos": (
            source.getlist("watcher_geo")
            if hasattr(source, "getlist")
            else source.get("watcher_geo", ["fr", "de", "it"])
        ),
        "watcher_extra_params": str(source.get("watcher_extra_params", "")).strip(),
    }


@app.route("/", methods=["GET", "POST"])
def login():
    if g.current_user:
        return redirect(url_for("dashboard"))

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()

        try:
            user = authenticate_user(username, password)
            if not user:
                raise ValueError("Wrong username or password, or access has expired.")
            session.clear()
            session["user_id"] = int(user["id"])
            return redirect(url_for("dashboard"))
        except Exception as exc:
            flash(str(exc), "error")

    return render_template("login.html")


@app.post("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/dashboard", methods=["GET", "POST"])
@login_required
def dashboard():
    result = None
    defaults = normalize_dashboard_defaults()

    if request.method == "POST":
        form = request.form
        action = form.get("action", "search").strip()
        defaults = normalize_dashboard_defaults(form)

        try:
            if action == "create_watcher":
                if not defaults["watcher_name"]:
                    raise ValueError("Watcher name is required.")
                if not defaults["watcher_query"]:
                    raise ValueError("Query is required for a watcher.")
                if not defaults["discord_webhook_url"]:
                    raise ValueError("Discord webhook URL is required.")
                selected_geos = expand_geos(defaults["watcher_selected_geos"])
                create_watcher(int(g.current_user["id"]), defaults, selected_geos)
                flash(
                    "Watcher created. Baseline saved and a startup ping was sent to Discord. Future alerts will include only fresh listings.",
                    "info",
                )
                return redirect(url_for("dashboard"))

            if action in {"toggle_watcher", "delete_watcher", "run_watcher"}:
                watcher_id = int(form.get("watcher_id", "0"))
                watcher = get_watcher_for_user(
                    watcher_id,
                    int(g.current_user["id"]),
                    g.current_user["role"] == "admin",
                )
                if not watcher:
                    raise ValueError("Watcher not found.")
                if action == "toggle_watcher":
                    enabled = form.get("enabled", "0") == "1"
                    set_watcher_enabled(watcher_id, enabled)
                    return redirect(url_for("dashboard"))
                if action == "delete_watcher":
                    delete_watcher(watcher_id)
                    return redirect(url_for("dashboard"))
                new_count, total_count = run_single_watcher(watcher_id)
                flash(
                    f"Watcher checked successfully. New items: {new_count}. Total unique in current scan: {total_count}.",
                    "info",
                )
                return redirect(url_for("dashboard"))

            if action == "toggle_favorite":
                added = toggle_favorite(int(g.current_user["id"]), form)
                flash("Added to favorites." if added else "Removed from favorites.", "info")
                if not defaults["query"]:
                    return redirect(url_for("dashboard"))

            geos = expand_geos(defaults["selected_geos"] or ["fr"])
            raw_extra = [line.strip() for line in defaults["extra_params"].splitlines() if line.strip()]
            result = run_search(
                query=defaults["query"],
                geos=geos,
                pages=safe_int(defaults["pages"]) or 1,
                delay=safe_float(defaults["delay"], 0.5) or 0.5,
                order=defaults["order"],
                price_from=safe_int(defaults["price_from"]),
                price_to=safe_int(defaults["price_to"]),
                extra_params=parse_extra_params(raw_extra),
                output_dir=OUTPUT_DIR,
            )
            if result.get("failures"):
                flash(
                    "Some geos failed: "
                    + " | ".join(result["failures"])
                    + ". This usually means Vinted blocked the server IP for that geo.",
                    "error",
                )
            elif result["unique_count"] == 0:
                flash("Search returned zero items. Try a different geo or remove some filters.", "info")
        except Exception as exc:
            logger.exception("Dashboard request failed")
            flash(str(exc), "error")

    favorites = list_favorites(int(g.current_user["id"]))
    favorite_urls = {row["item_url"] for row in favorites}
    watchers = decorate_watcher_statuses(list_watchers(int(g.current_user["id"])))
    return render_template(
        "dashboard.html",
        geo_options=GEO_DOMAINS,
        defaults=defaults,
        watchers=watchers,
        favorites=favorites,
        favorite_urls=favorite_urls,
        result=result,
    )


@app.route("/admin", methods=["GET", "POST"])
@admin_required
def admin_panel():
    if request.method == "POST":
        action = request.form.get("action", "").strip()
        user_id = int(request.form.get("user_id", "0"))
        try:
            if action == "create_user":
                username = request.form.get("username", "").strip()
                password = request.form.get("password", "").strip()
                duration = request.form.get("access_duration", "1m").strip()
                role = request.form.get("role", "user").strip()
                if len(username) < 3:
                    raise ValueError("Username must be at least 3 characters.")
                if len(password) < 6:
                    raise ValueError("Password must be at least 6 characters.")
                if get_user_by_username(username):
                    raise ValueError("This username is already taken.")
                if role not in {"user", "admin"}:
                    raise ValueError("Unsupported role.")
                create_user_with_access(
                    username=username,
                    password=password,
                    role=role,
                    access_expires_at=compute_access_expiry(duration),
                )
                flash("User created.", "info")
            elif action == "toggle_user_active":
                if user_id == int(g.current_user["id"]):
                    raise ValueError("You cannot disable the current admin session.")
                with get_db_connection() as connection:
                    user = connection.execute("SELECT active FROM users WHERE id = ?", (user_id,)).fetchone()
                    if not user:
                        raise ValueError("User not found.")
                    connection.execute(
                        "UPDATE users SET active = ? WHERE id = ?",
                        (0 if user["active"] else 1, user_id),
                    )
                    connection.commit()
                flash("User status updated.", "info")
            elif action == "toggle_user_role":
                if user_id == int(g.current_user["id"]):
                    raise ValueError("Change another account's role if needed.")
                with get_db_connection() as connection:
                    user = connection.execute("SELECT role FROM users WHERE id = ?", (user_id,)).fetchone()
                    if not user:
                        raise ValueError("User not found.")
                    next_role = "user" if user["role"] == "admin" else "admin"
                    connection.execute("UPDATE users SET role = ? WHERE id = ?", (next_role, user_id))
                    connection.commit()
                flash("User role updated.", "info")
            elif action == "set_user_access":
                if user_id == int(g.current_user["id"]):
                    raise ValueError("Do not change the current admin access from this panel.")
                duration = request.form.get("access_duration", "").strip()
                with get_db_connection() as connection:
                    user = connection.execute(
                        "SELECT id FROM users WHERE id = ?",
                        (user_id,),
                    ).fetchone()
                    if not user:
                        raise ValueError("User not found.")
                    connection.execute(
                        "UPDATE users SET active = 1, access_expires_at = ? WHERE id = ?",
                        (compute_access_expiry(duration), user_id),
                    )
                    connection.commit()
                flash("User access updated.", "info")
            elif action == "delete_user":
                if user_id == int(g.current_user["id"]):
                    raise ValueError("You cannot delete the current admin session.")
                with get_db_connection() as connection:
                    user = connection.execute(
                        "SELECT role FROM users WHERE id = ?",
                        (user_id,),
                    ).fetchone()
                    if not user:
                        raise ValueError("User not found.")
                    connection.execute("DELETE FROM users WHERE id = ?", (user_id,))
                    connection.commit()
                flash("User deleted.", "info")
        except Exception as exc:
            flash(str(exc), "error")
        return redirect(url_for("admin_panel"))

    with get_db_connection() as connection:
        users = connection.execute(
            """
            SELECT
                users.*,
                COUNT(DISTINCT watchers.id) AS watcher_count,
                COUNT(DISTINCT favorites.id) AS favorite_count
            FROM users
            LEFT JOIN watchers ON watchers.user_id = users.id
            LEFT JOIN favorites ON favorites.user_id = users.id
            GROUP BY users.id
            ORDER BY users.role DESC, users.created_at ASC
            """
        ).fetchall()

    decorated_users = []
    for user in users:
        item = dict(user)
        item["access_status"] = "active" if is_user_access_valid(user) else "expired"
        item["access_label"] = describe_access_window(user["access_expires_at"])
        decorated_users.append(item)

    watchers = list_watchers()
    return render_template("admin.html", users=decorated_users, watchers=watchers)


@app.get("/healthz")
def healthz():
    try:
        with get_db_connection() as connection:
            connection.execute("SELECT 1").fetchone()
        return {"ok": True, "db_path": str(DB_PATH)}, 200
    except Exception as exc:
        return {"ok": False, "error": str(exc)}, 500


@app.get("/download/<fmt>/<path:filename>")
@login_required
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


@app.get("/favicon.ico")
def favicon():
    return "", 204


@app.template_filter("basename")
def basename_filter(path: Path) -> str:
    return Path(path).name


@app.template_filter("urlquote")
def urlquote_filter(value: str) -> str:
    return quote(value)


if os.environ.get("SKIP_APP_BOOT") != "1":
    init_db()
    start_watcher_thread()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)
