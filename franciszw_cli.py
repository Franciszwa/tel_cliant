


import os
import sys
import asyncio
import sqlite3
import shutil
import subprocess
import threading
import mimetypes
import html
import urllib.parse
import webbrowser
import time
import math

from datetime import datetime
from zoneinfo import ZoneInfo
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.errors import SessionPasswordNeededError






load_dotenv()

API_ID = os.getenv("API_ID")
API_HASH = os.getenv("API_HASH")






class C:
    RESET = "\033[0m"

    BLACK = "\033[30m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"

    BRIGHT_BLACK = "\033[90m"
    BRIGHT_RED = "\033[91m"
    BRIGHT_GREEN = "\033[92m"
    BRIGHT_YELLOW = "\033[93m"
    BRIGHT_BLUE = "\033[94m"
    BRIGHT_MAGENTA = "\033[95m"
    BRIGHT_CYAN = "\033[96m"
    BRIGHT_WHITE = "\033[97m"

    BOLD = "\033[1m"
    DIM = "\033[2m"
    UNDERLINE = "\033[4m"

    BG_BLACK = "\033[40m"
    BG_RED = "\033[41m"
    BG_GREEN = "\033[42m"
    BG_YELLOW = "\033[43m"
    BG_BLUE = "\033[44m"
    BG_MAGENTA = "\033[45m"
    BG_CYAN = "\033[46m"
    BG_WHITE = "\033[47m"


def color(text, *styles):
    return "".join(styles) + str(text) + C.RESET


def title_text(text):
    return color(text, C.BOLD, C.BRIGHT_CYAN)


def accent(text):
    return color(text, C.BRIGHT_MAGENTA, C.BOLD)


def success(text):
    return color(text, C.BRIGHT_GREEN)


def warning(text):
    return color(text, C.BRIGHT_YELLOW)


def error(text):
    return color(text, C.BRIGHT_RED)


def muted(text):
    return color(text, C.BRIGHT_BLACK)


def info(text):
    return color(text, C.BRIGHT_BLUE)


def cyan(text):
    return color(text, C.BRIGHT_CYAN)


def white(text):
    return color(text, C.BRIGHT_WHITE)


def badge(text, background=C.BG_BLUE):
    return color(
        f" {text} ",
        C.BOLD,
        C.BRIGHT_WHITE,
        background
    )


if not API_ID or not API_HASH:
    print()
    print(
        error("ERROR")
        + " "
        + white("API_ID and API_HASH are required in .env.")
    )
    print()
    print(
        muted("Example:")
    )
    print(
        cyan("API_ID=12345678")
    )
    print(
        cyan("API_HASH=xxxxxxxxxxxxxxxxxxxxxxxx")
    )
    print()
    sys.exit(1)

try:
    API_ID = int(API_ID)
except ValueError:
    print(
        error("ERROR")
        + " "
        + white("API_ID must be a number.")
    )
    sys.exit(1)

SESSION_NAME = "telegram_cli"
DB_FILE = "telegram_cli.db"

MEDIA_DIR = os.path.abspath("telegram_media")






AUTO_DOWNLOAD_MAX_SIZE = 20 * 1024 * 1024

AUTO_DOWNLOAD_MEDIA_TYPES = {
    "image",
    "video",
}






AUTO_DOWNLOAD_CONCURRENCY = 4

DOWNLOAD_QUEUE_MAXSIZE = 500

DOWNLOAD_POLL_INTERVAL = 0.01






MAX_CHATS = 1000
DEFAULT_MESSAGES = 100
MAX_MESSAGES = 5000
OLDER_BATCH = 100

FAST_SYNC_MESSAGES = 100

STARTUP_UNREAD_LIMIT = 100
STARTUP_SYNC_CONCURRENCY = 4

HOST = "127.0.0.1"
PORT = 8765

IRAN_TZ = ZoneInfo("Asia/Tehran")

os.makedirs(MEDIA_DIR, exist_ok=True)






db = sqlite3.connect(
    DB_FILE,
    check_same_thread=False
)

db.execute("PRAGMA journal_mode=WAL")
db.execute("PRAGMA synchronous=NORMAL")
db.execute("PRAGMA busy_timeout=5000")
db.execute("PRAGMA temp_store=MEMORY")
db.execute("PRAGMA cache_size=-32000")

db.execute("""
CREATE TABLE IF NOT EXISTS chats (
    chat_id INTEGER PRIMARY KEY,
    name TEXT,
    username TEXT,
    chat_type TEXT,
    updated_at TEXT,
    unread_count INTEGER DEFAULT 0
)
""")

db.execute("""
CREATE TABLE IF NOT EXISTS messages (
    local_id INTEGER PRIMARY KEY AUTOINCREMENT,
    chat_id INTEGER NOT NULL,
    telegram_id INTEGER NOT NULL,
    sender_id INTEGER,
    sender_name TEXT,
    text TEXT,
    outgoing INTEGER DEFAULT 0,
    date TEXT,
    media_path TEXT,
    media_type TEXT,
    media_name TEXT,
    UNIQUE(chat_id, telegram_id)
)
""")

db.execute("""
CREATE INDEX IF NOT EXISTS idx_messages_chat
ON messages(chat_id, local_id)
""")

db.execute("""
CREATE INDEX IF NOT EXISTS idx_messages_chat_telegram
ON messages(chat_id, telegram_id)
""")

db.execute("""
CREATE TABLE IF NOT EXISTS new_messages (
    local_id INTEGER PRIMARY KEY,
    created_at TEXT
)
""")

db.commit()






def migrate_database():
    chat_columns = {
        row[1]
        for row in db.execute(
            "PRAGMA table_info(chats)"
        ).fetchall()
    }

    chat_required = {
        "chat_type": "TEXT",
        "username": "TEXT",
        "updated_at": "TEXT",
        "unread_count": "INTEGER DEFAULT 0",
    }

    for column, definition in chat_required.items():
        if column not in chat_columns:
            db.execute(
                f"""
                ALTER TABLE chats
                ADD COLUMN {column} {definition}
                """
            )

    message_columns = {
        row[1]
        for row in db.execute(
            "PRAGMA table_info(messages)"
        ).fetchall()
    }

    message_required = {
        "sender_id": "INTEGER",
        "sender_name": "TEXT",
        "text": "TEXT",
        "outgoing": "INTEGER DEFAULT 0",
        "date": "TEXT",
        "media_path": "TEXT",
        "media_type": "TEXT",
        "media_name": "TEXT",
    }

    for column, definition in message_required.items():
        if column not in message_columns:
            db.execute(
                f"""
                ALTER TABLE messages
                ADD COLUMN {column} {definition}
                """
            )

    db.commit()


migrate_database()






client = TelegramClient(
    SESSION_NAME,
    API_ID,
    API_HASH,
    connection_retries=5,
    retry_delay=0.1,
    request_retries=3,
)






current_chat = None
chat_cache = []

running = True

redraw_event = asyncio.Event()
render_lock = asyncio.Lock()

new_message_ids = set()

viewer_server = None

background_tasks = set()

startup_sync_started = False

download_queue = None
download_workers = []

queued_download_ids = set()
active_download_ids = set()

download_state_lock = None






def create_background_task(coro):
    task = asyncio.create_task(coro)

    background_tasks.add(task)

    def done_callback(done_task):
        background_tasks.discard(done_task)

        try:
            done_task.result()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(
                "\n"
                + error("BACKGROUND TASK ERROR")
                + ": "
                + str(e),
                flush=True
            )

    task.add_done_callback(done_callback)

    return task






def now_utc_iso():
    return datetime.now(
        ZoneInfo("UTC")
    ).isoformat()


def width():
    return max(
        70,
        min(
            shutil.get_terminal_size(
                (80, 24)
            ).columns,
            120
        )
    )


def franciszw_loading():
    clear()
    frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
    logo = [
        "╔══════════════════════════════════════════════════════════════╗",
        "║                         FRANCISZW                            ║",
        "║                 TELEGRAM PRIVATE VIEWER                    ║",
        "╚══════════════════════════════════════════════════════════════╝",
    ]
    for row in logo:
        print(color(row, C.BRIGHT_CYAN, C.BOLD))
    print()
    print(color("                    Initializing...", C.BRIGHT_WHITE, C.BOLD))
    print()
    for frame in frames:
        sys.stdout.write("\r" + color(f"                    {frame} Loading Franciszw", C.BRIGHT_MAGENTA, C.BOLD))
        sys.stdout.flush()
        time.sleep(0.055)
    sys.stdout.write("\r" + " " * max(1, width() - 1) + "\r")
    sys.stdout.flush()


def clear():
    sys.stdout.write(
        "\033[2J\033[3J\033[H"
    )
    sys.stdout.flush()


def line():
    return color(
        "─" * width(),
        C.BRIGHT_BLACK
    )


def cut(text, maximum):
    text = str(text or "")

    if len(text) <= maximum:
        return text

    if maximum <= 3:
        return text[:maximum]

    return text[:maximum - 3] + "..."


def header(title):
    w = width()

    title = cut(
        title,
        w - 6
    )

    print(
        color(
            "╭" + "─" * (w - 2) + "╮",
            C.BRIGHT_BLUE
        )
    )

    inner = (
        "│  "
        + title_text(title)
        + " " * max(
            0,
            w - 4 - len(title)
        )
        + "  │"
    )

    print(
        color(
            inner,
            C.BRIGHT_BLUE
        )
    )

    print(
        color(
            "╰" + "─" * (w - 2) + "╯",
            C.BRIGHT_BLUE
        )
    )


def format_size(size):
    if size is None:
        return "Unknown"

    try:
        size = int(size)
    except Exception:
        return "Unknown"

    if size < 1024:
        return f"{size} B"

    if size < 1024 * 1024:
        return f"{size / 1024:.1f} KB"

    if size < 1024 * 1024 * 1024:
        return f"{size / (1024 * 1024):.2f} MB"

    return f"{size / (1024 * 1024 * 1024):.2f} GB"






def format_progress_size(value):
    if value is None:
        return "0 B"

    value = float(value)

    if value < 1024:
        return f"{value:.0f} B"

    if value < 1024 * 1024:
        return f"{value / 1024:.1f} KB"

    if value < 1024 * 1024 * 1024:
        return f"{value / (1024 * 1024):.1f} MB"

    return f"{value / (1024 * 1024 * 1024):.2f} GB"


class DownloadProgress:

    def __init__(self, total):
        self.total = int(total or 0)
        self.start_time = time.monotonic()
        self.last_print = 0.0
        self.last_current = 0

    def callback(self, current, total):
        if total:
            self.total = int(total)

        current = int(current)

        now = time.monotonic()

        if (
            now - self.last_print < 0.10
            and current < self.total
        ):
            return

        self.last_print = now

        elapsed = max(
            now - self.start_time,
            0.001
        )

        speed = current / elapsed

        if speed > 0 and self.total > current:
            remaining = self.total - current
            eta = int(
                math.ceil(
                    remaining / speed
                )
            )
        else:
            eta = 0

        if self.total > 0:
            percent = (
                current / self.total
            ) * 100

            percent = min(
                100.0,
                max(0.0, percent)
            )
        else:
            percent = 0.0

        bar_width = 20

        filled = int(
            bar_width * percent / 100
        )

        filled = min(
            bar_width,
            max(0, filled)
        )

        bar = (
            color(
                "█" * filled,
                C.BRIGHT_CYAN
            )
            + color(
                "░" * (bar_width - filled),
                C.BRIGHT_BLACK
            )
        )

        current_text = format_progress_size(
            current
        )

        total_text = format_progress_size(
            self.total
        )

        speed_text = format_progress_size(
            speed
        ) + "/s"

        if current >= self.total and self.total > 0:
            eta_text = "0s"
        else:
            eta_text = f"{eta}s"

        text = (
            f"Downloading... "
            f"{bar} "
            f"{percent:5.1f}% "
            f"{current_text} / {total_text} "
            f"Speed: {speed_text} "
            f"ETA: {eta_text}"
        )

        terminal_width = width()

        if len(text) > terminal_width - 1:
            text = text[:terminal_width - 1]

        sys.stdout.write(
            "\r\033[2K" + text
        )

        sys.stdout.flush()

        self.last_current = current

    def finish(self):
        sys.stdout.write("\n")
        sys.stdout.flush()






def normalize_datetime(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value)

    if value.tzinfo is None:
        value = value.replace(
            tzinfo=ZoneInfo("UTC")
        )

    return value


def iran_time(value):
    try:
        value = normalize_datetime(value)

        return value.astimezone(
            IRAN_TZ
        ).strftime("%H:%M")

    except Exception:
        return "--:--"


def iran_full_time(value):
    try:
        value = normalize_datetime(value)

        return value.astimezone(
            IRAN_TZ
        ).strftime("%Y-%m-%d %H:%M:%S")

    except Exception:
        return "Unknown"


def telegram_date(message):
    try:
        dt = message.date

        if dt.tzinfo is None:
            dt = dt.replace(
                tzinfo=ZoneInfo("UTC")
            )

        return dt.isoformat()

    except Exception:
        return now_utc_iso()






def get_chat_type(entity):
    try:
        if entity.__class__.__name__ == "User":
            if getattr(
                entity,
                "bot",
                False
            ):
                return "bot"

            if getattr(
                entity,
                "deleted",
                False
            ):
                return "deleted"

            return "private"

        if entity.__class__.__name__ == "Channel":
            if getattr(
                entity,
                "broadcast",
                False
            ):
                return "channel"

            if getattr(
                entity,
                "megagroup",
                False
            ):
                return "group"

            return "channel"

        if entity.__class__.__name__ == "Chat":
            return "group"

    except Exception:
        pass

    return "other"


def is_private_user(entity):
    try:
        return (
            entity is not None
            and entity.__class__.__name__ == "User"
            and not getattr(
                entity,
                "bot",
                False
            )
            and not getattr(
                entity,
                "deleted",
                False
            )
        )

    except Exception:
        return False






def entity_name_sync(entity):
    try:
        title = getattr(
            entity,
            "title",
            None
        )

        if title:
            return title

        first = getattr(
            entity,
            "first_name",
            None
        )

        last = getattr(
            entity,
            "last_name",
            None
        )

        username = getattr(
            entity,
            "username",
            None
        )

        if first:
            name = first

            if last:
                name += " " + last

            return name

        if username:
            return username

        return str(
            getattr(
                entity,
                "id",
                "Unknown"
            )
        )

    except Exception:
        return "Unknown"


async def entity_name(entity):
    return entity_name_sync(entity)


def sender_name_from_entity(sender):
    try:
        if not sender:
            return "Unknown"

        first = getattr(
            sender,
            "first_name",
            None
        )

        last = getattr(
            sender,
            "last_name",
            None
        )

        title = getattr(
            sender,
            "title",
            None
        )

        username = getattr(
            sender,
            "username",
            None
        )

        if first:
            name = first

            if last:
                name += " " + last

            return name

        if title:
            return title

        if username:
            return username

        return str(
            getattr(
                sender,
                "id",
                "Unknown"
            )
        )

    except Exception:
        return "Unknown"


async def sender_name(message):
    try:
        sender = getattr(
            message,
            "sender",
            None
        )

        if sender:
            return sender_name_from_entity(
                sender
            )

        sender = await message.get_sender()

        return sender_name_from_entity(
            sender
        )

    except Exception:
        return "Unknown"






def save_chat(
    chat_id,
    name,
    username,
    chat_type,
    unread_count=None
):
    if chat_type != "private":
        return

    if unread_count is None:
        existing = db.execute(
            """
            SELECT unread_count
            FROM chats
            WHERE chat_id=?
            """,
            (chat_id,)
        ).fetchone()

        unread_count = (
            existing[0]
            if existing
            else 0
        )

    db.execute(
        """
        INSERT INTO chats
        (
            chat_id,
            name,
            username,
            chat_type,
            updated_at,
            unread_count
        )
        VALUES (?, ?, ?, ?, ?, ?)

        ON CONFLICT(chat_id)
        DO UPDATE SET
            name=excluded.name,
            username=excluded.username,
            chat_type=excluded.chat_type,
            updated_at=excluded.updated_at
        """,
        (
            chat_id,
            name or "Unknown",
            username,
            "private",
            now_utc_iso(),
            unread_count
        )
    )

    db.commit()


def set_chat_unread_count(
    chat_id,
    count
):
    db.execute(
        """
        UPDATE chats
        SET unread_count=?
        WHERE chat_id=?
        """,
        (
            max(0, int(count)),
            chat_id
        )
    )

    db.commit()


def increment_chat_unread(chat_id):
    db.execute(
        """
        UPDATE chats
        SET unread_count =
            COALESCE(unread_count, 0) + 1
        WHERE chat_id=?
        """,
        (chat_id,)
    )

    db.commit()


def get_chat_unread_count(chat_id):
    row = db.execute(
        """
        SELECT unread_count
        FROM chats
        WHERE chat_id=?
        """,
        (chat_id,)
    ).fetchone()

    if not row:
        return 0

    return int(row[0] or 0)






def save_message(
    chat_id,
    telegram_id,
    sender_id,
    sender_name_value,
    text,
    outgoing,
    date,
    media_path=None,
    media_type=None,
    media_name=None
):
    cursor = db.execute(
        """
        INSERT OR IGNORE INTO messages
        (
            chat_id,
            telegram_id,
            sender_id,
            sender_name,
            text,
            outgoing,
            date,
            media_path,
            media_type,
            media_name
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            chat_id,
            telegram_id,
            sender_id,
            sender_name_value or "Unknown",
            text or "",
            1 if outgoing else 0,
            date,
            media_path,
            media_type,
            media_name
        )
    )

    db.commit()

    return cursor.rowcount > 0


def update_media(
    local_id,
    media_path,
    media_type,
    media_name
):
    db.execute(
        """
        UPDATE messages
        SET
            media_path=?,
            media_type=?,
            media_name=?
        WHERE local_id=?
        """,
        (
            media_path,
            media_type,
            media_name,
            local_id
        )
    )

    db.commit()


def get_messages(
    chat_id,
    limit=100
):
    rows = db.execute(
        """
        SELECT
            local_id,
            telegram_id,
            sender_id,
            sender_name,
            text,
            outgoing,
            date,
            media_path,
            media_type,
            media_name
        FROM messages
        WHERE chat_id=?
        ORDER BY local_id DESC
        LIMIT ?
        """,
        (
            chat_id,
            limit
        )
    ).fetchall()

    return rows[::-1]


def get_message(local_id):
    return db.execute(
        """
        SELECT
            local_id,
            chat_id,
            telegram_id,
            sender_id,
            sender_name,
            text,
            outgoing,
            date,
            media_path,
            media_type,
            media_name
        FROM messages
        WHERE local_id=?
        """,
        (local_id,)
    ).fetchone()


def get_oldest_telegram_id(chat_id):
    row = db.execute(
        """
        SELECT telegram_id
        FROM messages
        WHERE chat_id=?
        ORDER BY telegram_id ASC
        LIMIT 1
        """,
        (chat_id,)
    ).fetchone()

    if not row:
        return None

    return row[0]






def add_new_message(local_id):
    db.execute(
        """
        INSERT OR IGNORE INTO new_messages
        (
            local_id,
            created_at
        )
        VALUES (?, ?)
        """,
        (
            local_id,
            now_utc_iso()
        )
    )

    db.commit()

    new_message_ids.add(local_id)


def remove_new_message(local_id):
    db.execute(
        """
        DELETE FROM new_messages
        WHERE local_id=?
        """,
        (local_id,)
    )

    db.commit()

    new_message_ids.discard(local_id)


def mark_chat_read(chat_id):
    if not chat_id:
        return

    rows = db.execute(
        """
        SELECT nm.local_id
        FROM new_messages nm
        JOIN messages m
            ON m.local_id=nm.local_id
        JOIN chats c
            ON c.chat_id=m.chat_id
        WHERE m.chat_id=?
          AND c.chat_type='private'
        """,
        (chat_id,)
    ).fetchall()

    ids = [
        row[0]
        for row in rows
    ]

    if ids:
        placeholders = ",".join(
            "?" for _ in ids
        )

        db.execute(
            f"""
            DELETE FROM new_messages
            WHERE local_id IN ({placeholders})
            """,
            tuple(ids)
        )

    db.execute(
        """
        UPDATE chats
        SET unread_count=0
        WHERE chat_id=?
        """,
        (chat_id,)
    )

    db.commit()

    for local_id in ids:
        new_message_ids.discard(
            local_id
        )


def load_new_messages():
    global new_message_ids

    rows = db.execute(
        """
        SELECT nm.local_id
        FROM new_messages nm
        JOIN messages m
            ON m.local_id=nm.local_id
        JOIN chats c
            ON c.chat_id=m.chat_id
        WHERE c.chat_type='private'
        ORDER BY nm.created_at DESC
        """
    ).fetchall()

    new_message_ids = {
        row[0]
        for row in rows
    }


def get_new_private_chats():
    rows = db.execute(
        """
        SELECT
            m.local_id,
            m.chat_id,
            c.name,
            c.username,
            m.sender_name,
            m.date,
            COALESCE(c.unread_count, 0)
        FROM messages m
        JOIN new_messages nm
            ON nm.local_id=m.local_id
        JOIN chats c
            ON c.chat_id=m.chat_id
        WHERE c.chat_type='private'
        AND nm.local_id = (
            SELECT nm2.local_id
            FROM new_messages nm2
            JOIN messages m2
                ON m2.local_id=nm2.local_id
            WHERE m2.chat_id=m.chat_id
            ORDER BY nm2.created_at DESC
            LIMIT 1
        )
        ORDER BY nm.created_at DESC
        """
    ).fetchall()

    return rows






def detect_media_type(message):
    if not message.media:
        return None

    try:
        if getattr(
            message,
            "photo",
            None
        ):
            return "image"

        document = getattr(
            message,
            "document",
            None
        )

        if document:
            mime = (
                getattr(
                    document,
                    "mime_type",
                    None
                )
                or ""
            )

            if mime.startswith("image/"):
                return "image"

            if mime.startswith("video/"):
                return "video"

            if mime.startswith("audio/"):
                return "audio"

            if mime == "application/pdf":
                return "pdf"

            return "file"

    except Exception:
        pass

    return "file"


def media_filename(message):
    try:
        document = getattr(
            message,
            "document",
            None
        )

        if document:
            for attr in (
                document.attributes or []
            ):
                filename = getattr(
                    attr,
                    "file_name",
                    None
                )

                if filename:
                    return filename

    except Exception:
        pass

    if getattr(
        message,
        "photo",
        None
    ):
        return f"photo_{message.id}.jpg"

    return f"media_{message.id}"


def get_media_size(message):
    try:
        file_obj = getattr(
            message,
            "file",
            None
        )

        if file_obj:
            size = getattr(
                file_obj,
                "size",
                None
            )

            if size is not None:
                return int(size)

    except Exception:
        pass

    try:
        document = getattr(
            message,
            "document",
            None
        )

        if document:
            size = getattr(
                document,
                "size",
                None
            )

            if size is not None:
                return int(size)

    except Exception:
        pass

    return None


def can_auto_download(message):
    if not message:
        return False

    if not message.media:
        return False

    media_type = detect_media_type(
        message
    )

    if media_type not in AUTO_DOWNLOAD_MEDIA_TYPES:
        return False

    size = get_media_size(
        message
    )

    if size is None:
        return False

    return size <= AUTO_DOWNLOAD_MAX_SIZE






def build_media_destination(
    chat_id,
    telegram_id,
    filename
):
    chat_dir = os.path.join(
        MEDIA_DIR,
        str(chat_id)
    )

    os.makedirs(
        chat_dir,
        exist_ok=True
    )

    filename = os.path.basename(
        filename
    )

    return os.path.join(
        chat_dir,
        f"{telegram_id}_{filename}"
    )






async def download_message_media(
    local_id,
    automatic=False
):
    row = get_message(local_id)

    if not row:
        if not automatic:
            print(
                "\n"
                + error("MESSAGE NOT FOUND")
            )

        return False

    (
        lid,
        chat_id,
        telegram_id,
        sender_id,
        sender,
        text,
        outgoing,
        date,
        media_path,
        media_type,
        media_name
    ) = row

    if media_path and os.path.isfile(
        media_path
    ):
        if not automatic:
            print(
                "\n"
                + info("ALREADY DOWNLOADED")
                + ":"
            )
            print(
                cyan(media_path)
            )

        return True

    try:
        entity = await client.get_entity(
            chat_id
        )

        if not is_private_user(entity):
            if not automatic:
                print(
                    "\n"
                    + error("INVALID PRIVATE MESSAGE")
                )

            return False

        message = await client.get_messages(
            entity,
            ids=telegram_id
        )

        if not message:
            if not automatic:
                print(
                    "\n"
                    + error("MESSAGE NOT FOUND")
                )

            return False

        if not message.media:
            if not automatic:
                print(
                    "\n"
                    + error("NO MEDIA FOUND")
                )

            return False

        media_type = detect_media_type(
            message
        )

        filename = media_filename(
            message
        )

        size = get_media_size(
            message
        )

        if automatic:
            if media_type not in AUTO_DOWNLOAD_MEDIA_TYPES:
                return False

            if size is None:
                return False

            if size > AUTO_DOWNLOAD_MAX_SIZE:
                return False

        destination = build_media_destination(
            chat_id,
            telegram_id,
            filename
        )

        if os.path.isfile(destination):
            mime = (
                mimetypes.guess_type(
                    destination
                )[0]
                or getattr(
                    getattr(
                        message,
                        "document",
                        None
                    ),
                    "mime_type",
                    None
                )
                or "application/octet-stream"
            )

            update_media(
                local_id,
                os.path.abspath(
                    destination
                ),
                mime,
                filename
            )

            return True

        progress = None

        if not automatic:
            print()
            print(
                f"{muted('File')}    : "
                f"{white(filename)}"
            )

            if size is not None:
                print(
                    f"{muted('Size')}    : "
                    f"{cyan(format_size(size))}"
                )

            print()

            progress = DownloadProgress(
                size
            )

        else:
            print(
                f"\n"
                f"{success('[AutoDownload]')} "
                f"{accent('START')} "
                f"{cyan(media_type)} "
                f"{white(format_size(size))} "
                f"{muted(f'message={local_id}')}",
                flush=True
            )

        if automatic:
            downloaded = await message.download_media(
                file=destination
            )

        else:
            downloaded = await message.download_media(
                file=destination,
                progress_callback=progress.callback
            )

            progress.finish()

        if not downloaded:
            if not automatic:
                print(
                    "\n"
                    + error("DOWNLOAD FAILED")
                )

            return False

        destination = os.path.abspath(
            downloaded
        )

        mime = (
            mimetypes.guess_type(
                destination
            )[0]
            or getattr(
                getattr(
                    message,
                    "document",
                    None
                ),
                "mime_type",
                None
            )
            or "application/octet-stream"
        )

        update_media(
            local_id,
            destination,
            mime,
            filename
        )

        if automatic:
            print(
                f"\n"
                f"{success('[AutoDownload]')} "
                f"{success('DONE')} "
                f"{muted(f'message={local_id}')} "
                f"{cyan(destination)}",
                flush=True
            )

            redraw_event.set()

        else:
            print()
            print(
                success("DOWNLOAD COMPLETE")
            )
            print(
                cyan(destination)
            )

        return True

    except asyncio.CancelledError:
        raise

    except Exception as e:
        if not automatic:
            print(
                f"\n"
                f"{error('DOWNLOAD ERROR')}: "
                f"{e}"
            )

        else:
            print(
                f"\n"
                f"{error('[AutoDownload] ERROR')} "
                f"{muted(f'message={local_id}:')} "
                f"{e}",
                flush=True
            )

        return False






async def queue_auto_download(local_id):
    if download_queue is None:
        return

    if download_state_lock is None:
        return

    async with download_state_lock:
        if local_id in queued_download_ids:
            return

        if local_id in active_download_ids:
            return

        queued_download_ids.add(
            local_id
        )

    try:
        download_queue.put_nowait(
            local_id
        )

    except asyncio.QueueFull:
        async with download_state_lock:
            queued_download_ids.discard(
                local_id
            )


async def auto_download_worker(
    worker_number
):
    while running:
        try:
            local_id = await download_queue.get()

            async with download_state_lock:
                queued_download_ids.discard(
                    local_id
                )

                active_download_ids.add(
                    local_id
                )

            try:
                await download_message_media(
                    local_id,
                    automatic=True
                )

            finally:
                async with download_state_lock:
                    active_download_ids.discard(
                        local_id
                    )

                download_queue.task_done()

        except asyncio.CancelledError:
            break

        except Exception as e:
            print(
                f"\n"
                f"{error('DOWNLOAD WORKER ERROR')} "
                f"{worker_number}: {e}",
                flush=True
            )


async def start_download_workers():
    global download_queue
    global download_workers
    global download_state_lock

    download_queue = asyncio.Queue(
        maxsize=DOWNLOAD_QUEUE_MAXSIZE
    )

    download_state_lock = asyncio.Lock()

    download_workers = []

    for number in range(
        AUTO_DOWNLOAD_CONCURRENCY
    ):
        task = asyncio.create_task(
            auto_download_worker(
                number + 1
            )
        )

        download_workers.append(
            task
        )


async def stop_download_workers():
    for task in download_workers:
        task.cancel()

    if download_workers:
        await asyncio.gather(
            *download_workers,
            return_exceptions=True
        )

    download_workers.clear()






async def auto_download_media(local_id):
    row = get_message(local_id)

    if not row:
        return

    (
        lid,
        chat_id,
        telegram_id,
        sender_id,
        sender,
        text,
        outgoing,
        date,
        media_path,
        media_type,
        media_name
    ) = row

    if media_type not in AUTO_DOWNLOAD_MEDIA_TYPES:
        return

    if media_path and os.path.isfile(
        media_path
    ):
        return

    await queue_auto_download(
        local_id
    )






async def save_telegram_message(
    chat_id,
    message,
    is_new=False
):
    if not message:
        return None

    name = await sender_name(
        message
    )

    date = telegram_date(
        message
    )

    media_type = None

    if message.media:
        media_type = detect_media_type(
            message
        )

    text = (
        message.message
        or ""
    )

    save_message(
        chat_id,
        message.id,
        message.sender_id,
        name,
        text,
        message.out,
        date,
        None,
        media_type,
        None
    )

    row = db.execute(
        """
        SELECT local_id
        FROM messages
        WHERE chat_id=?
          AND telegram_id=?
        """,
        (
            chat_id,
            message.id
        )
    ).fetchone()

    if not row:
        return None

    local_id = row[0]

    if (
        is_new
        and not message.out
    ):
        if (
            current_chat is None
            or current_chat["id"] != chat_id
        ):
            add_new_message(
                local_id
            )

            increment_chat_unread(
                chat_id
            )

        if can_auto_download(
            message
        ):
            await queue_auto_download(
                local_id
            )

    return local_id






async def sync_chat(
    chat,
    limit=FAST_SYNC_MESSAGES
):
    if not chat:
        return 0

    if chat["chat_type"] != "private":
        return 0

    count = 0

    try:
        messages = await client.get_messages(
            chat["entity"],
            limit=limit
        )

        for message in reversed(
            messages
        ):
            if not message:
                continue

            await save_telegram_message(
                chat["id"],
                message,
                is_new=False
            )

            count += 1

    except Exception as e:
        print(
            f"\n"
            f"{error('SYNC ERROR')}: {e}"
        )

    return count


async def background_sync_chat(
    chat_id,
    entity,
    limit=FAST_SYNC_MESSAGES
):
    try:
        messages = await client.get_messages(
            entity,
            limit=limit
        )

        for message in reversed(
            messages
        ):
            if not message:
                continue

            await save_telegram_message(
                chat_id,
                message,
                is_new=False
            )

        if (
            current_chat is not None
            and current_chat["id"] == chat_id
        ):
            redraw_event.set()

    except Exception as e:
        print(
            f"\n"
            f"{error('BACKGROUND SYNC ERROR')}: {e}"
        )






async def load_chats():
    global chat_cache

    chats = []

    try:
        async for dialog in client.iter_dialogs(
            limit=MAX_CHATS
        ):
            entity = dialog.entity

            if not is_private_user(entity):
                continue

            name = (
                dialog.name
                or entity_name_sync(entity)
            )

            username = getattr(
                entity,
                "username",
                None
            )

            unread_count = int(
                getattr(
                    dialog,
                    "unread_count",
                    0
                )
                or 0
            )

            save_chat(
                dialog.id,
                name,
                username,
                "private",
                unread_count
            )

            chats.append(
                {
                    "id": dialog.id,
                    "name": name,
                    "username": username,
                    "chat_type": "private",
                    "entity": entity,
                    "unread_count": unread_count
                }
            )

            latest = getattr(
                dialog,
                "message",
                None
            )

            if latest:
                try:
                    await save_telegram_message(
                        dialog.id,
                        latest,
                        is_new=False
                    )
                except Exception:
                    pass

    except Exception as e:
        print(
            f"\n"
            f"{error('PRIVATE CHAT LOADING ERROR')}: {e}"
        )

    chat_cache = chats

    load_new_messages()






async def sync_startup_unread_chat(
    chat,
    semaphore
):
    async with semaphore:
        try:
            unread_count = get_chat_unread_count(
                chat["id"]
            )

            if unread_count <= 0:
                return

            limit = min(
                unread_count,
                STARTUP_UNREAD_LIMIT
            )

            messages = await client.get_messages(
                chat["entity"],
                limit=limit
            )

            for message in reversed(
                messages
            ):
                if not message:
                    continue

                if message.out:
                    continue

                await save_telegram_message(
                    chat["id"],
                    message,
                    is_new=True
                )

        except Exception as e:
            print(
                f"\n"
                f"{error('STARTUP UNREAD SYNC ERROR')}: {e}"
            )


async def startup_unread_sync():
    semaphore = asyncio.Semaphore(
        STARTUP_SYNC_CONCURRENCY
    )

    tasks = []

    for chat in chat_cache:
        unread_count = get_chat_unread_count(
            chat["id"]
        )

        if unread_count <= 0:
            continue

        tasks.append(
            asyncio.create_task(
                sync_startup_unread_chat(
                    chat,
                    semaphore
                )
            )
        )

    if tasks:
        await asyncio.gather(
            *tasks,
            return_exceptions=True
        )

    load_new_messages()

    redraw_event.set()






def find_chat(value):
    value = value.strip()

    if not value:
        return None

    if value.isdigit():
        index = int(value) - 1

        if 0 <= index < len(chat_cache):
            chat = chat_cache[index]

            if chat["chat_type"] == "private":
                return chat

        return None

    value_lower = value.lower()

    for chat in chat_cache:
        if chat["chat_type"] != "private":
            continue

        name = (
            chat["name"]
            or ""
        ).lower()

        username = (
            chat["username"]
            or ""
        ).lower()

        if value_lower in (
            name,
            username,
            "@" + username
        ):
            return chat

    for chat in chat_cache:
        if chat["chat_type"] != "private":
            continue

        name = (
            chat["name"]
            or ""
        ).lower()

        if value_lower in name:
            return chat

    return None






async def sync_all_messages(chat):
    if not chat:
        return 0

    if chat["chat_type"] != "private":
        return 0

    print()
    print(
        cyan("SYNCING FULL PRIVATE CHAT HISTORY...")
    )
    print()

    count = 0

    try:
        async for message in client.iter_messages(
            chat["entity"],
            limit=None
        ):
            if not message:
                continue

            await save_telegram_message(
                chat["id"],
                message,
                is_new=False
            )

            count += 1

            if count % 100 == 0:
                print(
                    "\r"
                    + info(f"Messages synced: {count}"),
                    end="",
                    flush=True
                )

        print()
        print(
            success(f"Total synced: {count}")
        )

        return count

    except Exception as e:
        print(
            f"\n"
            f"{error('FULL SYNC ERROR')}: {e}"
        )

        return count






async def load_older_messages(
    chat,
    amount=OLDER_BATCH
):
    if not chat:
        return 0

    oldest_id = get_oldest_telegram_id(
        chat["id"]
    )

    if oldest_id is None:
        messages = await client.get_messages(
            chat["entity"],
            limit=amount
        )
    else:
        messages = await client.get_messages(
            chat["entity"],
            limit=amount,
            max_id=oldest_id
        )

    if not messages:
        return 0

    count = 0

    for message in reversed(
        messages
    ):
        if not message:
            continue

        await save_telegram_message(
            chat["id"],
            message,
            is_new=False
        )

        count += 1

    return count






@client.on(events.NewMessage)
async def new_message_handler(event):
    try:
        message = event.message

        if not message:
            return

        if message.out:
            return

        if not event.is_private:
            return

        chat_id = event.chat_id

        if chat_id is None:
            return

        try:
            entity = await event.get_chat()
        except Exception:
            entity = None

        if not is_private_user(entity):
            return

        chat = None

        for item in chat_cache:
            if item["id"] == chat_id:
                chat = item
                break

        if chat is None:
            name = entity_name_sync(
                entity
            )

            username = getattr(
                entity,
                "username",
                None
            )

            chat = {
                "id": chat_id,
                "name": name,
                "username": username,
                "chat_type": "private",
                "entity": entity,
                "unread_count": 0
            }

            chat_cache.insert(
                0,
                chat
            )

            save_chat(
                chat_id,
                name,
                username,
                "private",
                0
            )

        else:
            try:
                chat_cache.remove(chat)
            except ValueError:
                pass

            chat_cache.insert(
                0,
                chat
            )

        await save_telegram_message(
            chat_id,
            message,
            is_new=True
        )

        if (
            current_chat is not None
            and current_chat["id"] == chat_id
        ):
            mark_chat_read(
                chat_id
            )

        redraw_event.set()

    except Exception as e:
        print(
            f"\n"
            f"{error('NEW PRIVATE MESSAGE ERROR')}: {e}"
        )






def render_new_messages():
    rows = get_new_private_chats()

    if not rows:
        return

    print(
        color(
            "╭" + "─" * (width() - 2) + "╮",
            C.BRIGHT_RED
        )
    )

    title = "│  🔔 NEW PRIVATE MESSAGES"

    print(
        color(title, C.BRIGHT_RED, C.BOLD)
        + " " * max(
            0,
            width() - len(title) - 1
        )
        + color("│", C.BRIGHT_RED)
    )

    print(
        color(
            "├" + "─" * (width() - 2) + "┤",
            C.BRIGHT_RED
        )
    )

    for (
        local_id,
        chat_id,
        name,
        username,
        sender,
        date,
        unread_count
    ) in rows[:30]:

        display_name = (
            name
            or sender
            or str(chat_id)
        )

        extra = ""

        if unread_count > 1:
            extra = (
                f"  ({unread_count} new)"
            )

        print(
            color("│ ", C.BRIGHT_RED)
            + color(
                f"[{local_id}]",
                C.BRIGHT_YELLOW,
                C.BOLD
            )
            + " "
            + white(
                cut(display_name, 35)
            )
            + " "
            + muted(iran_time(date))
            + warning(extra)
        )

    print(
        color(
            "╰" + "─" * (width() - 2) + "╯",
            C.BRIGHT_RED
        )
    )

    print()






def render_chat_list():
    header(
        "*franciszw CLI  │  PRIVATE MESSAGES  │  ● ONLINE"
    )

    print()

    render_new_messages()

    print(
        accent("◆")
        + " "
        + cyan("PRIVATE CHATS")
    )

    print()

    if not chat_cache:
        print(
            muted("    No private chats found.")
        )

    else:
        for index, chat in enumerate(
            chat_cache,
            1
        ):
            name = (
                chat["name"]
                or "Unknown"
            )

            username = chat["username"]

            if username:
                name += " @" + username

            unread = get_chat_unread_count(
                chat["id"]
            )

            unread_text = ""

            if unread > 0:
                unread_text = (
                    "  "
                    + color(
                        f"● {unread}",
                        C.BRIGHT_RED,
                        C.BOLD
                    )
                )

            print(
                "    "
                + color(
                    f"[{index:04}]",
                    C.BRIGHT_CYAN,
                    C.BOLD
                )
                + " "
                + white(
                    cut(
                        name,
                        width() - 18
                    )
                )
                + unread_text
            )

    print()
    print(line())

    print(
        muted("cd <number/name>")
        + "  "
        + muted("|")
        + "  "
        + muted("new")
        + "  "
        + muted("|")
        + "  "
        + muted("opennew <id>")
        + "  "
        + muted("|")
        + "  "
        + muted("fish")
        + "  "
        + muted("|")
        + "  "
        + muted("!<command>")
        + "  "
        + muted("|")
        + "  "
        + muted("exit")
    )






def render_chat():
    title = (
        current_chat["name"]
        or "Unknown"
    )

    header(
        "TELEGRAM CLI  │  PRIVATE  │  CHAT: "
        + title
    )

    unread = get_chat_unread_count(
        current_chat["id"]
    )

    if unread > 0:
        print(
            color(
                f"● {unread} UNREAD",
                C.BRIGHT_RED,
                C.BOLD
            )
        )
    else:
        print(
            success("● READ")
        )

    print()

    rows = get_messages(
        current_chat["id"],
        DEFAULT_MESSAGES
    )

    if not rows:
        print(
            muted("No messages.")
        )

    for row in rows:
        (
            local_id,
            telegram_id,
            sender_id,
            sender,
            text,
            outgoing,
            date,
            media_path,
            media_type,
            media_name
        ) = row

        who = (
            "You"
            if outgoing
            else sender
        )

        sender_color = (
            C.BRIGHT_GREEN
            if outgoing
            else C.BRIGHT_CYAN
        )

        print(
            color(
                f"[{local_id}]",
                C.BRIGHT_YELLOW,
                C.BOLD
            )
            + " "
            + color(
                cut(who, 22),
                sender_color,
                C.BOLD
            )
            + " "
            + muted(
                iran_time(date)
            )
        )

        if text:
            for text_line in str(
                text
            ).splitlines():
                print(
                    "    "
                    + white(
                        cut(
                            text_line,
                            width() - 7
                        )
                    )
                )

        if media_path:
            print(
                "    "
                + success(
                    f"[{media_type or 'media'}]"
                )
                + " "
                + cyan(
                    media_name or ""
                )
            )

        elif media_type:
            print(
                "    "
                + warning(
                    f"[{media_type}]"
                )
                + " "
                + muted(
                    "(not downloaded)"
                )
            )

        print()

    print(line())

    print(
        cyan("pm <text>")
        + "  |  "
        + cyan("reply <id> <text>")
        + "  |  "
        + cyan("messages [n|all]")
    )

    print(
        cyan("older [n]")
        + "  |  "
        + cyan("d <id>")
        + "  |  "
        + cyan("cat <id>")
        + "  |  "
        + cyan("search <text>")
        + "  |  "
        + cyan("new")
        + "  |  "
        + cyan("back")
        + "  |  "
        + cyan("fish")
        + "  |  "
        + cyan("!<command>")
        + "  |  "
        + cyan("exit")
    )






async def redraw():
    async with render_lock:
        clear()

        if current_chat is None:
            render_chat_list()
        else:
            render_chat()

        sys.stdout.flush()


async def redraw_worker():
    while running:
        await redraw_event.wait()

        redraw_event.clear()

        if not running:
            break

        try:
            await redraw()
        except Exception:
            pass






class ViewerHandler(
    BaseHTTPRequestHandler
):
    def log_message(
        self,
        format,
        *args
    ):
        return

    def send_html(self, content):
        data = content.encode(
            "utf-8"
        )

        self.send_response(200)

        self.send_header(
            "Content-Type",
            "text/html; charset=utf-8"
        )

        self.send_header(
            "Content-Length",
            str(len(data))
        )

        self.end_headers()

        self.wfile.write(data)

    def send_file(self, path):
        try:
            file_size = os.path.getsize(
                path
            )

            mime = (
                mimetypes.guess_type(
                    path
                )[0]
                or "application/octet-stream"
            )

            self.send_response(200)

            self.send_header(
                "Content-Type",
                mime
            )

            self.send_header(
                "Content-Length",
                str(file_size)
            )

            self.send_header(
                "Cache-Control",
                "public, max-age=3600"
            )

            self.end_headers()

            with open(
                path,
                "rb"
            ) as f:
                while True:
                    chunk = f.read(
                        1024 * 1024
                    )

                    if not chunk:
                        break

                    self.wfile.write(
                        chunk
                    )

        except Exception:
            try:
                self.send_error(500)
            except Exception:
                pass

    def do_GET(self):
        parsed = urllib.parse.urlparse(
            self.path
        )

        path = parsed.path

        if path == "/":
            self.send_html(
                """
                <!doctype html>
                <html lang="en">
                <head>
                    <meta charset="utf-8">

                    <meta
                        name="viewport"
                        content="width=device-width,initial-scale=1"
                    >

                    <title>Franciszw</title>

                    <style>
                    * {
                        box-sizing:border-box;
                    }

                    body {
                        margin:0;
                        min-height:100vh;
                        background:
                            radial-gradient(
                                circle at top left,
                                #172554 0,
                                transparent 38%
                            ),
                            radial-gradient(
                                circle at bottom right,
                                #312e81 0,
                                transparent 34%
                            ),
                            #05070d;

                        color:#e5e7eb;
                        font-family:
                            Inter,
                            ui-sans-serif,
                            system-ui,
                            -apple-system,
                            BlinkMacSystemFont,
                            "Segoe UI",
                            sans-serif;

                        display:flex;
                        align-items:center;
                        justify-content:center;
                        padding:24px;
                    }

                    .card {
                        width:min(900px,100%);
                        background:
                            linear-gradient(
                                145deg,
                                rgba(17,24,39,.96),
                                rgba(9,13,23,.96)
                            );

                        border:1px solid #263244;
                        border-radius:24px;
                        padding:38px;

                        box-shadow:
                            0 25px 80px
                            rgba(0,0,0,.45),
                            0 0 40px
                            rgba(59,130,246,.08);
                    }

                    .brand {
                        display:flex;
                        align-items:center;
                        gap:14px;
                        margin-bottom:25px;
                    }

                    .logo {
                        width:52px;
                        height:52px;
                        border-radius:16px;

                        display:flex;
                        align-items:center;
                        justify-content:center;

                        background:
                            linear-gradient(
                                135deg,
                                #2563eb,
                                #7c3aed
                            );

                        box-shadow:
                            0 10px 30px
                            rgba(59,130,246,.35);

                        font-size:24px;
                    }

                    h1 {
                        margin:0;
                        font-size:28px;
                    }

                    .subtitle {
                        color:#94a3b8;
                        margin-top:5px;
                    }

                    .grid {
                        display:grid;
                        grid-template-columns:
                            repeat(
                                auto-fit,
                                minmax(200px,1fr)
                            );

                        gap:14px;
                        margin-top:25px;
                    }

                    .item {
                        background:#0b1220;
                        border:1px solid #1e293b;
                        border-radius:16px;
                        padding:18px;
                    }

                    .label {
                        color:#64748b;
                        font-size:12px;
                        text-transform:uppercase;
                        letter-spacing:.08em;
                    }

                    .value {
                        margin-top:8px;
                        color:#f8fafc;
                        font-weight:600;
                    }

                    .online {
                        color:#4ade80;
                    }
                    </style>
                </head>

                <body>
                    <div class="card">

                        <div class="brand">
                            <div class="logo">✈</div>

                            <div>
                                <h1>Franciszw Viewer</h1>
                                <div class="subtitle">
                                    Private Messages Dashboard
                                </div>
                            </div>
                        </div>

                        <div class="grid">

                            <div class="item">
                                <div class="label">
                                    Connection
                                </div>
                                <div class="value online">
                                    ● Connected
                                </div>
                            </div>

                            <div class="item">
                                <div class="label">
                                    Scope
                                </div>
                                <div class="value">
                                    Private Messages Only
                                </div>
                            </div>

                            <div class="item">
                                <div class="label">
                                    Auto Download
                                </div>
                                <div class="value">
                                    Images / Videos ≤ 20 MB
                                </div>
                            </div>

                            <div class="item">
                                <div class="label">
                                    Workers
                                </div>
                                <div class="value">
                                    4 Concurrent Downloads
                                </div>
                            </div>

                        </div>

                    </div>
                </body>
                </html>
                """
            )

            return

        if path.startswith(
            "/message/"
        ):
            try:
                local_id = int(
                    path.split("/")[-1]
                )
            except ValueError:
                self.send_error(400)
                return

            row = get_message(
                local_id
            )

            if not row:
                self.send_error(404)
                return

            self.send_html(
                build_message_html(row)
            )

            return

        if path.startswith(
            "/media/"
        ):
            relative = urllib.parse.unquote(
                path[len("/media/"):]
            )

            relative = os.path.normpath(
                relative
            )

            media_root = os.path.abspath(
                MEDIA_DIR
            )

            file_path = os.path.abspath(
                os.path.join(
                    media_root,
                    relative
                )
            )

            try:
                if os.path.commonpath(
                    [
                        media_root,
                        file_path
                    ]
                ) != media_root:
                    self.send_error(403)
                    return

            except Exception:
                self.send_error(403)
                return

            if not os.path.isfile(
                file_path
            ):
                self.send_error(404)
                return

            self.send_file(
                file_path
            )

            return

        self.send_error(404)


def start_viewer():
    global viewer_server

    try:
        viewer_server = ThreadingHTTPServer(
            (
                HOST,
                PORT
            ),
            ViewerHandler
        )

        thread = threading.Thread(
            target=viewer_server.serve_forever,
            daemon=True
        )

        thread.start()

        return viewer_server

    except Exception as e:
        print(
            f"{error('VIEWER ERROR')}: {e}"
        )

        return None






def html_style():
    return """
    <style>
    * {
        box-sizing:border-box;
    }

    body {
        margin:0;
        min-height:100vh;

        background:
            radial-gradient(
                circle at top left,
                #172554 0,
                transparent 35%
            ),
            radial-gradient(
                circle at bottom right,
                #312e81 0,
                transparent 35%
            ),
            #05070d;

        color:#e5e7eb;

        font-family:
            Inter,
            ui-sans-serif,
            system-ui,
            -apple-system,
            BlinkMacSystemFont,
            "Segoe UI",
            sans-serif;

        max-width:1000px;
        margin:auto;
        padding:30px;
    }

    .card {
        background:
            linear-gradient(
                145deg,
                rgba(17,24,39,.97),
                rgba(9,13,23,.97)
            );

        border:1px solid #263244;
        border-radius:22px;
        padding:28px;

        box-shadow:
            0 25px 80px
            rgba(0,0,0,.45),
            0 0 50px
            rgba(59,130,246,.08);
    }

    .brand {
        display:flex;
        align-items:center;
        gap:14px;
        margin-bottom:24px;
    }

    .logo {
        width:48px;
        height:48px;
        border-radius:15px;

        display:flex;
        align-items:center;
        justify-content:center;

        background:
            linear-gradient(
                135deg,
                #2563eb,
                #7c3aed
            );

        box-shadow:
            0 10px 30px
            rgba(59,130,246,.30);
    }

    h1,
    h2 {
        margin:0;
    }

    h2 {
        font-size:25px;
    }

    .meta {
        color:#94a3b8;
        line-height:2;
    }

    .text {
        white-space:pre-wrap;
        word-break:break-word;
        line-height:1.8;
        margin-top:24px;

        background:#0b1220;
        border:1px solid #1e293b;
        border-radius:16px;
        padding:20px;
    }

    img,
    video {
        display:block;
        max-width:100%;
        border-radius:16px;
        margin-top:20px;

        box-shadow:
            0 15px 40px
            rgba(0,0,0,.35);
    }

    audio {
        width:100%;
        margin-top:20px;
    }

    iframe {
        width:100%;
        height:650px;
        border:1px solid #263244;
        border-radius:16px;
        margin-top:20px;
        background:#020617;
    }

    a {
        color:#60a5fa;
    }

    code {
        color:#6ee7b7;
        background:#07130f;
        padding:3px 7px;
        border-radius:6px;
    }

    .warning {
        color:#fbbf24;
        background:#171207;
        border:1px solid #4a3710;
        padding:16px;
        border-radius:14px;
        margin-top:20px;
    }

    .badge {
        display:inline-block;
        padding:5px 10px;
        border-radius:999px;
        background:#172554;
        color:#93c5fd;
        border:1px solid #1d4ed8;
        font-size:12px;
        font-weight:700;
    }
    </style>
    """


def build_message_html(row):
    (
        local_id,
        chat_id,
        telegram_id,
        sender_id,
        sender,
        text,
        outgoing,
        date,
        media_path,
        media_type,
        media_name
    ) = row

    content = ""

    if text:
        content += (
            '<div class="text">'
            + html.escape(text)
            + '</div>'
        )

    if media_path:
        relative = os.path.relpath(
            media_path,
            MEDIA_DIR
        )

        url = (
            "/media/"
            + urllib.parse.quote(
                relative.replace(
                    os.sep,
                    "/"
                )
            )
        )

        mime = (
            media_type
            or ""
        ).lower()

        if (
            mime == "image"
            or mime.startswith("image/")
        ):
            content += (
                f'<img src="{url}" '
                f'alt="Image">'
            )

        elif (
            mime == "video"
            or mime.startswith("video/")
        ):
            content += (
                f'<video controls '
                f'playsinline '
                f'src="{url}"></video>'
            )

        elif (
            mime == "audio"
            or mime.startswith("audio/")
        ):
            content += (
                f'<audio controls '
                f'src="{url}"></audio>'
            )

        elif (
            mime == "application/pdf"
            or media_type == "pdf"
        ):
            content += (
                f'<iframe '
                f'src="{url}"></iframe>'
            )

        else:
            content += (
                '<p>'
                f'<a href="{url}" download>'
                'Download File'
                '</a>'
                '</p>'
            )

    elif media_type:
        if media_type in AUTO_DOWNLOAD_MEDIA_TYPES:
            content += (
                '<div class="warning">'
                'This image/video is larger than 20 MB '
                'and was not auto-downloaded.'
                '<br>'
                'Use CLI command '
                '<code>d '
                + str(local_id)
                + '</code> '
                'to download it manually.'
                '</div>'
            )

        else:
            content += (
                '<div class="meta">'
                'This file has not been downloaded yet.'
                '<br>'
                'Use CLI command '
                '<code>d '
                + str(local_id)
                + '</code> '
                'to download it manually.'
                '</div>'
            )

    if not content:
        content = (
            '<div class="meta">'
            '[empty message]'
            '</div>'
        )

    return f"""
    <!doctype html>
    <html lang="en">
    <head>
        <meta charset="utf-8">

        <meta
            name="viewport"
            content="width=device-width,initial-scale=1"
        >

        <title>
            Franciszw Message {local_id}
        </title>

        {html_style()}
    </head>

    <body>
        <div class="card">

            <div class="brand">
                <div class="logo">✈</div>

                <div>
                    <h2>
                        Franciszw Viewer
                    </h2>

                    <div class="meta">
                        Private Message
                    </div>
                </div>
            </div>

            <div class="meta">
                <span class="badge">
                    LOCAL #{local_id}
                </span>

                <br>

                Telegram ID:
                <code>{telegram_id}</code>

                <br>

                Sender:
                {html.escape(
                    sender or "Unknown"
                )}

                <br>

                Iran Time:
                {html.escape(
                    iran_full_time(date)
                )}
            </div>

            {content}

        </div>
    </body>
    </html>
    """






async def open_message(local_id):
    row = get_message(local_id)

    if not row:
        print(
            "\n"
            + error("MESSAGE NOT FOUND")
        )

        await asyncio.sleep(1)
        await redraw()

        return

    url = (
        f"http://{HOST}:{PORT}"
        f"/message/{local_id}"
    )

    print()
    print(
        accent("FRANCISZW VIEWER")
        + ":"
    )
    print(
        cyan(url)
    )
    print()

    try:
        await asyncio.to_thread(
            webbrowser.open,
            url
        )
    except Exception:
        pass

    await asyncio.sleep(0.5)

    await redraw()






async def show_new_messages():
    clear()

    header(
        "TELEGRAM CLI  │  NEW PRIVATE CHATS"
    )

    print()

    rows = get_new_private_chats()

    if not rows:
        print(
            success("No new private messages.")
        )

        print(line())

        return

    print(
        info("Showing private messages only.")
    )

    print()

    for (
        local_id,
        chat_id,
        name,
        username,
        sender,
        date,
        unread_count
    ) in rows:

        display_name = (
            name
            or sender
            or str(chat_id)
        )

        username_text = ""

        if username:
            username_text = (
                " @" + username
            )

        count_text = ""

        if unread_count > 1:
            count_text = (
                f" [{unread_count} unread]"
            )

        print(
            color(
                f"[{local_id}]",
                C.BRIGHT_YELLOW,
                C.BOLD
            )
            + " "
            + white(
                cut(display_name, 35)
            )
            + cyan(username_text)
            + " "
            + muted(iran_time(date))
            + warning(count_text)
        )

    print()
    print(line())

    print(
        cyan("opennew <id>")
        + " = open chat and mark as read"
    )

    print(
        cyan("back")
        + " = return"
    )






async def open_new_message(
    local_id
):
    global current_chat

    row = get_message(local_id)

    if not row:
        print(
            "\n"
            + error("NEW MESSAGE NOT FOUND")
        )

        await asyncio.sleep(1)
        await redraw()

        return

    (
        lid,
        chat_id,
        telegram_id,
        sender_id,
        sender,
        text,
        outgoing,
        date,
        media_path,
        media_type,
        media_name
    ) = row

    chat = None

    for c in chat_cache:
        if c["id"] == chat_id:
            chat = c
            break

    if not chat:
        try:
            entity = await client.get_entity(
                chat_id
            )

            if not is_private_user(entity):
                print(
                    "\n"
                    + error(
                        "This message is not from a private chat."
                    )
                )

                await asyncio.sleep(1)
                await redraw()

                return

            chat = {
                "id": chat_id,
                "name": entity_name_sync(
                    entity
                ),
                "username": getattr(
                    entity,
                    "username",
                    None
                ),
                "chat_type": "private",
                "entity": entity
            }

            chat_cache.insert(
                0,
                chat
            )

            save_chat(
                chat["id"],
                chat["name"],
                chat["username"],
                "private"
            )

        except Exception as e:
            print(
                f"\n"
                f"{error('CHAT ERROR')}: {e}"
            )

            await asyncio.sleep(1)
            await redraw()

            return

    mark_chat_read(chat_id)

    current_chat = chat

    await redraw()

    create_background_task(
        background_sync_chat(
            chat_id,
            chat["entity"],
            FAST_SYNC_MESSAGES
        )
    )






async def send_message(text):
    if current_chat is None:
        return

    message = await client.send_message(
        current_chat["entity"],
        text
    )

    me = await client.get_me()

    save_message(
        current_chat["id"],
        message.id,
        me.id if me else None,
        "You",
        text,
        True,
        telegram_date(message)
    )

    mark_chat_read(
        current_chat["id"]
    )

    await redraw()






async def reply_message(
    local_id,
    text
):
    if current_chat is None:
        return

    row = get_message(local_id)

    if not row:
        print(
            "\n"
            + error("MESSAGE ID NOT FOUND")
        )

        await asyncio.sleep(1)
        await redraw()

        return

    (
        lid,
        chat_id,
        telegram_id,
        sender_id,
        sender,
        old_text,
        outgoing,
        date,
        media_path,
        media_type,
        media_name
    ) = row

    if chat_id != current_chat["id"]:
        print(
            "\n"
            + error(
                "This message does not belong to the current chat."
            )
        )

        await asyncio.sleep(1)
        await redraw()

        return

    try:
        message = await client.send_message(
            current_chat["entity"],
            text,
            reply_to=int(
                telegram_id
            )
        )

    except Exception as e:
        print(
            f"\n"
            f"{error('REPLY ERROR')}: {e}"
        )

        await asyncio.sleep(1)
        await redraw()

        return

    me = await client.get_me()

    save_message(
        current_chat["id"],
        message.id,
        me.id if me else None,
        "You",
        text,
        True,
        telegram_date(message)
    )

    mark_chat_read(
        current_chat["id"]
    )

    await redraw()






async def search_messages(query):
    if current_chat is None:
        return

    rows = db.execute(
        """
        SELECT
            local_id,
            sender_name,
            text,
            outgoing,
            date
        FROM messages
        WHERE chat_id=?
          AND text LIKE ?
        ORDER BY local_id DESC
        LIMIT 100
        """,
        (
            current_chat["id"],
            "%" + query + "%"
        )
    ).fetchall()

    clear()

    header(
        "SEARCH  │  " + query
    )

    print()

    if not rows:
        print(
            warning("No results.")
        )

    for (
        local_id,
        sender,
        text,
        outgoing,
        date
    ) in rows:

        who = (
            "You"
            if outgoing
            else sender
        )

        print(
            color(
                f"[{local_id}]",
                C.BRIGHT_YELLOW,
                C.BOLD
            )
            + " "
            + cyan(
                cut(who, 22)
            )
            + " "
            + muted(
                iran_time(date)
            )
        )

        print(
            "    "
            + white(
                cut(
                    text or "[media]",
                    width() - 7
                )
            )
        )

        print()

    print(line())






async def cat_message(local_id):
    row = get_message(local_id)

    clear()

    header(
        f"MESSAGE {local_id}"
    )

    print()

    if not row:
        print(
            error("Message not found.")
        )

        await asyncio.sleep(1)
        await redraw()

        return

    (
        lid,
        chat_id,
        telegram_id,
        sender_id,
        sender,
        text,
        outgoing,
        date,
        media_path,
        media_type,
        media_name
    ) = row

    print(
        muted("Local ID")
        + " : "
        + cyan(str(lid))
    )

    print(
        muted("Telegram")
        + " : "
        + cyan(str(telegram_id))
    )

    print(
        muted("Sender")
        + "   : "
        + white(str(sender))
    )

    print(
        muted("Time")
        + "     : "
        + cyan(iran_full_time(date))
    )

    print()

    if text:
        print(
            accent("TEXT:")
        )
        print(
            white(text)
        )
        print()

    if media_path:
        print(
            muted("MEDIA TYPE")
            + " : "
            + cyan(media_type or "file")
        )

        print(
            muted("MEDIA NAME")
            + " : "
            + cyan(media_name or "file")
        )

        print(
            muted("LOCAL PATH")
            + " : "
            + cyan(media_path)
        )

    elif media_type:
        print(
            muted("MEDIA TYPE")
            + " : "
            + cyan(media_type)
        )

        print(
            muted("LOCAL PATH")
            + " : "
            + warning("not downloaded")
        )

        print(
            muted("Manual download")
            + " : "
            + cyan(f"d {local_id}")
        )

    else:
        print(
            muted("[no media]")
        )

    print()

    url = (
        f"http://{HOST}:{PORT}"
        f"/message/{local_id}"
    )

    print(
        accent("FRANCISZW VIEWER")
        + ":"
    )

    print(
        cyan(url)
    )

    print()

    try:
        await asyncio.to_thread(
            webbrowser.open,
            url
        )
    except Exception:
        pass

    print(line())






async def d_command(local_id):
    clear()

    header(
        f"DOWNLOAD  │  MESSAGE {local_id}"
    )

    print()

    success_result = await download_message_media(
        local_id,
        automatic=False
    )

    print()

    if success_result:
        print(
            success(
                "File successfully saved inside telegram_media."
            )
        )

        print(
            cyan(f"cat {local_id}")
        )

    print()

    await asyncio.to_thread(
        input,
        "Press Enter..."
    )

    await redraw()






async def older_command(
    amount=OLDER_BATCH
):
    if current_chat is None:
        return

    amount = max(
        1,
        min(
            amount,
            MAX_MESSAGES
        )
    )

    clear()

    header(
        "OLDER MESSAGES"
    )

    print()

    print(
        cyan(
            f"Loading {amount} older messages..."
        )
    )

    try:
        count = await load_older_messages(
            current_chat,
            amount
        )

        print()

        if count:
            print(
                success(
                    f"{count} older messages added."
                )
            )

        else:
            print(
                warning(
                    "No older messages found."
                )
            )

    except Exception as e:
        print(
            f"\n"
            f"{error('OLDER ERROR')}: {e}"
        )

    print()
    print(line())






async def fish_shell():
    clear()

    header(
        "FISH SHELL  │  EXIT = RETURN"
    )

    print()

    fish = shutil.which("fish")

    if not fish:
        print(
            error("Fish is not installed.")
        )

        print()
        print(
            cyan("pkg install fish")
        )

        print()

        await asyncio.to_thread(
            input,
            "Press Enter..."
        )

        await redraw()

        return

    try:
        await asyncio.to_thread(
            subprocess.run,
            [fish]
        )

    except Exception as e:
        print(
            f"\n"
            f"{error('FISH ERROR')}: {e}"
        )

        await asyncio.to_thread(
            input,
            "Press Enter..."
        )

    await redraw()






async def shell_command(command):
    command = command.strip()

    if not command:
        return

    fish = shutil.which("fish")

    if not fish:
        print(
            "\n"
            + error("Fish is not installed.")
        )

        await asyncio.sleep(1)
        await redraw()

        return

    clear()

    header(
        "SHELL"
    )

    print()

    print(
        muted("$ ")
        + cyan(command)
    )

    print()

    try:
        result = await asyncio.to_thread(
            subprocess.run,
            [
                fish,
                "-c",
                command
            ],
            text=True
        )

        print()

        exit_color = (
            C.BRIGHT_GREEN
            if result.returncode == 0
            else C.BRIGHT_RED
        )

        print(
            color(
                f"[exit code: {result.returncode}]",
                exit_color,
                C.BOLD
            )
        )

        print(line())

        await asyncio.to_thread(
            input,
            "Press Enter..."
        )

    except Exception as e:
        print(
            f"\n"
            f"{error('SHELL ERROR')}: {e}"
        )

        await asyncio.to_thread(
            input,
            "Press Enter..."
        )

    await redraw()






async def show_help(
    in_chat=False
):
    clear()

    header(
        "TELEGRAM CLI  │  HELP"
    )

    print()

    if not in_chat:
        print(
            cyan("ls")
            + muted("                  List private chats")
        )

        print(
            cyan("cd <number/name>")
            + muted("      Open a private chat")
        )

        print(
            cyan("new")
            + muted("                 Show new messages")
        )

        print(
            cyan("opennew <id>")
            + muted("         Open new message")
        )

        print(
            cyan("fish")
            + muted("                Open Fish shell")
        )

        print(
            cyan("!<linux command>")
            + muted("       Run shell command")
        )

        print(
            cyan("exit")
            + muted("                Exit application")
        )

    else:
        print(
            cyan("pm <text>")
            + muted("             Send message")
        )

        print(
            cyan("reply <id> <text>")
            + muted("      Reply to message")
        )

        print(
            cyan("messages [number]")
            + muted("      Show messages")
        )

        print(
            cyan("messages all")
            + muted("          Sync all messages")
        )

        print(
            cyan("older [number]")
            + muted("          Load older messages")
        )

        print(
            cyan("d <id>")
            + muted("               Download media")
        )

        print(
            cyan("cat <id>")
            + muted("             Open message viewer")
        )

        print(
            cyan("search <text>")
            + muted("        Search messages")
        )

        print(
            cyan("new")
            + muted("                 Show new messages")
        )

        print(
            cyan("back")
            + muted("               Return to chat list")
        )

        print(
            cyan("fish")
            + muted("                Open Fish shell")
        )

        print(
            cyan("!<linux command>")
            + muted("       Run shell command")
        )

        print(
            cyan("exit")
            + muted("                Exit application")
        )

    print()

    print(
        muted("Auto download")
        + " : "
        + cyan("image/video <= 20 MB")
    )

    print(
        muted("Concurrent downloads")
        + " : "
        + cyan(str(AUTO_DOWNLOAD_CONCURRENCY))
    )

    print(
        muted("Manual download")
        + " : "
        + cyan("d <id>")
    )

    print()
    print(line())






async def command_loop():
    global current_chat
    global running

    while running:
        try:
            if current_chat is None:
                prompt = (
                    color("tg", C.BRIGHT_BLUE, C.BOLD)
                    + color("> ", C.BRIGHT_WHITE)
                )
            else:
                prompt = (
                    color(
                        "tg/",
                        C.BRIGHT_BLUE,
                        C.BOLD
                    )
                    + color(
                        cut(
                            current_chat["name"],
                            25
                        ),
                        C.BRIGHT_CYAN,
                        C.BOLD
                    )
                    + color("> ", C.BRIGHT_WHITE)
                )

            command = await asyncio.to_thread(
                input,
                prompt
            )

            command = command.strip()

            if not command:
                continue

            if command.startswith("!"):
                await shell_command(
                    command[1:]
                )

                continue

            if current_chat is None:
                parts = command.split(
                    " ",
                    1
                )

                cmd = parts[0].lower()

                if cmd == "exit":
                    running = False
                    break

                if cmd == "ls":
                    await load_chats()
                    await redraw()
                    continue

                if cmd == "new":
                    await show_new_messages()
                    continue

                if cmd == "opennew":
                    if len(parts) < 2:
                        print(
                            "\n"
                            + warning(
                                "Usage: opennew <id>"
                            )
                        )

                        await asyncio.sleep(1)
                        await redraw()

                        continue

                    try:
                        local_id = int(
                            parts[1]
                        )

                    except ValueError:
                        print(
                            "\n"
                            + error(
                                "ID must be a number."
                            )
                        )

                        await asyncio.sleep(1)
                        await redraw()

                        continue

                    await open_new_message(
                        local_id
                    )

                    continue

                if cmd == "fish":
                    await fish_shell()
                    continue

                if cmd == "help":
                    await show_help(False)
                    continue

                if cmd == "clear":
                    await redraw()
                    continue

                if cmd == "cd":
                    if len(parts) < 2:
                        print(
                            "\n"
                            + warning(
                                "Usage: cd <number/name>"
                            )
                        )

                        await asyncio.sleep(1)
                        await redraw()

                        continue

                    chat = find_chat(
                        parts[1]
                    )

                    if not chat:
                        print(
                            "\n"
                            + error(
                                "Private chat not found."
                            )
                        )

                        await asyncio.sleep(1)
                        await redraw()

                        continue

                    current_chat = chat

                    mark_chat_read(
                        current_chat["id"]
                    )

                    await redraw()

                    create_background_task(
                        background_sync_chat(
                            current_chat["id"],
                            current_chat["entity"],
                            FAST_SYNC_MESSAGES
                        )
                    )

                    continue

                await shell_command(
                    command
                )

                continue

            parts = command.split(
                " ",
                2
            )

            cmd = parts[0].lower()

            if cmd == "back":
                mark_chat_read(
                    current_chat["id"]
                )

                current_chat = None

                await load_chats()
                await redraw()

                continue

            if cmd == "exit":
                running = False
                break

            if cmd == "clear":
                await redraw()
                continue

            if cmd == "fish":
                await fish_shell()
                continue

            if cmd == "help":
                await show_help(True)
                continue

            if cmd == "new":
                await show_new_messages()
                continue

            if cmd == "opennew":
                if len(parts) < 2:
                    print(
                        "\n"
                        + warning(
                            "Usage: opennew <id>"
                        )
                    )

                    await asyncio.sleep(1)
                    await redraw()

                    continue

                try:
                    local_id = int(
                        parts[1]
                    )

                except ValueError:
                    print(
                        "\n"
                        + error(
                            "ID must be a number."
                        )
                    )

                    await asyncio.sleep(1)
                    await redraw()

                    continue

                await open_new_message(
                    local_id
                )

                continue

            if cmd == "d":
                if len(parts) < 2:
                    print(
                        "\n"
                        + warning(
                            "Usage: d <id>"
                        )
                    )

                    await asyncio.sleep(1)
                    await redraw()

                    continue

                try:
                    local_id = int(
                        parts[1]
                    )

                except ValueError:
                    print(
                        "\n"
                        + error(
                            "ID must be a number."
                        )
                    )

                    await asyncio.sleep(1)
                    await redraw()

                    continue

                await d_command(
                    local_id
                )

                continue

            if cmd == "pm":
                text = command[
                    len("pm"):
                ].strip()

                if not text:
                    print(
                        "\n"
                        + warning(
                            "Usage: pm <text>"
                        )
                    )

                    await asyncio.sleep(1)
                    await redraw()

                    continue

                try:
                    await send_message(
                        text
                    )

                except Exception as e:
                    print(
                        f"\n"
                        f"{error('SEND ERROR')}: {e}"
                    )

                    await asyncio.sleep(1)
                    await redraw()

                continue

            if cmd == "reply":
                if len(parts) < 3:
                    print(
                        "\n"
                        + warning(
                            "Usage: reply <id> <text>"
                        )
                    )

                    await asyncio.sleep(1)
                    await redraw()

                    continue

                try:
                    local_id = int(
                        parts[1]
                    )

                except ValueError:
                    print(
                        "\n"
                        + error(
                            "ID must be a number."
                        )
                    )

                    await asyncio.sleep(1)
                    await redraw()

                    continue

                await reply_message(
                    local_id,
                    parts[2]
                )

                continue

            if cmd == "older":
                amount = OLDER_BATCH

                if len(parts) >= 2:
                    try:
                        amount = int(
                            parts[1]
                        )

                    except ValueError:
                        amount = OLDER_BATCH

                await older_command(
                    amount
                )

                continue

            if cmd == "messages":
                if (
                    len(parts) >= 2
                    and parts[1].lower() == "all"
                ):
                    clear()

                    header(
                        "MESSAGES  │  ALL"
                    )

                    print()

                    await sync_all_messages(
                        current_chat
                    )

                    rows = get_messages(
                        current_chat["id"],
                        1000000
                    )

                else:
                    amount = DEFAULT_MESSAGES

                    if len(parts) >= 2:
                        try:
                            amount = int(
                                parts[1]
                            )

                        except ValueError:
                            amount = DEFAULT_MESSAGES

                    amount = max(
                        1,
                        min(
                            amount,
                            MAX_MESSAGES
                        )
                    )

                    rows = get_messages(
                        current_chat["id"],
                        amount
                    )

                clear()

                header(
                    f"MESSAGES  │  {len(rows)}"
                )

                print()

                if not rows:
                    print(
                        muted("No messages.")
                    )

                for row in rows:
                    (
                        local_id,
                        telegram_id,
                        sender_id,
                        sender,
                        text,
                        outgoing,
                        date,
                        media_path,
                        media_type,
                        media_name
                    ) = row

                    who = (
                        "You"
                        if outgoing
                        else sender
                    )

                    print(
                        color(
                            f"[{local_id}]",
                            C.BRIGHT_YELLOW,
                            C.BOLD
                        )
                        + " "
                        + cyan(
                            cut(who, 22)
                        )
                        + " "
                        + muted(
                            iran_time(date)
                        )
                    )

                    if text:
                        print(
                            "    "
                            + white(
                                text.replace(
                                    "\n",
                                    "\n    "
                                )
                            )
                        )

                    if media_path:
                        print(
                            "    "
                            + success(
                                f"[{media_type or 'media'}]"
                            )
                            + " "
                            + cyan(
                                media_name or ""
                            )
                        )

                    elif media_type:
                        print(
                            "    "
                            + warning(
                                f"[{media_type}]"
                            )
                            + " "
                            + muted(
                                "(not downloaded)"
                            )
                        )

                    print()

                print(line())

                continue

            if cmd == "cat":
                if len(parts) < 2:
                    print(
                        "\n"
                        + warning(
                            "Usage: cat <id>"
                        )
                    )

                    await asyncio.sleep(1)
                    await redraw()

                    continue

                try:
                    local_id = int(
                        parts[1]
                    )

                except ValueError:
                    print(
                        "\n"
                        + error(
                            "ID must be a number."
                        )
                    )

                    await asyncio.sleep(1)
                    await redraw()

                    continue

                await cat_message(
                    local_id
                )

                continue

            if cmd == "search":
                query = command[
                    len("search"):
                ].strip()

                if not query:
                    await redraw()
                    continue

                await search_messages(
                    query
                )

                continue

            await shell_command(
                command
            )

        except KeyboardInterrupt:
            print()
            await redraw()

        except EOFError:
            running = False
            break






async def login():
    await client.connect()

    if await client.is_user_authorized():
        return

    print()

    phone = input(
        color(
            "Phone number: ",
            C.BRIGHT_CYAN,
            C.BOLD
        )
    ).strip()

    await client.send_code_request(
        phone
    )

    code = input(
        color(
            "Telegram code: ",
            C.BRIGHT_CYAN,
            C.BOLD
        )
    ).strip()

    try:
        await client.sign_in(
            phone=phone,
            code=code
        )

    except SessionPasswordNeededError:
        password = input(
            color(
                "2FA password: ",
                C.BRIGHT_CYAN,
                C.BOLD
            )
        )

        await client.sign_in(
            password=password
        )






def cleanup_non_private_data():
    db.execute(
        """
        DELETE FROM new_messages
        WHERE local_id IN (
            SELECT m.local_id
            FROM messages m
            LEFT JOIN chats c
                ON c.chat_id=m.chat_id
            WHERE c.chat_type IS NULL
               OR c.chat_type != 'private'
        )
        """
    )

    db.execute(
        """
        DELETE FROM messages
        WHERE chat_id NOT IN (
            SELECT chat_id
            FROM chats
            WHERE chat_type='private'
        )
        """
    )

    db.execute(
        """
        DELETE FROM chats
        WHERE chat_type IS NULL
           OR chat_type != 'private'
        """
    )

    db.commit()






async def main():
    global running
    global startup_sync_started

    clear()

    header(
        "TELEGRAM CLI  │  PRIVATE ONLY  │  CONNECTING..."
    )

    print()

    try:
        await login()

    except Exception as e:
        clear()

        header(
            "TELEGRAM CLI  │  CONNECTION ERROR"
        )

        print()

        print(
            error(str(e))
        )

        print()

        return

    cleanup_non_private_data()

    try:
        await load_chats()

    except Exception as e:
        print(
            f"{error('CHAT LOADING ERROR')}: {e}"
        )

    load_new_messages()

    await start_download_workers()

    start_viewer()

    worker = asyncio.create_task(
        redraw_worker()
    )

    try:
        await redraw()

        if not startup_sync_started:
            startup_sync_started = True

            create_background_task(
                startup_unread_sync()
            )

        await command_loop()

    finally:
        running = False

        redraw_event.set()

        await stop_download_workers()

        for task in list(
            background_tasks
        ):
            task.cancel()

        if background_tasks:
            await asyncio.gather(
                *list(background_tasks),
                return_exceptions=True
            )

        worker.cancel()

        try:
            await worker
        except asyncio.CancelledError:
            pass

        try:
            await client.disconnect()
        except Exception:
            pass

        try:
            if viewer_server:
                viewer_server.shutdown()
        except Exception:
            pass

        try:
            db.close()
        except Exception:
            pass

        clear()

        print(
            success("Franciszw closed.")
        )






if __name__ == "__main__":
    try:
        asyncio.run(
            main()
        )

    except KeyboardInterrupt:
        print(
            "\n"
            + warning("Stopped.")
        )
