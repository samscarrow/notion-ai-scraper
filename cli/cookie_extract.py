"""
cookie_extract.py — Extract Notion session token from Firefox's SQLite cookie store.

Inspired by jamalex/notion-py (MIT) — cookie auth pattern.
https://github.com/jamalex/notion-py
"""

import glob
import os
import shutil
import sqlite3
import tempfile


def get_firefox_cookies_db() -> str:
    """Return path to the most recently modified Firefox cookies.sqlite."""
    pattern = os.path.expanduser("~/.mozilla/firefox/*/cookies.sqlite")
    candidates = glob.glob(pattern)
    if not candidates:
        raise FileNotFoundError(
            "No Firefox cookies.sqlite found. "
            "Ensure Firefox is installed and you have logged into notion.so."
        )
    return max(candidates, key=os.path.getmtime)


def get_token_v2() -> str:
    """
    Extract token_v2 from Firefox's Notion cookies.

    Copies the DB first because Firefox holds a write lock while running.
    Raises ValueError if the cookie is not found (session expired or not logged in).
    """
    db_path = get_firefox_cookies_db()

    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        shutil.copy2(db_path, tmp_path)
        conn = sqlite3.connect(tmp_path)
        try:
            row = conn.execute(
                "SELECT value FROM moz_cookies "
                "WHERE host LIKE '%notion.so' AND name='token_v2' "
                "ORDER BY lastAccessed DESC LIMIT 1"
            ).fetchone()
        finally:
            conn.close()
    finally:
        os.unlink(tmp_path)

    if not row:
        raise ValueError(
            "token_v2 cookie not found for notion.so. "
            "Open Firefox, log into Notion, and try again."
        )

    return row[0]


def get_user_id() -> str | None:
    """
    Extract the Notion user ID from the notion_user_id cookie.
    Returns None if not found (non-fatal — used as x-notion-active-user-header).
    """
    db_path = get_firefox_cookies_db()

    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        shutil.copy2(db_path, tmp_path)
        conn = sqlite3.connect(tmp_path)
        try:
            row = conn.execute(
                "SELECT value FROM moz_cookies "
                "WHERE host LIKE '%notion.so' AND name='notion_user_id' "
                "ORDER BY lastAccessed DESC LIMIT 1"
            ).fetchone()
        finally:
            conn.close()
    finally:
        os.unlink(tmp_path)

    return row[0] if row else None
