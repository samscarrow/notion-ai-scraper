"""
cookie_extract.py — Extract Claude.ai cookies from Firefox's SQLite cookie store.

Adapted from notion-forge/cli/cookie_extract.py (MIT).
"""

import glob
import os
import shutil
import sqlite3
import tempfile


def _query_claude_cookies(db_path: str) -> list[tuple[str, str]]:
    """Return all Claude.ai cookies from a copied Firefox SQLite DB."""
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        shutil.copy2(db_path, tmp_path)
        conn = sqlite3.connect(tmp_path)
        try:
            return conn.execute(
                "SELECT name, value FROM moz_cookies "
                "WHERE host LIKE '%claude.ai' "
                "ORDER BY lastAccessed DESC"
            ).fetchall()
        finally:
            conn.close()
    finally:
        os.unlink(tmp_path)


def _get_firefox_cookies_db() -> str:
    """Return the best Firefox cookies.sqlite for Claude auth."""
    pattern = os.path.expanduser("~/.mozilla/firefox/*/cookies.sqlite")
    candidates = glob.glob(pattern)
    if not candidates:
        raise FileNotFoundError(
            "No Firefox cookies.sqlite found. "
            "Ensure Firefox is installed and you have logged into claude.ai."
        )
    candidates = sorted(candidates, key=os.path.getmtime, reverse=True)
    fallback = candidates[0]
    for db_path in candidates:
        try:
            rows = _query_claude_cookies(db_path)
        except (OSError, sqlite3.Error):
            continue
        cookies = {name: value for name, value in rows}
        if cookies.get("sessionKey"):
            return db_path
    return fallback


def get_all_cookies() -> dict[str, str]:
    """Extract all Claude.ai cookies from Firefox as a dict."""
    db_path = _get_firefox_cookies_db()
    rows = _query_claude_cookies(db_path)
    return {name: value for name, value in rows}


def get_cookie_header() -> str:
    """Build a full Cookie header string for Claude.ai requests."""
    cookies = get_all_cookies()
    if "sessionKey" not in cookies:
        raise ValueError(
            "sessionKey cookie not found for claude.ai. "
            "Open Firefox, log into Claude, and try again."
        )
    return "; ".join(f"{k}={v}" for k, v in cookies.items())
