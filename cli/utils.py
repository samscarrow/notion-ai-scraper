import re

_UUID_DASHLESS = re.compile(r'^[0-9a-f]{32}$')
_UUID_DASHED = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')


def _to_dashed_uuid(s: str) -> str:
    """Convert a 32-char hex string to dashed UUID format. Pass through if already dashed."""
    s = s.strip().lower()
    if _UUID_DASHED.match(s):
        return s
    if _UUID_DASHLESS.match(s):
        return f"{s[:8]}-{s[8:12]}-{s[12:16]}-{s[16:20]}-{s[20:]}"
    raise ValueError(f"Not a valid UUID: {s}")


def _name_to_key(name: str) -> str:
    """Convert an agent display name to a registry key (snake_case)."""
    return re.sub(r'[^a-z0-9]+', '_', name.lower()).strip('_')
