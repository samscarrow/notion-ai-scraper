from __future__ import annotations
import json
import threading
import time
from typing import Any
import config
import notion_api
from utils import _to_dashed_uuid

# Use config instance
CFG = config.get_config()

def _get_notion_api_client() -> notion_api.NotionAPIClient:
    return notion_api.NotionAPIClient(CFG.notion_token)

# Public API schema cache: db_id -> {prop_name: prop_type}
_db_schema_cache: dict[str, dict[str, str]] = {}
_db_schema_cache_time: dict[str, float] = {}
_db_schema_lock = threading.Lock()
_DB_SCHEMA_TTL = 300  # seconds — mirrors _AUTH_TTL pattern

# System audit properties suppressed from default output
_SYSTEM_PROP_TYPES = {"created_time", "last_edited_time", "created_by", "last_edited_by"}

# Relation title cache: page_id -> title string
_relation_title_cache: dict[str, str] = {}
_relation_title_lock = threading.Lock()


def _aggregate_pages(pages: list, schema: dict, show_props: list | None = None) -> str:
    """Compute per-column statistics from a list of Notion pages.

    Returns a markdown stats block. show_props filters which columns to include.
    """
    from collections import Counter

    n = len(pages)

    # Collect raw values per property
    # prop_data: {name: list of raw prop dicts}
    prop_data: dict[str, list] = {}
    for page in pages:
        for name, val in page.get("properties", {}).items():
            if show_props and name not in show_props:
                continue
            prop_data.setdefault(name, []).append(val)

    # Determine column order
    if show_props:
        col_order = [c for c in show_props if c in prop_data]
    else:
        # Sort, suppressing system props
        system_names = {nm for nm, pt in schema.items() if pt in _SYSTEM_PROP_TYPES}
        col_order = sorted(k for k in prop_data if k not in system_names)

    blocks: list[str] = [f"*{n} pages scanned*\n"]

    for name in col_order:
        vals = prop_data.get(name, [])
        ptype = schema.get(name, "")
        total = len(vals)

        if ptype in ("select", "status"):
            counter: Counter = Counter()
            for v in vals:
                raw = v.get(ptype) or {}
                label = raw.get("name") if isinstance(raw, dict) else None
                counter[label or "(empty)"] += 1
            non_empty = total - counter.get("(empty)", 0)
            lines = [f"## {name} ({ptype}) — {total} pages"]
            lines.append("| Value | Count |")
            lines.append("| --- | --- |")
            for label, cnt in counter.most_common():
                lines.append(f"| {label} | {cnt} |")
            blocks.append("\n".join(lines))

        elif ptype == "multi_select":
            counter = Counter()
            empty = 0
            for v in vals:
                opts = v.get("multi_select", [])
                if not opts:
                    empty += 1
                for opt in opts:
                    counter[opt.get("name", "?")] += 1
            lines = [f"## {name} (multi_select) — {total} pages, {empty} empty"]
            lines.append("| Value | Count |")
            lines.append("| --- | --- |")
            for label, cnt in counter.most_common(20):
                lines.append(f"| {label} | {cnt} |")
            blocks.append("\n".join(lines))

        elif ptype == "number":
            nums = [v.get("number") for v in vals if v.get("number") is not None]
            non_empty = len(nums)
            if non_empty == 0:
                blocks.append(f"## {name} (number) — 0 of {total} non-empty")
            else:
                mn = min(nums)
                mx = max(nums)
                mean = sum(nums) / non_empty
                blocks.append(
                    f"## {name} (number) — {non_empty} of {total} non-empty\n"
                    f"min: {mn} / mean: {mean:.4g} / max: {mx}"
                )

        elif ptype == "checkbox":
            true_count = sum(1 for v in vals if v.get("checkbox"))
            false_count = total - true_count
            blocks.append(
                f"## {name} (checkbox) — {total} pages\n"
                f"true: {true_count} / false: {false_count}"
            )

        elif ptype == "date":
            dates = []
            for v in vals:
                d = v.get("date")
                if d and d.get("start"):
                    dates.append(d["start"])
            non_empty = len(dates)
            if non_empty == 0:
                blocks.append(f"## {name} (date) — 0 of {total} non-empty")
            else:
                blocks.append(
                    f"## {name} (date) — {non_empty} of {total} non-empty\n"
                    f"earliest: {min(dates)} / latest: {max(dates)}"
                )

        elif ptype in ("relation", "people"):
            non_empty = sum(1 for v in vals if v.get(ptype))
            blocks.append(f"## {name} ({ptype}) — {non_empty} of {total} non-empty")

        elif ptype in (
            "title", "rich_text", "url", "email", "phone_number",
            "created_by", "last_edited_by", "created_time", "last_edited_time",
            "files", "formula", "rollup", "unique_id", "verification",
        ):
            non_empty = 0
            for v in vals:
                formatted = _format_property_value(v)
                if formatted.strip():
                    non_empty += 1
            blocks.append(f"## {name} ({ptype}) — {non_empty} of {total} non-empty")

        # skip unknown types silently

    return "\n\n".join(blocks)


def _get_db_schema(db_id: str) -> dict[str, str]:
    """Fetch and cache a database's property name -> type map via the public API."""
    now = time.monotonic()
    with _db_schema_lock:
        if db_id in _db_schema_cache and (now - _db_schema_cache_time.get(db_id, 0)) < _DB_SCHEMA_TTL:
            return _db_schema_cache[db_id]
    client = _get_notion_api_client()
    db = client.retrieve_database(db_id)
    schema = {
        name: prop_def["type"]
        for name, prop_def in db.get("properties", {}).items()
    }
    with _db_schema_lock:
        _db_schema_cache[db_id] = schema
        _db_schema_cache_time[db_id] = time.monotonic()
    return schema


# Filter type aliases: maps wrong-but-common filter keys to the correct one.
# e.g. caller uses "select" but the property is actually "status" type.
_FILTER_TYPE_ALIASES: dict[str, set[str]] = {
    "select": {"status", "select"},
    "status": {"status", "select"},
    "multi_select": {"multi_select"},
    "checkbox": {"checkbox"},
    "number": {"number"},
    "date": {"date", "created_time", "last_edited_time"},
    "rich_text": {"rich_text", "title", "url", "email", "phone_number"},
    "title": {"rich_text", "title"},
}


def _fix_filter(
    filter_obj: dict,
    schema: dict[str, str],
    name_map: dict[str, str] | None = None,
) -> dict:
    """Auto-correct a single filter condition to match the actual property type.

    Handles the common mistake of using {"property": "Status", "select": {...}}
    when the property is actually a "status" type (needs {"status": {...}}).
    Compound filters (and/or) are recursed into.
    name_map: optional {lower_name: canonical_name} for case-insensitive matching.
    """
    # Compound filter
    if "and" in filter_obj:
        filter_obj["and"] = [_fix_filter(f, schema, name_map) for f in filter_obj["and"]]
        return filter_obj
    if "or" in filter_obj:
        filter_obj["or"] = [_fix_filter(f, schema, name_map) for f in filter_obj["or"]]
        return filter_obj

    prop_name = filter_obj.get("property")
    # Case-insensitive name correction
    if prop_name and name_map:
        canonical = name_map.get(prop_name.lower())
        if canonical:
            filter_obj["property"] = canonical
            prop_name = canonical

    if not prop_name or prop_name not in schema:
        return filter_obj  # Can't fix what we don't know

    actual_type = schema[prop_name]

    # Find which key the caller used as the filter type
    # (everything except "property" is the filter type key)
    caller_keys = [k for k in filter_obj if k != "property"]
    if len(caller_keys) != 1:
        return filter_obj  # Ambiguous or empty, don't touch

    caller_type = caller_keys[0]
    if caller_type == actual_type:
        return filter_obj  # Already correct

    # Check if the caller's type is a known alias for the actual type
    aliases = _FILTER_TYPE_ALIASES.get(caller_type, set())
    if actual_type in aliases:
        # Swap the key: {"select": {"equals": "X"}} -> {"status": {"equals": "X"}}
        filter_obj[actual_type] = filter_obj.pop(caller_type)

    return filter_obj


def _resolve_relation_titles(
    page_ids: list[str],
    client: "notion_api.NotionAPIClient",
) -> dict[str, str]:
    """Batch-resolve page UUIDs to their title strings. Results are cached."""
    result: dict[str, str] = {}
    to_fetch: list[str] = []
    with _relation_title_lock:
        for pid in page_ids:
            if pid in _relation_title_cache:
                result[pid] = _relation_title_cache[pid]
            else:
                to_fetch.append(pid)

    if not to_fetch:
        return result

    import concurrent.futures

    def _fetch_title(pid: str) -> tuple[str, str]:
        try:
            page = client.retrieve_page(pid)
            props = page.get("properties", {})
            title = ""
            for prop_val in props.values():
                if prop_val.get("type") == "title":
                    title = "".join(t.get("plain_text", "") for t in prop_val.get("title", []))
                    break
            title = title.strip() or pid
        except Exception:
            title = pid  # degrade gracefully on access error
        return pid, title

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        for pid, title in executor.map(_fetch_title, to_fetch):
            result[pid] = title
            with _relation_title_lock:
                _relation_title_cache[pid] = title

    return result


def _format_property_value(prop: dict, client: "notion_api.NotionAPIClient | None" = None) -> str:
    """Extract a readable string from a Notion page property value.

    client: optional API client used to resolve relation page IDs to titles.
    """
    ptype = prop.get("type", "")
    if ptype == "title":
        return "".join(t.get("plain_text", "") for t in prop.get("title", []))
    if ptype == "rich_text":
        return "".join(t.get("plain_text", "") for t in prop.get("rich_text", []))
    if ptype == "select":
        sel = prop.get("select")
        return sel["name"] if sel else ""
    if ptype == "multi_select":
        return ", ".join(o["name"] for o in prop.get("multi_select", []))
    if ptype == "status":
        st = prop.get("status")
        return st["name"] if st else ""
    if ptype == "checkbox":
        return "Yes" if prop.get("checkbox") else "No"
    if ptype == "number":
        val = prop.get("number")
        return str(val) if val is not None else ""
    if ptype == "url":
        return prop.get("url") or ""
    if ptype == "email":
        return prop.get("email") or ""
    if ptype == "phone_number":
        return prop.get("phone_number") or ""
    if ptype == "date":
        d = prop.get("date")
        if not d:
            return ""
        start = d.get("start", "")
        end = d.get("end")
        return f"{start} → {end}" if end else start
    if ptype == "relation":
        rels = prop.get("relation", [])
        if not rels:
            return ""
        ids = [r.get("id", "") for r in rels if r.get("id")]
        if not ids:
            return ""
        if client:
            cap = 10
            fetch_ids = ids[:cap]
            titles = _resolve_relation_titles(fetch_ids, client)
            parts = [titles.get(i, i) for i in fetch_ids]
            if len(ids) > cap:
                parts.append(f"(+{len(ids) - cap} more)")
            return ", ".join(parts)
        return ", ".join(ids)
    if ptype == "people":
        people = prop.get("people", [])
        if not people:
            return ""
        names = []
        for p in people:
            names.append(p.get("name", p.get("id", "?")))
        return ", ".join(names)
    if ptype in ("created_time", "last_edited_time"):
        return prop.get(ptype, "")
    if ptype in ("created_by", "last_edited_by"):
        person = prop.get(ptype, {})
        return person.get("name", person.get("id", ""))
    if ptype == "files":
        files = prop.get("files", [])
        if not files:
            return ""
        return ", ".join(f.get("name", f.get("url", "?")) for f in files if isinstance(f, dict))
    if ptype == "formula":
        f = prop.get("formula", {})
        return str(f.get(f.get("type", ""), ""))
    if ptype == "rollup":
        r = prop.get("rollup", {})
        return str(r.get(r.get("type", ""), ""))
    if ptype == "unique_id":
        uid = prop.get("unique_id", {})
        prefix = uid.get("prefix", "")
        number = uid.get("number", "")
        return f"{prefix}-{number}" if prefix else str(number)
    if ptype == "verification":
        v = prop.get("verification", {})
        return v.get("state", "") if v else ""
    return str(prop)


def describe_database(database_id: str) -> str:
    """
    Show a Notion database's schema: property names, types, and select/status options.

    Call this BEFORE query_database if you don't know the exact property names and
    types. The output tells you which filter type to use for each property.

    database_id: The database page UUID (dashed or dashless).
    """
    db_id = _to_dashed_uuid(database_id)
    client = _get_notion_api_client()
    db = client.retrieve_database(db_id)

    # Extract title
    title_parts = db.get("title", [])
    title = "".join(t.get("plain_text", "") for t in title_parts) or "(untitled)"

    lines = [f"# {title}", f"ID: {db_id}", ""]
    lines.append("| Property | Type | Filter key | Options |")
    lines.append("| --- | --- | --- | --- |")

    for name, prop_def in sorted(db.get("properties", {}).items()):
        ptype = prop_def["type"]
        # The filter key is the same as the type for most properties
        filter_key = ptype
        options = ""

        if ptype == "select":
            opts = prop_def.get("select", {}).get("options", [])
            options = ", ".join(o["name"] for o in opts[:10])
            if len(opts) > 10:
                options += f" (+{len(opts) - 10} more)"
        elif ptype == "status":
            groups = prop_def.get("status", {}).get("groups", [])
            all_opts = []
            for g in groups:
                all_opts.extend(o["name"] for o in g.get("options", []))
            options = ", ".join(all_opts[:10])
        elif ptype == "multi_select":
            opts = prop_def.get("multi_select", {}).get("options", [])
            options = ", ".join(o["name"] for o in opts[:10])
            if len(opts) > 10:
                options += f" (+{len(opts) - 10} more)"

        lines.append(f"| {name} | {ptype} | `{filter_key}` | {options} |")

    # Cache the schema with timestamp
    schema = {name: prop_def["type"] for name, prop_def in db.get("properties", {}).items()}
    with _db_schema_lock:
        _db_schema_cache[db_id] = schema
        _db_schema_cache_time[db_id] = time.monotonic()

    return "\n".join(lines)


def query_database(
    database_id: str,
    filter: str = "",
    sorts: str = "",
    properties: str = "",
    limit: int = 50,
    cursor: str = "",
    aggregate: bool = False,
    max_tokens: int = 0,
    sample: bool = False,
) -> str:
    """
    Query a Notion database by ID and return rows as a formatted table.

    Auto-corrects common filter mistakes (e.g. using "select" filter for a "status"
    property). Property name matching is case-insensitive. System audit columns
    (created_by, created_time, etc.) are hidden by default — list them explicitly
    in 'properties' to show them.

    database_id: The database page UUID (dashed or dashless), NOT the collection://
        data source ID.
    filter: Optional JSON string with a Notion API filter object. The filter type
        key MUST match the property's actual type. Use describe_database to check.
        Example: '{"property": "Status", "status": {"equals": "Active"}}'
    sorts: Optional JSON string with a Notion API sorts array. Property names must
        match the database schema (case-insensitive).
        Example: '[{"property": "Name", "direction": "ascending"}]'
    properties: Comma-separated property names to display. If empty, shows all
        non-system properties. Case-insensitive.
    limit: Max rows to return (default 50, max 100). Ignored when aggregate=True.
    cursor: Pagination cursor from a previous query_database call with more results.
        Ignored when aggregate=True.
    aggregate: If True, fetch up to 200 pages and return per-column statistics
        (value frequencies, numeric ranges, non-empty counts) instead of a row
        table. ~90% token savings vs loading all rows. Use for "what values exist?"
        and "how is data distributed?" questions.
    max_tokens: If > 0, cap output size to approximately this many tokens
        (1 token ≈ 4 chars). Rows are dropped from the end if over budget, with a
        note appended. Has no effect when aggregate=True.
    sample: If True and rows > 10, show first half + last half of results with an
        omission separator, revealing data range without dense middle rows. Has no
        effect when aggregate=True.
    """
    client = _get_notion_api_client()
    db_id = _to_dashed_uuid(database_id)

    filter_obj = json.loads(filter) if filter else None
    sorts_obj = json.loads(sorts) if sorts else None

    # Fetch schema for validation, auto-correction, and case-insensitive matching
    try:
        schema = _get_db_schema(db_id)
    except Exception:
        schema = {}

    # Build case-insensitive name map: lower -> canonical
    prop_name_map = {n.lower(): n for n in schema} if schema else {}

    # Auto-correct filter types and property names against actual schema
    if filter_obj and schema:
        filter_obj = _fix_filter(filter_obj, schema, prop_name_map)

    # Validate and case-correct sort property names
    if sorts_obj and schema:
        for sort_item in sorts_obj:
            prop = sort_item.get("property", "")
            if prop:
                canonical = prop_name_map.get(prop.lower())
                if canonical:
                    sort_item["property"] = canonical
                elif prop not in schema:
                    available = ", ".join(sorted(schema.keys()))
                    return (
                        f"Sort property \"{prop}\" not found in database.\n"
                        f"Available properties: {available}"
                    )

    # Normalize show_props to canonical casing
    show_props: list[str] | None = None
    if properties:
        raw = [p.strip() for p in properties.split(",") if p.strip()]
        show_props = [prop_name_map.get(p.lower(), p) for p in raw]

    # --- Aggregate mode: fetch up to 200 pages, return column statistics ---
    if aggregate:
        _AGG_CAP = 200
        pages = client.query_all(db_id, filter_payload=filter_obj, page_size=100)
        if len(pages) > _AGG_CAP:
            pages = pages[:_AGG_CAP]
        if not pages:
            return "No results."
        return _aggregate_pages(pages, schema, show_props)

    # --- Standard row-table mode ---
    payload: dict = {"page_size": min(limit, 100)}
    if filter_obj:
        payload["filter"] = filter_obj
    if sorts_obj:
        payload["sorts"] = sorts_obj
    if cursor:
        payload["start_cursor"] = cursor.strip()

    result = client._request("POST", f"databases/{db_id}/query", payload)
    pages = result.get("results", [])

    if not pages:
        return "No results."

    # Determine which system columns to suppress when no explicit properties given
    system_names: set[str] = set()
    if not show_props and schema:
        system_names = {name for name, ptype in schema.items() if ptype in _SYSTEM_PROP_TYPES}

    # Build table rows — always include page URL for actionability
    rows = []
    for page in pages:
        props = page.get("properties", {})
        row = {"_url": page.get("url", page.get("id", ""))}
        for name, val in props.items():
            if show_props and name not in show_props:
                continue
            if not show_props and name in system_names:
                continue
            row[name] = _format_property_value(val, client=client)
        rows.append(row)

    if not rows:
        return f"{len(pages)} pages returned but no displayable properties."

    # Get column order: always lead with _url, then requested or all props
    if show_props:
        columns = ["_url"] + [c for c in show_props if c in rows[0]]
    else:
        all_cols = [c for c in rows[0].keys() if c != "_url"]
        columns = ["_url"] + sorted(all_cols)

    # --- Semantic sampling: head + tail with omission row ---
    if sample and len(rows) > 10:
        import math
        head_n = math.ceil(len(rows) / 2)
        tail_n = len(rows) - head_n
        omitted = len(rows) - head_n - tail_n
        omission_row = {c: "..." for c in columns}
        omission_row[columns[0]] = f"*({omitted} rows omitted)*"
        rows = rows[:head_n] + [omission_row] + rows[len(rows) - tail_n:]

    # Format as markdown table
    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join("---" for _ in columns) + " |"
    lines = [header, separator]
    max_cell = 300
    for row in rows:
        cells = []
        for c in columns:
            val = row.get(c, "").replace("|", "\\|").replace("\n", " ")
            if len(val) > max_cell:
                val = val[: max_cell - 1] + "…"
            cells.append(val)
        lines.append("| " + " | ".join(cells) + " |")

    total_note = ""
    if result.get("has_more") and result.get("next_cursor"):
        next_cur = result["next_cursor"]
        total_note = (
            f"\n\n_Showing {len(pages)} rows. More results exist._\n"
            f'_Next page: call query\\_database with cursor="{next_cur}"_'
        )

    output = "\n".join(lines) + total_note

    # --- Token budget enforcement ---
    if max_tokens > 0:
        budget_chars = max_tokens * 4
        if len(output) > budget_chars:
            # Binary-search: drop rows from the end until we fit
            lo, hi = 0, len(rows)
            while lo < hi:
                mid = (lo + hi + 1) // 2
                candidate_lines = [header, separator]
                for row in rows[:mid]:
                    cells = []
                    for c in columns:
                        val = row.get(c, "").replace("|", "\\|").replace("\n", " ")
                        if len(val) > max_cell:
                            val = val[: max_cell - 1] + "…"
                        cells.append(val)
                    candidate_lines.append("| " + " | ".join(cells) + " |")
                if len("\n".join(candidate_lines)) <= budget_chars:
                    lo = mid
                else:
                    hi = mid - 1
            shown = lo
            truncated_lines = [header, separator]
            for row in rows[:shown]:
                cells = []
                for c in columns:
                    val = row.get(c, "").replace("|", "\\|").replace("\n", " ")
                    if len(val) > max_cell:
                        val = val[: max_cell - 1] + "…"
                    cells.append(val)
                truncated_lines.append("| " + " | ".join(cells) + " |")
            budget_note = (
                f"\n\n> Showing {shown} of {len(rows)} rows "
                f"(token budget: {max_tokens} tokens — "
                f"use aggregate=True or narrow with properties= for full coverage)"
            )
            output = "\n".join(truncated_lines) + budget_note

    return output


def count_database(
    database_id: str,
    filter: str = "",
    exact: bool = False,
) -> str:
    """
    Count rows in a Notion database, optionally matching a filter.

    Use this for "how many?" and "does X exist?" questions instead of fetching
    rows with query_database.

    database_id: The database page UUID (dashed or dashless).
    filter: Optional JSON filter (same format as query_database).
    exact: If False (default), returns a fast existence check using 1 API call —
        answers "0 rows", "1 row", or "at least 2 rows". If True, pages through
        the full database to return an exact count (may make multiple API calls).
    """
    client = _get_notion_api_client()
    db_id = _to_dashed_uuid(database_id)

    filter_obj = json.loads(filter) if filter else None

    # Apply same schema validation and auto-correction as query_database
    try:
        schema = _get_db_schema(db_id)
        prop_name_map = {n.lower(): n for n in schema}
    except Exception:
        schema = {}
        prop_name_map = {}

    if filter_obj and schema:
        filter_obj = _fix_filter(filter_obj, schema, prop_name_map)

    if not exact:
        # Fast path: 1 API call, answers existence only
        result = client.query_database(db_id, filter_payload=filter_obj, page_size=1)
        count = len(result.get("results", []))
        if count == 0:
            return "0 rows match."
        if result.get("has_more"):
            return "At least 2 rows match. Use exact=True for a precise count."
        return "1 row matches."

    # Exact count: page through the full result set
    pages = client.query_all(db_id, filter_payload=filter_obj)
    n = len(pages)
    return f"{n} {'row' if n == 1 else 'rows'} match."


