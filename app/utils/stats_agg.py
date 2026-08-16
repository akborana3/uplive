"""Incremental user-stats aggregation.

Instead of recomputing premium/DC/language breakdowns by scanning the full
``users`` dict on every /stats click (O(n), gets slow once a bot has tens of
thousands of users), we keep a running aggregate that is updated in O(1) at
the moment a user record is created or changed, and read in O(1) (well,
O(distinct dc/lang values), which is tiny — a few dozen at most).

Schema (stored at ``assistant_data["stats_agg"]``)::

    {
        "premium_count": int,
        "by_dc":   {"1": {"total": int, "premium": int}, "unknown": {...}},
        "by_lang": {"hi": {"total": int, "premium": int}, "unknown": {...}},
    }

"total" and "normal" user counts are *not* duplicated here — the caller
derives them from ``len(users)`` (O(1) in CPython) and
``total - premium_count``.
"""

from __future__ import annotations

from typing import Any

UNKNOWN = "unknown"


def empty_stats_agg() -> dict[str, Any]:
    return {"premium_count": 0, "by_dc": {}, "by_lang": {}}


def dc_bucket_key(dc_id: int | str | None) -> str:
    if dc_id is None:
        return UNKNOWN
    return str(dc_id)


def lang_bucket_key(lang_code: str | None) -> str:
    if not lang_code:
        return UNKNOWN
    return lang_code.strip().lower()


def _bump(bucket_map: dict[str, dict[str, int]], key: str, premium: bool, delta: int) -> None:
    bucket = bucket_map.setdefault(key, {"total": 0, "premium": 0})
    bucket["total"] += delta
    if premium:
        bucket["premium"] += delta
    if bucket["total"] <= 0:
        bucket_map.pop(key, None)


def agg_add(agg: dict[str, Any], dc_key: str, lang_key: str, premium: bool) -> None:
    if premium:
        agg["premium_count"] = agg.get("premium_count", 0) + 1
    _bump(agg.setdefault("by_dc", {}), dc_key, premium, +1)
    _bump(agg.setdefault("by_lang", {}), lang_key, premium, +1)


def agg_remove(agg: dict[str, Any], dc_key: str, lang_key: str, premium: bool) -> None:
    if premium:
        agg["premium_count"] = max(0, agg.get("premium_count", 0) - 1)
    _bump(agg.setdefault("by_dc", {}), dc_key, premium, -1)
    _bump(agg.setdefault("by_lang", {}), lang_key, premium, -1)


def agg_update(
    agg: dict[str, Any],
    old_dc: str,
    old_lang: str,
    old_premium: bool,
    new_dc: str,
    new_lang: str,
    new_premium: bool,
) -> None:
    """Move a user's contribution from the old bucket combo to the new one."""
    if (old_dc, old_lang, old_premium) == (new_dc, new_lang, new_premium):
        return
    agg_remove(agg, old_dc, old_lang, old_premium)
    agg_add(agg, new_dc, new_lang, new_premium)


def rebuild_stats_agg(users: dict[str, Any]) -> dict[str, Any]:
    """Full O(n) rebuild — only used for one-off migration/repair, never on
    the hot /stats read path."""
    agg = empty_stats_agg()
    for rec in users.values():
        dc_key = rec.get("dc_id") or UNKNOWN
        lang_key = rec.get("lang_code") or UNKNOWN
        agg_add(agg, dc_key, lang_key, bool(rec.get("premium")))
    return agg
