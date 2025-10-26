#!/usr/bin/env python3
"""
Planner/result cache utilities for JSON queries.
Provides canonicalization and fingerprinting for stable plan/result reuse.
"""
import hashlib
import json
from typing import Dict, Any

CANON_KEYS = ("select", "from", "where", "group_by", "order_by", "limit")
OP_ORDER = {"eq": 0, "neq": 1, "lt": 2, "lte": 3, "gt": 4, "gte": 5, "between": 6, "in": 7}


def canon(q: Dict[str, Any]) -> Dict[str, Any]:
    c = {k: q.get(k) for k in CANON_KEYS}
    # Normalize where list deterministically
    wh = c.get("where") or []
    c["where"] = sorted(
        wh,
        key=lambda w: (
            w.get("col"),
            OP_ORDER.get(w.get("op"), 99),
            json.dumps(w.get("val"), sort_keys=True, separators=(",", ":")),
        ),
    )
    # Normalize select order (stable order of dict aggregates)
    sel = []
    for it in c.get("select") or []:
        if isinstance(it, dict):
            k, v = next(iter(it.items()))
            sel.append({k.upper(): v})
        else:
            sel.append(it)
    c["select"] = sel
    # Normalize group_by/order_by ordering
    if c.get("group_by"):
        c["group_by"] = sorted(c["group_by"])
    if c.get("order_by"):
        c["order_by"] = sorted(c["order_by"], key=lambda o: (o.get("col"), (o.get("dir") or "asc").lower()))
    return c


def fingerprint(q: Dict[str, Any]) -> str:
    c = canon(q)
    blob = json.dumps(c, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode()).hexdigest()
