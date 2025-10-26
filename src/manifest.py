#!/usr/bin/env python3
"""
Parquet day manifest utilities.
Writes and reads a manifest mapping CAST(day AS DATE) to parquet glob patterns.
"""
from pathlib import Path
import json
import urllib.parse
from typing import Dict, List, Optional


def write_manifest(lake_dir: str) -> str:
    root = Path(lake_dir) / "events"
    days: Dict[str, List[str]] = {}
    if not root.exists():
        return ""
    for child in sorted(root.iterdir()):
        if not child.is_dir():
            continue
        name = child.name  # e.g., day=2024-01-01%2008%3A00%3A00%2B00
        if not name.startswith("day="):
            continue
        enc = name[len("day=") :]
        decoded = urllib.parse.unquote(enc)
        # Expect like 2024-01-01 08:00:00+00
        iso_day = decoded[:10]
        pat = str(child / "**/*.parquet")
        days.setdefault(iso_day, []).append(pat)
    manifest = {"days": days}
    out = Path(lake_dir) / "manifest.json"
    out.write_text(json.dumps(manifest, indent=2))
    return str(out)


def load_manifest(lake_dir: str) -> Dict[str, List[str]]:
    p = Path(lake_dir) / "manifest.json"
    if not p.exists():
        return {}
    obj = json.loads(p.read_text())
    return obj.get("days", {})


def patterns_for_where(days_map: Dict[str, List[str]], where: List[dict]) -> Optional[List[str]]:
    if not where:
        return None
    # Extract day filters
    day_eq = [w for w in where if w.get("col") == "day" and w.get("op") == "eq"]
    day_between = [w for w in where if w.get("col") == "day" and w.get("op") == "between"]
    if day_eq:
        d = str(day_eq[0].get("val"))
        return days_map.get(d, [])
    if day_between:
        lo, hi = day_between[0].get("val")
        lo = str(lo)
        hi = str(hi)
        out: List[str] = []
        for day, pats in days_map.items():
            if lo <= day <= hi:
                out.extend(pats)
        return sorted(set(out))
    return None
