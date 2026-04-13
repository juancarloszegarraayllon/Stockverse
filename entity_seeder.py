"""Entity seeder — extracts teams from live-score feeds and upserts
them into the entities + entity_aliases tables.

Runs periodically alongside the score flush loop.  Each feed cycle
may surface new teams (new league added, new season starts, etc.),
so the seeder is designed to be idempotent: it never creates
duplicates, only fills in gaps.

Canonical name = ESPN displayName (most human-readable).  When a
team appears from SofaScore or SportsDB first, their display name
becomes the canonical until ESPN provides one.
"""
import logging
import unicodedata
from typing import Any, Dict, List, Set, Tuple

log = logging.getLogger("entity_seeder")


def _normalize(s: Any) -> str:
    """Lowercase + strip accents for alias matching."""
    if not s:
        return ""
    s = unicodedata.normalize("NFD", str(s).lower())
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return s.strip()


def extract_teams(games: List[Dict[str, Any]], source: str) -> List[Dict[str, Any]]:
    """Extract unique teams from a list of game dicts.

    Returns a list of dicts, each with:
      canonical_name, entity_type, sport, league, aliases [(alias, source, normalized)]
    """
    # Key: (normalized_display, sport) to deduplicate
    seen: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for g in games:
        sport = g.get("sport", "")
        league = g.get("league", "")
        if not sport:
            continue

        for side in ("home", "away"):
            display = g.get(f"{side}_display", "")
            if not display or len(display) < 2:
                continue

            norm = _normalize(display)
            key = (norm, sport)

            if key in seen:
                # Add any new aliases from this game
                team = seen[key]
                # Prefer longer league names
                if league and len(league) > len(team.get("league") or ""):
                    team["league"] = league
            else:
                team = {
                    "canonical_name": display,
                    "entity_type": "team",
                    "sport": sport,
                    "league": league,
                    "aliases": [],
                    "_norm": norm,
                }
                seen[key] = team

            # Collect aliases: display name + abbreviation
            aliases_set: Set[str] = set()
            if display:
                aliases_set.add(display)
            abbr = g.get(f"{side}_abbr", "")
            if abbr and len(abbr) >= 2:
                aliases_set.add(abbr)

            for alias in aliases_set:
                normalized = _normalize(alias)
                if not normalized:
                    continue
                # Check if this exact (alias, source) is already tracked
                existing = {(a["alias"], a["source"]) for a in team["aliases"]}
                if (alias, source) not in existing:
                    team["aliases"].append({
                        "alias": alias,
                        "source": source,
                        "normalized": normalized,
                    })

    return list(seen.values())
