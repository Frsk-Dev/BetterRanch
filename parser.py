import logging
import re
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger("betterranch")


@dataclass
class RanchEvent:
    event_type: str
    player_name: str
    value: float
    quantity: int = 1


# Each entry: embed title -> (event_type, compiled regex)
# Groups differ per pattern — see parse_embed() for extraction logic.
#
# Pattern notes (eggs/milk/deposit/withdrawal):
#   @.+?(\d{15,})  — "@Display Name 101079..." form: non-greedy match handles
#                    multi-word display names; captures the snowflake ID (15+ digits)
#   <@(\d+)>       — "<@123456789>" form: captures user ID as group 2
#   \s+\S.*?\s+    — game player name skipped (handles single or multi-word names)
#
# Cattle patterns use a different embed format with no Discord mention.
_PATTERNS: dict[str, tuple[str, re.Pattern]] = {
    "Eggs Added": (
        "eggs",
        re.compile(
            r"(?:@.+?(\d{15,})|<@(\d+)>)\s+\S.*?\s+Added Eggs to ranch id \d+\s*:\s*(\d+)",
            re.DOTALL | re.IGNORECASE,
        ),
    ),
    "Milk Added": (
        "milk",
        re.compile(
            r"(?:@.+?(\d{15,})|<@(\d+)>)\s+\S.*?\s+Added Milk to ranch id \d+\s*:\s*(\d+)",
            re.DOTALL | re.IGNORECASE,
        ),
    ),
    "Cash Withdrawal": (
        "withdrawal",
        re.compile(
            r"(?:@.+?(\d{15,})|<@(\d+)>)\s+\S.*?\s+Withdrawal of ([\d.]+)\s*\$",
            re.DOTALL | re.IGNORECASE,
        ),
    ),
    "Cash Deposit": (
        "deposit",
        re.compile(
            r"(?:@.+?(\d{15,})|<@(\d+)>)\s+\S.*?\s+Deposit of ([\d.]+)\s*\$",
            re.DOTALL | re.IGNORECASE,
        ),
    ),
    "Bought Cattle": (
        "cattle_buy",
        re.compile(
            r"Player \*\*(.+?)\*\* bought \*\*(\d+)\*\* \*\*\S+\*\* cattle for \*\*([\d.]+)\$?\*\*",
            re.DOTALL | re.IGNORECASE,
        ),
    ),
    "Cattle Sale": (
        "cattle_sell",
        re.compile(
            r"Player (.+?) sold (\d+) \S+ for ([\d.]+)\$",
            re.DOTALL | re.IGNORECASE,
        ),
    ),
}


# Camp channel patterns — player name comes from embed title, not description.
_CAMP_MATERIALS  = re.compile(r"Materials added:\s*([\d.]+)", re.IGNORECASE)
_CAMP_SUPPLIES   = re.compile(r"Delivered Supplies:\s*([\d.]+)", re.IGNORECASE)
_CAMP_STOCK_SALE = re.compile(r"Made a Sale Of (\d+) Of Stock For \$([\d.]+)", re.IGNORECASE)


def parse_camp_embed(title: str, description: str) -> Optional[RanchEvent]:
    """Parse a camp channel embed. Player name is the embed title."""
    if not title or not description:
        return None

    player_name = title.strip()

    match = _CAMP_MATERIALS.search(description)
    if match:
        return RanchEvent("materials", player_name, float(match.group(1)))

    match = _CAMP_SUPPLIES.search(description)
    if match:
        return RanchEvent("supplies", player_name, float(match.group(1)))

    match = _CAMP_STOCK_SALE.search(description)
    if match:
        # value = revenue, quantity = stock sold
        return RanchEvent("stock_sale", player_name, float(match.group(2)), int(match.group(1)))

    return None


def parse_embed(title: str, description: str) -> Optional[RanchEvent]:
    """Parse a Discord embed into a RanchEvent, or return None if unrecognised."""
    entry = _PATTERNS.get(title.strip())
    if not entry:
        return None

    event_type, pattern = entry
    match = pattern.search(description)
    if not match:
        logger.warning(f"PARSE  title matched '{title}' but regex failed — description: {description!r}")
        return None

    groups = match.groups()

    if event_type in ("eggs", "milk"):
        # groups: (id_from_@format, id_from_<@>_format, amount)
        user_id = groups[0] or groups[1]
        return RanchEvent(event_type, f"<@{user_id}>", float(groups[2]))

    if event_type in ("withdrawal", "deposit"):
        # groups: (id_from_@format, id_from_<@>_format, amount)
        user_id = groups[0] or groups[1]
        return RanchEvent(event_type, f"<@{user_id}>", float(groups[2]))

    if event_type in ("cattle_buy", "cattle_sell"):
        # groups: (player_name, quantity, total_value)
        return RanchEvent(event_type, groups[0], float(groups[2]), int(groups[1]))

    return None
