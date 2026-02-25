"""
Quick sanity-check for multi-guild database logic.
Run with: python test_db.py
Uses a temporary DB file — does not touch betterranch.db.
"""

import os
import sqlite3

# Point the module at a throwaway DB before importing.
os.environ["_TEST_DB"] = "1"
import database as db

db.DB_PATH = "test_betterranch.db"

# Clean slate each run.
if os.path.exists(db.DB_PATH):
    os.remove(db.DB_PATH)

GUILD_A = "111111111111111111"
GUILD_B = "222222222222222222"

PASS = "\033[92mPASS\033[0m"
FAIL = "\033[91mFAIL\033[0m"

failures = 0


def check(label: str, condition: bool) -> None:
    global failures
    status = PASS if condition else FAIL
    print(f"  [{status}] {label}")
    if not condition:
        failures += 1


print("\n=== Init DB ===")
db.init_db()
check("DB file created", os.path.exists(db.DB_PATH))

# Run init_db again to confirm migration path doesn't explode.
db.init_db()
check("init_db idempotent (no error on second call)", True)

print("\n=== Guild config ===")
check("Guild A config is None before setup", db.get_guild_config(GUILD_A) is None)

db.upsert_guild_config(GUILD_A, "100", "200")
cfg = db.get_guild_config(GUILD_A)
check("Guild A ranch channel saved", cfg["ranch_channel_id"] == "100")
check("Guild A camp channel saved",  cfg["camp_channel_id"]  == "200")

db.upsert_guild_config(GUILD_A, "101", "201")
cfg = db.get_guild_config(GUILD_A)
check("Guild A config updates (upsert)", cfg["ranch_channel_id"] == "101")

check("Guild B config still None", db.get_guild_config(GUILD_B) is None)

print("\n=== Event insertion ===")
r1 = db.insert_event("eggs", "Alice", 50, guild_id=GUILD_A, message_id="msg1")
r2 = db.insert_event("eggs", "Alice", 30, guild_id=GUILD_A, message_id="msg2")
r3 = db.insert_event("eggs", "Bob",   40, guild_id=GUILD_B, message_id="msg3")
r4 = db.insert_event("milk", "Alice", 10, guild_id=GUILD_A, message_id="msg4")
check("Insert event A1 succeeds", r1 is True)
check("Insert event A2 succeeds", r2 is True)
check("Insert event B1 succeeds", r3 is True)
check("Duplicate message_id rejected", db.insert_event("eggs", "Alice", 99, guild_id=GUILD_A, message_id="msg1") is False)

print("\n=== Guild isolation ===")
rows_a = db.get_collection_stats("eggs", "alltime", None, guild_id=GUILD_A)
rows_b = db.get_collection_stats("eggs", "alltime", None, guild_id=GUILD_B)
check("Guild A sees 1 egg player (Alice)", len(rows_a) == 1 and rows_a[0]["player_name"] == "Alice")
check("Guild A total eggs = 80",           rows_a[0]["total"] == 80)
check("Guild B sees 1 egg player (Bob)",   len(rows_b) == 1 and rows_b[0]["player_name"] == "Bob")
check("Guild B total eggs = 40",           rows_b[0]["total"] == 40)

print("\n=== Player autocomplete isolation ===")
names_a = db.get_player_names(["eggs", "milk"], guild_id=GUILD_A)
names_b = db.get_player_names(["eggs", "milk"], guild_id=GUILD_B)
check("Guild A players: Alice only",  names_a == ["Alice"])
check("Guild B players: Bob only",    names_b == ["Bob"])

print("\n=== Guild config preserve on partial update ===")
db.upsert_guild_config(GUILD_A, "999", None)
cfg = db.get_guild_config(GUILD_A)
check("Ranch channel updated to 999",   cfg["ranch_channel_id"] == "999")
check("Camp channel now None after wipe", cfg["camp_channel_id"] is None)

print("\n=== Cleanup ===")
os.remove(db.DB_PATH)
check("Temp DB removed", not os.path.exists(db.DB_PATH))

print(f"\n{'All tests passed!' if failures == 0 else f'{failures} test(s) FAILED'}\n")
exit(0 if failures == 0 else 1)
