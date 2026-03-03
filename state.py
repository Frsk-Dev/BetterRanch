"""Shared mutable state accessible from both the bot thread and the Flask thread."""
import asyncio

bot_instance = None
bot_loop: asyncio.AbstractEventLoop | None = None

# guild_id -> {"scanning": bool, "processed": int, "added": int}
scan_status: dict[str, dict] = {}
