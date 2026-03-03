"""Reusable channel scanning logic shared by the /scan command and the web settings save."""
import logging

import database as db
import parser as event_parser
import state

logger = logging.getLogger("betterranch")


async def scan_channel(
    bot,
    channel_id: int,
    guild_id: str,
    is_camp: bool,
    limit: int | None = None,
) -> tuple[int, int]:
    """Scan a Discord channel's history and import all parseable events.

    Returns (messages_processed, events_added).
    """
    channel = bot.get_channel(channel_id)
    if channel is None:
        try:
            channel = await bot.fetch_channel(channel_id)
        except Exception as exc:
            logger.warning(f"SCAN  could not fetch channel {channel_id}: {exc}")
            return 0, 0

    label = "CAMP" if is_camp else "RANCH"
    logger.info(f"SCAN  started — channel={channel_id} ({label})  guild={guild_id}")

    # Initialise / merge status for this guild (two channels may scan concurrently).
    existing = state.scan_status.get(guild_id, {"processed": 0, "added": 0})
    state.scan_status[guild_id] = {
        "scanning": True,
        "processed": existing["processed"],
        "added": existing["added"],
    }

    processed = 0
    added = 0

    async for message in channel.history(limit=limit, oldest_first=False):
        for embed in message.embeds:
            if not embed.title or not embed.description:
                continue

            if is_camp:
                event = event_parser.parse_camp_embed(embed.title, embed.description)
            else:
                event = event_parser.parse_embed(embed.title, embed.description)

            if event:
                was_new = db.insert_event(
                    event_type=event.event_type,
                    player_name=event.player_name,
                    value=event.value,
                    quantity=event.quantity,
                    message_id=str(message.id),
                    guild_id=guild_id,
                )
                if was_new:
                    added += 1

        processed += 1
        if processed % 50 == 0:
            state.scan_status[guild_id] = {
                "scanning": True,
                "processed": existing["processed"] + processed,
                "added": existing["added"] + added,
            }
            logger.info(f"SCAN  progress — channel={channel_id}  scanned={processed}  imported={added}")

    logger.info(f"SCAN  complete — channel={channel_id}  scanned={processed}  imported={added}")
    state.scan_status[guild_id] = {
        "scanning": False,
        "processed": existing["processed"] + processed,
        "added": existing["added"] + added,
    }
    return processed, added
