import logging
import os

import discord
from discord.ext import commands

import database as db
import parser as event_parser

logger = logging.getLogger("betterranch")


class BetterRanchBot(commands.Bot):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        intents.message_content = True
        super().__init__(command_prefix="!br", intents=intents)

        ranch_str = os.getenv("RANCH_CHANNEL_ID", "")
        camp_str  = os.getenv("CAMP_CHANNEL_ID", "")
        self.ranch_channel_id: int | None = int(ranch_str) if ranch_str.isdigit() else None
        self.camp_channel_id:  int | None = int(camp_str)  if camp_str.isdigit()  else None

    async def setup_hook(self) -> None:
        db.init_db()

        from commands import setup_commands
        setup_commands(self)

        await self.tree.sync()
        logger.info("Slash commands synced to Discord.")

    async def on_ready(self) -> None:
        logger.info(f"BetterRanch online as {self.user} (ID: {self.user.id})")
        logger.info(f"Ranch channel : {self.ranch_channel_id or 'not set'}")
        logger.info(f"Camp channel  : {self.camp_channel_id  or 'not set'}")

        await self.change_presence(
            activity=discord.Activity(type=discord.ActivityType.watching, name="the ranch")
        )

    async def on_message(self, message: discord.Message) -> None:
        # Ignore our own messages.
        if message.author == self.user:
            return

        if not message.embeds:
            return

        channel_id = message.channel.id

        for embed in message.embeds:
            if not embed.title or not embed.description:
                continue

            # Ranch channel — title is the event type, player is in the description.
            if self.ranch_channel_id and channel_id == self.ranch_channel_id:
                event = event_parser.parse_embed(embed.title, embed.description)
                if event:
                    stored = db.insert_event(
                        event_type=event.event_type,
                        player_name=event.player_name,
                        value=event.value,
                        quantity=event.quantity,
                        message_id=str(message.id),
                    )
                    if stored:
                        logger.info(
                            f"RANCH  {event.event_type:<14} | {event.player_name:<16} | "
                            f"value={event.value}  qty={event.quantity}"
                        )
                else:
                    logger.debug(f"RANCH  unrecognised embed: '{embed.title}'")

            # Camp channel — title is the player name, detail is in the description.
            elif self.camp_channel_id and channel_id == self.camp_channel_id:
                event = event_parser.parse_camp_embed(embed.title, embed.description)
                if event:
                    stored = db.insert_event(
                        event_type=event.event_type,
                        player_name=event.player_name,
                        value=event.value,
                        quantity=event.quantity,
                        message_id=str(message.id),
                    )
                    if stored:
                        logger.info(
                            f"CAMP   {event.event_type:<14} | {event.player_name:<16} | "
                            f"value={event.value}"
                        )
                else:
                    logger.debug(f"CAMP   unrecognised embed: '{embed.title}'")

        await self.process_commands(message)
