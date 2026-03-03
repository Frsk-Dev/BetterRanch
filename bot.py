import logging

import discord
from discord.ext import commands

import database as db
import parser as event_parser

logger = logging.getLogger("betterranch")


class BetterRanchBot(commands.Bot):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        super().__init__(command_prefix="!br", intents=intents)

    async def setup_hook(self) -> None:
        db.init_db()

        from commands import setup_commands
        setup_commands(self)

        await self.tree.sync()
        logger.info("Slash commands synced to Discord.")

    async def on_ready(self) -> None:
        logger.info(f"BetterRanch online as {self.user} (ID: {self.user.id})")
        logger.info(f"Serving {len(self.guilds)} guild(s).")

        await self.change_presence(
            activity=discord.Activity(type=discord.ActivityType.watching, name="the ranch")
        )

    async def on_message(self, message: discord.Message) -> None:
        # Ignore our own messages and DMs.
        if message.author == self.user:
            return

        if not message.embeds or not message.guild:
            return

        guild_id = str(message.guild.id)
        config = db.get_guild_config(guild_id)

        # Silently ignore guilds that haven't run /setup yet.
        if not config:
            return

        ranch_channel_id = int(config["ranch_channel_id"]) if config["ranch_channel_id"] else None
        camp_channel_id  = int(config["camp_channel_id"])  if config["camp_channel_id"]  else None
        channel_id = message.channel.id

        for embed in message.embeds:
            if not embed.title or not embed.description:
                continue

            # Ranch channel — title is the event type, player is in the description.
            if ranch_channel_id and channel_id == ranch_channel_id:
                event = event_parser.parse_embed(embed.title, embed.description)
                if event:
                    stored = db.insert_event(
                        event_type=event.event_type,
                        player_name=event.player_name,
                        value=event.value,
                        quantity=event.quantity,
                        message_id=str(message.id),
                        guild_id=guild_id,
                    )
                    if stored:
                        logger.info(
                            f"RANCH  {event.event_type:<14} | {event.player_name:<16} | "
                            f"value={event.value}  qty={event.quantity}  guild={guild_id}"
                        )
                else:
                    logger.debug(f"RANCH  unrecognised embed: '{embed.title}'")

            # Camp channel — title is the player name, detail is in the description.
            elif camp_channel_id and channel_id == camp_channel_id:
                event = event_parser.parse_camp_embed(embed.title, embed.description)
                if event:
                    stored = db.insert_event(
                        event_type=event.event_type,
                        player_name=event.player_name,
                        value=event.value,
                        quantity=event.quantity,
                        message_id=str(message.id),
                        guild_id=guild_id,
                    )
                    if stored:
                        logger.info(
                            f"CAMP   {event.event_type:<14} | {event.player_name:<16} | "
                            f"value={event.value}  guild={guild_id}"
                        )
                else:
                    logger.debug(f"CAMP   unrecognised embed: '{embed.title}'")

        await self.process_commands(message)
