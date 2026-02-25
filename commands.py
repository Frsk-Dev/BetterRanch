import logging
from typing import Literal, Optional

import discord
from discord import app_commands

import database as db
import parser as event_parser

logger = logging.getLogger("betterranch")


def _log_cmd(interaction: discord.Interaction, **kwargs) -> None:
    """Log a slash command invocation with its parameters."""
    params = "  ".join(f"{k}={v}" for k, v in kwargs.items() if v is not None)
    logger.info(f"CMD  /{interaction.command.name:<18} | {interaction.user} | guild={interaction.guild_id} | {params}")


async def _require_ranch_channel(interaction: discord.Interaction) -> bool:
    """Return True if the interaction is in the configured ranch channel, otherwise send an error."""
    config = db.get_guild_config(str(interaction.guild_id))
    if not config or not config["ranch_channel_id"]:
        await interaction.response.send_message(
            "This server hasn't been set up yet. An admin needs to run `/setup` first.",
            ephemeral=True,
        )
        return False

    ranch_id = int(config["ranch_channel_id"])
    if interaction.channel.id != ranch_id:
        ch = interaction.guild.get_channel(ranch_id)
        mention = ch.mention if ch else f"<#{ranch_id}>"
        await interaction.response.send_message(
            f"Ranch commands can only be used in {mention}.", ephemeral=True
        )
        return False

    return True


async def _require_camp_channel(interaction: discord.Interaction) -> bool:
    """Return True if the interaction is in the configured camp channel, otherwise send an error."""
    config = db.get_guild_config(str(interaction.guild_id))
    if not config or not config["camp_channel_id"]:
        await interaction.response.send_message(
            "This server hasn't been set up yet. An admin needs to run `/setup` first.",
            ephemeral=True,
        )
        return False

    camp_id = int(config["camp_channel_id"])
    if interaction.channel.id != camp_id:
        ch = interaction.guild.get_channel(camp_id)
        mention = ch.mention if ch else f"<#{camp_id}>"
        await interaction.response.send_message(
            f"Camp commands can only be used in {mention}.", ephemeral=True
        )
        return False

    return True


_PERIOD_LABELS = {
    "day": "Last 24 Hours",
    "week": "Last 7 Days",
    "alltime": "All Time",
}

_RANCH_TYPES = ["eggs", "milk", "deposit", "withdrawal", "cattle_buy", "cattle_sell"]
_CAMP_TYPES  = ["materials", "supplies", "stock_sale"]

PeriodChoice = Literal["day", "week", "alltime"]


def _label(period: str) -> str:
    return _PERIOD_LABELS.get(period, "All Time")


def _val(row, key: str) -> float:
    return row[key] or 0.0 if row else 0.0


def _sign(n: float) -> str:
    return "+" if n >= 0 else ""


# ---------------------------------------------------------------------------
# Autocomplete helpers — defined at module level so decorators can reference them
# ---------------------------------------------------------------------------

async def _ranch_player_ac(
    interaction: discord.Interaction,
    current: str,
) -> list[app_commands.Choice[str]]:
    names = db.get_player_names(_RANCH_TYPES, guild_id=str(interaction.guild_id))
    return [
        app_commands.Choice(name=name, value=name)
        for name in names
        if current.lower() in name.lower()
    ][:25]


async def _camp_player_ac(
    interaction: discord.Interaction,
    current: str,
) -> list[app_commands.Choice[str]]:
    names = db.get_player_names(_CAMP_TYPES, guild_id=str(interaction.guild_id))
    return [
        app_commands.Choice(name=name, value=name)
        for name in names
        if current.lower() in name.lower()
    ][:25]


# ---------------------------------------------------------------------------
# Register all slash commands onto the bot tree
# ---------------------------------------------------------------------------

def setup_commands(bot: discord.ext.commands.Bot) -> None:

    # ------------------------------------------------------------------
    # /setup  (admin only — configure ranch and camp channels)
    # ------------------------------------------------------------------
    @bot.tree.command(name="setup", description="Configure ranch and camp channels for this server (requires Manage Server)")
    @app_commands.describe(
        ranch_channel="The channel where ranch events are posted",
        camp_channel="The channel where camp events are posted",
    )
    async def setup_cmd(
        interaction: discord.Interaction,
        ranch_channel: Optional[discord.TextChannel] = None,
        camp_channel: Optional[discord.TextChannel] = None,
    ) -> None:
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message(
                "You need **Manage Server** permission to run `/setup`.", ephemeral=True
            )
            return

        if not ranch_channel and not camp_channel:
            await interaction.response.send_message(
                "Provide at least one channel to configure.", ephemeral=True
            )
            return

        # Preserve existing values for any channel not explicitly provided.
        existing = db.get_guild_config(str(interaction.guild_id))
        ranch_id = str(ranch_channel.id) if ranch_channel else (existing["ranch_channel_id"] if existing else None)
        camp_id  = str(camp_channel.id)  if camp_channel  else (existing["camp_channel_id"]  if existing else None)

        db.upsert_guild_config(str(interaction.guild_id), ranch_id, camp_id)
        migrated = db.migrate_null_events(str(interaction.guild_id))
        logger.info(f"CMD  /setup             | guild={interaction.guild_id} | ranch={ranch_id}  camp={camp_id}  migrated={migrated}")

        lines = ["**BetterRanch configured!**"]
        if ranch_id:
            lines.append(f"Ranch channel → <#{ranch_id}>")
        if camp_id:
            lines.append(f"Camp channel  → <#{camp_id}>")
        if migrated:
            lines.append(f"Migrated **{migrated}** existing events to this server.")

        await interaction.response.send_message("\n".join(lines), ephemeral=True)

    # ------------------------------------------------------------------
    # /eggs
    # ------------------------------------------------------------------
    @bot.tree.command(name="eggs", description="Show egg collection stats")
    @app_commands.describe(
        period="Time period — day / week / alltime (default: alltime)",
        player="Filter to a specific player (optional)",
    )
    @app_commands.autocomplete(player=_ranch_player_ac)
    async def eggs_cmd(
        interaction: discord.Interaction,
        period: PeriodChoice = "alltime",
        player: Optional[str] = None,
    ) -> None:
        if not await _require_ranch_channel(interaction):
            return
        _log_cmd(interaction, period=period, player=player)
        guild_id = str(interaction.guild_id)
        rows = db.get_collection_stats("eggs", period, player, guild_id)
        logger.info(f"CMD  /eggs             → {len(rows)} player(s) returned")
        embed = discord.Embed(
            title=f"🥚  Egg Collection — {_label(period)}",
            color=0xF5C518,
        )

        if not rows:
            embed.description = "No egg data found for this period."
        elif player:
            r = rows[0]
            avg = r["total"] / r["collections"]
            embed.description = f"**Player:** {r['player_name']}"
            embed.add_field(name="Total Eggs", value=f"{int(r['total']):,}", inline=True)
            embed.add_field(name="Collections", value=str(r["collections"]),  inline=True)
            embed.add_field(name="Avg / Run",   value=f"{avg:.1f}",           inline=True)
        else:
            total_eggs = sum(r["total"] for r in rows)
            total_runs = sum(r["collections"] for r in rows)
            lines = [
                f"`{i}.` **{r['player_name']}** — {int(r['total']):,} eggs ({r['collections']} runs)"
                for i, r in enumerate(rows, 1)
            ]
            embed.description = "\n".join(lines)
            embed.set_footer(text=f"Total: {int(total_eggs):,} eggs across {total_runs} runs")

        await interaction.response.send_message(embed=embed)

    # ------------------------------------------------------------------
    # /milk
    # ------------------------------------------------------------------
    @bot.tree.command(name="milk", description="Show milk collection stats")
    @app_commands.describe(
        period="Time period — day / week / alltime (default: alltime)",
        player="Filter to a specific player (optional)",
    )
    @app_commands.autocomplete(player=_ranch_player_ac)
    async def milk_cmd(
        interaction: discord.Interaction,
        period: PeriodChoice = "alltime",
        player: Optional[str] = None,
    ) -> None:
        if not await _require_ranch_channel(interaction):
            return
        _log_cmd(interaction, period=period, player=player)
        guild_id = str(interaction.guild_id)
        rows = db.get_collection_stats("milk", period, player, guild_id)
        logger.info(f"CMD  /milk             → {len(rows)} player(s) returned")
        embed = discord.Embed(
            title=f"🥛  Milk Collection — {_label(period)}",
            color=0xDDEEFF,
        )

        if not rows:
            embed.description = "No milk data found for this period."
        elif player:
            r = rows[0]
            avg = r["total"] / r["collections"]
            embed.description = f"**Player:** {r['player_name']}"
            embed.add_field(name="Total Milk", value=f"{int(r['total']):,}", inline=True)
            embed.add_field(name="Collections", value=str(r["collections"]),  inline=True)
            embed.add_field(name="Avg / Run",   value=f"{avg:.1f}",           inline=True)
        else:
            total_milk = sum(r["total"] for r in rows)
            total_runs = sum(r["collections"] for r in rows)
            lines = [
                f"`{i}.` **{r['player_name']}** — {int(r['total']):,} milk ({r['collections']} runs)"
                for i, r in enumerate(rows, 1)
            ]
            embed.description = "\n".join(lines)
            embed.set_footer(text=f"Total: {int(total_milk):,} milk across {total_runs} runs")

        await interaction.response.send_message(embed=embed)

    # ------------------------------------------------------------------
    # /ledger
    # ------------------------------------------------------------------
    @bot.tree.command(name="ledger", description="Show cash deposit / withdrawal stats")
    @app_commands.describe(
        period="Time period — day / week / alltime (default: alltime)",
        player="Filter to a specific player (optional)",
    )
    @app_commands.autocomplete(player=_ranch_player_ac)
    async def ledger_cmd(
        interaction: discord.Interaction,
        period: PeriodChoice = "alltime",
        player: Optional[str] = None,
    ) -> None:
        if not await _require_ranch_channel(interaction):
            return
        _log_cmd(interaction, period=period, player=player)
        guild_id = str(interaction.guild_id)
        data = db.get_ledger_stats(period, player, guild_id)
        deposits    = data["deposit"]
        withdrawals = data["withdrawal"]

        total_in  = sum(r["total"] for r in deposits)
        total_out = sum(r["total"] for r in withdrawals)
        net = total_in - total_out
        logger.info(f"CMD  /ledger           → in=${total_in:,.2f}  out=${total_out:,.2f}  net={_sign(net)}${net:,.2f}")

        embed = discord.Embed(
            title=f"💰  Cash Ledger — {_label(period)}",
            color=0x4CAF50 if net >= 0 else 0xE53935,
        )
        embed.add_field(name="Total Deposited", value=f"${total_in:,.2f}",          inline=True)
        embed.add_field(name="Total Withdrawn", value=f"${total_out:,.2f}",         inline=True)
        embed.add_field(name="Net Flow",        value=f"{_sign(net)}${net:,.2f}",   inline=True)

        if not player:
            if deposits:
                embed.add_field(
                    name="Deposits by Player",
                    value="\n".join(
                        f"**{r['player_name']}** — ${r['total']:,.2f} ({r['count']} transactions)"
                        for r in deposits
                    ),
                    inline=False,
                )
            if withdrawals:
                embed.add_field(
                    name="Withdrawals by Player",
                    value="\n".join(
                        f"**{r['player_name']}** — ${r['total']:,.2f} ({r['count']} transactions)"
                        for r in withdrawals
                    ),
                    inline=False,
                )

        if not deposits and not withdrawals:
            embed.description = "No ledger data found for this period."

        await interaction.response.send_message(embed=embed)

    # ------------------------------------------------------------------
    # /cattle
    # ------------------------------------------------------------------
    @bot.tree.command(name="cattle", description="Show cattle buy / sell stats")
    @app_commands.describe(
        period="Time period — day / week / alltime (default: alltime)",
        player="Filter to a specific player (optional)",
    )
    @app_commands.autocomplete(player=_ranch_player_ac)
    async def cattle_cmd(
        interaction: discord.Interaction,
        period: PeriodChoice = "alltime",
        player: Optional[str] = None,
    ) -> None:
        if not await _require_ranch_channel(interaction):
            return
        _log_cmd(interaction, period=period, player=player)
        guild_id = str(interaction.guild_id)
        data  = db.get_cattle_stats(period, player, guild_id)
        buys  = data["cattle_buy"]
        sells = data["cattle_sell"]

        total_spent   = sum(r["total_value"] for r in buys)
        total_revenue = sum(r["total_value"] for r in sells)
        total_bought  = sum(r["total_qty"]   for r in buys)
        total_sold    = sum(r["total_qty"]   for r in sells)
        profit = total_revenue - total_spent
        logger.info(f"CMD  /cattle           → bought={total_bought}  sold={total_sold}  profit={_sign(profit)}${profit:,.2f}")

        embed = discord.Embed(
            title=f"🐄  Cattle Stats — {_label(period)}",
            color=0x8B4513,
        )
        embed.add_field(name="Cattle Bought", value=f"{total_bought} head\n${total_spent:,.2f} spent",    inline=True)
        embed.add_field(name="Cattle Sold",   value=f"{total_sold} head\n${total_revenue:,.2f} earned",   inline=True)
        embed.add_field(name="Net Profit",    value=f"{_sign(profit)}${profit:,.2f}",                     inline=True)

        if not player and sells:
            embed.add_field(
                name="Sales by Player",
                value="\n".join(
                    f"**{r['player_name']}** — {r['total_qty']} head for ${r['total_value']:,.2f} ({r['transactions']} sales)"
                    for r in sells
                ),
                inline=False,
            )

        if not player and buys:
            embed.add_field(
                name="Purchases by Player",
                value="\n".join(
                    f"**{r['player_name']}** — {r['total_qty']} head for ${r['total_value']:,.2f} ({r['transactions']} buys)"
                    for r in buys
                ),
                inline=False,
            )

        if not buys and not sells:
            embed.description = "No cattle data found for this period."

        await interaction.response.send_message(embed=embed)

    # ------------------------------------------------------------------
    # /ranch_summary
    # ------------------------------------------------------------------
    @bot.tree.command(name="ranch_summary", description="Show a full ranch summary")
    @app_commands.describe(
        period="Time period — day / week / alltime (default: alltime)",
        player="Filter to a specific player (optional)",
    )
    @app_commands.autocomplete(player=_ranch_player_ac)
    async def ranch_summary_cmd(
        interaction: discord.Interaction,
        period: PeriodChoice = "alltime",
        player: Optional[str] = None,
    ) -> None:
        if not await _require_ranch_channel(interaction):
            return
        _log_cmd(interaction, period=period, player=player)
        guild_id = str(interaction.guild_id)
        data = db.get_summary_stats(period, player, guild_id)

        def v(key: str) -> float:
            return _val(data[key], "total")

        def c(key: str) -> int:
            return int(_val(data[key], "count"))

        def q(key: str) -> int:
            return int(_val(data[key], "total_qty"))

        net_cash    = v("deposit") - v("withdrawal")
        cattle_prof = v("cattle_sell") - v("cattle_buy")

        title = f"🤠  {'Ranch' if not player else player} Summary — {_label(period)}"
        embed = discord.Embed(title=title, color=0xC8860A)

        embed.add_field(name="🥚 Eggs Collected", value=f"{int(v('eggs')):,} ({c('eggs')} runs)",   inline=True)
        embed.add_field(name="🥛 Milk Collected", value=f"{int(v('milk')):,} ({c('milk')} runs)",   inline=True)
        embed.add_field(name="\u200b",            value="\u200b",                                   inline=True)

        embed.add_field(name="💰 Deposited",      value=f"${v('deposit'):,.2f}",                    inline=True)
        embed.add_field(name="💸 Withdrawn",      value=f"${v('withdrawal'):,.2f}",                 inline=True)
        embed.add_field(name="📊 Net Cash",       value=f"{_sign(net_cash)}${net_cash:,.2f}",       inline=True)

        embed.add_field(name="🐄 Cattle Bought",  value=f"{q('cattle_buy')} head (${v('cattle_buy'):,.2f})",     inline=True)
        embed.add_field(name="🐄 Cattle Sold",    value=f"{q('cattle_sell')} head (${v('cattle_sell'):,.2f})",   inline=True)
        embed.add_field(name="📈 Cattle Profit",  value=f"{_sign(cattle_prof)}${cattle_prof:,.2f}",              inline=True)

        await interaction.response.send_message(embed=embed)

    # ------------------------------------------------------------------
    # /materials
    # ------------------------------------------------------------------
    @bot.tree.command(name="materials", description="Show camp materials contribution stats")
    @app_commands.describe(
        period="Time period — day / week / alltime (default: alltime)",
        player="Filter to a specific player (optional)",
    )
    @app_commands.autocomplete(player=_camp_player_ac)
    async def materials_cmd(
        interaction: discord.Interaction,
        period: PeriodChoice = "alltime",
        player: Optional[str] = None,
    ) -> None:
        if not await _require_camp_channel(interaction):
            return
        _log_cmd(interaction, period=period, player=player)
        guild_id = str(interaction.guild_id)
        rows = db.get_collection_stats("materials", period, player, guild_id)
        logger.info(f"CMD  /materials        → {len(rows)} player(s) returned")
        embed = discord.Embed(
            title=f"🪵  Camp Materials — {_label(period)}",
            color=0xA0522D,
        )

        if not rows:
            embed.description = "No materials data found for this period."
        elif player:
            r = rows[0]
            avg = r["total"] / r["collections"]
            embed.description = f"**Player:** {r['player_name']}"
            embed.add_field(name="Total Materials", value=f"{r['total']:,.1f}",  inline=True)
            embed.add_field(name="Donations",       value=str(r["collections"]), inline=True)
            embed.add_field(name="Avg / Donation",  value=f"{avg:.2f}",          inline=True)
        else:
            total_mat  = sum(r["total"] for r in rows)
            total_runs = sum(r["collections"] for r in rows)
            lines = [
                f"`{i}.` **{r['player_name']}** — {r['total']:,.1f} materials ({r['collections']} donations)"
                for i, r in enumerate(rows, 1)
            ]
            embed.description = "\n".join(lines)
            embed.set_footer(text=f"Total: {total_mat:,.1f} materials across {total_runs} donations")

        await interaction.response.send_message(embed=embed)

    # ------------------------------------------------------------------
    # /supplies
    # ------------------------------------------------------------------
    @bot.tree.command(name="supplies", description="Show camp supplies delivery stats")
    @app_commands.describe(
        period="Time period — day / week / alltime (default: alltime)",
        player="Filter to a specific player (optional)",
    )
    @app_commands.autocomplete(player=_camp_player_ac)
    async def supplies_cmd(
        interaction: discord.Interaction,
        period: PeriodChoice = "alltime",
        player: Optional[str] = None,
    ) -> None:
        if not await _require_camp_channel(interaction):
            return
        _log_cmd(interaction, period=period, player=player)
        guild_id = str(interaction.guild_id)
        rows = db.get_collection_stats("supplies", period, player, guild_id)
        logger.info(f"CMD  /supplies         → {len(rows)} player(s) returned")
        embed = discord.Embed(
            title=f"📦  Camp Supplies — {_label(period)}",
            color=0x5C6BC0,
        )

        if not rows:
            embed.description = "No supplies data found for this period."
        elif player:
            r = rows[0]
            avg = r["total"] / r["collections"]
            embed.description = f"**Player:** {r['player_name']}"
            embed.add_field(name="Total Supplies", value=f"{int(r['total']):,}", inline=True)
            embed.add_field(name="Deliveries",     value=str(r["collections"]),  inline=True)
            embed.add_field(name="Avg / Delivery", value=f"{avg:.1f}",           inline=True)
        else:
            total_sup  = sum(r["total"] for r in rows)
            total_runs = sum(r["collections"] for r in rows)
            lines = [
                f"`{i}.` **{r['player_name']}** — {int(r['total']):,} supplies ({r['collections']} deliveries)"
                for i, r in enumerate(rows, 1)
            ]
            embed.description = "\n".join(lines)
            embed.set_footer(text=f"Total: {int(total_sup):,} supplies across {total_runs} deliveries")

        await interaction.response.send_message(embed=embed)

    # ------------------------------------------------------------------
    # /stock
    # ------------------------------------------------------------------
    @bot.tree.command(name="stock", description="Show camp stock sale stats")
    @app_commands.describe(
        period="Time period — day / week / alltime (default: alltime)",
        player="Filter to a specific player (optional)",
    )
    @app_commands.autocomplete(player=_camp_player_ac)
    async def stock_cmd(
        interaction: discord.Interaction,
        period: PeriodChoice = "alltime",
        player: Optional[str] = None,
    ) -> None:
        if not await _require_camp_channel(interaction):
            return
        _log_cmd(interaction, period=period, player=player)
        guild_id = str(interaction.guild_id)
        stock_rows = db.get_sales_stats(["stock_sale"], period, player, guild_id)
        logger.info(f"CMD  /stock            → {len(stock_rows)} player(s) returned")

        total_qty     = sum(r["total_qty"]   for r in stock_rows)
        total_revenue = sum(r["total_value"] for r in stock_rows)

        embed = discord.Embed(
            title=f"📦  Stock Sales — {_label(period)}",
            color=0x26A69A,
        )

        if not stock_rows:
            embed.description = "No stock sale data found for this period."
        else:
            embed.add_field(name="Total Stock Sold", value=f"{total_qty:,}",           inline=True)
            embed.add_field(name="Total Revenue",    value=f"${total_revenue:,.2f}",   inline=True)
            embed.add_field(name="\u200b",           value="\u200b",                   inline=True)

            if not player:
                embed.add_field(
                    name="Sales by Player",
                    value="\n".join(
                        f"`{i}.` **{r['player_name']}** — {r['total_qty']:,} stock for ${r['total_value']:,.2f} ({r['transactions']} sales)"
                        for i, r in enumerate(stock_rows, 1)
                    ),
                    inline=False,
                )
            else:
                r = stock_rows[0]
                avg_price = r["total_value"] / r["total_qty"] if r["total_qty"] else 0
                embed.add_field(name="Player",       value=r["player_name"],               inline=True)
                embed.add_field(name="Stock Sold",   value=f"{r['total_qty']:,}",           inline=True)
                embed.add_field(name="Avg Price",    value=f"${avg_price:,.2f} / unit",     inline=True)

        await interaction.response.send_message(embed=embed)

    # ------------------------------------------------------------------
    # /camp_summary
    # ------------------------------------------------------------------
    @bot.tree.command(name="camp_summary", description="Show a full camp contribution summary")
    @app_commands.describe(
        period="Time period — day / week / alltime (default: alltime)",
        player="Filter to a specific player (optional)",
    )
    @app_commands.autocomplete(player=_camp_player_ac)
    async def camp_summary_cmd(
        interaction: discord.Interaction,
        period: PeriodChoice = "alltime",
        player: Optional[str] = None,
    ) -> None:
        if not await _require_camp_channel(interaction):
            return
        _log_cmd(interaction, period=period, player=player)
        guild_id = str(interaction.guild_id)
        mat_rows   = db.get_collection_stats("materials",   period, player, guild_id)
        sup_rows   = db.get_collection_stats("supplies",    period, player, guild_id)
        stock_rows = db.get_sales_stats(["stock_sale"],     period, player, guild_id)

        total_mat     = sum(r["total"]       for r in mat_rows)
        total_sup     = sum(r["total"]       for r in sup_rows)
        total_stock   = sum(r["total_qty"]   for r in stock_rows)
        total_revenue = sum(r["total_value"] for r in stock_rows)
        logger.info(
            f"CMD  /camp_summary     → materials={total_mat:,.1f}  "
            f"supplies={int(total_sup)}  stock_sold={total_stock}  revenue=${total_revenue:,.2f}"
        )

        title = f"🏕️  {'Camp' if not player else player} Summary — {_label(period)}"
        embed = discord.Embed(title=title, color=0x6D4C41)

        embed.add_field(name="🪵 Total Materials", value=f"{total_mat:,.1f}",          inline=True)
        embed.add_field(name="📦 Total Supplies",  value=f"{int(total_sup):,}",        inline=True)
        embed.add_field(name="💵 Stock Revenue",   value=f"${total_revenue:,.2f} ({total_stock:,} sold)", inline=True)

        if not player and mat_rows:
            embed.add_field(
                name="Materials by Player",
                value="\n".join(
                    f"**{r['player_name']}** — {r['total']:,.1f} ({r['collections']} donations)"
                    for r in mat_rows
                ),
                inline=False,
            )

        if not player and sup_rows:
            embed.add_field(
                name="Supplies by Player",
                value="\n".join(
                    f"**{r['player_name']}** — {int(r['total']):,} ({r['collections']} deliveries)"
                    for r in sup_rows
                ),
                inline=False,
            )

        if not player and stock_rows:
            embed.add_field(
                name="Stock Sales by Player",
                value="\n".join(
                    f"**{r['player_name']}** — {r['total_qty']:,} stock for ${r['total_value']:,.2f} ({r['transactions']} sales)"
                    for r in stock_rows
                ),
                inline=False,
            )

        if not mat_rows and not sup_rows and not stock_rows:
            embed.description = "No camp data found for this period."

        await interaction.response.send_message(embed=embed)

    # ------------------------------------------------------------------
    # /scan  (admin only — backfills historical data)
    # ------------------------------------------------------------------
    @bot.tree.command(
        name="scan",
        description="Scan this channel's history to import past events (requires Manage Server)",
    )
    @app_commands.describe(limit="How many messages to scan (default: entire channel history)")
    async def scan_cmd(
        interaction: discord.Interaction,
        limit: Optional[int] = None,
    ) -> None:
        if not interaction.user.guild_permissions.manage_guild:
            await interaction.response.send_message(
                "You need **Manage Server** permission to run a scan.", ephemeral=True
            )
            return

        if limit is not None:
            limit = max(limit, 1)
        await interaction.response.defer(ephemeral=True)

        guild_id = str(interaction.guild_id)
        config = db.get_guild_config(guild_id)

        # Determine whether this is the camp or ranch channel.
        is_camp = (
            config is not None
            and config["camp_channel_id"] is not None
            and int(config["camp_channel_id"]) == interaction.channel.id
        )
        channel_label = "CAMP" if is_camp else "RANCH"
        logger.info(f"CMD  /scan  started — channel={channel_label}  limit={limit or 'all'}  by={interaction.user}  guild={guild_id}")

        processed = 0
        added = 0

        async for message in interaction.channel.history(limit=limit, oldest_first=False):
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
            if processed % 100 == 0:
                logger.info(f"CMD  /scan  progress — scanned={processed}  imported={added}")

        logger.info(f"CMD  /scan  complete — scanned={processed}  imported={added}")
        await interaction.followup.send(
            f"Scan complete! Scanned **{processed}** messages, imported **{added}** new events.",
            ephemeral=True,
        )
