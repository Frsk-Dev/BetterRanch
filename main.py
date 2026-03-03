import asyncio
import logging
import os
import threading

from dotenv import load_dotenv

import state
from bot import BetterRanchBot
from app import app as flask_app


load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger("betterranch")


async def _run_bot() -> None:
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise RuntimeError("DISCORD_TOKEN is not set. Add it to your .env file.")
    bot = BetterRanchBot()
    async with bot:
        await bot.start(token)


def _bot_thread() -> None:
    asyncio.run(_run_bot())


if __name__ == "__main__":
    # Start the Discord bot in a background daemon thread.
    thread = threading.Thread(target=_bot_thread, daemon=True)
    thread.start()

    # Start Flask in the main thread.
    # use_reloader=False is required when running alongside a background thread.
    try:
        flask_app.run(
            host="0.0.0.0",
            port=5000,
            debug=True,
            use_reloader=False,
        )
    except KeyboardInterrupt:
        pass
    finally:
        # Cleanly close the bot so Discord marks it offline immediately.
        if state.bot_instance and state.bot_loop and not state.bot_loop.is_closed():
            logger.info("Shutting down bot…")
            future = asyncio.run_coroutine_threadsafe(
                state.bot_instance.close(), state.bot_loop
            )
            try:
                future.result(timeout=5)
            except Exception:
                pass
        logger.info("Shutdown complete.")
