import asyncio
import logging
import os

from dotenv import load_dotenv

from bot import BetterRanchBot


load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


async def main() -> None:
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise RuntimeError("DISCORD_TOKEN is not set. Add it to your .env file.")

    bot = BetterRanchBot()
    async with bot:
        await bot.start(token)


if __name__ == "__main__":
    asyncio.run(main())
