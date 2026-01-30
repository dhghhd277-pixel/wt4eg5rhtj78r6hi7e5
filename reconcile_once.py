import asyncio
import os

from dotenv import load_dotenv
from telegram import Bot

import bot as botmod


class _Ctx:
    def __init__(self, bot: Bot):
        self.bot = bot
        self.application = None


async def main() -> int:
    load_dotenv()

    token = os.getenv("TOKEN")
    if not token:
        print("TOKEN is missing in .env")
        return 2

    if not botmod._ensure_yookassa_configured():
        print("YooKassa is not configured (check YOOKASSA_SHOP_ID/YOOKASSA_SECRET_KEY and yookassa install)")
        return 2

    pending_all = botmod.read_pending_orders()
    if not pending_all:
        print("No pending orders")
        return 0

    ctx = _Ctx(Bot(token=token))

    processed = 0
    succeeded = 0
    for pending in list(pending_all):
        pid = pending.get("payment_id")
        if not pid:
            continue
        processed += 1
        try:
            payment = botmod.Payment.find_one(str(pid))
            status = getattr(payment, "status", None)
        except Exception as e:
            print(f"{pid}: ERROR {type(e).__name__}: {e}")
            continue

        print(f"{pid}: status={status}")
        if status == "succeeded":
            ok = await botmod._finalize_paid_pending(ctx, pending)
            if ok:
                succeeded += 1

    print(f"Checked: {processed}, finalized: {succeeded}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
