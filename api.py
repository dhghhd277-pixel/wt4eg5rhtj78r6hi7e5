import os
import json
from pathlib import Path
import logging
from fastapi import FastAPI, Request
from dotenv import load_dotenv
from telegram import Bot
from bot import (
    DATA_DIR,
    ORDERS_FILE,
    ADMINS_FILE,
    PENDING_FILE,
    PROD_FILE,
    read_json,
    write_json,
    create_order,
    clear_cart,
    _interprocess_lock,
    _products_lock_path,
    _pending_lock_path,
    _orders_lock_path,
    _orders_pending_lock_path,
)

BASE_DIR = Path(__file__).resolve().parent
# Load .env relative to this file to avoid cwd-dependent failures on servers
load_dotenv(dotenv_path=BASE_DIR / ".env")
TOKEN = os.getenv("TOKEN")
app = FastAPI()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Avoid leaking bot token in httpx INFO logs.
logging.getLogger("httpx").setLevel(logging.WARNING)

def read_pending():
    try:
        data = json.loads(Path(PENDING_FILE).read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []

def write_pending(data):
    # Keep writes atomic and consistent with bot.py
    with _interprocess_lock(_pending_lock_path()):
        write_json(Path(PENDING_FILE), data if isinstance(data, list) else [])

@app.post("/yookassa/webhook")
async def yookassa_webhook(request: Request):
    # Read raw body to allow signature verification if SDK is available.
    raw = await request.body()
    payment = None
    data = None
    # Try to verify/parse via YooKassa SDK notification factory (if installed)
    try:
        from yookassa.domain.notification import WebhookNotificationFactory

        try:
            notification = WebhookNotificationFactory().create(raw.decode())
            payment = notification.object
            logger.info("YooKassa notification parsed by SDK")
        except Exception:
            logger.exception("YooKassa notification verification failed")
            return {"status": "invalid_signature"}
    except Exception:
        # SDK not available; fall back to plain JSON parsing (less secure)
        try:
            data = json.loads(raw.decode())
        except Exception:
            logger.exception("Invalid webhook JSON body")
            return {"status": "ignored"}

    if data is None:
        # If SDK parsed, build a minimal data structure compatible with older flow
        try:
            data = {"event": getattr(payment, "event", "payment.succeeded"), "object": payment}
        except Exception:
            logger.exception("Unable to build data from notification")
            return {"status": "ignored"}

    event = data.get("event")
    if event == "payment.succeeded":
        if payment is None:
            payment = data.get("object", {})

        # Payment id (SDK object or dict)
        incoming_payment_id = None
        try:
            if isinstance(payment, dict):
                incoming_payment_id = payment.get("id")
            else:
                incoming_payment_id = getattr(payment, "id", None)
        except Exception:
            incoming_payment_id = None
        meta = None
        try:
            # payment may be an SDK object or dict
            meta = payment.get("metadata") if isinstance(payment, dict) else getattr(payment, "metadata", None)
        except Exception:
            meta = {}
        meta = meta or {}
        order_id_raw = meta.get("order_id")
        try:
            order_id = int(order_id_raw)
        except Exception:
            logger.warning("Invalid order_id in webhook metadata: %s", order_id_raw)
            return {"status": "ignored"}
        user_id = int(meta.get("user_id")) if meta.get("user_id") else None
        # Make pending->orders transition idempotent under a shared lock.
        with _interprocess_lock(_orders_pending_lock_path()):
            pend = read_pending()
            pending = next((p for p in pend if int(p.get("id", 0)) == order_id), None)
            if not pending:
                # If we already created an order for this payment, treat as OK.
                try:
                    pid = str(incoming_payment_id or "").strip()
                    if pid:
                        with _interprocess_lock(_orders_lock_path()):
                            orders_now = read_json(ORDERS_FILE, default=[])
                            if any(str(o.get("payment_id")) == pid for o in (orders_now or [])):
                                return {"status": "ok"}
                except Exception:
                    pass
                return {"status": "ignored"}

            # Idempotency: if order already exists for payment_id, just remove pending and exit.
            try:
                pid = str(pending.get("payment_id") or incoming_payment_id or "").strip()
            except Exception:
                pid = ""
            if pid:
                try:
                    with _interprocess_lock(_orders_lock_path()):
                        orders_now = read_json(ORDERS_FILE, default=[])
                        if any(str(o.get("payment_id")) == pid for o in (orders_now or [])):
                            pend2 = [p for p in pend if int(p.get("id", 0)) != order_id]
                            write_pending(pend2)
                            return {"status": "ok"}
                except Exception:
                    pass

            # create real order
            class U:
                def __init__(self, uid, username):
                    self.id = uid
                    self.username = username
                    self.first_name = None
                    self.last_name = None

            user = U(user_id, None)
            items = pending.get("items", [])
            address = pending.get("address", "")
            delivery = pending.get("delivery")
            order = create_order(
                user,
                items,
                address,
                delivery,
                number=pending.get("number"),
                payment_id=(pending.get("payment_id") or incoming_payment_id),
                created_at=pending.get("created_at"),
            )

            # decrease stock and alert admins if low/out-of-stock (skip if already reserved)
            try:
                events = []
                if pending.get("reserved"):
                    prods_all = read_json(PROD_FILE, default=[])
                    prods_by_id = {int(p.get("id")): p for p in (prods_all or []) if p.get("id") is not None}
                    seen = set()
                    for it in order.get("items", []):
                        try:
                            pid2 = int(it.get("product_id", 0))
                        except Exception:
                            continue
                        if pid2 in seen:
                            continue
                        seen.add(pid2)
                        p = prods_by_id.get(pid2)
                        if not p:
                            continue
                        new_stock = int(p.get("stock", 0) or 0)
                        if new_stock == 0:
                            events.append(("out", p.copy()))
                        elif new_stock <= 3:
                            events.append(("low", p.copy()))
                else:
                    with _interprocess_lock(_products_lock_path()):
                        prods_all = read_json(PROD_FILE, default=[])
                        for it in order.get("items", []):
                            for p in (prods_all or []):
                                if int(p.get("id", 0)) == int(it.get("product_id", 0)):
                                    old_stock = int(p.get("stock", 0) or 0)
                                    p["stock"] = max(0, old_stock - int(it.get("qty", 1)))
                                    new_stock = int(p.get("stock", 0) or 0)
                                    if new_stock == 0:
                                        events.append(("out", p.copy()))
                                    elif new_stock <= 3 and old_stock > 3:
                                        events.append(("low", p.copy()))
                                    break
                        write_json(PROD_FILE, prods_all)
                if TOKEN:
                    try:
                        bot = Bot(token=TOKEN)
                        admins = read_json(ADMINS_FILE, default=[])
                        for kind, prod_event in events:
                            for aid in (admins or []):
                                try:
                                    if kind == "out":
                                        await bot.send_message(chat_id=aid, text=(
                                            "⛔ Товар закончился\n\n"
                                            f"💊 {prod_event.get('name','-')}\n"
                                            f"🆔 ID: {prod_event.get('id')}"
                                        ))
                                    else:
                                        await bot.send_message(chat_id=aid, text=(
                                            "⚠️ Мало товара\n\n"
                                            f"💊 {prod_event.get('name','-')}\n"
                                            f"📦 Осталось: {prod_event.get('stock', 0)} шт\n"
                                            f"🆔 ID: {prod_event.get('id')}"
                                        ))
                                except Exception:
                                    pass
                    except Exception:
                        pass
            except Exception:
                pass

            # clear cart on successful payment if checkout was from cart
            try:
                if pending.get("type") == "cart" and user_id:
                    clear_cart(user_id)
            except Exception:
                pass

            # remove from pending
            pend = [p for p in pend if int(p.get("id", 0)) != order_id]
            write_pending(pend)

            # notify user
            if TOKEN and user_id:
                try:
                    bot = Bot(token=TOKEN)
                    await bot.send_message(
                        chat_id=user_id,
                        text=(
                            f"✅ Оплата заказа #{order['number']} прошла успешно\n\n"
                            "📦 Заказ оформлен. Ожидайте, когда администратор начнет обработку.\n"
                            "Когда появится ссылка для отслеживания — мы сообщим.\n\n"
                            "Вы можете смотреть статус в разделе «📦 Мои заказы»."
                        ),
                    )
                except Exception:
                    pass

            # notify admins about new order
            if TOKEN:
                try:
                    bot = Bot(token=TOKEN)
                    admins = read_json(ADMINS_FILE, default=[])
                    items2 = order.get("items", []) or []
                    lines = []
                    for it in items2[:10]:
                        lines.append(f"• {it.get('name','-')} × {it.get('qty',1)}")
                    if len(items2) > 10:
                        lines.append(f"… ещё {len(items2) - 10} поз.")
                    delivery2 = order.get("delivery") or "-"

                    client = order.get("client") or {}
                    fio = " ".join(
                        [
                            (client.get("last_name") or "").strip(),
                            (client.get("first_name") or "").strip(),
                            (client.get("patronymic") or "").strip(),
                        ]
                    ).strip() or "-"
                    phone = (client.get("phone") or "-").strip() or "-"
                    uname = (order.get("username") or "").strip()
                    tg = f"@{uname}" if uname else "-"

                    text = (
                        "🆕 Новый оплаченный заказ\n\n"
                        f"🧾 Заказ #{order.get('number')}\n"
                        f"💰 Сумма: {order.get('total', 0)} ₽\n"
                        f"🚚 Доставка: {delivery2}\n"
                        f"📍 Адрес: {order.get('address','-')}\n\n"
                        f"👤 Клиент: {fio}\n"
                        f"📞 Телефон: {phone}\n"
                        f"Telegram: {tg}\n"
                        f"ID: {order.get('user_id')}\n\n"
                        "📦 Товары:\n" + ("\n".join(lines) if lines else "• -")
                    )
                    for aid in (admins or []):
                        try:
                            await bot.send_message(chat_id=aid, text=text)
                        except Exception:
                            pass
                except Exception:
                    pass
            return {"status": "ok"}
    return {"status": "ignored"}
