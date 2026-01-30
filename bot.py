from dotenv import load_dotenv
import os
import json
import logging
import sys
from pathlib import Path
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, KeyboardButton, BotCommand
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, CallbackQueryHandler, MessageHandler, filters
import asyncio
from telegram.error import BadRequest
import uuid
import re
import unicodedata
try:
    from yookassa import Payment, Configuration
except Exception:
    Payment = None
    Configuration = None

BASE_DIR = Path(__file__).resolve().parent
# Load .env relative to this file to avoid cwd-dependent failures on servers
load_dotenv(dotenv_path=BASE_DIR / ".env")

# python-telegram-bot 21.x is not compatible with Python 3.13 (Updater slots crash).
# Prefer Python 3.12/3.11 (or pin in Docker). We warn instead of hard-failing to avoid surprise shutdowns.
if sys.version_info >= (3, 13):
    try:
        print(
            "\nВНИМАНИЕ: python-telegram-bot 21.x не поддерживает Python 3.13 — возможны падения.\n"
            "Рекомендуется Python 3.12/3.11 или запуск через Docker с Python <= 3.12.\n",
            file=sys.stderr,
        )
    except Exception:
        pass

# Basic logging for the application
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Avoid leaking bot token in httpx INFO logs (PTB uses httpx internally).
logging.getLogger("httpx").setLevel(logging.WARNING)

TOKEN = os.getenv("TOKEN")

# Default admins (empty). Use `admins.json` to manage admin IDs instead of hardcoding.
ADMINS = set()

# Use a path relative to this file so running from a different CWD still works.
DATA_DIR = BASE_DIR / "data"
CATS_FILE = DATA_DIR / "categories.json"
PROD_FILE = DATA_DIR / "products.json"
CART_FILE = DATA_DIR / "carts.json"
FAV_FILE = DATA_DIR / "favs.json"
ADMINS_FILE = DATA_DIR / "admins.json"
ORDERS_FILE = DATA_DIR / "orders.json"
BROADS_FILE = DATA_DIR / "broadcasts.json"
NOTIF_FILE = DATA_DIR / "notifications.json"
USERS_FILE = DATA_DIR / "users.json"
ADDR_FILE = DATA_DIR / "addresses.json"
PROFILE_FILE = DATA_DIR / "profiles.json"
PENDING_FILE = DATA_DIR / "pending_orders.json"
WAIT_NOTIFY_FILE = DATA_DIR / "notify.json"


REVIEWS_CHANNEL_URL = "https://t.me/+l8oJasM2uBkyOGVi"


def _profile_next_missing_state(profile: dict | None) -> str | None:
    """Return the next profile collection state required to proceed with checkout."""
    prof = profile or {}
    if not (prof.get("first_name") or "").strip():
        return "profile_first_name"
    if not (prof.get("last_name") or "").strip():
        return "profile_last_name"
    if not (prof.get("patronymic") or "").strip():
        return "profile_patronymic"
    if not (prof.get("phone") or "").strip():
        return "profile_phone"
    return None


def _reviews_url_keyboard(label: str = "⭐ Отзывы") -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(label, url=REVIEWS_CHANNEL_URL)]])


async def _send_reviews_link(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    await update.message.reply_text(
        "⭐ Отзывы\n\nНажмите кнопку ниже, чтобы перейти в канал с отзывами 👇",
        reply_markup=_reviews_url_keyboard("⭐ Отзывы"),
    )


async def reviews_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await _send_reviews_link(update, context)


async def _send_review_prompt(context: ContextTypes.DEFAULT_TYPE, user_id: int, order_number: int | str | None = None) -> None:
    text = (
        "🤖 Привет!\n"
        "Я вижу, что твой заказ уже доставлен ✅\n"
        "Надеюсь, всё понравилось! Буду очень рад, если ты оставишь отзыв — это реально помогает нам становиться лучше 🙌\n"
        "Нажми на кнопку ниже 👇"
    )
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("⭐ Оставить отзыв", callback_data="review_open")]])
    try:
        await context.bot.send_message(chat_id=int(user_id), text=text, reply_markup=keyboard)
    except Exception:
        pass


def ensure_data_files():
    DATA_DIR.mkdir(exist_ok=True)
    if not CATS_FILE.exists():
        CATS_FILE.write_text("[]", encoding="utf-8")
    if not PROD_FILE.exists():
        PROD_FILE.write_text("[]", encoding="utf-8")
    if not CART_FILE.exists():
        CART_FILE.write_text("[]", encoding="utf-8")
    if not FAV_FILE.exists():
        FAV_FILE.write_text("[]", encoding="utf-8")
    if not ADMINS_FILE.exists():
        ADMINS_FILE.write_text(json.dumps(list(ADMINS)), encoding="utf-8")
    if not ORDERS_FILE.exists():
        ORDERS_FILE.write_text("[]", encoding="utf-8")
    if not BROADS_FILE.exists():
        BROADS_FILE.write_text("[]", encoding="utf-8")
    if not NOTIF_FILE.exists():
        default = {"new_product": {"enabled": False, "template": "🆕 Появился новый товар!\n\n{name}\n💰 Цена: {price}\n\n👇 Нажмите, чтобы посмотреть"}}
        NOTIF_FILE.write_text(json.dumps(default, ensure_ascii=False, indent=2), encoding="utf-8")
    if not USERS_FILE.exists():
        USERS_FILE.write_text("[]", encoding="utf-8")
    if not ADDR_FILE.exists():
        ADDR_FILE.write_text("{}", encoding="utf-8")
    if not PROFILE_FILE.exists():
        PROFILE_FILE.write_text("{}", encoding="utf-8")
    if not PENDING_FILE.exists():
        PENDING_FILE.write_text("[]", encoding="utf-8")
    if not WAIT_NOTIFY_FILE.exists():
        WAIT_NOTIFY_FILE.write_text("{}", encoding="utf-8")


def _read_wait_notify_map() -> dict:
    with _interprocess_lock(_wait_notify_lock_path()):
        data = read_json(WAIT_NOTIFY_FILE, default={})
        return data if isinstance(data, dict) else {}


def _write_wait_notify_map(data: dict) -> None:
    if not isinstance(data, dict):
        data = {}
    with _interprocess_lock(_wait_notify_lock_path()):
        write_json(WAIT_NOTIFY_FILE, data)


def subscribe_notify(user_id: int, product_id: int) -> bool:
    """Subscribe a user to restock notifications for a product. Returns True if added."""
    ensure_data_files()
    data = _read_wait_notify_map()
    key = str(int(product_id))
    users = data.get(key)
    if not isinstance(users, list):
        users = []
    if int(user_id) in users:
        data[key] = users
        _write_wait_notify_map(data)
        return False
    users.append(int(user_id))
    data[key] = users
    _write_wait_notify_map(data)
    return True


async def notify_users_product_available(context: ContextTypes.DEFAULT_TYPE, product_id: int, product_name: str | None = None) -> None:
    """Notify and clear subscriptions when product becomes available again."""
    ensure_data_files()
    data = _read_wait_notify_map()
    key = str(int(product_id))
    users = data.get(key)
    if not isinstance(users, list) or not users:
        return

    msg = "🎉 Товар снова в наличии!"
    if product_name:
        msg = msg + f"\n\n{product_name}"

    delivered = 0
    for uid in list(users):
        try:
            await context.bot.send_message(chat_id=int(uid), text=msg)
            delivered += 1
        except Exception:
            pass

    # Admin log
    try:
        admins = read_json(ADMINS_FILE)
    except Exception:
        admins = []
    if admins:
        name_line = f"\nТовар: {product_name}" if product_name else ""
        admin_text = (
            f"📢 Уведомления о поступлении отправлены: {delivered} пользователям"
            f"\nID товара: {product_id}"
            f"{name_line}"
        )
        for aid in admins:
            try:
                await context.bot.send_message(chat_id=int(aid), text=admin_text)
            except Exception:
                pass

    # Clear subscriptions even if delivery failed for some users to avoid infinite growth
    data.pop(key, None)
    _write_wait_notify_map(data)


def read_json(path: Path, default=None):
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        # Guard against accidentally persisted JSON null -> Python None.
        if data is None:
            return default if default is not None else []
        return data
    except Exception:
        return default if default is not None else []


def write_json(path: Path, data):
    # Atomic write: write to a temporary file and replace the target.
    try:
        tmp_path = path.parent / (path.name + ".tmp")
        tmp_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        try:
            os.replace(str(tmp_path), str(path))
        except Exception:
            # Fallback: try direct write (last resort)
            path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        try:
            logger.exception("Failed to write JSON to %s", path)
        except Exception:
            pass


def _normalize_photos(value) -> list[str]:
    if not value:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        out = []
        for v in value:
            if isinstance(v, str) and v.strip():
                out.append(v.strip())
        return out
    return []


def _split_text_chunks(text: str, limit: int = 4096) -> list[str]:
    if not isinstance(text, str) or not text:
        return []
    if len(text) <= limit:
        return [text]
    chunks = []
    start = 0
    while start < len(text):
        end = min(len(text), start + limit)
        chunks.append(text[start:end])
        start = end
    return chunks


def _sanitize_human_text(value, *, fallback: str = "", max_len: int | None = None) -> str:
    """Normalize user/admin-provided text.

    - Convert to string
    - Unicode normalize
    - Remove control/non-printable characters
    - Replace newlines/tabs with spaces and collapse whitespace
    """
    try:
        s = "" if value is None else str(value)
    except Exception:
        s = ""
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    s = "".join(ch for ch in s if ch.isprintable())
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        s = fallback
    if max_len is not None and max_len > 0:
        s = s[:max_len]
    return s


def sanitize_product_name(value) -> str:
    return _sanitize_human_text(value, fallback="Товар", max_len=255)


def sanitize_yookassa_description(value) -> str:
    return _sanitize_human_text(value, fallback="Товар", max_len=128)


async def _screen_render_message(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    text: str,
    reply_markup=None,
):
    """Render a single 'screen' message for message-based navigation.

    If we know the last screen message, try to edit it; otherwise send a new one.
    """
    chat_id = update.effective_chat.id if update.effective_chat else None
    if chat_id is None:
        return

    screen = context.user_data.get("screen") if hasattr(context, "user_data") else None
    msg_id = None
    if isinstance(screen, dict) and int(screen.get("chat_id", 0) or 0) == int(chat_id):
        try:
            msg_id = int(screen.get("message_id"))
        except Exception:
            msg_id = None

    bot = context.bot
    if msg_id:
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=text, reply_markup=reply_markup)
            return
        except BadRequest as e:
            low = str(e).lower()
            if "no text" in low or "there is no text" in low:
                try:
                    await bot.edit_message_caption(chat_id=chat_id, message_id=msg_id, caption=text, reply_markup=reply_markup)
                    return
                except Exception:
                    pass
        except Exception:
            pass

    try:
        msg = await update.message.reply_text(text, reply_markup=reply_markup)
        context.user_data["screen"] = {"chat_id": int(chat_id), "message_id": int(msg.message_id)}
    except Exception:
        pass


async def _screen_edit_by_ids(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    message_id: int,
    text: str,
    reply_markup=None,
):
    bot = context.bot
    try:
        await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text, reply_markup=reply_markup)
        return
    except BadRequest as e:
        low = str(e).lower()
        if "no text" in low or "there is no text" in low:
            try:
                await bot.edit_message_caption(chat_id=chat_id, message_id=message_id, caption=text, reply_markup=reply_markup)
            except Exception:
                pass
        return
    except Exception:
        return


async def _send_message_safe(
    bot,
    chat_id: int,
    text: str,
    reply_markup=None,
):
    """Send text safely: respect 4096 char limit; keep keyboard on first chunk."""
    chunks = _split_text_chunks(text or "", limit=4096)
    if not chunks:
        return await bot.send_message(chat_id=chat_id, text="", reply_markup=reply_markup)
    first = True
    last_msg = None
    for chunk in chunks:
        if first:
            last_msg = await bot.send_message(chat_id=chat_id, text=chunk, reply_markup=reply_markup)
            first = False
        else:
            last_msg = await bot.send_message(chat_id=chat_id, text=chunk)
    return last_msg


async def _send_photo_safe(
    bot,
    chat_id: int,
    photo,
    caption: str,
    reply_markup=None,
):
    """Send a photo safely.

    If caption is too long for Telegram (1024), send the full text first (split to 4096),
    then send the photo with a short caption and keyboard.
    """
    cap = caption or ""
    if isinstance(cap, str) and len(cap) > 1024:
        # Send full text BEFORE the photo to avoid "photo then long wall of text".
        for chunk in _split_text_chunks(cap, limit=4096):
            try:
                await bot.send_message(chat_id=chat_id, text=chunk)
            except Exception:
                try:
                    logger.exception("Failed to send extra text chunk")
                except Exception:
                    pass
        cap = "📄 Полное описание выше"
    msg = await bot.send_photo(chat_id=chat_id, photo=photo, caption=cap, reply_markup=reply_markup)
    return msg


def get_next_id(items):
    if not items:
        return 1
    return max(item.get("id", 0) for item in items) + 1


def _find_user_record(path: Path, user_id: int):
    data = read_json(path)
    rec = next((r for r in data if r.get("user_id") == user_id), None)
    return data, rec


def add_admin(admin_id: int):
    with _interprocess_lock(_admins_lock_path()):
        data = read_json(ADMINS_FILE, default=[])
        try:
            ids = [int(x) for x in (data or [])]
        except Exception:
            ids = []
        if int(admin_id) not in ids:
            ids.append(int(admin_id))
        write_json(ADMINS_FILE, ids)


def remove_admin(admin_id: int):
    with _interprocess_lock(_admins_lock_path()):
        data = read_json(ADMINS_FILE, default=[])
        try:
            ids = [int(x) for x in (data or [])]
        except Exception:
            ids = []
        ids = [i for i in ids if i != int(admin_id)]
        write_json(ADMINS_FILE, ids)


def _cart_lock_path() -> Path:
    return DATA_DIR / ".lock_carts"


def _products_lock_path() -> Path:
    return DATA_DIR / ".lock_products"


def _favs_lock_path() -> Path:
    return DATA_DIR / ".lock_favs"


def _admins_lock_path() -> Path:
    return DATA_DIR / ".lock_admins"


def _users_lock_path() -> Path:
    return DATA_DIR / ".lock_users"


def _orders_lock_path() -> Path:
    return DATA_DIR / ".lock_orders"


def _pending_lock_path() -> Path:
    return DATA_DIR / ".lock_pending"


def _orders_pending_lock_path() -> Path:
    # Used for cross-file operations (e.g. unique number/id sequence across orders + pending)
    return DATA_DIR / ".lock_orders_pending"


def _addresses_lock_path() -> Path:
    return DATA_DIR / ".lock_addresses"


def _profiles_lock_path() -> Path:
    return DATA_DIR / ".lock_profiles"


def _broadcasts_lock_path() -> Path:
    return DATA_DIR / ".lock_broadcasts"


def _notifications_lock_path() -> Path:
    return DATA_DIR / ".lock_notifications"


def _cats_lock_path() -> Path:
    return DATA_DIR / ".lock_categories"


def _wait_notify_lock_path() -> Path:
    return DATA_DIR / ".lock_wait_notify"


def _interprocess_lock(lock_path: Path):
    """Simple cross-platform interprocess lock using a lock file."""
    from contextlib import contextmanager

    @contextmanager
    def _lock():
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        f = open(lock_path, "a+b")
        try:
            try:
                if os.name == "nt":
                    import msvcrt
                    f.seek(0)
                    msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 1)
                else:
                    import fcntl
                    fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            except Exception:
                # If locking fails, continue without a hard lock (best-effort)
                pass
            yield
        finally:
            try:
                if os.name == "nt":
                    import msvcrt
                    f.seek(0)
                    msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except Exception:
                pass
            try:
                f.close()
            except Exception:
                pass

    return _lock()


def _normalize_cart_items(items, prods_by_id: dict[int, dict] | None = None):
    """Normalize cart items to list of dicts: {product_id, qty, price}."""
    if not isinstance(items, list):
        return []
    if not items:
        return []

    # New format
    if isinstance(items[0], dict):
        out = []
        for it in items:
            try:
                pid = int(it.get("product_id") if it.get("product_id") is not None else it.get("id"))
            except Exception:
                continue
            try:
                qty = int(it.get("qty", 1) or 1)
            except Exception:
                qty = 1
            if qty < 1:
                qty = 1
            price = it.get("price")
            if price is None and prods_by_id and pid in prods_by_id:
                price = prods_by_id[pid].get("price", 0)
            out.append({"product_id": pid, "qty": qty, "price": price if price is not None else 0})
        return out

    # Legacy format: list of product IDs
    out = []
    for raw in items:
        try:
            pid = int(raw)
        except Exception:
            continue
        price = 0
        if prods_by_id and pid in prods_by_id:
            price = prods_by_id[pid].get("price", 0)
        out.append({"product_id": pid, "qty": 1, "price": price})
    return out


def _read_cart_data() -> list:
    data = read_json(CART_FILE, default=[])
    return data if isinstance(data, list) else []


def get_cart_items(user_id: int) -> list[dict]:
    """Return cart items for user in normalized new format. Migrates legacy format on read."""
    with _interprocess_lock(_cart_lock_path()):
        data = _read_cart_data()
        rec = next((r for r in data if int(r.get("user_id", 0)) == int(user_id)), None)
        if not rec:
            return []
        items_raw = rec.get("items", [])
        prods = read_json(PROD_FILE, default=[])
        prods_by_id = {int(p.get("id")): p for p in prods if p.get("id") is not None}
        items_norm = _normalize_cart_items(items_raw, prods_by_id=prods_by_id)
        # Write back if migrated/normalized
        if items_norm != items_raw:
            rec["items"] = items_norm
            write_json(CART_FILE, data)
        return items_norm


def is_in_cart(user_id: int, prod_id: int) -> bool:
    try:
        pid = int(prod_id)
    except Exception:
        return False
    for it in get_cart_items(int(user_id)):
        if int(it.get("product_id", 0)) == pid:
            return True
    return False


def add_to_cart(user_id: int, prod_id: int, qty: int = 1, price=0):
    """Add/increment product in cart (new schema)."""
    try:
        pid = int(prod_id)
    except Exception:
        return
    try:
        qty_i = int(qty or 1)
    except Exception:
        qty_i = 1
    if qty_i < 1:
        qty_i = 1

    with _interprocess_lock(_cart_lock_path()):
        data = _read_cart_data()
        rec = next((r for r in data if int(r.get("user_id", 0)) == int(user_id)), None)
        if not rec:
            rec = {"user_id": int(user_id), "items": []}
            data.append(rec)

        prods = read_json(PROD_FILE, default=[])
        prods_by_id = {int(p.get("id")): p for p in prods if p.get("id") is not None}
        items_norm = _normalize_cart_items(rec.get("items", []), prods_by_id=prods_by_id)

        found = False
        for it in items_norm:
            if int(it.get("product_id", 0)) == pid:
                it["qty"] = int(it.get("qty", 1) or 1) + qty_i
                # keep stored price if present; otherwise set it
                if it.get("price") is None:
                    it["price"] = price
                found = True
                break
        if not found:
            items_norm.append({"product_id": pid, "qty": qty_i, "price": price})
        rec["items"] = items_norm
        write_json(CART_FILE, data)


def get_cart(user_id: int):
    """Backward-compatible: return list of product IDs in cart."""
    out = []
    for it in get_cart_items(int(user_id)):
        try:
            out.append(int(it.get("product_id")))
        except Exception:
            pass
    return out


def clear_cart(user_id: int):
    with _interprocess_lock(_cart_lock_path()):
        data = _read_cart_data()
        data = [r for r in data if int(r.get("user_id", 0)) != int(user_id)]
        write_json(CART_FILE, data)


def remove_from_cart(user_id: int, prod_id: int):
    try:
        pid = int(prod_id)
    except Exception:
        return
    with _interprocess_lock(_cart_lock_path()):
        data = _read_cart_data()
        changed = False
        for r in data:
            if int(r.get("user_id", 0)) == int(user_id):
                prods = read_json(PROD_FILE, default=[])
                prods_by_id = {int(p.get("id")): p for p in prods if p.get("id") is not None}
                items_norm = _normalize_cart_items(r.get("items", []), prods_by_id=prods_by_id)
                new_items = [it for it in items_norm if int(it.get("product_id", 0)) != pid]
                if new_items != items_norm:
                    r["items"] = new_items
                    changed = True
                break
        if changed:
            write_json(CART_FILE, data)


def _reserve_stock_for_pending(pending: dict) -> tuple[bool, str | None]:
    """Reserve stock (decrement from products) for a pending order atomically under a file lock."""
    try:
        items = pending.get("items", []) or []
    except Exception:
        items = []
    if not items:
        return False, "Пустой заказ"

    with _interprocess_lock(_products_lock_path()):
        prods_all = read_json(PROD_FILE, default=[])
        prods_by_id = {int(p.get("id")): p for p in prods_all if p.get("id") is not None}

        # filter missing products (do not fail entire order)
        filtered_items: list[dict] = []
        for it in items:
            try:
                pid = int(it.get("product_id"))
                qty = int(it.get("qty", 1) or 1)
            except Exception:
                return False, "Некорректные товары"
            if qty < 1:
                qty = 1
            prod = prods_by_id.get(pid)
            if not prod:
                continue
            it2 = dict(it)
            it2["qty"] = qty
            it2["name"] = it2.get("name") or prod.get("name")
            filtered_items.append(it2)

        if not filtered_items:
            pending["items"] = []
            pending["total"] = 0
            return False, "Все товары удалены"

        # validate stock for remaining
        for it in filtered_items:
            pid = int(it.get("product_id"))
            qty = int(it.get("qty", 1) or 1)
            prod = prods_by_id.get(pid)
            stock = int(prod.get("stock", 0) or 0)
            if stock < qty:
                name = (prod.get("name") or f"#{pid}").strip()
                return False, f"Недостаточно товара: {name} (доступно {stock})"

        # apply
        for it in filtered_items:
            pid = int(it.get("product_id"))
            qty = int(it.get("qty", 1) or 1)
            prod = prods_by_id.get(pid)
            prod["stock"] = max(0, int(prod.get("stock", 0) or 0) - qty)

        # Keep pending consistent with what's actually reserved
        pending["items"] = filtered_items
        try:
            pending["total"] = sum((float(x.get("price", 0) or 0) * int(x.get("qty", 1) or 1)) for x in filtered_items)
        except Exception:
            pass

        write_json(PROD_FILE, prods_all)

    # mark reserved on pending
    try:
        from time import time
        pending["reserved"] = True
        pending["reserved_at"] = time()
    except Exception:
        pass
    return True, None


def _release_stock_for_pending(pending: dict) -> None:
    """Release previously reserved stock back to products (best-effort)."""
    try:
        if not pending.get("reserved"):
            return
        items = pending.get("items", []) or []
    except Exception:
        return
    if not items:
        return
    with _interprocess_lock(_products_lock_path()):
        prods_all = read_json(PROD_FILE, default=[])
        prods_by_id = {int(p.get("id")): p for p in prods_all if p.get("id") is not None}
        for it in items:
            try:
                pid = int(it.get("product_id"))
                qty = int(it.get("qty", 1) or 1)
            except Exception:
                continue
            prod = prods_by_id.get(pid)
            if not prod:
                continue
            prod["stock"] = int(prod.get("stock", 0) or 0) + max(1, qty)
        write_json(PROD_FILE, prods_all)


def add_to_fav(user_id: int, prod_id: int):
    with _interprocess_lock(_favs_lock_path()):
        data = read_json(FAV_FILE, default=[])
        data = data if isinstance(data, list) else []
        rec = next((r for r in data if r.get("user_id") == user_id), None)
        if not rec:
            rec = {"user_id": user_id, "items": []}
            data.append(rec)
        if prod_id not in (rec.get("items") or []):
            rec.setdefault("items", []).append(prod_id)
        write_json(FAV_FILE, data)


def get_favs(user_id: int):
    with _interprocess_lock(_favs_lock_path()):
        data = read_json(FAV_FILE, default=[])
        data = data if isinstance(data, list) else []
        rec = next((r for r in data if r.get("user_id") == user_id), None)
        return rec.get("items", []) if rec else []


def clear_favs(user_id: int):
    with _interprocess_lock(_favs_lock_path()):
        data = read_json(FAV_FILE, default=[])
        data = data if isinstance(data, list) else []
        data = [r for r in data if r.get("user_id") != user_id]
        write_json(FAV_FILE, data)


def is_admin(user_id: int) -> bool:
    # read admins from persistent file
    try:
        admins = read_json(ADMINS_FILE)
        # ensure ints
        admins_int = [int(x) for x in admins]
        return int(user_id) in admins_int
    except Exception:
        return user_id in ADMINS


def read_orders():
    return read_json(ORDERS_FILE)


def write_orders(data):
    write_json(ORDERS_FILE, data)


def get_orders_counts():
    orders = read_orders()
    counts = {"new": 0, "processing": 0, "done": 0, "cancelled": 0}
    for o in orders:
        st = o.get("status", "new")
        if st == "new":
            counts["new"] += 1
        elif st == "processing":
            counts["processing"] += 1
        elif st == "done":
            counts["done"] += 1
        elif st == "cancelled":
            counts["cancelled"] += 1
    return counts


def format_dt(ts: float):
    from datetime import datetime
    try:
        return datetime.fromtimestamp(ts).strftime("%d.%m.%Y %H:%M")
    except Exception:
        return "-"


def create_order(
    user,
    items,
    address_text: str,
    delivery_method: str = None,
    *,
    number: int | None = None,
    payment_id: str | None = None,
    created_at: float | None = None,
):
    """Create and persist an order.

    `user` is a telegram-like User object, `items` is list of dicts with keys: product_id, name, qty, price.
    If `number` is provided, it will be preserved (useful to match the payment description/order number).
    """
    with _interprocess_lock(_orders_lock_path()):
        orders = read_orders() or []
        new_id = get_next_id(orders)
        order_number = int(number) if number is not None else (1000 + len(orders) + 1)
        total = sum((it.get("price", 0) * it.get("qty", 1)) for it in items)
        from time import time
        now = time()
        created_ts = float(created_at) if created_at is not None else now
        # attach stored client profile if present
        profiles = read_profiles()
        profile = profiles.get(str(user.id), {})
        order = {
            "id": new_id,
            "number": order_number,
            "user_id": int(user.id),
            "username": user.username or "",
            "full_name": f"{user.first_name or ''} {user.last_name or ''}".strip(),
            "items": items,
            "total": total,
            "address": address_text,
            "delivery": delivery_method,
            "status": "new",
            "tracking_link": None,
            "created_at": created_ts,
            "updated_at": now,
            "client": {
                "first_name": profile.get("first_name"),
                "last_name": profile.get("last_name"),
                "patronymic": profile.get("patronymic"),
                "phone": profile.get("phone"),
            },
        }
        if payment_id:
            order["payment_id"] = str(payment_id)
        orders.append(order)
        write_json(ORDERS_FILE, orders)
        return order


def find_order(order_id: int):
    orders = read_orders()
    return next((o for o in orders if o.get("id") == order_id), None)


def update_order(order):
    with _interprocess_lock(_orders_lock_path()):
        orders = read_orders() or []
        for i, o in enumerate(orders):
            if o.get("id") == order.get("id"):
                orders[i] = order
                write_json(ORDERS_FILE, orders)
                return True
        return False


def _orders_by_created_date():
    from datetime import datetime
    orders = read_orders()
    # ensure created_at normalized
    for o in orders:
        if not o.get("created_at"):
            o["created_at"] = 0
    return orders


def compute_stats_summary():
    from datetime import datetime, timedelta
    orders = read_orders()
    # exclude cancelled for revenue
    non_cancel = [o for o in orders if o.get("status") != "cancelled"]
    total_orders = len(orders)
    total_revenue = sum(o.get("total", 0) for o in non_cancel)

    now = datetime.now()
    today = now.date()
    yesterday = (now - timedelta(days=1)).date()
    last7_from = (now - timedelta(days=6)).date()  # inclusive 7 days

    def sum_for_date(target_date):
        s = 0
        for o in non_cancel:
            try:
                d = datetime.fromtimestamp(o.get("created_at", 0)).date()
                if d == target_date:
                    s += o.get("total", 0)
            except Exception:
                pass
        return s

    today_sum = sum_for_date(today)
    yesterday_sum = sum_for_date(yesterday)
    last7_sum = 0
    for o in non_cancel:
        try:
            d = datetime.fromtimestamp(o.get("created_at", 0)).date()
            if d >= last7_from:
                last7_sum += o.get("total", 0)
        except Exception:
            pass

    return {
        "total_orders": total_orders,
        "total_revenue": total_revenue,
        "today": today_sum,
        "yesterday": yesterday_sum,
        "last7": last7_sum,
        "counts": get_orders_counts(),
    }


def stats_details():
    from datetime import datetime
    orders = read_orders()
    if not orders:
        return None
    total_orders = len(orders)
    non_cancel = [o for o in orders if o.get("status") != "cancelled"]
    avg_check = int(sum(o.get("total", 0) for o in non_cancel) / len(non_cancel)) if non_cancel else 0
    created_dates = [o.get("created_at", 0) for o in orders if o.get("created_at")]
    first = format_dt(min(created_dates)) if created_dates else "-"
    last = format_dt(max(created_dates)) if created_dates else "-"
    clients = len(set(o.get("user_id") for o in orders if o.get("user_id")))
    return {
        "total_orders": total_orders,
        "avg_check": avg_check,
        "first": first,
        "last": last,
        "clients": clients,
    }


def top_products(limit: int = 3):
    # count product sales across non-cancelled orders
    from collections import Counter
    orders = read_orders()
    counter = Counter()
    for o in orders:
        if o.get("status") == "cancelled":
            continue
        for it in o.get("items", []):
            name = it.get("name") or f"#{it.get('product_id')}"
            qty = int(it.get("qty", 1))
            counter[name] += qty
    top = counter.most_common(limit)
    return top


def read_broadcasts():
    return read_json(BROADS_FILE)


def write_broadcasts(data):
    with _interprocess_lock(_broadcasts_lock_path()):
        write_json(BROADS_FILE, data)


def read_notifications():
    with _interprocess_lock(_notifications_lock_path()):
        data = read_json(NOTIF_FILE, default={})
        return data if isinstance(data, dict) else {}


def write_notifications(cfg):
    with _interprocess_lock(_notifications_lock_path()):
        write_json(NOTIF_FILE, cfg if isinstance(cfg, dict) else {})


def get_recipients_list():
    # collect user ids from carts, favs, orders
    users = set()
    # include explicit users list (those who started the bot)
    try:
        raw_users = read_json(USERS_FILE)
        for u in raw_users:
            try:
                users.add(int(u))
            except Exception:
                pass
    except Exception:
        pass
    for rec in read_json(CART_FILE):
        try:
            users.add(int(rec.get("user_id")))
        except Exception:
            pass
    for rec in read_json(FAV_FILE):
        try:
            users.add(int(rec.get("user_id")))
        except Exception:
            pass
    for rec in read_orders():
        try:
            users.add(int(rec.get("user_id")))
        except Exception:
            pass
    # exclude admins
    try:
        adm = set(int(x) for x in read_json(ADMINS_FILE))
    except Exception:
        adm = set()
    users = users - adm
    return list(users)


def read_users():
    return read_json(USERS_FILE)


def write_users(data):
    with _interprocess_lock(_users_lock_path()):
        write_json(USERS_FILE, data)


def add_user_if_new(user_id: int):
    try:
        ensure_data_files()
    except Exception:
        pass
    try:
        with _interprocess_lock(_users_lock_path()):
            data = read_users()
            # normalize to ints
            try:
                ids = [int(x) for x in (data or [])]
            except Exception:
                ids = []
            if int(user_id) not in ids:
                ids.append(int(user_id))
                write_json(USERS_FILE, ids)
                return True
    except Exception:
        pass
    return False


def get_users_count() -> int:
    """Count unique users who ever triggered /start (tracked in users.json)."""
    try:
        ensure_data_files()
    except Exception:
        pass
    try:
        data = read_users()
        ids = []
        try:
            ids = [int(x) for x in (data or [])]
        except Exception:
            ids = []
        return len(set(ids))
    except Exception:
        return 0


def save_broadcast_record(entry):
    data = read_broadcasts()
    data.append(entry)
    write_broadcasts(data)


def read_addresses():
    return read_json(ADDR_FILE, default={})


def write_addresses(data):
    with _interprocess_lock(_addresses_lock_path()):
        write_json(ADDR_FILE, data if isinstance(data, dict) else {})


def read_profiles():
    return read_json(PROFILE_FILE, default={})


def write_profiles(data):
    with _interprocess_lock(_profiles_lock_path()):
        write_json(PROFILE_FILE, data if isinstance(data, dict) else {})


def read_pending_orders():
    return read_json(PENDING_FILE)


def write_pending_orders(data):
    with _interprocess_lock(_pending_lock_path()):
        write_json(PENDING_FILE, data)


def next_order_number(orders=None, pend=None):
    """Next order number unique across orders + pending."""
    if orders is None or pend is None:
        with _interprocess_lock(_orders_pending_lock_path()):
            orders = read_orders() or []
            pend = read_pending_orders() or []
    orders = orders or []
    pend = pend or []
    all_numbers = [o.get("number", 0) for o in (orders + pend)]
    return max(all_numbers, default=1000) + 1


def create_pending_order(user, items, address_text: str, delivery_method: str | None, order_type: str | None = None):
    total = sum((it.get("price", 0) * it.get("qty", 1)) for it in items)
    with _interprocess_lock(_orders_pending_lock_path()):
        orders = read_orders() or []
        pend = read_pending_orders() or []
        new_id = get_next_id(pend)
        number = next_order_number(orders=orders, pend=pend)
    from time import time
    now = time()
    profiles = read_profiles()
    profile = profiles.get(str(user.id), {})
    pending = {
        "id": new_id,
        "number": number,
        "user_id": int(user.id),
        "username": user.username or "",
        "full_name": f"{user.first_name or ''} {user.last_name or ''}".strip(),
        "items": items,
        "total": total,
        "address": address_text,
        "delivery": delivery_method,
        "status": "new",  # awaiting payment
        "created_at": now,
        "client": {
            "first_name": profile.get("first_name"),
            "last_name": profile.get("last_name"),
            "patronymic": profile.get("patronymic"),
            "phone": profile.get("phone"),
        },
        # preserve checkout source to allow cart cleanup on success
        "type": order_type,
        "payment_id": None,
    }
    with _interprocess_lock(_pending_lock_path()):
        pend_now = read_pending_orders() or []
        pend_now.append(pending)
        write_json(PENDING_FILE, pend_now)
    return pending


def create_yookassa_payment(order_like):
    if Payment is None or Configuration is None:
        raise RuntimeError("YooKassa SDK not installed")

    shop_id = os.getenv("YOOKASSA_SHOP_ID")
    secret_key = os.getenv("YOOKASSA_SECRET_KEY")
    if not shop_id or not secret_key:
        raise RuntimeError("YooKassa credentials missing: set YOOKASSA_SHOP_ID and YOOKASSA_SECRET_KEY in .env")

    # Configure SDK credentials
    Configuration.account_id = shop_id
    Configuration.secret_key = secret_key

    from decimal import Decimal, ROUND_HALF_UP

    def money(value) -> str:
        try:
            return str(Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
        except Exception:
            return "0.00"

    def normalize_phone(raw: str | None) -> str | None:
        if not raw:
            return None
        s = str(raw).strip()
        # keep digits only
        digits = "".join(ch for ch in s if ch.isdigit())
        if not digits:
            return None
        # common RU normalization: 8XXXXXXXXXX -> 7XXXXXXXXXX
        if len(digits) == 11 and digits.startswith("8"):
            digits = "7" + digits[1:]
        return digits

    return_url = os.getenv("YOOKASSA_RETURN_URL") or f"https://t.me/{os.getenv('BOT_USERNAME','YOUR_BOT_USERNAME')}"
    # Deterministic idempotence key to prevent duplicate payments on retries.
    try:
        oid = int(order_like.get("id"))
        idempotence_key = f"pay_{oid}"
    except Exception:
        idempotence_key = str(uuid.uuid4())
    idempotence_key = str(idempotence_key)[:64]

    # Build receipt per YooKassa requirements
    try:
        vat_code = int(os.getenv("YOOKASSA_VAT_CODE") or 1)
    except Exception:
        vat_code = 1
    try:
        tax_system_code = int(os.getenv("YOOKASSA_TAX_SYSTEM_CODE") or 1)
    except Exception:
        tax_system_code = 1

    items_receipt = []
    receipt_sum = Decimal("0.00")
    for it in (order_like.get("items") or []):
        try:
            qty = Decimal(str(it.get("qty", 1) or 1))
        except Exception:
            qty = Decimal("1")
        if qty <= 0:
            qty = Decimal("1")
        # YooKassa accepts quantity with decimals; keep 2 dp to be safe
        qty_str = str(qty.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

        price_str = money(it.get("price", 0))
        try:
            line_total = (Decimal(price_str) * Decimal(qty_str)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        except Exception:
            line_total = Decimal("0.00")
        receipt_sum += line_total

        items_receipt.append({
            "description": sanitize_yookassa_description(it.get("name", "Товар")),
            "quantity": qty_str,
            "amount": {
                "value": price_str,
                "currency": "RUB"
            },
            "vat_code": vat_code,
            "payment_subject": "commodity",
            "payment_mode": "full_payment"
        })

    if not items_receipt:
        raise RuntimeError("Receipt is missing items (cart is empty)")

    receipt = {
        "type": "payment",
        "items": items_receipt,
        "tax_system_code": tax_system_code,
    }

    # 54-ФЗ: receipt MUST contain customer contact (phone or email)
    client = order_like.get("client") or {}
    phone_norm = normalize_phone(client.get("phone"))
    email = (client.get("email") or os.getenv("YOOKASSA_DEFAULT_EMAIL") or "").strip() or None
    full_name = f"{client.get('first_name') or ''} {client.get('last_name') or ''} {client.get('patronymic') or ''}".strip()

    if not phone_norm and not email:
        raise RuntimeError("Receipt customer contact missing: phone/email is required")

    receipt["customer"] = {}
    if phone_norm:
        receipt["customer"]["phone"] = phone_norm
    if email:
        receipt["customer"]["email"] = email
    if full_name:
        receipt["customer"]["full_name"] = full_name

    # Ensure payment amount matches receipt sum (common cause of 'illegal receipt')
    amount_value = money(receipt_sum)

    payment = Payment.create({
        "amount": {
            "value": amount_value,
            "currency": "RUB"
        },
        "confirmation": {
            "type": "redirect",
            "return_url": return_url
        },
        "capture": True,
        "receipt": receipt,
        "description": f"Оплата заказа #{order_like['number']}",
        "metadata": {
            "order_id": order_like["id"],
            "user_id": order_like["user_id"],
            "source": "telegram_bot"
        }
    }, idempotence_key)

    return payment.confirmation.confirmation_url, payment.id


async def notify_admin_low_stock(context: ContextTypes.DEFAULT_TYPE, product: dict):
    text = (
        "⚠️ *Мало товара*\n\n"
        f"{(product.get('name') or '-').strip()}\n"
        f"📦 Осталось: {product.get('stock', 0)} шт\n"
        f"🆔 ID: {product.get('id')}"
    )
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔎 Открыть товар", callback_data=f"prod:{product.get('id')}")],
        [InlineKeyboardButton("➕ Пополнить товар", callback_data=f"admin_restock:{product.get('id')}")],
    ])
    admins = read_json(ADMINS_FILE)
    for aid in admins:
        try:
            await _send_message_safe(context.bot, chat_id=int(aid), text=text, reply_markup=keyboard)
        except Exception:
            pass


async def notify_admin_out_of_stock(context: ContextTypes.DEFAULT_TYPE, product: dict):
    text = (
        "⛔ *Товар закончился*\n\n"
        f"{(product.get('name') or '-').strip()}\n"
        f"🆔 ID: {product.get('id')}"
    )
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🔎 Открыть товар", callback_data=f"prod:{product.get('id')}")],
        [InlineKeyboardButton("➕ Пополнить товар", callback_data=f"admin_restock:{product.get('id')}")],
    ])
    admins = read_json(ADMINS_FILE)
    for aid in admins:
        try:
            await _send_message_safe(context.bot, chat_id=int(aid), text=text, reply_markup=keyboard)
        except Exception:
            pass


async def notify_admin_new_order(context: ContextTypes.DEFAULT_TYPE, order: dict):
    """Notify admins that a new paid order was created."""
    try:
        admins = read_json(ADMINS_FILE)
    except Exception:
        admins = []
    if not admins:
        return
    items = order.get("items", []) or []
    lines = []
    for it in items[:10]:
        lines.append(f"• {it.get('name','-')} × {it.get('qty',1)}")
    if len(items) > 10:
        lines.append(f"… ещё {len(items) - 10} поз.")
    delivery = order.get("delivery") or "-"

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
        "🆕 *Новый оплаченный заказ*\n\n"
        f"🧾 Заказ #{order.get('number')}\n"
        f"💰 Сумма: {order.get('total', 0)} ₽\n"
        f"🚚 Доставка: {delivery}\n"
        f"📍 Адрес: {order.get('address','-')}\n\n"
        f"👤 Клиент: {fio}\n"
        f"📞 Телефон: {phone}\n"
        f"Telegram: `{tg}`\n"
        f"ID: {order.get('user_id')}\n\n"
        "📦 Товары:\n" + ("\n".join(lines) if lines else "• -")
    )
    for aid in admins:
        try:
            await context.bot.send_message(chat_id=aid, text=text, parse_mode="Markdown")
        except Exception:
            pass


async def do_send_broadcast(context, text: str, photo: str | None, recipients: list):
    delivered = 0
    bot = context.bot
    for uid in recipients:
        try:
            if photo:
                await _send_photo_safe(bot, chat_id=int(uid), photo=photo, caption=text)
            else:
                await _send_message_safe(bot, chat_id=int(uid), text=text)
            delivered += 1
        except Exception:
            # skip users who blocked bot
            continue
    return delivered


async def notify_new_product(context: ContextTypes.DEFAULT_TYPE, product: dict):
    """Notify all users about a newly added product using notifications settings."""
    try:
        cfg = read_notifications()
    except Exception:
        cfg = {}
    np = cfg.get("new_product", {})
    if not np.get("enabled"):
        return
    template = np.get(
        "template",
        "🆕 Появился новый товар!\n\n{name}\n💰 Цена: {price}\n\n👇 Нажмите, чтобы посмотреть",
    )
    try:
        text = template.format(name=product.get("name"), price=product.get("price"))
    except Exception:
        name = product.get("name", "Товар")
        price = product.get("price", "-")
        text = f"🆕 Появился новый товар!\n\n{name}\n💰 Цена: {price}\n\n👇 Нажмите, чтобы посмотреть"

    keyboard = InlineKeyboardMarkup(
        [[InlineKeyboardButton(product.get("name", "Товар"), callback_data=f"user_prod:{product.get('id')}")]]
    )

    recipients = get_recipients_list()
    bot = context.bot
    for uid in recipients:
        try:
            await bot.send_message(chat_id=uid, text=text, reply_markup=keyboard)
        except Exception:
            # ignore users who blocked the bot or errors
            pass


def admin_keyboard():
    # Главное меню для админа: те же кнопки, что у пользователя, + кнопка админ‑панель
    return ReplyKeyboardMarkup([
        ["📂 Каталоги", "🛒 Корзина"],
        ["⭐ Избранное", "📦 Мои заказы"],
        ["🛠 Админ панель"]
    ], resize_keyboard=True)


def admin_menu_keyboard():
    # Removed settings button as requested
    return ReplyKeyboardMarkup(
        [["📂 Каталог", "📦 Заказы"], ["📊 Статистика", "📢 Рассылка"], ["🔙 Выйти из админки"]],
        resize_keyboard=True,
    )


def user_main_keyboard():
    return ReplyKeyboardMarkup(
        [["📂 Каталоги", "🛒 Корзина"], ["⭐ Избранное", "📦 Мои заказы"], ["ℹ️ О магазине", "⭐ Отзывы"]],
        resize_keyboard=True,
    )


def main_menu_keyboard(user_id: int):
    return admin_keyboard() if is_admin(user_id) else user_main_keyboard()


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = update.effective_user.id
    try:
        ensure_data_files()
    except Exception:
        pass
    # ensure we track this user for broadcasts
    try:
        add_user_if_new(user_id)
    except Exception:
        pass
    try:
        users_count = get_users_count()
    except Exception:
        users_count = 0
    text = (
        "Привет 👋\n"
        f"👥 Пользователей: {users_count}\n"
        "Добро пожаловать в наш магазин.\n\n"
        "ℹ️ Информация — кнопка ‘О магазине’\n"
        "🛒 Заказы — через каталог\n"
        "📞 Поддержка — @asudarew"
    )
    if is_admin(user_id):
        await update.message.reply_text(text, reply_markup=admin_keyboard())
    else:
        await update.message.reply_text(text, reply_markup=user_main_keyboard())


async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = update.message.text
    user_id = update.effective_user.id
    try:
        print(f"MSG from {user_id}: text={text}")
    except Exception:
        print("MSG received: (unable to read user/text)")
    # track user activity for broadcasts
    try:
        add_user_if_new(user_id)
    except Exception:
        pass
    # allow user to cancel any in-progress operation via text
    if text in ("Отмена", "❌ Отмена", "/cancel"):
        context.user_data.pop("state", None)
        context.user_data.pop("pending_order", None)
        await update.message.reply_text("Отменено.")
        return
    # user menu: handle these texts for all users (admins too)
    t = text.strip()
    if t == "📂 Каталоги":
        context.user_data.pop("state", None)
        context.user_data.pop("pending_order", None)
        await show_user_categories(update, context)
        try:
            await update.message.delete()
        except Exception as e:
            logger.debug(f"Cannot delete message: {e}")
        return
    if t == "🛒 Корзина":
        context.user_data.pop("state", None)
        context.user_data.pop("pending_order", None)
        await show_user_cart(update, context)
        try:
            await update.message.delete()
        except Exception as e:
            logger.debug(f"Cannot delete message: {e}")
        return
    if t == "⭐ Избранное":
        context.user_data.pop("state", None)
        context.user_data.pop("pending_order", None)
        await show_user_favorites(update, context)
        try:
            await update.message.delete()
        except Exception as e:
            logger.debug(f"Cannot delete message: {e}")
        return
    if t == "📦 Мои заказы":
        context.user_data.pop("state", None)
        context.user_data.pop("pending_order", None)
        await show_user_orders(update, context)
        try:
            await update.message.delete()
        except Exception as e:
            logger.debug(f"Cannot delete message: {e}")
        return
    if t == "ℹ️ О магазине":
        text_info = (
            "🏪 *Информация о магазине*\n\n"
            "Мы — онлайн-магазин.\n"
            "Работаем через Telegram-бота.\n\n"
            "📦 Что умеет бот:\n"
            "• Просмотр каталога товаров\n"
            "• Добавление в корзину и избранное\n"
            "• Оформление заказа\n"
            "• Выбор доставки\n"
            "• Просмотр статуса заказов\n\n"
            "📞 Поддержка:\n"
            "Если возникли вопросы — напишите в поддержку:\n"
            "👉 @asudarew\n\n"
            "⏰ Поддержка отвечает в рабочее время"
        )
        await update.message.reply_text(text_info, parse_mode="Markdown")
        return

    if t in ("⭐ Отзывы", "Отзывы"):
        await _send_reviews_link(update, context)
        return
    # allow entering product name even if we're in photos-collecting state
    state = context.user_data.get("state")
    if state and state.startswith("addprod_photos:"):
        _, cat_id = state.split(":")
        cat_id = int(cat_id)
        newp = context.user_data.get("new_product") or {"photos": [], "category_id": cat_id}
        newp["name"] = text.strip()
        newp["category_id"] = cat_id
        context.user_data["new_product"] = newp
        context.user_data["state"] = f"addprod_desc:{cat_id}"
        await update.message.reply_text("✅ Название добавлено\n✏️ Введите описание товара")
        return
    if text == "🛠 Админ панель" and is_admin(user_id):
        await update.message.reply_text("Вы в админ-панели:", reply_markup=admin_menu_keyboard())
        return

    if text == "📂 Каталог" and is_admin(user_id):
        await show_categories(update, context)
        return

    if text == "📦 Заказы" and is_admin(user_id):
        await show_orders_admin(update, context)
        return

    if text == "📊 Статистика" and is_admin(user_id):
        await show_stats_admin(update, context)
        return

    # Settings removed from admin menu

    if text == "📢 Рассылка" and is_admin(user_id):
        await show_broadcast_menu(update, context)
        return

    if text == "🔙 Выйти из админки" and is_admin(user_id):
        await update.message.reply_text("Вы вышли из админки.", reply_markup=admin_keyboard())
        return

    # user-facing catalogs/cart/favs/orders handled above for all users

    if text == "🔙 Назад" and is_admin(user_id):
        await update.message.reply_text("Возврат.", reply_markup=admin_keyboard())
        return

    state = context.user_data.get("state")
    if state == "adding_category":
        name = text.strip()
        cats = read_json(CATS_FILE)
        new_id = get_next_id(cats)
        parent = context.user_data.pop("parent_cat", None)
        item = {"id": new_id, "name": name}
        if parent is not None:
            item["parent_id"] = parent
        cats.append(item)
        write_json(CATS_FILE, cats)
        await update.message.reply_text(f"✅ Каталог «{name}» успешно создан")
        context.user_data.pop("state", None)
        # if created as subcategory, reopen parent view, otherwise show root categories
        if parent is not None:
                await show_category(update.message, context, parent)
        else:
            await show_categories(update, context)
        return

    if state == "adding_admin":
        text_id = text.strip()
        try:
            aid = int(text_id)
        except ValueError:
            await update.message.reply_text("Неверный ID. Введите numeric ID пользователя.")
            return
        add_admin(aid)
        context.user_data.pop("state", None)
        await update.message.reply_text(f"✅ Админ {aid} добавлен.")
        await update.message.reply_text("Вы в админ-панели:", reply_markup=admin_menu_keyboard())
        return

    if state and state.startswith("admin_adding_tracking:"):
        _, oid = state.split(":",1)
        try:
            oid = int(oid)
        except Exception:
            context.user_data.pop("state", None)
            await update.message.reply_text("Неверный идентификатор заказа.")
            return
        link = text.strip()
        order = find_order(oid)
        if not order:
            context.user_data.pop("state", None)
            await update.message.reply_text("Заказ не найден")
            return
        order['tracking_link'] = link
        from time import time
        order['updated_at'] = time()
        update_order(order)
        context.user_data.pop("state", None)
        await update.message.reply_text(f"✅ Ссылка для отслеживания сохранена для заказа #{order.get('number')}")
        # notify customer
        try:
            await context.bot.send_message(chat_id=order.get('user_id'), text=f"В ваш заказ #{order.get('number')} добавлена ссылка для отслеживания:\n{link}")
        except Exception:
            pass
        return

    if state == "removing_admin":
        text_id = text.strip()
        try:
            aid = int(text_id)
        except ValueError:
            await update.message.reply_text("Неверный ID. Введите numeric ID пользователя.")
            return
        remove_admin(aid)
        context.user_data.pop("state", None)
        await update.message.reply_text(f"✅ Админ {aid} удалён (если был).")
        await update.message.reply_text("Вы в админ-панели:", reply_markup=admin_menu_keyboard())
        return

    if state and state.startswith("renaming_cat:"):
        _, cat_id = state.split(":")
        cat_id = int(cat_id)
        new_name = text.strip()
        with _interprocess_lock(_cats_lock_path()):
            cats = read_json(CATS_FILE, default=[])
            cats = cats if isinstance(cats, list) else []
            for c in cats:
                if c["id"] == cat_id:
                    c["name"] = new_name
                    break
            write_json(CATS_FILE, cats)
        context.user_data.pop("state", None)
        await update.message.reply_text(f"✅ Каталог переименован")
        await show_category(update.message, context, cat_id)
        return

    if state and state.startswith("addprod_name:"):
        _, cat_id = state.split(":")
        cat_id = int(cat_id)
        # preserve photos if already present
        newp = context.user_data.get("new_product") or {"photos": [], "category_id": cat_id}
        newp["name"] = sanitize_product_name(text)
        newp["category_id"] = cat_id
        context.user_data["new_product"] = newp
        context.user_data["state"] = f"addprod_desc:{cat_id}"
        await update.message.reply_text("✅ Название добавлено\n✏️ Введите описание товара")
        return

    if state and state.startswith("addprod_desc:"):
        _, cat_id = state.split(":")
        context.user_data["new_product"]["description"] = text.strip()
        context.user_data["state"] = f"addprod_price:{cat_id}"
        await update.message.reply_text("💲 Введите цену (числом)")
        return

    # Profile collection flow: first name, last name, patronymic, phone
    if state == "profile_first_name":
        with _interprocess_lock(_profiles_lock_path()):
            profiles = read_profiles()
            uid = str(update.effective_user.id)
            profiles.setdefault(uid, {})
            profiles[uid]["first_name"] = update.message.text.strip()
            write_json(PROFILE_FILE, profiles)
        context.user_data["state"] = "profile_last_name"
        await update.message.reply_text("👤 Введите *фамилию*:", parse_mode="Markdown")
        return

    if state == "profile_last_name":
        with _interprocess_lock(_profiles_lock_path()):
            profiles = read_profiles()
            uid = str(update.effective_user.id)
            profiles.setdefault(uid, {})
            profiles[uid]["last_name"] = update.message.text.strip()
            write_json(PROFILE_FILE, profiles)
        context.user_data["state"] = "profile_patronymic"
        await update.message.reply_text("👤 Введите *отчество*:", parse_mode="Markdown")
        return

    if state == "profile_patronymic":
        with _interprocess_lock(_profiles_lock_path()):
            profiles = read_profiles()
            uid = str(update.effective_user.id)
            profiles.setdefault(uid, {})
            profiles[uid]["patronymic"] = update.message.text.strip()
            write_json(PROFILE_FILE, profiles)
        # If phone already exists (old users), do not force re-entering it.
        try:
            phone_existing = (profiles.get(uid, {}).get("phone") or "").strip()
        except Exception:
            phone_existing = ""
        if phone_existing:
            context.user_data.pop("state", None)
            await update.message.reply_text(
                "✅ Данные сохранены\n\n🔒 Ваши данные используются только для оформления заказа",
                reply_markup=main_menu_keyboard(update.effective_user.id),
            )
            if context.user_data.get("pending_order"):
                await show_delivery_selection_from_context(update, context)
            return

        context.user_data["state"] = "profile_phone"
        keyboard = ReplyKeyboardMarkup([[KeyboardButton("📞 Отправить номер", request_contact=True)]], resize_keyboard=True, one_time_keyboard=True)
        await update.message.reply_text("📞 Отправьте номер телефона кнопкой ниже\nили введите вручную:", reply_markup=keyboard)
        return

    if state == "profile_phone":
        # accept manual entry if present
        phone = update.message.text.strip() if update.message and update.message.text else None
        if phone:
            with _interprocess_lock(_profiles_lock_path()):
                profiles = read_profiles()
                uid = str(update.effective_user.id)
                profiles.setdefault(uid, {})
                profiles[uid]["phone"] = phone
                write_json(PROFILE_FILE, profiles)
            context.user_data.pop("state", None)
            await update.message.reply_text(
                "✅ Данные сохранены\n\n🔒 Ваши данные используются только для оформления заказа",
                reply_markup=main_menu_keyboard(update.effective_user.id),
            )
            if context.user_data.get("pending_order"):
                await show_delivery_selection_from_context(update, context)
            return

    # New PVZ input per delivery type
    if state and state.startswith("pvz_input:"):
        delivery = state.split(":", 1)[1]
        pvz = update.message.text.strip()
        with _interprocess_lock(_addresses_lock_path()):
            addrs = read_addresses()
            uid = str(update.effective_user.id)
            raw = addrs.get(uid)
            # migrate old list format to dict-per-delivery on first PVZ input
            if raw is None:
                addrs[uid] = {}
            elif isinstance(raw, list):
                addrs[uid] = {}
            # ensure nested structure per delivery
            addrs[uid].setdefault(delivery, [])
            if pvz and pvz not in addrs[uid][delivery]:
                addrs[uid][delivery].append(pvz)
            write_json(ADDR_FILE, addrs)
        pending = context.user_data.get("pending_order", {})
        pending["address"] = pvz
        pending["delivery"] = delivery
        context.user_data["pending_order"] = pending
        context.user_data.pop("state", None)
        await update.message.reply_text("✅ ПВЗ сохранён")
        await finalize_order(update, context)
        return

    if state and state.startswith("addprod_price:"):
        _, cat_id = state.split(":")
        price_text = update.message.text.strip().replace(",", ".")
        try:
            price = int(float(price_text))
        except ValueError:
            await update.message.reply_text("Неверный формат цены. Введите число.")
            return
        context.user_data["new_product"]["price"] = price
        context.user_data["state"] = "addprod_stock"
        await update.message.reply_text(
            "📦 Введите *доступное количество* товара:",
            parse_mode="Markdown"
        )
        return

    if state == "addprod_stock":
        try:
            stock = int(update.message.text.strip())
        except ValueError:
            await update.message.reply_text("Неверный формат количества. Введите целое число.")
            return
        prod = context.user_data.get("new_product", {})
        try:
            prod["name"] = sanitize_product_name(prod.get("name"))
        except Exception:
            pass
        prod["stock"] = stock
        with _interprocess_lock(_products_lock_path()):
            prods = read_json(PROD_FILE, default=[])
            prods = prods if isinstance(prods, list) else []
            new_id = get_next_id(prods)
            prod["id"] = new_id
            prods.append(prod)
            write_json(PROD_FILE, prods)
        context.user_data.pop("new_product", None)
        context.user_data.pop("state", None)
        await update.message.reply_text("✅ Товар добавлен")
        try:
            await notify_new_product(context, prod)
        except Exception:
            pass
        # Notify subscribers if this product was previously awaited (rare but safe)
        try:
            if int(prod.get("stock", 0) or 0) > 0:
                await notify_users_product_available(context, int(prod.get("id")), prod.get("name"))
        except Exception:
            pass
        # show product card to admin
        await send_product_card(update.message.chat_id, context, prod)
        return

    if state and state.startswith("editprod_name:"):
        _, prod_id = state.split(":")
        prod_id = int(prod_id)
        new_name = sanitize_product_name(text)
        prod = None
        with _interprocess_lock(_products_lock_path()):
            prods = read_json(PROD_FILE, default=[])
            prods = prods if isinstance(prods, list) else []
            for p in prods:
                if p.get("id") == prod_id:
                    p["name"] = new_name
                    cat_id = p.get("category_id")
                    prod = dict(p)
                    break
            write_json(PROD_FILE, prods)
        context.user_data.pop("state", None)
        await update.message.reply_text("✅ Название обновлено")
        # show updated product card
        if prod:
            await send_product_card(update.message.chat_id, context, prod)
        return

    if state and state.startswith("editprod_desc:"):
        _, prod_id = state.split(":")
        prod_id = int(prod_id)
        new_desc = text.strip()
        prod = None
        with _interprocess_lock(_products_lock_path()):
            prods = read_json(PROD_FILE, default=[])
            prods = prods if isinstance(prods, list) else []
            for p in prods:
                if p.get("id") == prod_id:
                    p["description"] = new_desc
                    cat_id = p.get("category_id")
                    prod = dict(p)
                    break
            write_json(PROD_FILE, prods)
        context.user_data.pop("state", None)
        await update.message.reply_text("✅ Описание обновлено")
        if prod:
            await send_product_card(update.message.chat_id, context, prod)
        return

    if state and state.startswith("editprod_price:"):
        _, prod_id = state.split(":")
        prod_id = int(prod_id)
        price_text = text.strip().replace(",", ".")
        try:
            price = float(price_text)
        except ValueError:
            await update.message.reply_text("Неверный формат цены. Введите число.")
            return
        prod = None
        with _interprocess_lock(_products_lock_path()):
            prods = read_json(PROD_FILE, default=[])
            prods = prods if isinstance(prods, list) else []
            for p in prods:
                if p.get("id") == prod_id:
                    p["price"] = price
                    cat_id = p.get("category_id")
                    prod = dict(p)
                    break
            write_json(PROD_FILE, prods)
        context.user_data.pop("state", None)
        await update.message.reply_text("✅ Цена обновлена")
        if prod:
            await send_product_card(update.message.chat_id, context, prod)
        return

    if state and state.startswith("admin_restock_input:"):
        try:
            prod_id = int(state.split(":")[1])
        except Exception:
            context.user_data.pop("state", None)
            await update.message.reply_text("Ошибка: товар не найден")
            return
        try:
            raw = (update.message.text or "").strip()
            # allow formats: "+10", "-9", "- 9", "10" (set absolute)
            compact = raw.replace(" ", "")
            if not compact:
                raise ValueError
            if compact[0] in ("+", "-"):
                delta = int(compact)
                if delta == 0:
                    raise ValueError
                mode = "delta"
            else:
                new_abs = int(compact)
                if new_abs < 0:
                    raise ValueError
                mode = "abs"
        except ValueError:
            await update.message.reply_text(
                "❌ Введите число в формате: +10 (прибавить), -9 (убавить), 25 (установить остаток)."
            )
            return
        prod = None
        name = None
        stock = None
        old_stock = 0
        with _interprocess_lock(_products_lock_path()):
            prods = read_json(PROD_FILE, default=[])
            prods = prods if isinstance(prods, list) else []
            for p in prods:
                if p.get("id") == prod_id:
                    old_stock = int(p.get("stock", 0) or 0)
                    if mode == "delta":
                        p["stock"] = max(0, old_stock + int(delta))
                    else:
                        p["stock"] = int(new_abs)
                    name = p.get("name")
                    stock = p.get("stock")
                    prod = dict(p)
                    break
            write_json(PROD_FILE, prods)
        context.user_data.pop("state", None)
        await update.message.reply_text(
            f"✅ Остаток обновлён: *{name}*\n📦 В наличии: {stock} шт",
            parse_mode="Markdown"
        )
        # show updated card if possible
        if prod:
            await send_product_card(update.message.chat_id, context, prod)

        # If product became available from 0 -> >0, notify subscribed users
        try:
            if int(old_stock) <= 0 and int(stock or 0) > 0:
                await notify_users_product_available(context, int(prod_id), name)
        except Exception:
            pass
        return

    if state == "broadcast_text":
        # admin entered broadcast text
        b = {"text": text.strip(), "photo": None}
        context.user_data["broadcast"] = b
        context.user_data["state"] = "broadcast_confirm"
        recipients = get_recipients_list()
        cnt = len(recipients)
        # show preview (no photo yet)
        keyboard = [
            [InlineKeyboardButton("🚀 Отправить", callback_data="broadcast_send") , InlineKeyboardButton("❌ Отмена", callback_data="broadcast_cancel")],
            [InlineKeyboardButton("➕ Добавить фото", callback_data="broadcast_add_photo")],
        ]
        await update.message.reply_text(f"📢 Предпросмотр рассылки:\n\n{b['text']}\n\nПолучателей: {cnt}", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if state == "notif_edit_new_product":
        # admin set new product notification template
        tpl = text.strip()
        cfg = read_notifications()
        cfg.setdefault("new_product", {})["template"] = tpl
        write_notifications(cfg)
        context.user_data.pop("state", None)
        await update.message.reply_text("✅ Шаблон сохранён.")
        return

    # ordering flow: single product or cart
    if state and state.startswith("ordering_prod:"):
        _, prod_id = state.split(":")
        prod_id = int(prod_id)
        # user provided address text -> save pending order and ask for delivery method
        address = text.strip()
        prods = read_json(PROD_FILE)
        prod = next((p for p in prods if p.get("id") == prod_id), None)
        if not prod:
            await update.message.reply_text("Ошибка: товар не найден")
            context.user_data.pop("state", None)
            return
        items = [{"product_id": prod_id, "name": prod.get("name"), "qty": 1, "price": prod.get("price", 0)}]
        # store pending order until delivery method selected, then show delivery options
        context.user_data["pending_order"] = {"type": "prod", "items": items, "address": address}
        context.user_data.pop("state", None)
        await show_delivery_selection_from_context(update, context)
        return

    if state == "ordering_new_address":
        # save new address and ask for delivery method
        addr = text.strip()
        uid = str(update.effective_user.id)
        addrs = read_addresses()
        user_addrs = addrs.get(uid, [])
        if addr not in user_addrs:
            user_addrs.append(addr)
            addrs[uid] = user_addrs
            write_addresses(addrs)
        context.user_data.pop("state", None)
        pending = context.user_data.get("pending_order")
        if not pending:
            await update.message.reply_text(f"✅ Адрес сохранён: {addr}")
            return
        # save address to pending order and ask for delivery method
        pending["address"] = addr
        context.user_data["pending_order"] = pending
        await show_delivery_selection_from_context(update, context)
        return

    if state and state == "ordering_cart":
        # save cart + address as pending and ask for delivery method
        address = text.strip()
        user = update.effective_user.id
        cart_items = get_cart_items(user)
        if not cart_items:
            await update.message.reply_text("Ваша корзина пуста.")
            context.user_data.pop("state", None)
            return
        prods = read_json(PROD_FILE)
        prods_by_id = {int(p.get("id")): p for p in prods if p.get("id") is not None}
        items = []
        removed_any = False
        for ci in cart_items:
            try:
                pid = int(ci.get("product_id"))
                qty = int(ci.get("qty", 1) or 1)
            except Exception:
                continue
            p = prods_by_id.get(pid)
            if not p:
                removed_any = True
                try:
                    remove_from_cart(user, pid)
                except Exception:
                    pass
                continue
            stock = int(p.get("stock", 0) or 0)
            if stock <= 0 or qty > stock:
                name = (p.get("name") or "-").strip()
                await update.message.reply_text(f"❌ Недостаточно товара: {name}\nДоступно: {stock}")
                context.user_data.pop("state", None)
                return
            price = ci.get("price")
            if price is None:
                price = p.get("price", 0)
            items.append({"product_id": pid, "name": p.get('name'), "qty": qty, "price": price})

        if not items:
            msg = "Ваша корзина пуста."
            if removed_any:
                msg = "🛒 В корзине были удалённые товары — я убрал их. Корзина пуста."
            await update.message.reply_text(msg)
            context.user_data.pop("state", None)
            return
        context.user_data["pending_order"] = {"type": "cart", "items": items, "address": address}
        context.user_data.pop("state", None)
        await show_delivery_selection_from_context(update, context)
        return


async def show_categories(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ensure_data_files()
    text, markup = get_categories_markup()
    # delete previous media messages (if any) to avoid thumbnail previews
    try:
        await _cleanup_last_media(context, update.message.chat_id)
    except Exception:
        pass
    # send as a normal message
    await update.message.reply_text(text, reply_markup=markup)


def get_categories_markup():
    cats = read_json(CATS_FILE)
    # show only root categories (no parent_id)
    root_cats = [c for c in cats if c.get("parent_id") is None]
    text = "📂 Основные каталоги\nВыберите каталог или создайте новый"
    keyboard = []
    for c in root_cats:
        keyboard.append([InlineKeyboardButton(f"🗂 {c['name']}", callback_data=f"cat:{c['id']}")])
    keyboard.append([
        InlineKeyboardButton("➕ Добавить каталог", callback_data="add_category"),
    ])
    return text, InlineKeyboardMarkup(keyboard)


def get_cat_name(cat_id: int) -> str:
    cats = read_json(CATS_FILE)
    for c in cats:
        if c["id"] == cat_id:
            return c["name"]
    return "-"


async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    try:
        await _callback_handler_impl(update, context)
    except Exception as e:
        # Ensure Telegram UI isn't stuck on "loading"
        try:
            if query:
                try:
                    await query.answer()
                except Exception:
                    pass
        except Exception:
            pass

        # User-facing error message
        try:
            if query:
                await safe_edit_message(query, "❌ Произошла ошибка. Попробуйте ещё раз через пару секунд.")
            else:
                uid = update.effective_user.id if update and update.effective_user else None
                if uid is not None:
                    await context.bot.send_message(chat_id=int(uid), text="❌ Произошла ошибка. Попробуйте ещё раз.")
        except Exception:
            pass

        try:
            logger.exception("Unhandled exception in callback_handler: %s", e)
        except Exception:
            pass


async def _callback_handler_impl(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    try:
        print(f"CB from {query.from_user.id}: data={query.data}")
    except Exception:
        print("CB received: (unable to read user/data)")
    await query.answer()
    # Remember the current message as the main UI screen.
    try:
        if query and query.message:
            context.user_data["screen"] = {"chat_id": int(query.message.chat_id), "message_id": int(query.message.message_id)}
    except Exception:
        pass
    data = query.data
    user_id = query.from_user.id

    # Admin safety: if some admin restock flow mistakenly uses user qty_* callbacks,
    # interpret them as selecting a product for restock (not user quantity controls).
    try:
        if is_admin(user_id) and (data.startswith("qty_inc:") or data.startswith("qty_dec:")):
            msg_text = ""
            try:
                msg_text = (query.message.text or query.message.caption or "")
            except Exception:
                msg_text = ""
            low = (msg_text or "").lower()
            if "пополн" in low and "выберите" in low:
                prod_id = int(data.split(":", 1)[1])
                context.user_data["state"] = f"admin_restock_input:{prod_id}"
                await safe_edit_message(query, "➕ Введите количество для пополнения товара:")
                return
    except Exception:
        pass
    # Handle user-facing callbacks first (always), so user buttons work even if admin callbacks exist
    if (
        data.startswith("user_") or
        data.startswith("notify:") or
        data.startswith("use_address:") or
        data.startswith("delivery:") or
        data.startswith("delivery_select:") or
        data.startswith("new_pvz:") or
        data.startswith("use_pvz:") or
        data.startswith("qty_inc:") or
        data.startswith("qty_dec:") or
        data in ("profile_ok", "edit_profile", "new_address", "user_clear_cart", "user_clear_favs", "user_back_to_cats", "user_back_to_menu", "review_open") or
        # handle admin-style back only for non-admin users to avoid kicking admins to user menu
        ((data in ("back_admin", "back_to_cats", "back")) and not is_admin(user_id))
    ):
        # If an admin-style back callback is received by a non-admin user, map it to main menu
        if data in ("back_admin", "back_to_cats", "back") and not is_admin(user_id):
            context.user_data.pop("state", None)
            context.user_data.pop("pending_order", None)
            await safe_edit_message(query, "Главное меню", reply_markup=user_main_keyboard())
            return

        if data == "review_open":
            # Delete the prompt message and send a direct URL button.
            try:
                await query.message.delete()
            except Exception:
                pass
            try:
                await context.bot.send_message(
                    chat_id=query.from_user.id,
                    text="⭐ Оставьте отзыв в нашем канале 👇",
                    reply_markup=_reviews_url_keyboard("⭐ Оставить отзыв"),
                )
            except Exception:
                pass
            return
        if data.startswith("user_cat:"):
            cat_id = int(data.split(":", 1)[1])
            # If leaving a product card, clear qty selections. Do NOT delete the screen message,
            # because we reuse it (one-screen UX). Media (photo) is cleaned separately.
            try:
                lp_id = context.chat_data.get("last_product_msg_id")
                lp_chat = context.chat_data.get("last_product_chat")
                lp_is_media = bool(context.chat_data.get("last_product_is_media"))
                if lp_id and lp_chat and lp_id == query.message.message_id and lp_chat == query.message.chat_id:
                    context.user_data.pop("qty_map", None)
                    # Only delete if the product screen itself was a media message (legacy behavior)
                    if lp_is_media:
                        try:
                            await context.bot.delete_message(chat_id=query.message.chat_id, message_id=query.message.message_id)
                        except Exception:
                            pass
                        context.chat_data.pop("last_product_msg_id", None)
                        context.chat_data.pop("last_product_chat", None)
                        context.chat_data.pop("last_product_is_media", None)
                        return
            except Exception:
                pass
            text, markup = get_user_category_markup(cat_id)
            try:
                await _cleanup_last_media(context, query.message.chat_id)
            except Exception:
                pass
            await safe_edit_message(query, text, reply_markup=markup)
            try:
                context.chat_data["last_category_msg_id"] = query.message.message_id
                context.chat_data["last_category_chat"] = query.message.chat_id
            except Exception:
                pass
            return

        if data == "user_back_to_cats":
            # clear transient state when user goes back
            context.user_data.pop("state", None)
            context.user_data.pop("pending_order", None)
            context.user_data.pop("qty_map", None)  # Clear quantity selections
            text, markup = get_user_categories_markup()
            try:
                await _cleanup_last_media(context, query.message.chat_id)
            except Exception:
                pass
            await safe_edit_message(query, text, reply_markup=markup)
            return
        if data == "user_back_to_menu":
            context.user_data.pop("state", None)
            context.user_data.pop("pending_order", None)
            try:
                await _cleanup_last_media(context, query.message.chat_id)
            except Exception:
                pass
            if is_admin(user_id):
                await safe_edit_message(query, "Главное меню", reply_markup=admin_keyboard())
            else:
                await safe_edit_message(query, "Главное меню", reply_markup=user_main_keyboard())
            return

        if data.startswith("user_order:"):
            oid = int(data.split(":",1)[1])
            order = find_order(oid)
            if not order or int(order.get('user_id',0)) != int(user_id):
                await safe_edit_message(query, "Заказ не найден")
                return
            # One-screen: show summary in-place (no extra messages).
            items = order.get('items', []) or []
            lines = []
            for it in items[:20]:
                name = (it.get('name') or '-').strip()
                qty = it.get('qty', 1)
                price = it.get('price', 0)
                lines.append(f"• {name} × {qty} — {price} ₽")
            if len(items) > 20:
                lines.append(f"… ещё {len(items) - 20} поз.")

            st = order.get('status', 'new')
            delivery = order.get('delivery') or "-"
            address = order.get('address') or "-"
            text = (
                f"📦 Заказ #{order.get('number')}\n"
                f"📌 Статус: {st}\n"
                f"💰 Итого: {order.get('total', 0)} ₽\n"
                f"🚚 Доставка: {delivery}\n"
                f"📍 Адрес: {address}\n\n"
                "📦 Товары:\n" + ("\n".join(lines) if lines else "• -")
            )
            kb = []
            if order.get('tracking_link') and order.get('status') in ("processing", "done"):
                kb.append([InlineKeyboardButton("🔎 Отследить заказ", url=order.get('tracking_link'))])
            kb.append([InlineKeyboardButton("🔙 Назад", callback_data="user_back_to_cats")])
            await safe_edit_message(query, text, reply_markup=InlineKeyboardMarkup(kb))
            return

        if data.startswith("user_prod:"):
            prod_id = int(data.split(":", 1)[1])
            prods = read_json(PROD_FILE)
            prod = next((p for p in prods if p.get("id") == prod_id), None)
            if prod:
                await send_product_card_user(query.message.chat_id, context, prod)
            else:
                await safe_edit_message(query, "Товар не найден")
            return

        if data.startswith("notify:"):
            try:
                prod_id = int(data.split(":", 1)[1])
            except Exception:
                await query.answer("Ошибка", show_alert=True)
                return
            added = False
            try:
                added = subscribe_notify(int(query.from_user.id), int(prod_id))
            except Exception:
                added = False
            try:
                if added:
                    await query.answer("✅ Вы подписались", show_alert=True)
                else:
                    await query.answer("ℹ️ Вы уже подписаны", show_alert=True)
            except Exception:
                pass

            # UX: replace the notify button to prevent repeated taps
            try:
                prods = read_json(PROD_FILE)
                prod = next((p for p in prods if int(p.get("id", 0)) == int(prod_id)), None)
                if prod:
                    stock = int(prod.get("stock", 0) or 0)
                    # Only relevant when out of stock
                    if stock <= 0:
                        uid = int(query.from_user.id)
                        in_fav = False
                        try:
                            in_fav = int(prod_id) in get_favs(uid)
                        except Exception:
                            in_fav = False
                        keyboard = []
                        if not in_fav:
                            keyboard.append([InlineKeyboardButton("⭐ В избранное", callback_data=f"user_fav:{prod_id}")])
                        keyboard.append([InlineKeyboardButton("⏳ Ожидаем поступление", callback_data="noop")])
                        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data=f"user_cat:{prod.get('category_id')}")])
                        await safe_edit_reply_markup(query, InlineKeyboardMarkup(keyboard))
            except Exception:
                pass
            return

        if data.startswith("user_add_to_cart:"):
            prod_id = int(data.split(":", 1)[1])
            user = query.from_user.id
            # enforce stock availability
            prods_all = read_json(PROD_FILE)
            prod_cur = next((p for p in prods_all if p.get("id") == prod_id), None)
            if not prod_cur:
                await query.answer("Товар не найден", show_alert=True)
                return
            stock = int(prod_cur.get("stock", 0) or 0)
            qty_map = context.user_data.setdefault("qty_map", {})
            cur_qty = int(qty_map.get(prod_id, 1))
            if stock <= 0 or cur_qty > stock:
                await query.answer(f"❌ Доступное количество: {stock}", show_alert=True)
                return
            add_to_cart(user, prod_id, qty=cur_qty, price=prod_cur.get("price", 0))
            # build temporary keyboard with confirmation
            prods = read_json(PROD_FILE)
            prod = next((p for p in prods if p.get("id") == prod_id), None)
            kb = []
            # qty controls only when it makes sense (stock >= 2)
            if stock >= 2:
                kb.append([InlineKeyboardButton("➖", callback_data=f"qty_dec:{prod_id}"), InlineKeyboardButton(str(cur_qty), callback_data="noop"), InlineKeyboardButton("➕", callback_data=f"qty_inc:{prod_id}")])
            kb.append([InlineKeyboardButton("✅ Добавлено в корзину", callback_data="noop")])
            in_fav = prod_id in get_favs(user)
            if not in_fav:
                kb.append([InlineKeyboardButton("⭐ В избранное", callback_data=f"user_fav:{prod_id}")])
            kb.append([InlineKeyboardButton("💳 Заказать", callback_data=f"user_buy:{prod_id}")])
            kb.append([InlineKeyboardButton("🔙 Назад", callback_data=f"user_cat:{prod.get('category_id') if prod else 0}")])
            await safe_edit_reply_markup(query, InlineKeyboardMarkup(kb))
            async def _revert():
                await asyncio.sleep(3)
                final = []
                if stock >= 2:
                    qty_map2 = context.user_data.setdefault("qty_map", {})
                    cur2 = int(qty_map2.get(prod_id, 1))
                    final.append([InlineKeyboardButton("➖", callback_data=f"qty_dec:{prod_id}"), InlineKeyboardButton(str(cur2), callback_data="noop"), InlineKeyboardButton("➕", callback_data=f"qty_inc:{prod_id}")])
                in_fav2 = prod_id in get_favs(user)
                if not in_fav2:
                    final.append([InlineKeyboardButton("⭐ В избранное", callback_data=f"user_fav:{prod_id}")])
                final.append([InlineKeyboardButton("💳 Заказать", callback_data=f"user_buy:{prod_id}")])
                final.append([InlineKeyboardButton("🔙 Назад", callback_data=f"user_cat:{prod.get('category_id') if prod else 0}")])
                try:
                    await context.bot.edit_message_reply_markup(chat_id=query.message.chat_id, message_id=query.message.message_id, reply_markup=InlineKeyboardMarkup(final))
                except Exception:
                    pass
            context.application.create_task(_revert())
            return

        if data.startswith("user_fav:"):
            prod_id = int(data.split(":", 1)[1])
            user = query.from_user.id
            add_to_fav(user, prod_id)
            prods = read_json(PROD_FILE)
            prod = next((p for p in prods if p.get("id") == prod_id), None)
            # keep qty controls row
            qty_map = context.user_data.setdefault("qty_map", {})
            cur_qty = int(qty_map.get(prod_id, 1))
            stock = int((prod.get("stock", 0) if prod else 0) or 0)
            kb = []
            if stock >= 2:
                kb.append([InlineKeyboardButton("➖", callback_data=f"qty_dec:{prod_id}"), InlineKeyboardButton(str(cur_qty), callback_data="noop"), InlineKeyboardButton("➕", callback_data=f"qty_inc:{prod_id}")])
            kb.append([InlineKeyboardButton("✅ Добавлено в избранное", callback_data="noop")])
            in_cart = is_in_cart(user, prod_id)
            if not in_cart:
                kb.append([InlineKeyboardButton("🛒 В корзину", callback_data=f"user_add_to_cart:{prod_id}")])
            kb.append([InlineKeyboardButton("💳 Заказать", callback_data=f"user_buy:{prod_id}")])
            kb.append([InlineKeyboardButton("🔙 Назад", callback_data=f"user_cat:{prod.get('category_id') if prod else 0}")])
            await safe_edit_reply_markup(query, InlineKeyboardMarkup(kb))
            async def _revert():
                await asyncio.sleep(3)
                final = []
                qty_map2 = context.user_data.setdefault("qty_map", {})
                cur2 = int(qty_map2.get(prod_id, 1))
                if stock >= 2:
                    final.append([InlineKeyboardButton("➖", callback_data=f"qty_dec:{prod_id}"), InlineKeyboardButton(str(cur2), callback_data="noop"), InlineKeyboardButton("➕", callback_data=f"qty_inc:{prod_id}")])
                in_cart2 = is_in_cart(user, prod_id)
                if not in_cart2:
                    final.append([InlineKeyboardButton("🛒 В корзину", callback_data=f"user_add_to_cart:{prod_id}")])
                final.append([InlineKeyboardButton("💳 Заказать", callback_data=f"user_buy:{prod_id}")])
                final.append([InlineKeyboardButton("🔙 Назад", callback_data=f"user_cat:{prod.get('category_id') if prod else 0}")])
                try:
                    await context.bot.edit_message_reply_markup(chat_id=query.message.chat_id, message_id=query.message.message_id, reply_markup=InlineKeyboardMarkup(final))
                except Exception:
                    pass
            context.application.create_task(_revert())
            return
        if data == "noop":
            await query.answer()
            return

        if data == "user_buy_cart":
            # prepare items from cart and ask for address selection
            user = query.from_user.id
            cart_items = get_cart_items(user)
            if not cart_items:
                await safe_edit_message(query, "Ваша корзина пуста.")
                return
            prods = read_json(PROD_FILE)
            prods_by_id = {int(p.get("id")): p for p in prods if p.get("id") is not None}
            items = []
            removed_any = False
            # validate product existence + stock for requested qty
            for ci in cart_items:
                pid = int(ci.get("product_id"))
                qty = int(ci.get("qty", 1) or 1)
                p = prods_by_id.get(pid)
                if not p:
                    removed_any = True
                    try:
                        remove_from_cart(user, pid)
                    except Exception:
                        pass
                    continue
                stock = int(p.get("stock", 0) or 0)
                if stock <= 0 or qty > stock:
                    name = (p.get("name") or "-").strip()
                    await safe_edit_message(query, f"❌ Недостаточно товара: {name}\nДоступно: {stock}")
                    return
                price = ci.get("price")
                if price is None:
                    price = p.get("price", 0)
                items.append({"product_id": pid, "name": p.get('name'), "qty": qty, "price": price})

            if not items:
                msg = "🛒 Ваша корзина пуста."
                if removed_any:
                    msg = "🛒 В корзине были удалённые товары — я убрал их. Корзина пуста."
                await safe_edit_message(query, msg)
                return
            context.user_data["pending_order"] = {"type": "cart", "items": items}
            # If profile missing, collect it; otherwise show confirmation screen
            profiles = read_profiles()
            uid = str(query.from_user.id)
            missing_state = _profile_next_missing_state(profiles.get(uid))
            if missing_state:
                context.user_data["state"] = missing_state
                prefix = "ℹ️ Я убрал удалённые товары из корзины.\n\n" if removed_any else ""
                if missing_state == "profile_phone":
                    keyboard = ReplyKeyboardMarkup(
                        [[KeyboardButton("📞 Отправить номер", request_contact=True)]],
                        resize_keyboard=True,
                        one_time_keyboard=True,
                    )
                    try:
                        await query.message.reply_text(
                            prefix + "📞 Отправьте номер телефона кнопкой ниже\nили введите вручную:",
                            reply_markup=keyboard,
                        )
                    except Exception:
                        await safe_edit_message(query, prefix + "📞 Отправьте номер телефона кнопкой ниже\nили введите вручную:")
                    return
                prompt = {
                    "profile_first_name": "👤 Введите *имя*:",
                    "profile_last_name": "👤 Введите *фамилию*:",
                    "profile_patronymic": "👤 Введите *отчество*:",
                }.get(missing_state, "👤 Введите данные:")
                await safe_edit_message(query, prefix + prompt)
                return
            await show_profile_confirmation(query, context)
            return

        if data.startswith("user_buy:"):
            prod_id = int(data.split(":", 1)[1])
            prods = read_json(PROD_FILE)
            p = next((x for x in prods if x.get('id') == prod_id), None)
            if not p:
                await safe_edit_message(query, "Товар не найден")
                return
            # enforce stock availability for buy
            stock = int(p.get("stock", 0) or 0)
            qty_map = context.user_data.setdefault("qty_map", {})
            qty = int(qty_map.get(prod_id, 1))
            if qty > stock:
                await query.answer(f"❌ Доступное количество: {stock}", show_alert=True)
                return
            items = [{"product_id": p.get('id'), "name": p.get('name'), "qty": qty, "price": p.get('price',0)}]
            context.user_data["pending_order"] = {"type": "single", "items": items}
            # If profile missing, collect it; otherwise show confirmation screen
            profiles = read_profiles()
            uid = str(query.from_user.id)
            missing_state = _profile_next_missing_state(profiles.get(uid))
            if missing_state:
                context.user_data["state"] = missing_state
                if missing_state == "profile_phone":
                    keyboard = ReplyKeyboardMarkup(
                        [[KeyboardButton("📞 Отправить номер", request_contact=True)]],
                        resize_keyboard=True,
                        one_time_keyboard=True,
                    )
                    try:
                        await query.message.reply_text(
                            "📞 Отправьте номер телефона кнопкой ниже\nили введите вручную:",
                            reply_markup=keyboard,
                        )
                    except Exception:
                        await safe_edit_message(query, "📞 Отправьте номер телефона кнопкой ниже\nили введите вручную:")
                    return
                prompt = {
                    "profile_first_name": "👤 Введите *имя*:",
                    "profile_last_name": "👤 Введите *фамилию*:",
                    "profile_patronymic": "👤 Введите *отчество*:",
                }.get(missing_state, "👤 Введите данные:")
                await safe_edit_message(query, prompt)
                return
            await show_profile_confirmation(query, context)
            return

        if data.startswith("qty_inc:"):
            prod_id = int(data.split(":")[1])
            prods = read_json(PROD_FILE)
            prod = next((p for p in prods if p["id"] == prod_id), None)
            if not prod:
                return
            stock = int(prod.get("stock", 0) or 0)
            qty_map = context.user_data.setdefault("qty_map", {})
            cur = int(qty_map.get(prod_id, 1))
            if cur >= stock:
                await query.answer(f"❌ Доступное количество: {stock}", show_alert=True)
                return
            cur += 1
            qty_map[prod_id] = cur
            in_cart = is_in_cart(query.from_user.id, prod_id)
            in_fav = prod_id in get_favs(query.from_user.id)
            keyboard = [
                [InlineKeyboardButton("➖", callback_data=f"qty_dec:{prod_id}"), InlineKeyboardButton(str(cur), callback_data="noop"), InlineKeyboardButton("➕", callback_data=f"qty_inc:{prod_id}")]
            ]
            if not in_cart and stock > 0:
                keyboard.append([InlineKeyboardButton("🛒 В корзину", callback_data=f"user_add_to_cart:{prod_id}")])
            if not in_fav:
                keyboard.append([InlineKeyboardButton("⭐ В избранное", callback_data=f"user_fav:{prod_id}")])
            if stock > 0:
                keyboard.append([InlineKeyboardButton("💳 Заказать", callback_data=f"user_buy:{prod_id}")])
            keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data=f"user_cat:{prod.get('category_id')}")])
            await safe_edit_reply_markup(query, InlineKeyboardMarkup(keyboard))
            return

        if data.startswith("qty_dec:"):
            prod_id = int(data.split(":")[1])
            prods = read_json(PROD_FILE)
            prod = next((p for p in prods if p["id"] == prod_id), None)
            if not prod:
                return
            stock = int(prod.get("stock", 0) or 0)
            qty_map = context.user_data.setdefault("qty_map", {})
            cur = int(qty_map.get(prod_id, 1))
            cur = max(1, cur - 1)
            qty_map[prod_id] = cur
            in_cart = is_in_cart(query.from_user.id, prod_id)
            in_fav = prod_id in get_favs(query.from_user.id)
            keyboard = [
                [InlineKeyboardButton("➖", callback_data=f"qty_dec:{prod_id}"), InlineKeyboardButton(str(cur), callback_data="noop"), InlineKeyboardButton("➕", callback_data=f"qty_inc:{prod_id}")]
            ]
            if not in_cart and stock > 0:
                keyboard.append([InlineKeyboardButton("🛒 В корзину", callback_data=f"user_add_to_cart:{prod_id}")])
            if not in_fav:
                keyboard.append([InlineKeyboardButton("⭐ В избранное", callback_data=f"user_fav:{prod_id}")])
            if stock > 0:
                keyboard.append([InlineKeyboardButton("💳 Заказать", callback_data=f"user_buy:{prod_id}")])
            keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data=f"user_cat:{prod.get('category_id')}")])
            await safe_edit_reply_markup(query, InlineKeyboardMarkup(keyboard))
            return
            # If profile missing, collect it; otherwise show confirmation screen
            profiles = read_profiles()
            uid = str(query.from_user.id)
            if uid not in profiles:
                context.user_data["state"] = "profile_first_name"
                await safe_edit_message(query, "👤 Введите *имя*:")
                return
            await show_profile_confirmation(query, context)
            return

        if data == "new_address":
            context.user_data["state"] = "ordering_new_address"
            await safe_edit_message(query, "✏️ Введите новый адрес:")
            return

        if data.startswith("use_address:"):
            # callback contains index
            idx = int(data.split(":",1)[1])
            uid = str(query.from_user.id)
            addrs = read_addresses()
            user_addrs = addrs.get(uid, [])
            if idx < 0 or idx >= len(user_addrs):
                await safe_edit_message(query, "Адрес не найден")
                return
            address = user_addrs[idx]
            pending = context.user_data.get("pending_order")
            if not pending:
                await safe_edit_message(query, "Нет ожидаемого заказа.")
                return
            # save address to pending order and ask for delivery
            pending["address"] = address
            context.user_data["pending_order"] = pending
            await show_delivery_selection(query, context)
            return

        if data == "edit_profile":
            # allow user to update stored profile during checkout
            context.user_data["state"] = "profile_first_name"
            await safe_edit_message(query, "👤 Введите *имя*:")
            return

        if data == "profile_ok":
            # proceed to delivery selection after confirming profile
            profiles = read_profiles()
            uid = str(query.from_user.id)
            missing_state = _profile_next_missing_state(profiles.get(uid))
            if missing_state:
                context.user_data["state"] = missing_state
                if missing_state == "profile_phone":
                    keyboard = ReplyKeyboardMarkup(
                        [[KeyboardButton("📞 Отправить номер", request_contact=True)]],
                        resize_keyboard=True,
                        one_time_keyboard=True,
                    )
                    try:
                        await query.message.reply_text(
                            "📞 Отправьте номер телефона кнопкой ниже\nили введите вручную:",
                            reply_markup=keyboard,
                        )
                    except Exception:
                        await safe_edit_message(query, "📞 Отправьте номер телефона кнопкой ниже\nили введите вручную:")
                    return
                prompt = {
                    "profile_first_name": "👤 Введите *имя*:",
                    "profile_last_name": "👤 Введите *фамилию*:",
                    "profile_patronymic": "👤 Введите *отчество*:",
                }.get(missing_state, "👤 Введите данные:")
                await safe_edit_message(query, prompt)
                return
            await show_delivery_selection(query, context)
            return

        if data.startswith("delivery_select:"):
            delivery = data.split(":", 1)[1]
            pending = context.user_data.get("pending_order")
            if not pending:
                await safe_edit_message(query, "Ошибка: заказ не найден")
                return
            pending["delivery"] = delivery
            context.user_data["pending_order"] = pending
            context.user_data["state"] = f"address_select:{delivery}"
            await show_pvz_selection(query, context, delivery)
            return

        if data.startswith("new_pvz:"):
            delivery = data.split(":", 1)[1]
            context.user_data["state"] = f"pvz_input:{delivery}"
            await safe_edit_message(query, f"✏️ Введите ближайший ПВЗ *{delivery}*:")
            return

        if data.startswith("use_pvz:"):
            # format: use_pvz:DeliveryName:index
            parts = data.split(":", 2)
            if len(parts) < 3:
                await safe_edit_message(query, "Ошибка формата данных")
                return
            delivery = parts[1]
            idx = int(parts[2])
            uid = str(query.from_user.id)
            addrs = read_addresses()
            user_addrs = addrs.get(uid, {})
            if isinstance(user_addrs, dict):
                pvz_list = user_addrs.get(delivery, [])
            else:
                pvz_list = []
            if idx < 0 or idx >= len(pvz_list):
                await safe_edit_message(query, "ПВЗ не найден")
                return
            pvz = pvz_list[idx]
            pending = context.user_data.get("pending_order")
            if not pending:
                await safe_edit_message(query, "Нет ожидаемого заказа.")
                return
            pending["address"] = pvz
            pending["delivery"] = delivery
            context.user_data["pending_order"] = pending
            context.user_data.pop("state", None)
            await safe_edit_message(query, f"✅ ПВЗ выбран: {pvz}")
            await finalize_order(query, context)
            return

        if data.startswith("delivery:"):
            _, delivery_method = data.split(":", 1)
            pending = context.user_data.get("pending_order")
            if not pending:
                await safe_edit_message(query, "Ошибка: заказ не найден")
                return
            # Save delivery method and proceed to PVZ selection; order will be created after payment
            pending["delivery"] = delivery_method
            context.user_data["pending_order"] = pending
            context.user_data["state"] = f"address_select:{delivery_method}"
            await show_pvz_selection(query, context, delivery_method)
            return
            # (legacy follow-ups removed; order creation deferred to payment webhook)
            return

        if data == "user_clear_cart":
            user = query.from_user.id
            clear_cart(user)
            context.user_data.pop("qty_map", None)  # Clear quantity selections
            try:
                await query.answer("Корзина очищена")
            except Exception:
                pass
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="user_back_to_cats")]])
            await safe_edit_message(query, "🛒 Ваша корзина пуста.", reply_markup=kb)
            return

        if data == "user_clear_favs":
            user = query.from_user.id
            clear_favs(user)
            try:
                await query.answer("Избранное очищено")
            except Exception:
                pass
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Назад", callback_data="user_back_to_cats")]])
            await safe_edit_message(query, "⭐ У вас нет избранных товаров.", reply_markup=kb)
            return

        # handled above or unknown user callback -> return to avoid falling into admin handlers
        return

    # Admin-only callbacks
    if data.startswith("admin_restock_select:") and is_admin(user_id):
        try:
            prod_id = int(data.split(":", 1)[1])
        except Exception:
            await safe_edit_message(query, "Ошибка: товар не найден")
            return
        context.user_data["state"] = f"admin_restock_input:{prod_id}"
        await safe_edit_message(
            query,
            f"➕ Изменить остаток товара (ID {prod_id})\n"
            "Введите: +10 (прибавить), -9 (убавить), 25 (установить остаток).",
        )
        return

    if data == "admin_manage":
        # show admins list and management buttons
        admins_list = read_json(ADMINS_FILE)
        admins_list = [str(x) for x in admins_list]
        text = "👥 Список админов:\n" + "\n".join(admins_list)
        keyboard = [
            [InlineKeyboardButton("➕ Добавить админа", callback_data="admin_add")],
            [InlineKeyboardButton("❌ Удалить админа", callback_data="admin_remove")],
            [InlineKeyboardButton("🔙 Назад", callback_data="back_admin")],
        ]
        await safe_edit_message(query, text, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data.startswith("admin_restock:"):
        prod_id = int(data.split(":")[1])
        context.user_data["state"] = f"admin_restock_input:{prod_id}"
        await safe_edit_message(
            query,
            "➕ Изменить остаток товара\n"
            "Введите: +10 (прибавить), -9 (убавить), 25 (установить остаток).",
        )
        return

    if data == "admin_add":
        context.user_data["state"] = "adding_admin"
        await safe_edit_message(query, "✏️ Введите numeric ID нового админа")
        return

    if data == "admin_remove":
        context.user_data["state"] = "removing_admin"
        await safe_edit_message(query, "✏️ Введите numeric ID админа для удаления")
        return

    if data == "admin_welcome":
        await safe_edit_message(query, "Функция настройки текста приветствия — в разработке.")
        return

    if data == "admin_notify":
        await safe_edit_message(query, "Функция настройки уведомлений — в разработке.")
        return

    # Broadcasts admin
    if data == "broadcast_create":
        context.user_data["state"] = "broadcast_text"
        context.user_data.pop("broadcast", None)
        await safe_edit_message(query, "✏️ Введите текст рассылки (поддерживается текст).")
        return

    if data == "broadcast_history":
        broads = read_broadcasts()
        if not broads:
            await safe_edit_message(query, "История рассылок пуста.")
            return
        keyboard = []
        for b in broads[::-1]:
            ts = format_dt(b.get("created_at", 0))
            keyboard.append([InlineKeyboardButton(f"📢 {ts} — {b.get('type','manual').capitalize()}", callback_data=f"broadcast_item:{b.get('id')}")])
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_admin")])
        await safe_edit_message(query, "📊 История рассылок:", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data.startswith("broadcast_item:"):
        bid = int(data.split(":",1)[1])
        b = next((x for x in read_broadcasts() if x.get("id") == bid), None)
        if not b:
            await safe_edit_message(query, "Рассылка не найдена")
            return
        ts = format_dt(b.get("created_at",0))
        text = f"📢 Рассылка от {ts}\n\nТип: {b.get('type','manual').capitalize()}\nПолучателей: {b.get('recipients',0)}\nДоставлено: {b.get('delivered',0)}\n\nТекст:\n{b.get('text','')}"
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back_admin")]]
        await safe_edit_message(query, text, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "broadcast_notifications":
        cfg = read_notifications()
        np = cfg.get("new_product", {})
        status_text = "✅ Включено" if np.get("enabled") else "❌ Выключено"
        text = (
            f"🔔 Уведомления\n\n"
            f"📦 Новый товар\nСтатус: {status_text}\n\n"
            f"✏️ Текст уведомления:\n{np.get('template','')}"
        )
        # Toggle button shows current state (Вкл/Выкл) with emoji; pressing keeps this screen
        toggle_label = status_text
        keyboard = [
            [InlineKeyboardButton(toggle_label, callback_data="notif_toggle_new_product")],
            [InlineKeyboardButton("✏️ Редактировать текст", callback_data="notif_edit_new_product")],
            [InlineKeyboardButton("🔙 Назад", callback_data="back_admin")],
        ]
        await safe_edit_message(query, text, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "notif_toggle_new_product":
        # Toggle state and re-render the notifications screen without leaving broadcast menu
        cfg = read_notifications()
        np = cfg.get("new_product", {})
        np["enabled"] = not np.get("enabled", False)
        cfg["new_product"] = np
        write_notifications(cfg)
        status_text = "✅ Включено" if np.get("enabled") else "❌ Выключено"
        text = (
            f"🔔 Уведомления\n\n"
            f"📦 Новый товар\nСтатус: {status_text}\n\n"
            f"✏️ Текст уведомления:\n{np.get('template','')}"
        )
        toggle_label = status_text
        keyboard = [
            [InlineKeyboardButton(toggle_label, callback_data="notif_toggle_new_product")],
            [InlineKeyboardButton("✏️ Редактировать текст", callback_data="notif_edit_new_product")],
            [InlineKeyboardButton("🔙 Назад", callback_data="back_admin")],
        ]
        await safe_edit_message(query, text, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "notif_edit_new_product":
        context.user_data["state"] = "notif_edit_new_product"
        await safe_edit_message(query, "✏️ Введите шаблон уведомления для нового товара. Используйте {name} и {price}.")
        return

    # Admin orders filters
    if data in ("orders_new", "orders_processing", "orders_done", "orders_cancelled"):
        status_map = {"orders_new": "new", "orders_processing": "processing", "orders_done": "done", "orders_cancelled": "cancelled"}
        status = status_map.get(data)
        orders = [o for o in read_orders() if o.get("status") == status]
        if not orders:
            await safe_edit_message(query, "Список заказов пуст.")
            return
        keyboard = []
        for o in orders:
            items = o.get('items', []) or []
            pos_count = len(items)
            qty_total = 0
            for it in items:
                try:
                    qty_total += int(it.get('qty', 1) or 1)
                except Exception:
                    qty_total += 1
            keyboard.append([
                InlineKeyboardButton(
                    f"🧾 #{o.get('number')} | {pos_count} поз. / {qty_total} шт | {o.get('total',0)} ₽",
                    callback_data=f"order_item:{o.get('id')}"
                )
            ])
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_admin")])
        title_map = {"new": "🟢 Новые заказы", "processing": "🟡 В обработке", "done": "🔵 Завершённые", "cancelled": "❌ Отменённые"}
        await safe_edit_message(query, title_map.get(status, "Заказы"), reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data.startswith("order_item:"):
        oid = int(data.split(":",1)[1])
        order = find_order(oid)
        if not order:
            await safe_edit_message(query, "Заказ не найден")
            return
        # build order card
        lines = []
        for it in order.get("items", []):
            lines.append(f"• {(it.get('name') or '-').strip()} — {it.get('qty',1)} шт — {it.get('price',0)} ₽")
        items_text = "\n".join(lines)
        created = format_dt(order.get("created_at",0))
        delivery_text = f"\n🚚 Доставка: {order.get('delivery', '-')}" if order.get('delivery') else ""
        client = order.get('client', {})
        name_line = (client.get('first_name') or '')
        if client.get('last_name'):
            name_line = name_line + ' ' + client.get('last_name')
        if client.get('patronymic'):
            name_line = name_line + ' ' + client.get('patronymic')
        name_line = name_line.strip() or order.get('full_name')
        phone_line = client.get('phone') or '-'
        text = f"🧾 Заказ #{order.get('number')}\nСтатус: {('🟢 Новый' if order.get('status')=='new' else '🟡 В обработке' if order.get('status')=='processing' else '🔵 Завершён' if order.get('status')=='done' else '❌ Отменён')}\n\n👤 Клиент:\nИмя: {name_line}\nТелефон: {phone_line}\nTelegram: @{order.get('username')}\nID: {order.get('user_id')}\n\n📦 Товары:\n{items_text}\n\n💰 Итого: {order.get('total')} ₽\n\n📍 Адрес / способ получения:\n{order.get('address')}{delivery_text}\n\n🕒 Дата: {created}"
        # buttons by status
        keyboard = []
        st = order.get('status')
        if st == 'new':
            keyboard.append([InlineKeyboardButton("🟡 Взять в обработку", callback_data=f"order_take:{oid}"), InlineKeyboardButton("❌ Отменить заказ", callback_data=f"order_cancel:{oid}")])
        elif st == 'processing':
            keyboard.append([InlineKeyboardButton("🔵 Завершить заказ", callback_data=f"order_complete:{oid}"), InlineKeyboardButton("❌ Отменить заказ", callback_data=f"order_cancel:{oid}")])
        else:
            # done or cancelled — only back
            pass
        # allow admin to add or change tracking link
        if order.get('tracking_link'):
            keyboard.append([InlineKeyboardButton("✏️ Изменить ссылку отслеживания", callback_data=f"order_add_tracking:{oid}")])
        else:
            keyboard.append([InlineKeyboardButton("🔗 Добавить ссылку отслеживания", callback_data=f"order_add_tracking:{oid}")])
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_admin")])
        await safe_edit_message(query, text, reply_markup=InlineKeyboardMarkup(keyboard))
        return
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_admin")])
        await safe_edit_message(query, text, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data.startswith("order_add_tracking:"):
        oid = int(data.split(":",1)[1])
        context.user_data["state"] = f"admin_adding_tracking:{oid}"
        await safe_edit_message(query, "✏️ Введите ссылку для отслеживания (URL) для этого заказа:")
        return

    if data.startswith("order_take:"):
        oid = int(data.split(":",1)[1])
        order = find_order(oid)
        if not order:
            await safe_edit_message(query, "Заказ не найден")
            return
        order['status'] = 'processing'
        from time import time
        order['updated_at'] = time()
        update_order(order)
        await safe_edit_message(query, f"✅ Заказ #{order.get('number')} взят в обработку")
        # notify customer
        try:
            await context.bot.send_message(int(order.get('user_id')), f"🔔 Ваш заказ #{order.get('number')} взят в обработку")
        except Exception:
            pass
        return

    if data.startswith("order_complete:"):
        oid = int(data.split(":",1)[1])
        order = find_order(oid)
        if not order:
            await safe_edit_message(query, "Заказ не найден")
            return
        order['status'] = 'done'
        from time import time
        order['completed_at'] = time()
        order['updated_at'] = order['completed_at']
        update_order(order)
        await safe_edit_message(query, f"✅ Заказ #{order.get('number')} отмечен как завершённый")
        try:
            await _send_review_prompt(context, int(order.get('user_id')), order.get('number'))
        except Exception:
            pass
        return

    if data.startswith("order_cancel:"):
        oid = int(data.split(":",1)[1])
        order = find_order(oid)
        if not order:
            await safe_edit_message(query, "Заказ не найден")
            return
        order['status'] = 'cancelled'
        from time import time
        order['updated_at'] = time()
        update_order(order)
        await safe_edit_message(query, f"❌ Заказ #{order.get('number')} отменён")
        try:
            await context.bot.send_message(int(order.get('user_id')), f"🔔 Ваш заказ #{order.get('number')} был отменён")
        except Exception:
            pass
        return

    if data == "stats_more":
        det = stats_details()
        if not det:
            await safe_edit_message(query, "Статистика пуста.")
            return
        total_orders = int(det.get('total_orders', 0))
        avg_check = int(det.get('avg_check') or 0)
        first = det.get('first') or "-"
        last = det.get('last') or "-"
        clients = int(det.get('clients', 0))
        text = (
            f"📊 Подробная статистика\n\n"
            f"📦 Всего заказов: {total_orders}\n"
            f"💰 Средний чек: {avg_check} ₽\n\n"
            f"📆 Первый заказ: {first}\n"
            f"📆 Последний заказ: {last}\n\n"
            f"👤 Клиентов всего: {clients}"
        )
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back_admin")]]
        try:
            await _cleanup_last_media(context, query.message.chat_id)
        except Exception:
            pass
        await safe_edit_message(query, text, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "stats_top":
        top = top_products(10)
        if not top:
            await safe_edit_message(query, "Топ товаров пуст.")
            return
        lines = []
        medals = ["🥇","🥈","🥉"]
        for i, (name, cnt) in enumerate(top[:10]):
            medal = medals[i] if i < 3 else f"#{i+1}"
            lines.append(f"{medal} {name} — {cnt} продаж")
        text = "🏆 Топ товаров\n\n" + "\n".join(lines)
        keyboard = [[InlineKeyboardButton("🔙 Назад", callback_data="back_admin")]]
        try:
            await _cleanup_last_media(context, query.message.chat_id)
        except Exception:
            pass
        await safe_edit_message(query, text, reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "add_category":
        context.user_data["state"] = "adding_category"
        await safe_edit_message(query, "✏️ Введите название нового каталога")
        return


    if data.startswith("add_subcat:"):
        # treat as add category (no nesting implemented) but show prompt
        parent_id = int(data.split(":", 1)[1])
        context.user_data["state"] = "adding_category"
        context.user_data["parent_cat"] = parent_id
        await safe_edit_message(query, "✏️ Введите название нового каталога (будет создан как основной)")
        return

    if data == "back_admin":
        # clear transient state when returning to admin menu
        context.user_data.pop("state", None)
        context.user_data.pop("pending_order", None)
        await safe_edit_message(query, "Вернулись в админ-меню.")
        try:
            await _cleanup_last_media(context, query.message.chat_id)
        except Exception:
            pass
        await query.message.reply_text("Выберите раздел:", reply_markup=admin_menu_keyboard())
        return

    if data.startswith("cat:"):
        cat_id = int(data.split(":", 1)[1])
        # replace the categories message with the category view
        text, markup = get_category_markup(cat_id)
        try:
            await _cleanup_last_media(context, query.message.chat_id)
        except Exception:
            pass
        await safe_edit_message(query, text, reply_markup=markup)
        return

    if data == "back_to_cats":
        # clear transient state when returning to categories
        context.user_data.pop("state", None)
        context.user_data.pop("pending_order", None)
        text, markup = get_categories_markup()
        try:
            await _cleanup_last_media(context, query.message.chat_id)
            
        except Exception:
            pass
        await safe_edit_message(query, text, reply_markup=markup)
        return

    if data.startswith("rename_cat:"):
        cat_id = int(data.split(":", 1)[1])
        context.user_data["state"] = f"renaming_cat:{cat_id}"
        await safe_edit_message(query, f"✏️ Введите новое название каталога «{get_cat_name(cat_id)}»")
        return

    if data.startswith("delcat_confirm:"):
        cat_id = int(data.split(":", 1)[1])
        # delete category and all its subcategories recursively
        def delete_cat_recursive(cid):
            cats_all = read_json(CATS_FILE)
            # find children
            children = [c for c in cats_all if c.get("parent_id") == cid]
            for ch in children:
                delete_cat_recursive(ch["id"])
            # remove this category
            cats_remaining = [c for c in cats_all if c["id"] != cid]
            write_json(CATS_FILE, cats_remaining)
            # remove products in this category
            prods_all = read_json(PROD_FILE)
            prods_remaining = [p for p in prods_all if p.get("category_id") != cid]
            write_json(PROD_FILE, prods_remaining)

        delete_cat_recursive(cat_id)
        # update original message to show refreshed categories list
        text, markup = get_categories_markup()
        await safe_edit_message(query, "🗑 Каталог удалён")
        await query.message.reply_text("Обновлён список:")
        await query.message.reply_text(text, reply_markup=markup)
        return

    if data.startswith("delcat:"):
        cat_id = int(data.split(":", 1)[1])
        keyboard = [
            [InlineKeyboardButton("✅ Да, удалить", callback_data=f"delcat_confirm:{cat_id}"), InlineKeyboardButton("❌ Отмена", callback_data="cancel")]
        ]
        await safe_edit_message(query, f"⚠️ Вы уверены, что хотите удалить каталог «{get_cat_name(cat_id)}»?\nВсе товары внутри будут удалены.", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data == "cancel":
        # clear any in-progress state when user cancels
        context.user_data.pop("state", None)
        context.user_data.pop("pending_order", None)
        await safe_edit_message(query, "Отмена")
        return

    if data == "broadcast_add_photo":
        context.user_data["state"] = "broadcast_photo_wait"
        await safe_edit_message(query, "📸 Пришлите фото для рассылки (опционально).")
        return

    if data == "broadcast_cancel":
        context.user_data.pop("broadcast", None)
        context.user_data.pop("state", None)
        await safe_edit_message(query, "Отмена рассылки")
        return

    if data == "broadcast_send":
        b = context.user_data.get("broadcast") or {}
        text_b = b.get("text","")
        photo = b.get("photo")
        recipients = get_recipients_list()
        cnt = len(recipients)
        # perform sending (async)
        delivered = await do_send_broadcast(context, text_b, photo, recipients)
        from time import time
        entry = {
            "id": get_next_id(read_broadcasts()),
            "type": "manual",
            "text": text_b,
            "photo": photo,
            "recipients": cnt,
            "delivered": delivered,
            "created_at": time(),
        }
        save_broadcast_record(entry)
        context.user_data.pop("broadcast", None)
        context.user_data.pop("state", None)
        await safe_edit_message(query, f"✅ Рассылка отправлена. Доставлено: {delivered} из {cnt}")
        return

    if data.startswith("show_prod_add:"):
        cat_id = int(data.split(":", 1)[1])
        context.user_data["state"] = f"addprod_photos:{cat_id}"
        # initialize product storage
        context.user_data["new_product"] = {"photos": [], "category_id": cat_id}
        await safe_edit_message(query, "📸 Прикрепите фото товара (не больше двух). После фото введите название или пришлите ещё фото.")
        return

    if data.startswith("list_edit_products:"):
        cat_id = int(data.split(":", 1)[1])
        await list_products_for_edit(query, context, cat_id)
        return

    if data.startswith("list_del_products:"):
        cat_id = int(data.split(":", 1)[1])
        await list_products_for_delete(query, context, cat_id)
        return

    if data.startswith("prod:"):
        prod_id = int(data.split(":", 1)[1])
        if is_admin(user_id):
            prods = read_json(PROD_FILE)
            prod = next((p for p in prods if int(p.get("id", 0)) == int(prod_id)), None)
            if not prod:
                await safe_edit_message(query, "Товар не найден")
                return
            try:
                await safe_edit_message(query, "✅ Открываю карточку товара…")
            except Exception:
                pass
            try:
                await _cleanup_last_media(context, query.message.chat_id)
            except Exception:
                pass
            await send_product_card(query.message.chat_id, context, prod)
        else:
            await show_product_actions(query, context, prod_id)
        return

    if data.startswith("prod_edit:"):
        prod_id = int(data.split(":", 1)[1])
        await show_product_actions(query, context, prod_id)
        return

    if data.startswith("prod_editmenu:"):
        prod_id = int(data.split(":", 1)[1])
        # show edit options menu
        keyboard = [
            [InlineKeyboardButton("✏️ Изменить название", callback_data=f"editprod:name:{prod_id}"), InlineKeyboardButton("✏️ Изменить описание", callback_data=f"editprod:desc:{prod_id}")],
            [InlineKeyboardButton("✏️ Изменить цену", callback_data=f"editprod:price:{prod_id}"), InlineKeyboardButton("✏️ Изменить фото", callback_data=f"editprodphoto:{prod_id}")],
            [InlineKeyboardButton("🔙 Назад", callback_data=f"prod:{prod_id}")],
        ]
        await safe_edit_message(query, "Выберите, что изменить:", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    if data.startswith("editprod:"):
        _, action, prod_id = data.split(":")
        prod_id = int(prod_id)
        try:
            prods = read_json(PROD_FILE)
            prod = next((p for p in prods if p.get("id") == prod_id), None)
        except Exception:
            prod = None
        if not prod:
            await safe_edit_message(query, "❌ Товар не найден")
            return
        if action == "name":
            context.user_data["state"] = f"editprod_name:{prod_id}"
            current = prod.get("name", "-")
            await safe_edit_message(query, f"✏️ Текущее название:\n{current}\n\nВведите новое название")
            return
        if action == "desc":
            context.user_data["state"] = f"editprod_desc:{prod_id}"
            current = prod.get("description", "-")
            await safe_edit_message(query, f"✏️ Текущее описание:\n{current}\n\nВведите новое описание")
            return
        if action == "price":
            context.user_data["state"] = f"editprod_price:{prod_id}"
            current = prod.get("price", "-")
            await safe_edit_message(query, f"💲 Текущая цена: {current} ₽\n\nВведите новую цену")
            return

    if data.startswith("editprodphoto:"):
        prod_id = int(data.split(":", 1)[1])
        context.user_data["state"] = f"editprod_photos:{prod_id}"
        # temporary storage for incoming photos
        context.user_data["edit_photos"] = []
        await safe_edit_message(query, "📸 Пришлите новые фото товара (не больше двух). После этого фото будут применены и карточка обновится.")
        return

    if data.startswith("delprod_confirm:"):
        prod_id = int(data.split(":", 1)[1])
        prods = read_json(PROD_FILE)
        prod = next((p for p in prods if p["id"] == prod_id), None)
        if prod:
            cat_id = prod["category_id"]
            prods = [p for p in prods if p["id"] != prod_id]
            write_json(PROD_FILE, prods)
            await safe_edit_message(query, "🗑 Товар удалён")
            await show_category(query.message, context, cat_id)
        else:
            await safe_edit_message(query, "Товар не найден")
        return



async def show_category_callback(update: Update, context: ContextTypes.DEFAULT_TYPE, cat_id: int) -> None:
    query = update.callback_query
    text, markup = get_category_markup(cat_id)
    await safe_edit_message(query, text, reply_markup=markup)


async def show_category(message_obj, context: ContextTypes.DEFAULT_TYPE, cat_id: int) -> None:
    ensure_data_files()
    text, markup = get_category_markup(cat_id)
    # remove last media to avoid preview thumbnail above category message
    try:
        # message_obj may be update.message or query.message
        chat_id = message_obj.chat_id if hasattr(message_obj, "chat_id") else message_obj.chat.id
        await _cleanup_last_media(context, chat_id)
    except Exception:
        pass
    await message_obj.reply_text(text, reply_markup=markup)


async def show_orders_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ensure_data_files()
    counts = get_orders_counts()
    text = f"📦 Заказы\n\n🟢 Новые ({counts.get('new',0)})\n🟡 В обработке ({counts.get('processing',0)})\n🔵 Завершённые ({counts.get('done',0)})\n❌ Отменённые ({counts.get('cancelled',0)})"
    keyboard = [
        [InlineKeyboardButton(f"🟢 Новые ({counts.get('new',0)})", callback_data="orders_new")],
        [InlineKeyboardButton(f"🟡 В обработке ({counts.get('processing',0)})", callback_data="orders_processing")],
        [InlineKeyboardButton(f"🔵 Завершённые ({counts.get('done',0)})", callback_data="orders_done")],
        [InlineKeyboardButton(f"❌ Отменённые ({counts.get('cancelled',0)})", callback_data="orders_cancelled")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_admin")],
    ]
    try:
        await _cleanup_last_media(context, update.message.chat_id)
    except Exception:
        pass
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))


async def show_stats_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ensure_data_files()
    cats = read_json(CATS_FILE)
    prods = read_json(PROD_FILE)
    stats = compute_stats_summary()
    counts = stats.get("counts", {})
    total_revenue = int(stats.get("total_revenue", 0))
    text = (
        f"📊 Статистика магазина\n\n"
        f"📂 Каталогов: {len(cats)}\n"
        f"💊 Товаров: {len(prods)}\n\n"
        f"🟢 Новых заказов: {counts.get('new',0)}\n"
        f"🟡 В обработке: {counts.get('processing',0)}\n"
        f"🔵 Завершённых: {counts.get('done',0)}\n"
        f"❌ Отменённых: {counts.get('cancelled',0)}\n\n"
        f"💰 Общий оборот: {total_revenue} ₽\n\n"
        f"📅 За сегодня: {int(stats.get('today',0))} ₽\n"
        f"📅 За вчера: {int(stats.get('yesterday',0))} ₽\n"
        f"📅 За 7 дней: {int(stats.get('last7',0))} ₽\n"
    )
    keyboard = [
        [InlineKeyboardButton("📊 Подробнее", callback_data="stats_more")],
        [InlineKeyboardButton("🏆 Топ товаров", callback_data="stats_top")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_admin")],
    ]
    try:
        await _cleanup_last_media(context, update.message.chat_id)
    except Exception:
        pass
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))


async def show_settings_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # show settings menu with admin management
    text = "⚙️ Настройки\nВыберите опцию"
    keyboard = [
        [InlineKeyboardButton("👤 Админы", callback_data="admin_manage")],
        [InlineKeyboardButton("📝 Текст приветствия", callback_data="admin_welcome")],
        [InlineKeyboardButton("🔔 Уведомления", callback_data="admin_notify")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_admin")],
    ]
    try:
        await _cleanup_last_media(context, update.message.chat_id)
    except Exception:
        pass
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))


async def show_broadcast_menu(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = "📢 Рассылка\n\nВыберите опцию"
    keyboard = [
        [InlineKeyboardButton("📨 Создать рассылку", callback_data="broadcast_create")],
        [InlineKeyboardButton("📊 История рассылок", callback_data="broadcast_history")],
        [InlineKeyboardButton("🔔 Уведомления", callback_data="broadcast_notifications")],
        [InlineKeyboardButton("🔙 Назад", callback_data="back_admin")],
    ]
    try:
        await _cleanup_last_media(context, update.message.chat_id)
    except Exception:
        pass
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))


def get_category_markup(cat_id: int):
    cats = read_json(CATS_FILE)
    prods = [p for p in read_json(PROD_FILE) if p.get("category_id") == cat_id]
    # count products in this category
    prod_count = len(prods)
    text = f"📂 Каталог: {get_cat_name(cat_id)}\nТовары: {prod_count}"
    keyboard = []
    # children categories
    children = [c for c in cats if c.get("parent_id") == cat_id]
    for ch in children:
        keyboard.append([InlineKeyboardButton(f"🗂 {ch['name']}", callback_data=f"cat:{ch['id']}")])
    # main actions
    keyboard.append([InlineKeyboardButton("➕ Добавить товар", callback_data=f"show_prod_add:{cat_id}" )])
    # list products as buttons with their names (open product actions)
    for p in prods:
        keyboard.append([InlineKeyboardButton(f"{(p.get('name') or '-').strip()}", callback_data=f"prod:{p['id']}")])
    keyboard.append([InlineKeyboardButton("✏️ Изменить каталог", callback_data=f"rename_cat:{cat_id}"), InlineKeyboardButton("❌ Удалить каталог", callback_data=f"delcat:{cat_id}")])
    # allow adding sub-catalog only if current is root (no parent)
    current = next((c for c in cats if c["id"] == cat_id), {})
    if current.get("parent_id") is None:
        keyboard.append([InlineKeyboardButton("➕ Добавить каталог", callback_data=f"add_subcat:{cat_id}")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="back_to_cats")])
    return text, InlineKeyboardMarkup(keyboard)


async def _cleanup_last_media(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """Delete last media messages saved in chat_data for this chat to avoid thumbnail previews."""
    last = context.chat_data.pop("last_media_ids", None)
    last_chat = context.chat_data.pop("last_media_chat", None)
    if not last or last_chat != chat_id:
        return
    bot = context.bot
    for msg_id in last:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=msg_id)
        except Exception:
            # ignore deletion errors
            pass


async def safe_edit_message(query, text: str, reply_markup: InlineKeyboardMarkup = None):
    """Try to edit message text; if original message has no text (e.g. it's a media group), try edit_message_caption.
    If both fail, send a new message in the chat as a fallback.
    """
    try:
        if reply_markup is not None:
            await query.edit_message_text(text, reply_markup=reply_markup)
        else:
            await query.edit_message_text(text)
        return
    except BadRequest as e:
        msg = str(e).lower()
        if "no text" in msg or "there is no text" in msg or "message to edit" in msg:
            # try edit caption (works if message has media with caption)
            try:
                if reply_markup is not None:
                    await query.edit_message_caption(text, reply_markup=reply_markup)
                else:
                    await query.edit_message_caption(text)
                return
            except Exception:
                pass
        # fallback: reply in chat
    try:
        chat_id = query.message.chat_id
        bot = query._bot
        if reply_markup is not None:
            await bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup)
        else:
            await bot.send_message(chat_id=chat_id, text=text)
    except Exception:
        # last-resort: ignore
        return


async def safe_edit_reply_markup(query, reply_markup: InlineKeyboardMarkup):
    """Edit only the inline keyboard without changing text/caption."""
    try:
        await query.edit_message_reply_markup(reply_markup=reply_markup)
        return
    except Exception:
        try:
            chat_id = query.message.chat_id
            msg_id = query.message.message_id
            await query._bot.edit_message_reply_markup(chat_id=chat_id, message_id=msg_id, reply_markup=reply_markup)
            return
        except Exception:
            return


async def list_products_for_edit(query, context, cat_id: int) -> None:
    prods = [p for p in read_json(PROD_FILE) if p.get("category_id") == cat_id]
    if not prods:
        await safe_edit_message(query, "Список товаров пуст.")
        return
    keyboard = []
    for p in prods:
        keyboard.append([InlineKeyboardButton(f"{(p.get('name') or '-').strip()}", callback_data=f"prod:{p['id']}")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data=f"cat:{cat_id}")])
    await safe_edit_message(query, "Выберите товар для редактирования:", reply_markup=InlineKeyboardMarkup(keyboard))


async def finalize_order(source, context: ContextTypes.DEFAULT_TYPE):
    """Initiate payment for pending order and send pay link; real order will be created on webhook payment.succeeded."""
    user = source.from_user if hasattr(source, "from_user") else source.effective_user
    pending_ctx = context.user_data.pop("pending_order", None)
    if not pending_ctx:
        return
    pending = create_pending_order(
        user,
        pending_ctx.get("items", []),
        pending_ctx.get("address", ""),
        pending_ctx.get("delivery"),
        pending_ctx.get("type")
    )

    # Reserve stock immediately to prevent concurrent purchases of the last items.
    ok, err = _reserve_stock_for_pending(pending)
    if not ok:
        try:
            # remove pending
            with _interprocess_lock(_pending_lock_path()):
                pend_all = read_pending_orders() or []
                pend_all = [p for p in pend_all if int(p.get("id", 0)) != int(pending.get("id", 0))]
                write_json(PENDING_FILE, pend_all)
        except Exception:
            pass
        await context.bot.send_message(chat_id=user.id, text=f"❌ Не удалось оформить заказ: {err or 'нет в наличии'}")
        return

    # Persist reservation flags and any item filtering/total recompute done during reservation.
    try:
        with _interprocess_lock(_pending_lock_path()):
            pend_all = read_pending_orders() or []
            for po in pend_all:
                if int(po.get("id", 0)) == int(pending.get("id", 0)):
                    po["reserved"] = True
                    po["reserved_at"] = pending.get("reserved_at")
                    if pending.get("items") is not None:
                        po["items"] = pending.get("items")
                    if pending.get("total") is not None:
                        po["total"] = pending.get("total")
                    break
            write_json(PENDING_FILE, pend_all)
    except Exception:
        pass
    try:
        pay_url, payment_id = create_yookassa_payment(pending)
    except Exception as e:
        # Release reserved stock if payment creation failed
        try:
            _release_stock_for_pending(pending)
        except Exception:
            pass
        try:
            with _interprocess_lock(_pending_lock_path()):
                pend_all = read_pending_orders() or []
                pend_all = [p for p in pend_all if int(p.get("id", 0)) != int(pending.get("id", 0))]
                write_json(PENDING_FILE, pend_all)
        except Exception:
            pass
        await context.bot.send_message(chat_id=user.id, text=f"❌ Ошибка создания оплаты: {e}")
        return
    try:
        with _interprocess_lock(_pending_lock_path()):
            pend_all = read_pending_orders() or []
            for po in pend_all:
                if int(po.get("id", 0)) == int(pending.get("id", 0)):
                    po["payment_id"] = str(payment_id)
                    break
            write_json(PENDING_FILE, pend_all)
    except Exception:
        pass
    try:
        await context.bot.send_message(
            chat_id=user.id,
            text=(
                f"💰 Сумма к оплате: {pending['total']} ₽\n\n"
                "Нажмите кнопку ниже для оплаты:"
            ),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("💳 Оплатить", url=pay_url)]])
        )
    except Exception:
        pass

    # Fallback: poll YooKassa payment status and finalize without webhook
    try:
        context.application.create_task(poll_payment_and_finalize(
            context,
            int(user.id),
            int(pending.get("id")),
            str(payment_id),
            user_username=user.username,
            user_first_name=user.first_name,
            user_last_name=user.last_name,
        ))
    except Exception:
        pass


async def list_products_for_delete(query, context, cat_id: int) -> None:
    prods = [p for p in read_json(PROD_FILE) if p.get("category_id") == cat_id]
    if not prods:
        await safe_edit_message(query, "Список товаров пуст.")
        return
    keyboard = []
    for p in prods:
        keyboard.append([InlineKeyboardButton(f"{(p.get('name') or '-').strip()}", callback_data=f"prod:{p['id']}")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data=f"cat:{cat_id}")])
    await safe_edit_message(query, "Выберите товар для удаления:", reply_markup=InlineKeyboardMarkup(keyboard))


async def show_product_actions(query, context, prod_id: int) -> None:
    prods = read_json(PROD_FILE)
    prod = next((p for p in prods if p["id"] == prod_id), None)
    if not prod:
        await query.edit_message_text("Товар не найден")
        return
    cat_id = prod["category_id"]
    text = f"Товар: {prod['name']}\n{prod.get('description','')}\nЦена: {prod.get('price','-')}"
    keyboard = [
        [InlineKeyboardButton("✏️ Редактировать товар", callback_data=f"prod_editmenu:{prod_id}")],
        [InlineKeyboardButton("❌ Удалить товар", callback_data=f"delprod_confirm:{prod_id}")],
        [InlineKeyboardButton("🔙 Назад", callback_data=f"cat:{cat_id}")],
    ]
    await safe_edit_message(query, text, reply_markup=InlineKeyboardMarkup(keyboard))


async def photo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Handle photos for add/edit product and broadcast
    state = context.user_data.get("state")
    if not state:
        return

    # add-product photos
    if state.startswith("addprod_photos:"):
        _, cat_id = state.split(":")
        cat_id = int(cat_id)
        photos = context.user_data.setdefault("new_product", {}).setdefault("photos", [])
        file_id = update.message.photo[-1].file_id
        if file_id not in photos:
            photos.append(file_id)
        if len(photos) > 2:
            await update.message.reply_text("⚠️ Остальные фото игнорированы")
            photos[:] = photos[:2]
        if len(photos) >= 2:
            context.user_data["state"] = f"addprod_name:{cat_id}"
            await update.message.reply_text("✅ Фото получено\n✏️ Введите название товара")
        else:
            await update.message.reply_text("✅ Фото получено\nМожно отправить ещё фото или введите название товара")
        return

    # edit-product photos
    if state.startswith("editprod_photos:"):
        _, prod_id = state.split(":")
        prod_id = int(prod_id)
        photos = context.user_data.setdefault("edit_photos", [])
        file_id = update.message.photo[-1].file_id
        if file_id not in photos:
            photos.append(file_id)
        if len(photos) > 2:
            await update.message.reply_text("⚠️ Остальные фото игнорированы")
            photos[:] = photos[:2]
        else:
            await update.message.reply_text("✅ Фото получено")

        # update product immediately with collected photos
        prod = None
        updated = False
        with _interprocess_lock(_products_lock_path()):
            prods = read_json(PROD_FILE, default=[])
            prods = prods if isinstance(prods, list) else []
            for p in prods:
                if p.get("id") == prod_id:
                    p["photos"] = photos.copy()
                    updated = True
                    cat_id = p.get("category_id")
                    prod = dict(p)
                    break
            if updated:
                write_json(PROD_FILE, prods)

        if updated:
            await update.message.reply_text("✅ Фото товара обновлены")
            if prod:
                await send_product_card(update.message.chat_id, context, prod)
        context.user_data.pop("state", None)
        context.user_data.pop("edit_photos", None)
        return

    # broadcast photo
    if state.startswith("broadcast_photo_wait:") or state == "broadcast_photo_wait":
        file_id = update.message.photo[-1].file_id
        context.user_data.setdefault("broadcast", {})["photo"] = file_id
        # move to confirm
        context.user_data["state"] = "broadcast_confirm"
        recipients = get_recipients_list()
        cnt = len(recipients)
        b = context.user_data.get("broadcast", {})
        keyboard = [
            [InlineKeyboardButton("🚀 Отправить", callback_data="broadcast_send"), InlineKeyboardButton("❌ Отмена", callback_data="broadcast_cancel")],
        ]
        try:
            await update.message.reply_photo(photo=file_id, caption=f"📢 Предпросмотр рассылки:\n\n{b.get('text','')}\n\nПолучателей: {cnt}")
        except Exception:
            await update.message.reply_text(f"📢 Предпросмотр рассылки:\n\n{b.get('text','')}\n\nПолучателей: {cnt}")
        await update.message.reply_text("Вы можете отправить рассылку или отменить её.", reply_markup=InlineKeyboardMarkup(keyboard))
        return


async def contact_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Handle contact share during profile collection
    state = context.user_data.get("state")
    if state != "profile_phone":
        return
    phone = update.message.contact.phone_number if update.message and update.message.contact else None
    if not phone:
        return
    with _interprocess_lock(_profiles_lock_path()):
        profiles = read_profiles()
        uid = str(update.effective_user.id)
        profiles.setdefault(uid, {})
        profiles[uid]["phone"] = phone
        write_json(PROFILE_FILE, profiles)
    context.user_data.pop("state", None)
    await update.message.reply_text(
        "✅ Данные сохранены\n\n🔒 Ваши данные используются только для оформления заказа",
        reply_markup=main_menu_keyboard(update.effective_user.id),
    )
    if context.user_data.get("pending_order"):
        await show_delivery_selection_from_context(update, context)
    return


async def send_product_card(chat_id: int, context: ContextTypes.DEFAULT_TYPE, prod: dict):
    # send product card with photo when available (admin view)
    photos = _normalize_photos(prod.get("photos") or prod.get("photo") or prod.get("image"))
    bot = context.bot
    # format card: title, blank line, description, blank line, price
    title = prod.get('name','-')
    desc = prod.get('description','-')
    price = prod.get('price','-')
    stock = int(prod.get('stock', 0) or 0)
    text = (
        f"Название: {title}\n\n"
        f"Описание:\n{desc}\n\n"
        f"Цена: {price} ₽\n"
        f"📦 В наличии: {stock} шт"
    )
    keyboard = [
        [InlineKeyboardButton("✏️ Редактировать товар", callback_data=f"prod_editmenu:{prod['id']}" )],
        [InlineKeyboardButton("➕ Пополнить товар", callback_data=f"admin_restock:{prod['id']}")],
        [InlineKeyboardButton("❌ Удалить товар", callback_data=f"delprod_confirm:{prod['id']}")],
        [InlineKeyboardButton("🔙 Назад", callback_data=f"cat:{prod.get('category_id')}")],
    ]
    if photos:
        # Photo first, then text with buttons (prevents caption-length issues for long descriptions).
        try:
            photo_msg = await bot.send_photo(chat_id=chat_id, photo=photos[0])
            context.chat_data["last_media_ids"] = [photo_msg.message_id]
            context.chat_data["last_media_chat"] = chat_id
        except Exception:
            photo_msg = None
        try:
            # Truncate to 4096 to keep a single message with buttons.
            txt = text
            if isinstance(txt, str) and len(txt) > 4096:
                txt = txt[:4050].rstrip() + "\n… (описание сокращено)"
            msg = await bot.send_message(chat_id=chat_id, text=txt, reply_markup=InlineKeyboardMarkup(keyboard))
            try:
                context.user_data["screen"] = {"chat_id": int(chat_id), "message_id": int(msg.message_id)}
            except Exception:
                pass
            return
        except Exception:
            try:
                logger.exception("Failed to send admin product text after photo (prod_id=%s)", prod.get("id"))
            except Exception:
                pass

    # Text fallback (also handles very long descriptions > 4096)
    try:
        await _send_message_safe(bot, chat_id, text, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception:
        try:
            logger.exception("Failed to send admin product text card (prod_id=%s)", prod.get("id"))
        except Exception:
            pass


async def poll_payment_and_finalize(
    context: ContextTypes.DEFAULT_TYPE,
    user_id: int,
    pending_id: int,
    payment_id: str,
    user_username: str | None = None,
    user_first_name: str | None = None,
    user_last_name: str | None = None,
):
    """Poll YooKassa for payment status; on success, create order, adjust stock, notify, and clear pending.
    Works as a fallback when webhook is not available.
    """
    # Ensure YooKassa SDK configured (create_yookassa_payment sets Configuration globally)
    poll_interval = float(os.getenv("YOOKASSA_POLL_INTERVAL", "5"))
    max_attempts = int(os.getenv("YOOKASSA_POLL_MAX", "120"))  # ~10 minutes by default

    if Payment is None:
        return

    attempt = 0
    while attempt < max_attempts:
        attempt += 1
        try:
            payment = Payment.find_one(payment_id)
            status = getattr(payment, "status", None)
            if status == "succeeded":
                # Read pending order by ID
                with _interprocess_lock(_pending_lock_path()):
                    pend_all = read_pending_orders() or []
                    pending = next((p for p in pend_all if int(p.get("id", 0)) == int(pending_id)), None)
                if not pending:
                    # Already processed (maybe via webhook)
                    try:
                        await context.bot.send_message(chat_id=user_id, text="✅ Оплата прошла, заказ подтверждён")
                    except Exception:
                        pass
                    return

                # Idempotency: if an order with this payment_id already exists, just remove pending and exit.
                try:
                    pid_check = str(pending.get("payment_id") or payment_id or "").strip()
                except Exception:
                    pid_check = ""
                if pid_check:
                    try:
                        with _interprocess_lock(_orders_lock_path()):
                            existing_orders = read_orders() or []
                            if any(str(o.get("payment_id")) == pid_check for o in existing_orders):
                                try:
                                    with _interprocess_lock(_pending_lock_path()):
                                        pend_all2 = read_pending_orders() or []
                                        pend_all2 = [p for p in pend_all2 if int(p.get("id", 0)) != int(pending_id)]
                                        write_json(PENDING_FILE, pend_all2)
                                except Exception:
                                    pass
                                try:
                                    await context.bot.send_message(chat_id=user_id, text="✅ Оплата прошла, заказ подтверждён")
                                except Exception:
                                    pass
                                return
                    except Exception:
                        pass

                # Build user object
                class U:
                    def __init__(self, uid, username, first_name, last_name):
                        self.id = uid
                        self.username = username
                        self.first_name = first_name
                        self.last_name = last_name

                user_obj = U(user_id, user_username, user_first_name, user_last_name)
                items = pending.get("items", [])
                address = pending.get("address", "")
                delivery = pending.get("delivery")

                # Create real order (preserve pending number so it matches payment description)
                order = create_order(
                    user_obj,
                    items,
                    address,
                    delivery,
                    number=pending.get("number"),
                    payment_id=pending.get("payment_id"),
                    created_at=pending.get("created_at"),
                )

                # Decrease stock and alert admins if low/out-of-stock (skip if already reserved)
                try:
                try:
                    with _interprocess_lock(_products_lock_path()):
                        if pending.get("reserved"):
                            # stock was already decreased at payment creation
                            prods_all = read_json(PROD_FILE, default=[])
                            prods_all = prods_all if isinstance(prods_all, list) else []
                            prods_by_id = {int(p.get("id")): p for p in prods_all if p.get("id") is not None}
                            events = []
                            seen = set()
                            for it in order.get("items", []):
                                try:
                                    pid = int(it.get("product_id", 0))
                                except Exception:
                                    continue
                                if pid in seen:
                                    continue
                                seen.add(pid)
                                p = prods_by_id.get(pid)
                                if not p:
                                    continue
                                new_stock = int(p.get("stock", 0) or 0)
                                if new_stock == 0:
                                    events.append(("out", p.copy()))
                                elif new_stock <= 3:
                                    events.append(("low", p.copy()))
                        else:
                            prods_all = read_json(PROD_FILE, default=[])
                            prods_all = prods_all if isinstance(prods_all, list) else []
                            events = []
                            for it in order.get("items", []):
                                for p in prods_all:
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

                    admins = read_json(ADMINS_FILE)
                    for kind, prod_event in events:
                        for aid in admins:
                            try:
                                if kind == "out":
                                    await context.bot.send_message(chat_id=aid, text=(
                                        "⛔ Товар закончился\n\n"
                                        f"💊 {prod_event.get('name','-')}\n"
                                        f"🆔 ID: {prod_event.get('id')}"
                                    ))
                                else:
                                    await context.bot.send_message(chat_id=aid, text=(
                                        "⚠️ Мало товара\n\n"
                                        f"💊 {prod_event.get('name','-')}\n"
                                        f"📦 Осталось: {prod_event.get('stock', 0)} шт\n"
                                        f"🆔 ID: {prod_event.get('id')}"
                                    ))
                            except Exception:
                                pass
                except Exception:
                    pass

                # Clear cart if checkout was from cart
                try:
                    if pending.get("type") == "cart":
                        clear_cart(user_id)
                except Exception:
                    pass

                # Remove from pending
                # Remove from pending
                try:
                    with _interprocess_lock(_pending_lock_path()):
                        pend_all = read_pending_orders() or []
                        pend_all = [p for p in pend_all if int(p.get("id", 0)) != int(pending_id)]
                        write_json(PENDING_FILE, pend_all)
                except Exception:
                    pass

                # Notify user
                try:
                    await context.bot.send_message(
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

                # Notify admins
                try:
                    await notify_admin_new_order(context, order)
                except Exception:
                    pass
                return

            elif status in ("canceled", "expired"):  # optional handling
                # Release reserved stock and remove pending
                try:
                    pend_all = read_pending_orders()
                    pending = next((p for p in pend_all if int(p.get("id", 0)) == int(pending_id)), None)
                    if pending:
                        _release_stock_for_pending(pending)
                        pend_all = [p for p in pend_all if int(p.get("id", 0)) != int(pending_id)]
                        write_pending_orders(pend_all)
                except Exception:
                    pass
                try:
                    await context.bot.send_message(chat_id=user_id, text="❌ Оплата не прошла")
                except Exception:
                    pass
                return

        except Exception:
            # ignore transient errors
            pass

        try:
            await asyncio.sleep(poll_interval)
        except Exception:
            pass


def get_user_categories_markup():
    cats = read_json(CATS_FILE)
    root_cats = [c for c in cats if c.get("parent_id") is None]
    text = "📂 Каталоги\nВыберите каталог"
    keyboard = []
    for c in root_cats:
        keyboard.append([InlineKeyboardButton(f"🗂 {c['name']}", callback_data=f"user_cat:{c['id']}")])
    return text, InlineKeyboardMarkup(keyboard)


def get_user_category_markup(cat_id: int):
    cats = read_json(CATS_FILE)
    prods = [p for p in read_json(PROD_FILE) if p.get("category_id") == cat_id]
    prod_count = len(prods)
    text = f"📂 Каталог: {get_cat_name(cat_id)}\nТовары: {prod_count}"
    keyboard = []
    # children categories
    children = [c for c in cats if c.get("parent_id") == cat_id]
    for ch in children:
        keyboard.append([InlineKeyboardButton(f"🗂 {ch['name']}", callback_data=f"user_cat:{ch['id']}")])
    # products
    for p in prods:
        keyboard.append([InlineKeyboardButton(f"{(p.get('name') or '-').strip()}", callback_data=f"user_prod:{p['id']}")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="user_back_to_cats")])
    return text, InlineKeyboardMarkup(keyboard)


async def show_address_selection(query, context: ContextTypes.DEFAULT_TYPE):
    """Show saved addresses for user and option to add new."""
    uid = str(query.from_user.id)
    addrs = read_addresses()
    user_addrs = addrs.get(uid, [])
    keyboard = []
    for i, a in enumerate(user_addrs):
        label = f"Адрес: {a if len(a) <= 30 else a[:27] + '...'}"
        keyboard.append([InlineKeyboardButton(label, callback_data=f"use_address:{i}")])
    keyboard.append([InlineKeyboardButton("📝 Новый адрес", callback_data="new_address")])
    keyboard.append([InlineKeyboardButton("✏️ Изменить профиль", callback_data="edit_profile")])
    keyboard.append([InlineKeyboardButton("🔙 Отмена", callback_data="user_back_to_cats")])
    try:
        await _cleanup_last_media(context, query.message.chat_id)
    except Exception:
        pass
    await safe_edit_message(query, "✏️ Выберите адрес или добавьте новый:", reply_markup=InlineKeyboardMarkup(keyboard))


async def show_address_selection_from_context(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show saved addresses using a normal message (not editing a callback message)."""
    uid = str(update.effective_user.id)
    addrs = read_addresses()
    user_addrs = addrs.get(uid, [])
    keyboard = []
    for i, a in enumerate(user_addrs):
        label = f"Адрес: {a if len(a) <= 30 else a[:27] + '...'}"
        keyboard.append([InlineKeyboardButton(label, callback_data=f"use_address:{i}")])
    keyboard.append([InlineKeyboardButton("📝 Новый адрес", callback_data="new_address")])
    keyboard.append([InlineKeyboardButton("✏️ Изменить профиль", callback_data="edit_profile")])
    keyboard.append([InlineKeyboardButton("🔙 Отмена", callback_data="user_back_to_cats")])
    try:
        await _cleanup_last_media(context, update.message.chat_id)
    except Exception:
        pass
    await update.message.reply_text("✏️ Выберите адрес или добавьте новый:", reply_markup=InlineKeyboardMarkup(keyboard))


async def show_profile_confirmation(query, context: ContextTypes.DEFAULT_TYPE):
    """Show stored profile with options to confirm or edit before checkout."""
    profiles = read_profiles()
    uid = str(query.from_user.id)
    prof = profiles.get(uid, {})
    first_name = prof.get("first_name") or "—"
    last_name = prof.get("last_name") or "—"
    patronymic = prof.get("patronymic") or "—"
    phone = prof.get("phone") or "—"
    text = (
        f"👤 Профиль\n\n"
        f"Имя: {first_name}\n"
        f"Фамилия: {last_name}\n"
        f"Отчество: {patronymic}\n"
        f"Телефон: {phone}\n\n"
        f"Подтвердите данные или измените."
    )
    keyboard = [
        [InlineKeyboardButton("✅ Всё верно", callback_data="profile_ok")],
        [InlineKeyboardButton("✏️ Изменить профиль", callback_data="edit_profile")],
        [InlineKeyboardButton("🔙 Отмена", callback_data="user_back_to_cats")],
    ]
    await safe_edit_message(query, text, reply_markup=InlineKeyboardMarkup(keyboard))


async def show_user_categories(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ensure_data_files()
    text, markup = get_user_categories_markup()
    try:
        await _cleanup_last_media(context, update.message.chat_id)
    except Exception:
        pass
    await _screen_render_message(update, context, text, reply_markup=markup)


async def show_user_category(update_obj, context: ContextTypes.DEFAULT_TYPE, cat_id: int) -> None:
    ensure_data_files()
    text, markup = get_user_category_markup(cat_id)
    try:
        # update_obj could be query or message
        chat_id = update_obj.chat_id if hasattr(update_obj, "chat_id") else update_obj.message.chat.id
        await _cleanup_last_media(context, chat_id)
    except Exception:
        pass
    if isinstance(update_obj, Update):
        await _screen_render_message(update_obj, context, text, reply_markup=markup)
        return
    try:
        await update_obj.reply_text(text, reply_markup=markup)
    except Exception:
        pass


async def show_user_orders(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show the user's orders with status and navigation."""
    user_id = update.effective_user.id
    orders = [o for o in read_orders() if int(o.get('user_id', 0)) == int(user_id)]
    if not orders:
        keyboard = [[InlineKeyboardButton("📂 Перейти в каталог", callback_data="user_back_to_cats")]]
        try:
            await _cleanup_last_media(context, update.message.chat_id)
        except Exception:
            pass
        await _screen_render_message(update, context, "📦 У вас ещё нет заказов.", reply_markup=InlineKeyboardMarkup(keyboard))
        return
    keyboard = []
    emoji_map = {"new": "🔴", "processing": "🟡", "done": "🟢", "cancelled": "🔴"}
    for o in orders:
        st = o.get('status', 'new')
        emoji = emoji_map.get(st, "ℹ️")
        label = f"{emoji} Заказ #{o.get('number')} — {st}"
        keyboard.append([InlineKeyboardButton(label, callback_data=f"user_order:{o.get('id')}")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="user_back_to_cats")])
    try:
        await _cleanup_last_media(context, update.message.chat_id)
    except Exception:
        pass
    await _screen_render_message(update, context, "📦 Ваши заказы:", reply_markup=InlineKeyboardMarkup(keyboard))


async def show_delivery_selection(query, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("🚚 Яндекс", callback_data="delivery_select:Яндекс")],
        [InlineKeyboardButton("🚛 СДЭК", callback_data="delivery_select:СДЭК")],
        [InlineKeyboardButton("📦 Почта России", callback_data="delivery_select:Почта России")],
        [InlineKeyboardButton("🔙 Назад", callback_data="user_back_to_menu")],
    ]
    await safe_edit_message(query, "🚚 Выберите способ доставки:", reply_markup=InlineKeyboardMarkup(keyboard))


async def show_delivery_selection_from_context(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("🚚 Яндекс", callback_data="delivery_select:Яндекс")],
        [InlineKeyboardButton("🚛 СДЭК", callback_data="delivery_select:СДЭК")],
        [InlineKeyboardButton("📦 Почта России", callback_data="delivery_select:Почта России")],
        [InlineKeyboardButton("🔙 Назад", callback_data="user_back_to_menu")],
    ]

    # IMPORTANT: During profile collection we often send a confirmation message first ("✅ Данные сохранены...").
    # If we only edit the previous "screen" message, users may not notice the next step.
    # So for message-driven flows we explicitly send a new message and update the screen pointer.
    try:
        if update.message:
            msg = await update.message.reply_text(
                "🚚 Выберите способ доставки:",
                reply_markup=InlineKeyboardMarkup(keyboard),
            )
            try:
                context.user_data["screen"] = {"chat_id": int(msg.chat_id), "message_id": int(msg.message_id)}
            except Exception:
                pass
            return
    except Exception:
        pass

    await _screen_render_message(update, context, "🚚 Выберите способ доставки:", reply_markup=InlineKeyboardMarkup(keyboard))


async def show_pvz_selection(query, context: ContextTypes.DEFAULT_TYPE, delivery: str):
    addrs = read_addresses()
    uid = str(query.from_user.id)
    raw = addrs.get(uid)
    if isinstance(raw, dict):
        user_addrs = raw.get(delivery, [])
    else:
        # old format (list of addresses) -> no saved PVZ yet for this delivery
        user_addrs = []
    keyboard = []
    for i, addr in enumerate(user_addrs):
        keyboard.append([InlineKeyboardButton(addr, callback_data=f"use_pvz:{delivery}:{i}")])
    keyboard.append([InlineKeyboardButton("➕ Ввести новый ПВЗ", callback_data=f"new_pvz:{delivery}")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="user_back_to_menu")])
    await safe_edit_message(query, f"📍 Введите или выберите ПВЗ для *{delivery}*:", reply_markup=InlineKeyboardMarkup(keyboard))


async def send_product_card_user(chat_id: int, context: ContextTypes.DEFAULT_TYPE, prod: dict):
    photos = _normalize_photos(prod.get("photos") or prod.get("photo") or prod.get("image"))
    bot = context.bot
    title = prod.get('name','-')
    desc = prod.get('description','-')
    price = prod.get('price','-')
    stock = int(prod.get('stock', 0) or 0)
    availability_line = f"📦 В наличии: {stock} шт" if stock > 0 else "⛔ Нет в наличии"
    text = (
        f"{title}\n\n"
        f"{desc}\n\n"
        f"💰 Цена: {price} ₽\n"
        f"{availability_line}"
    )

    # determine whether this product is already in user's cart or favorites
    try:
        user_id = int(chat_id)
    except Exception:
        user_id = None

    in_cart = False
    in_fav = False
    if user_id is not None:
        try:
            in_cart = is_in_cart(user_id, prod.get('id'))
        except Exception:
            in_cart = False
        try:
            in_fav = prod.get('id') in get_favs(user_id)
        except Exception:
            in_fav = False

    # current quantity per product (only show controls when stock >= 2)
    show_qty_controls = stock >= 2
    qty_map = context.user_data.setdefault("qty_map", {})
    cur_qty = int(qty_map.get(prod.get('id'), 1))
    cur_qty = 1 if cur_qty < 1 else cur_qty
    if cur_qty > stock:
        cur_qty = stock if stock > 0 else 1
    # If we don't show controls (stock 0/1), force qty=1
    if not show_qty_controls:
        cur_qty = 1
    qty_map[prod.get('id')] = cur_qty

    # build keyboard: hide add buttons if already present
    keyboard = []
    # quantity controls row (only when stock >= 2)
    if show_qty_controls:
        keyboard.append([
            InlineKeyboardButton("➖", callback_data=f"qty_dec:{prod['id']}"),
            InlineKeyboardButton(str(cur_qty), callback_data="noop"),
            InlineKeyboardButton("➕", callback_data=f"qty_inc:{prod['id']}")
        ])
    if not in_cart and stock > 0:
        keyboard.append([InlineKeyboardButton("🛒 В корзину", callback_data=f"user_add_to_cart:{prod['id']}")])
    if not in_fav:
        keyboard.append([InlineKeyboardButton("⭐ В избранное", callback_data=f"user_fav:{prod['id']}")])
    if stock > 0:
        keyboard.append([InlineKeyboardButton("💳 Заказать", callback_data=f"user_buy:{prod['id']}")])
    else:
        # Role split: admins should get restock, users should get notify
        try:
            if user_id is not None and is_admin(int(user_id)):
                keyboard.append([InlineKeyboardButton("➕ Пополнить товар", callback_data=f"admin_restock:{prod['id']}")])
            else:
                keyboard.append([InlineKeyboardButton("🔔 Уведомить о поступлении", callback_data=f"notify:{prod['id']}")])
        except Exception:
            keyboard.append([InlineKeyboardButton("🔔 Уведомить о поступлении", callback_data=f"notify:{prod['id']}")])
    keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data=f"user_cat:{prod.get('category_id')}")])

    if photos:
        # Photo first (no buttons), then a single text message with buttons.
        # This avoids "ломается" when description is too long for photo caption.
        try:
            photo_msg = await bot.send_photo(chat_id=chat_id, photo=photos[0])
            context.chat_data["last_media_ids"] = [photo_msg.message_id]
            context.chat_data["last_media_chat"] = chat_id
        except Exception:
            pass

        txt = text
        if isinstance(txt, str) and len(txt) > 4096:
            txt = txt[:4050].rstrip() + "\n… (описание сокращено)"
        msg = await bot.send_message(chat_id=chat_id, text=txt, reply_markup=InlineKeyboardMarkup(keyboard))
        try:
            # Main screen message should be TEXT (editable up to 4096), not photo caption.
            context.user_data["screen"] = {"chat_id": int(chat_id), "message_id": int(msg.message_id)}
        except Exception:
            pass
        try:
            context.chat_data["last_product_msg_id"] = msg.message_id
            context.chat_data["last_product_chat"] = chat_id
            context.chat_data["last_product_is_media"] = False
        except Exception:
            pass
        return

    # Text fallback (no photo)
    txt = text
    if isinstance(txt, str) and len(txt) > 4096:
        txt = txt[:4050].rstrip() + "\n… (описание сокращено)"
    msg = await _send_message_safe(bot, chat_id, txt, reply_markup=InlineKeyboardMarkup(keyboard))
    try:
        context.user_data["screen"] = {"chat_id": int(chat_id), "message_id": int(msg.message_id)}
    except Exception:
        pass
    try:
        context.chat_data["last_product_msg_id"] = msg.message_id
        context.chat_data["last_product_chat"] = chat_id
        context.chat_data["last_product_is_media"] = False
    except Exception:
        pass


async def show_user_cart(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ensure_data_files()
    try:
        user = update.effective_user.id
        cart_items = get_cart_items(user)
        if not cart_items:
            try:
                await _cleanup_last_media(context, update.message.chat_id)
            except Exception:
                pass
            await _screen_render_message(update, context, "🛒 Ваша корзина пуста.")
            return
        prods = read_json(PROD_FILE)
        prods_by_id = {int(p.get("id")): p for p in prods if p.get("id") is not None}
        keyboard = []
        lines = []
        total = 0.0
        removed_pids: set[int] = set()
        for ci in cart_items:
            try:
                pid = int(ci.get("product_id"))
            except Exception:
                continue
            p = prods_by_id.get(pid)
            if not p:
                removed_pids.add(pid)
                continue
            price_raw = ci.get("price")
            if price_raw is None:
                price_raw = p.get("price", 0)
            try:
                price = float(price_raw)
            except (TypeError, ValueError):
                price = 0.0
            try:
                qty = int(ci.get("qty", 1) or 1)
            except Exception:
                qty = 1
            if qty < 1:
                qty = 1
            total += price * qty
            name = (p.get('name') or '-').strip()
            # show stored price per unit
            lines.append(f"• {name} — {price_raw} ₽ × {qty}")
            keyboard.append([InlineKeyboardButton(f"🛒 {name}", callback_data=f"user_prod:{p.get('id')}")])

        if removed_pids:
            for pid in removed_pids:
                try:
                    remove_from_cart(user, pid)
                except Exception:
                    pass

        if not lines:
            msg = "🛒 Ваша корзина пуста."
            if removed_pids:
                msg = "🛒 В корзине были удалённые товары — я убрал их. Корзина пуста."
            try:
                await _cleanup_last_media(context, update.message.chat_id)
            except Exception:
                pass
            await _screen_render_message(update, context, msg)
            return
        total_str = str(int(round(total))) if abs(total - round(total)) < 1e-9 else (f"{total:.2f}".rstrip("0").rstrip("."))
        # buy all / clear / back
        keyboard.append([InlineKeyboardButton("💳 Заказать всё", callback_data="user_buy_cart")])
        keyboard.append([InlineKeyboardButton("🧹 Очистить корзину", callback_data="user_clear_cart")])
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="user_back_to_cats")])
        text = "🛒 Ваша корзина:\n" + "\n".join(lines) + f"\n\n💰 Общая сумма: {total_str} ₽"
        try:
            await _cleanup_last_media(context, update.message.chat_id)
        except Exception:
            pass
        await _screen_render_message(update, context, text, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        try:
            await _screen_render_message(update, context, "❌ Ошибка загрузки корзины")
        except Exception:
            pass
        try:
            print(e)
        except Exception:
            pass


async def show_user_favorites(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    ensure_data_files()
    try:
        user = update.effective_user.id
        items = get_favs(user)
        if not items:
            try:
                await _cleanup_last_media(context, update.message.chat_id)
            except Exception:
                pass
            await _screen_render_message(update, context, "⭐ У вас нет избранных товаров.")
            return
        prods = read_json(PROD_FILE)
        keyboard = []
        lines = []
        total = 0.0
        for pid in items:
            p = next((x for x in prods if x.get("id") == pid), None)
            if p:
                price_raw = p.get("price", 0)
                try:
                    price = float(price_raw)
                except (TypeError, ValueError):
                    price = 0.0
                total += price
                name = (p.get('name') or '-').strip()
                lines.append(f"• {name} — {p.get('price','-')} ₽")
                keyboard.append([InlineKeyboardButton(f"⭐ {name}", callback_data=f"user_prod:{p.get('id')}")])
        total_str = str(int(round(total))) if abs(total - round(total)) < 1e-9 else (f"{total:.2f}".rstrip("0").rstrip("."))
        keyboard.append([InlineKeyboardButton("🧹 Очистить избранное", callback_data="user_clear_favs")])
        keyboard.append([InlineKeyboardButton("🔙 Назад", callback_data="user_back_to_cats")])
        text = "⭐ Ваше избранное:\n" + "\n".join(lines) + f"\n\n💰 Общая сумма: {total_str} ₽"
        try:
            await _cleanup_last_media(context, update.message.chat_id)
        except Exception:
            pass
        await _screen_render_message(update, context, text, reply_markup=InlineKeyboardMarkup(keyboard))
    except Exception as e:
        try:
            await _screen_render_message(update, context, "❌ Ошибка загрузки избранного")
        except Exception:
            logger.exception("Failed to send error reply to user")
        try:
            logger.exception("Error in favorites handler: %s", e)
        except Exception:
            pass


def register_handlers(app):
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("reviews", reviews_command))
    app.add_handler(MessageHandler(filters.PHOTO, photo_handler))
    app.add_handler(MessageHandler(filters.CONTACT, contact_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.add_handler(CallbackQueryHandler(callback_handler))


def _ensure_yookassa_configured() -> bool:
    """Configure YooKassa SDK globally. Returns True if available & configured."""
    if Payment is None or Configuration is None:
        return False
    shop_id = os.getenv("YOOKASSA_SHOP_ID")
    secret_key = os.getenv("YOOKASSA_SECRET_KEY")
    if not shop_id or not secret_key:
        return False
    Configuration.account_id = shop_id
    Configuration.secret_key = secret_key
    return True


async def _finalize_paid_pending(context: ContextTypes.DEFAULT_TYPE, pending: dict) -> bool:
    """Convert a pending order into a real order and notify the user. Returns True if finalized."""
    try:
        user_id = int(pending.get("user_id"))
    except Exception:
        return False

    # If an order with this payment_id already exists, just remove pending.
    payment_id = pending.get("payment_id")
    if payment_id:
        for o in read_orders():
            if str(o.get("payment_id")) == str(payment_id):
                # Try to notify user even if the order was created elsewhere (e.g. webhook)
                try:
                    await context.bot.send_message(
                        chat_id=user_id,
                        text=f"✅ Оплата заказа #{o.get('number', pending.get('number'))} прошла успешно",
                    )
                except Exception:
                    pass
                with _interprocess_lock(_pending_lock_path()):
                    pend_all = read_pending_orders() or []
                    pend_all = [p for p in pend_all if str(p.get("payment_id")) != str(payment_id)]
                    write_json(PENDING_FILE, pend_all)
                return True

    # Build a minimal telegram-like user object
    class U:
        def __init__(self, uid, username, first_name, last_name):
            self.id = uid
            self.username = username
            self.first_name = first_name
            self.last_name = last_name

    user_obj = U(
        user_id,
        pending.get("username"),
        (pending.get("client") or {}).get("first_name"),
        (pending.get("client") or {}).get("last_name"),
    )

    order = create_order(
        user_obj,
        pending.get("items", []),
        pending.get("address", ""),
        pending.get("delivery"),
        number=pending.get("number"),
        payment_id=pending.get("payment_id"),
        created_at=pending.get("created_at"),
    )

    # Decrease stock and notify admins (skip decrement if already reserved)
    try:
        events = []
        if pending.get("reserved"):
            prods_all = read_json(PROD_FILE)
            prods_by_id = {int(p.get("id")): p for p in prods_all if p.get("id") is not None}
            seen = set()
            for it in order.get("items", []):
                try:
                    pid = int(it.get("product_id", 0))
                except Exception:
                    continue
                if pid in seen:
                    continue
                seen.add(pid)
                p = prods_by_id.get(pid)
                if not p:
                    continue
                new_stock = int(p.get("stock", 0) or 0)
                if new_stock == 0:
                    events.append(("out", p.copy()))
                elif new_stock <= 3:
                    events.append(("low", p.copy()))
        else:
            prods_all = read_json(PROD_FILE)
            for it in order.get("items", []):
                for p in prods_all:
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

        for kind, prod_event in events:
            if kind == "out":
                await notify_admin_out_of_stock(context, prod_event)
            else:
                await notify_admin_low_stock(context, prod_event)
    except Exception:
        pass

    # Clear cart if checkout was from cart
    try:
        if pending.get("type") == "cart":
            clear_cart(user_id)
    except Exception:
        pass

    # Remove pending
    try:
        with _interprocess_lock(_pending_lock_path()):
            pend_all = read_pending_orders() or []
            pend_all = [p for p in pend_all if int(p.get("id", 0)) != int(pending.get("id", 0))]
            write_json(PENDING_FILE, pend_all)
    except Exception:
        pass

    # Notify user
    try:
        await context.bot.send_message(
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

    # Notify admins
    try:
        await notify_admin_new_order(context, order)
    except Exception:
        pass

    return True


async def reconcile_pending_payments_loop(app):
    """Background loop to finalize paid orders even if webhook is unreachable or bot restarts."""
    interval = float(os.getenv("YOOKASSA_RECONCILE_INTERVAL", "20"))
    if interval < 5:
        interval = 5
    while True:
        await reconcile_pending_payments_once(app)

        try:
            await asyncio.sleep(interval)
        except Exception:
            pass


async def reconcile_pending_payments_once(app) -> None:
    """One-shot reconciliation run."""
    try:
        if not _ensure_yookassa_configured():
            return
        pend_all = read_pending_orders()
        if not pend_all:
            return
        for pending in list(pend_all):
            pid = pending.get("payment_id")
            if not pid:
                continue
            try:
                payment = Payment.find_one(str(pid))
                status = getattr(payment, "status", None)
            except Exception:
                continue
            if status == "succeeded":
                class Ctx:
                    def __init__(self, bot):
                        self.bot = bot
                        self.application = app
                await _finalize_paid_pending(Ctx(app.bot), pending)
            elif status in ("canceled", "expired"):
                try:
                    uid = int(pending.get("user_id"))
                    await app.bot.send_message(chat_id=uid, text="❌ Оплата не прошла или была отменена")
                except Exception:
                    pass
                try:
                    _release_stock_for_pending(pending)
                except Exception:
                    pass
                try:
                    with _interprocess_lock(_pending_lock_path()):
                        pend_now = read_pending_orders() or []
                        pend_now = [p for p in pend_now if str(p.get("payment_id")) != str(pid)]
                        write_json(PENDING_FILE, pend_now)
                except Exception:
                    pass
    except Exception:
        return


def main() -> None:
    if not TOKEN:
        print("ERROR: TOKEN not set. Put your bot token into a .env file or set TOKEN env var.")
        return

    ensure_data_files()

    async def _post_init(application):
        # Publish bot commands (left-side commands menu in Telegram).
        # Keep existing commands; ensure /start exists; remove /starts if present.
        try:
            existing = await application.bot.get_my_commands()
            commands = [c for c in (existing or []) if c.command != "starts"]
            existing_map = {c.command: c.description for c in commands}
            if "start" not in existing_map:
                commands.insert(0, BotCommand("start", "Запустить бота"))
            if "reviews" not in existing_map:
                commands.append(BotCommand("reviews", "Отзывы"))
            await application.bot.set_my_commands(commands)
        except Exception:
            pass

        # Optional reconciliation (webhook should be the primary source of truth).
        def _truthy_env(name: str, default: str = "0") -> bool:
            v = (os.getenv(name, default) or "").strip().lower()
            return v in ("1", "true", "yes", "y", "on")

        if _truthy_env("YOOKASSA_RECONCILE_ON_STARTUP", "1"):
            try:
                await reconcile_pending_payments_once(application)
            except Exception:
                pass

        if _truthy_env("YOOKASSA_ENABLE_RECONCILE_LOOP", "0"):
            try:
                application.bot_data["reconcile_task"] = asyncio.create_task(
                    reconcile_pending_payments_loop(application)
                )
            except Exception:
                pass

    async def _post_shutdown(application):
        try:
            task = application.bot_data.get("reconcile_task")
        except Exception:
            task = None
        if task:
            try:
                task.cancel()
            except Exception:
                pass
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass

    # Increase request timeouts to avoid startup failures on slow networks (getMe timeout)
    try:
        from telegram.request import HTTPXRequest
        req = HTTPXRequest(connect_timeout=30, read_timeout=30, write_timeout=30, pool_timeout=30)
        app = ApplicationBuilder().token(TOKEN).request(req).post_init(_post_init).post_shutdown(_post_shutdown).build()
    except Exception:
        app = ApplicationBuilder().token(TOKEN).post_init(_post_init).post_shutdown(_post_shutdown).build()
    register_handlers(app)

    print("Bot is running (press Ctrl-C to stop)")
    app.run_polling()


if __name__ == "__main__":
    main()
