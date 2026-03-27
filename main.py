import asyncio
import json
import logging
import os
import random
import time

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.utils.exceptions import RetryAfter
from aiogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    LabeledPrice,
)

TOKEN = 8610520965:AAFwWbiQzkLJS6-h4LjQjtd_QA2E4w3BWRQ
PREMIUM_STARS = 10
PREMIUM_FILE = "premium_users.json"
GROUPS_FILE = "user_groups.json"
CHANNELS_FILE = "user_channels.json"
USERS_FILE = "all_users.json"
DEV_WHITELIST_FILE = "dev_whitelist.json"

logging.basicConfig(level=logging.INFO)

bot = Bot(token=TOKEN)
dp = Dispatcher(bot)

# --- Хранилище (in-memory, без персистентности) ---
user_words = {}      # uid -> str (текущее слово)
word_mode = {}       # uid -> "custom" | "random_normal" | "random_mat"
spamming = {}        # uid -> {chat_id: asyncio.Task}
user_cd = {}         # uid -> float (задержка в секундах)
user_state = {}      # uid -> state string
dev_sessions = {}    # uid -> float (timestamp последней авторизации разработчика)
turbo_mode = {}      # uid -> bool (турбо-режим только для премиум)
user_pending_chan = {}  # uid -> {"ch_id": int, "action": str}
# user_groups, user_channels, premium_users — загружаются из файлов ниже

# --- Константы турбо-режима ---
TURBO_BURST_SIZE = 5       # сообщений за одну пачку
TURBO_BURST_DELAY = 0.05   # секунд между сообщениями внутри пачки
TURBO_PAUSE = 0.8          # секунд паузы между пачками

DEV_PASSWORD = "spammingshit"

PROMO_TEXT = "@grupposnos_robot ЛУЧШИЙ ДЛЯ SP4mА, SP4M П0СТ0В В К4Н4ЛЕ И CH0C ГРУПП!"


# --- Постоянное хранение данных ---
def _load_json(path, default):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return default


def _save_json(path, data):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
    except Exception as e:
        logging.warning(f"Не удалось сохранить {path}: {e}")


def load_premium() -> set:
    return set(_load_json(PREMIUM_FILE, []))


def save_premium(users: set):
    _save_json(PREMIUM_FILE, list(users))


def load_groups() -> dict:
    # {str(uid): {str(chat_id): title}}
    raw = _load_json(GROUPS_FILE, {})
    return {int(uid): {int(cid): title for cid, title in chats.items()}
            for uid, chats in raw.items()}


def save_groups(groups: dict):
    _save_json(GROUPS_FILE, {str(uid): {str(cid): title for cid, title in chats.items()}
                              for uid, chats in groups.items()})


def load_channels() -> dict:
    # {str(uid): {str(channel_id): info_dict}}
    raw = _load_json(CHANNELS_FILE, {})
    return {int(uid): {int(cid): info for cid, info in channels.items()}
            for uid, channels in raw.items()}


def save_channels(channels: dict):
    _save_json(CHANNELS_FILE, {str(uid): {str(cid): info for cid, info in chans.items()}
                                for uid, chans in channels.items()})


def load_all_users() -> dict:
    # {str(uid): {"first_seen": float, "name": str, "username": str}}
    return _load_json(USERS_FILE, {})


def save_all_users(users: dict):
    _save_json(USERS_FILE, users)


def track_user(uid: int, name: str, username: str):
    key = str(uid)
    if key not in all_users:
        all_users[key] = {
            "first_seen": time.time(),
            "name": name,
            "username": username,
        }
        save_all_users(all_users)


premium_users: set = load_premium()
user_groups: dict = load_groups()
user_channels: dict = load_channels()
all_users: dict = load_all_users()

# --- Словари ---
NORMAL_WORDS = [
    "привет", "солнце", "небо", "звезда", "море", "лес", "река", "гора",
    "цветок", "птица", "кошка", "собака", "дерево", "камень", "ветер",
    "дождь", "снег", "огонь", "вода", "земля", "время", "жизнь", "мир",
    "день", "ночь", "утро", "вечер", "человек", "сердце", "душа",
    "любовь", "дружба", "счастье", "удача", "мечта", "надежда", "вера",
    "книга", "музыка", "танец", "игра", "спорт", "наука", "искусство",
    "дом", "семья", "улыбка", "радость", "покой", "тишина", "свобода",
]

MAT_WORDS = [
    "блядь", "хуй", "пизда", "ёбаный", "сука", "мудак",
    "залупа", "пиздец", "ёбать", "нахуй", "похуй", "заебал",
    "ёбнутый", "пиздатый", "хуйня", "блять", "пиздабол",
    "мразь", "ублюдок", "урод", "ёб твою мать", "шлюха",
    "пиздануться", "охуеть", "ёбаное", "хуесос",
]


# --- Вспомогательные клавиатуры ---
def premium_kb():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("👑 КУПИТЬ ПРЕМИУМ [10 ЗВЕЗД] ⭐️", callback_data="buy_premium"))
    return kb


def groups_kb(uid):
    groups = user_groups.get(uid, {})
    kb = InlineKeyboardMarkup()
    for chat_id, title in groups.items():
        kb.add(InlineKeyboardButton(title, callback_data=f"group_{chat_id}"))
    return kb, bool(groups)


def channels_kb(uid):
    channels = user_channels.get(uid, {})
    kb = InlineKeyboardMarkup()
    for ch_id, info in channels.items():
        label = f"📢 {info['title']}"
        kb.add(InlineKeyboardButton(label, callback_data=f"chans_{ch_id}"))
    kb.add(InlineKeyboardButton("➕ Добавить канал", callback_data="chan_add"))
    return kb, bool(channels)


# --- Отслеживание добавления бота в группу ---
@dp.message_handler(content_types=types.ContentType.NEW_CHAT_MEMBERS)
async def bot_added_to_group(msg: types.Message):
    me = await bot.get_me()
    for member in msg.new_chat_members:
        if member.id == me.id:
            uid = msg.from_user.id
            if uid not in user_groups:
                user_groups[uid] = {}
            user_groups[uid][msg.chat.id] = msg.chat.title or f"Группа {msg.chat.id}"
            save_groups(user_groups)
            try:
                await bot.send_message(
                    uid,
                    f"✅ Бот добавлен в группу *{msg.chat.title}*\n"
                    f"Используй /groups для управления.",
                    parse_mode="Markdown",
                )
            except Exception:
                pass


# --- /start ---
@dp.message_handler(commands=["start"])
async def cmd_start(msg: types.Message):
    if msg.chat.type != "private":
        return
    u = msg.from_user
    track_user(u.id, u.full_name, u.username or "")
    await msg.answer(
        "👋 Привет! Я бот для рассылки сообщений в группы и каналы.\n\n"
        "📋 Команды:\n"
        "/word — установить слово для отправки\n"
        "/groups — ваши группы\n"
        "/channel — ваши каналы (спам в комментариях)\n"
        "/cd — установить задержку между сообщениями\n"
        "/stop — остановить все рассылки\n"
        "/linkjoin — добавить группу по ссылке\n"
        "/turbo — 🚀 турбо-режим спама [только премиум]\n"
        "/premium — купить премиум"
    )


# --- /premium ---
@dp.message_handler(commands=["premium"])
async def cmd_premium(msg: types.Message):
    if msg.chat.type != "private":
        return
    uid = msg.from_user.id
    if uid in premium_users:
        await msg.answer("✅ У вас уже есть премиум статус!")
        return
    await msg.answer(
        "👑 *ПРЕМИУМ*\n\n"
        "Премиум функции:\n"
        "• ✏️ Своё слово для спама в /word\n"
        "• 🤬 Матерные слова в /word\n"
        "• ⚡️ Задержка менее 2 секунд в /cd\n"
        "• 🚀 Турбо-режим (/turbo) — пачки по 5 сообщений с паузой 0.05с\n"
        "• 🔥 Спам всех постов канала сразу\n\n"
        "🆓 Без премиума бот спамит промо-текстом:\n"
        f"`{PROMO_TEXT}`\n\n"
        "💰 Стоимость: *10 звёзд* навсегда",
        parse_mode="Markdown",
        reply_markup=premium_kb(),
    )


@dp.callback_query_handler(lambda c: c.data == "buy_premium")
async def process_buy_premium(call: types.CallbackQuery):
    await call.answer()
    prices = [LabeledPrice(label="Премиум навсегда", amount=PREMIUM_STARS)]
    await bot.send_invoice(
        chat_id=call.from_user.id,
        title="👑 Премиум навсегда",
        description="Своё слово + маты в /word, турбо-режим, спам всех постов, задержка менее 2 секунд",
        payload="premium_purchase",
        provider_token="",
        currency="XTR",
        prices=prices,
    )


@dp.pre_checkout_query_handler()
async def pre_checkout(query: types.PreCheckoutQuery):
    await query.answer(ok=True)


@dp.message_handler(content_types=types.ContentType.SUCCESSFUL_PAYMENT)
async def successful_payment(msg: types.Message):
    if msg.successful_payment.invoice_payload == "premium_purchase":
        premium_users.add(msg.from_user.id)
        save_premium(premium_users)
        await msg.answer(
            "🎉 Оплата прошла успешно!\n"
            "✅ *Премиум активирован навсегда!*\n\n"
            "Теперь вам доступны:\n"
            "• 🤬 Матерные слова в /word\n"
            "• ⚡️ Задержка менее 2 секунд в /cd\n"
            "• 🚀 Турбо-режим спама — команда /turbo",
            parse_mode="Markdown",
        )


# --- /word ---
@dp.message_handler(commands=["word"])
async def cmd_word(msg: types.Message):
    if msg.chat.type != "private":
        return
    uid = msg.from_user.id
    kb = InlineKeyboardMarkup(row_width=2)
    if uid in premium_users:
        kb.add(InlineKeyboardButton("✏️ Своё слово", callback_data="word_custom"))
    else:
        kb.add(InlineKeyboardButton("✏️ Своё слово 🔒", callback_data="word_custom_locked"))
    kb.add(InlineKeyboardButton("🎲 Случайные слова", callback_data="word_random"))
    current = _get_word(uid)
    await msg.answer(
        f"Выбери тип слова для спама:\n"
        f"Сейчас: <code>{current}</code>\n\n"
        f"🆓 Рандомные слова — бесплатно\n"
        f"✏️ Своё слово — только для 👑 премиум",
        parse_mode="HTML",
        reply_markup=kb,
    )


@dp.callback_query_handler(lambda c: c.data == "word_custom_locked")
async def word_custom_locked_cb(call: types.CallbackQuery):
    await call.answer("🔒 Только для премиум!", show_alert=True)
    await call.message.answer(
        "⚠️ Своё слово для спама — только *премиум* функция!\n\n"
        "Без премиума бот спамит промо-текстом:\n"
        f"`{PROMO_TEXT}`\n\n"
        "Купи премиум за 10 звёзд навсегда 👑",
        parse_mode="Markdown",
        reply_markup=premium_kb(),
    )


@dp.callback_query_handler(lambda c: c.data == "word_custom")
async def word_custom_cb(call: types.CallbackQuery):
    uid = call.from_user.id
    if uid not in premium_users:
        await call.answer("🔒 Только для премиум!", show_alert=True)
        await call.message.answer(
            "⚠️ Своё слово для спама — только *премиум* функция!\n\n"
            "Без премиума бот спамит промо-текстом:\n"
            f"`{PROMO_TEXT}`\n\n"
            "Купи премиум за 10 звёзд навсегда 👑",
            parse_mode="Markdown",
            reply_markup=premium_kb(),
        )
        return
    user_state[uid] = "waiting_word"
    await call.message.edit_text("✏️ Напиши слово, которое бот будет отправлять:")
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "word_random")
async def word_random_cb(call: types.CallbackQuery):
    uid = call.from_user.id
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("📖 Обычные слова", callback_data="random_normal"))
    if uid in premium_users:
        kb.add(InlineKeyboardButton("🤬 Маты", callback_data="random_mat"))
    else:
        kb.add(InlineKeyboardButton("🤬 Маты 🔒", callback_data="random_mat_locked"))
    await call.message.edit_text("Выбери тип случайных слов:", reply_markup=kb)
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "random_mat_locked")
async def random_mat_locked_cb(call: types.CallbackQuery):
    await call.answer("🔒 Только для премиум!", show_alert=True)
    await call.message.answer(
        "⚠️ Матерные слова доступны только в *премиум*!\n"
        "Премиум стоит всего 10 звёзд навсегда",
        parse_mode="Markdown",
        reply_markup=premium_kb(),
    )


@dp.callback_query_handler(lambda c: c.data == "random_normal")
async def set_random_normal_cb(call: types.CallbackQuery):
    uid = call.from_user.id
    word_mode[uid] = "random_normal"
    sample = random.choice(NORMAL_WORDS)
    user_words[uid] = sample
    await call.message.edit_text(
        f"✅ Установлены случайные обычные слова!\nПример: *{sample}*",
        parse_mode="Markdown",
    )
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "random_mat")
async def set_random_mat_cb(call: types.CallbackQuery):
    uid = call.from_user.id
    if uid not in premium_users:
        await call.answer("🔒 Только для премиум!", show_alert=True)
        await call.message.answer(
            "⚠️ Матерные слова доступны только в *премиум*!\n"
            "Премиум стоит всего 10 звёзд навсегда",
            parse_mode="Markdown",
            reply_markup=premium_kb(),
        )
        return
    word_mode[uid] = "random_mat"
    sample = random.choice(MAT_WORDS)
    user_words[uid] = sample
    await call.message.edit_text(
        f"✅ Установлены случайные матерные слова!\nПример: *{sample}*",
        parse_mode="Markdown",
    )
    await call.answer()


# --- /groups ---
@dp.message_handler(commands=["groups"])
async def cmd_groups(msg: types.Message):
    if msg.chat.type != "private":
        return
    uid = msg.from_user.id
    kb, has_groups = groups_kb(uid)
    if not has_groups:
        await msg.answer(
            "❌ У вас нет групп.\n\n"
            "Добавьте меня в группу вручную или используйте /linkjoin"
        )
        return
    await msg.answer("📋 Ваши группы:", reply_markup=kb)


@dp.callback_query_handler(lambda c: c.data.startswith("group_"))
async def select_group_cb(call: types.CallbackQuery):
    chat_id = int(call.data.split("_", 1)[1])
    uid = call.from_user.id
    title = user_groups.get(uid, {}).get(chat_id, f"Группа {chat_id}")

    is_running = uid in spamming and chat_id in spamming[uid]
    status_text = "🟢 Рассылка идёт" if is_running else "🔴 Рассылка остановлена"

    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🚀 Начать снос", callback_data=f"startsend_{chat_id}"))
    kb.add(InlineKeyboardButton("⏹ Остановить снос", callback_data=f"stopsend_{chat_id}"))
    kb.add(InlineKeyboardButton("◀️ Назад к группам", callback_data="back_groups"))

    await call.message.edit_text(
        f"📌 Группа: *{title}*\n"
        f"Статус: {status_text}\n\n"
        f"Для начала сноса нажмите на кнопку снизу",
        parse_mode="Markdown",
        reply_markup=kb,
    )
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "back_groups")
async def back_groups_cb(call: types.CallbackQuery):
    uid = call.from_user.id
    kb, has_groups = groups_kb(uid)
    if not has_groups:
        await call.message.edit_text("❌ У вас нет групп.")
        await call.answer()
        return
    await call.message.edit_text("📋 Ваши группы:", reply_markup=kb)
    await call.answer()


# --- /channel ---
@dp.message_handler(commands=["channel"])
async def cmd_channel(msg: types.Message):
    if msg.chat.type != "private":
        return
    uid = msg.from_user.id
    kb, _ = channels_kb(uid)
    channels = user_channels.get(uid, {})
    if not channels:
        await msg.answer(
            "📢 *Ваши каналы*\n\n"
            "У вас пока нет добавленных каналов.\n"
            "Нажмите кнопку ниже чтобы добавить канал:",
            parse_mode="Markdown",
            reply_markup=kb,
        )
    else:
        await msg.answer("📢 *Ваши каналы:*", parse_mode="Markdown", reply_markup=kb)


@dp.callback_query_handler(lambda c: c.data == "chan_add")
async def chan_add_cb(call: types.CallbackQuery):
    user_state[call.from_user.id] = "waiting_channel"
    await call.message.edit_text(
        "📢 Введи юзернейм канала или ссылку на него.\n\n"
        "Примеры:\n"
        "• @channelname\n"
        "• https://t.me/channelname\n\n"
        "⚠️ Бот должен быть добавлен в канал или в его группу обсуждений."
    )
    await call.answer()


@dp.callback_query_handler(lambda c: c.data.startswith("chans_"))
async def select_channel_cb(call: types.CallbackQuery):
    data = call.data
    uid = call.from_user.id
    ch_id = int(data.split("_", 1)[1])
    info = user_channels.get(uid, {}).get(ch_id)
    if not info:
        await call.answer("❌ Канал не найден", show_alert=True)
        return

    title = info["title"]
    discussion_id = info.get("discussion_id")
    can_post = info.get("can_post")
    is_running = uid in spamming and ch_id in spamming[uid]
    status_text = "🟢 Снос идёт" if is_running else "🔴 Снос остановлен"

    if discussion_id:
        if can_post is True:
            access_text = "💬 Комментарии: ✅ доступ есть"
        elif can_post is False:
            access_text = "💬 Комментарии: ❌ бот не в группе обсуждений"
        else:
            access_text = "💬 Комментарии: ❓ доступ не проверен"
    else:
        access_text = "📨 Без группы обсуждений — спам в канал (нужны права админа)"

    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🚀 Начать снос", callback_data=f"chanstart_{ch_id}"))
    kb.add(InlineKeyboardButton("⏹ Остановить снос", callback_data=f"chanstop_{ch_id}"))
    kb.add(InlineKeyboardButton("📨 Спам поста", callback_data=f"chanpost_{ch_id}"))
    if uid in premium_users:
        kb.add(InlineKeyboardButton("🔥 Спам всех постов", callback_data=f"chanallposts_{ch_id}"))
    else:
        kb.add(InlineKeyboardButton("🔥 Спам всех постов 🔒", callback_data="chanallpostslocked"))
    kb.add(InlineKeyboardButton("🔍 Проверить доступ", callback_data=f"chancheck_{ch_id}"))
    kb.add(InlineKeyboardButton("🔄 Обновить инфо", callback_data=f"chanrefresh_{ch_id}"))
    kb.add(InlineKeyboardButton("🗑 Удалить канал", callback_data=f"chandel_{ch_id}"))
    kb.add(InlineKeyboardButton("◀️ Назад", callback_data="back_channels"))

    await call.message.edit_text(
        f"📢 Канал: *{title}*\n"
        f"Статус: {status_text}\n"
        f"{access_text}",
        parse_mode="Markdown",
        reply_markup=kb,
    )
    await call.answer()


@dp.callback_query_handler(lambda c: c.data == "back_channels")
async def back_channels_cb(call: types.CallbackQuery):
    uid = call.from_user.id
    kb, _ = channels_kb(uid)
    await call.message.edit_text("📢 *Ваши каналы:*", parse_mode="Markdown", reply_markup=kb)
    await call.answer()


async def check_discussion_access(discussion_id: int):
    """Проверяет, может ли бот писать в группу обсуждений.
    Возвращает (может_писать: bool, заголовок_группы: str, юзернейм: str|None)
    """
    try:
        me = await bot.get_me()
        group_chat = await bot.get_chat(discussion_id)
        group_title = group_chat.title or f"Группа {discussion_id}"
        group_username = getattr(group_chat, "username", None)
        try:
            member = await bot.get_chat_member(discussion_id, me.id)
            can_post = member.status in ("member", "administrator", "creator")
        except Exception:
            can_post = False
        return can_post, group_title, group_username
    except Exception:
        return False, f"Группа {discussion_id}", None


@dp.callback_query_handler(lambda c: c.data.startswith("chanrefresh_"))
async def chan_refresh_cb(call: types.CallbackQuery):
    uid = call.from_user.id
    ch_id = int(call.data.split("_", 1)[1])
    info = user_channels.get(uid, {}).get(ch_id)
    if not info:
        await call.answer("❌ Канал не найден", show_alert=True)
        return
    try:
        chat = await bot.get_chat(ch_id)
        discussion_id = getattr(chat, "linked_chat_id", None)
        user_channels[uid][ch_id]["title"] = chat.title or info["title"]
        user_channels[uid][ch_id]["discussion_id"] = discussion_id

        if discussion_id:
            can_post, group_title, group_username = await check_discussion_access(discussion_id)
            user_channels[uid][ch_id]["can_post"] = can_post
            status = "✅ Бот в группе, доступ есть" if can_post else f"❌ Бот не в группе обсуждений «{group_title}»"
        else:
            status = "ℹ️ У канала нет группы обсуждений"

        save_channels(user_channels)
        await call.answer(f"🔄 Обновлено!\n{status}", show_alert=True)
    except Exception as e:
        await call.answer(f"❌ Ошибка: {e}", show_alert=True)


@dp.callback_query_handler(lambda c: c.data.startswith("chancheck_"))
async def chan_check_cb(call: types.CallbackQuery):
    uid = call.from_user.id
    ch_id = int(call.data.split("_", 1)[1])
    info = user_channels.get(uid, {}).get(ch_id)
    if not info:
        await call.answer("❌ Канал не найден", show_alert=True)
        return

    discussion_id = info.get("discussion_id")
    if not discussion_id:
        await call.answer("ℹ️ У этого канала нет группы обсуждений (комментариев)", show_alert=True)
        return

    await call.answer("🔍 Проверяю доступ...")
    can_post, group_title, group_username = await check_discussion_access(discussion_id)
    user_channels[uid][ch_id]["can_post"] = can_post
    save_channels(user_channels)

    if can_post:
        await call.message.answer(
            f"✅ *Доступ к комментариям есть!*\n\n"
            f"Бот уже в группе обсуждений *{group_title}*.\n"
            f"Можешь запускать снос.",
            parse_mode="Markdown",
        )
    else:
        link = f"@{group_username}" if group_username else f"(ID: `{discussion_id}`)"
        await call.message.answer(
            f"❌ *Бот не в группе обсуждений*\n\n"
            f"Название группы: *{group_title}*\n"
            f"Ссылка/юзернейм: {link}\n\n"
            f"📋 *Как исправить:*\n"
            f"1. Открой ту группу обсуждений\n"
            f"2. Добавь бота @grupposnos\\_robot в эту группу как участника\n"
            f"3. Вернись сюда и нажми 🔍 Проверить доступ снова\n\n"
            f"_Комментарии канала — это отдельная группа, бот должен быть там отдельно._",
            parse_mode="Markdown",
        )


@dp.callback_query_handler(lambda c: c.data.startswith("chandel_"))
async def chan_del_cb(call: types.CallbackQuery):
    uid = call.from_user.id
    ch_id = int(call.data.split("_", 1)[1])
    if uid in spamming and ch_id in spamming[uid]:
        spamming[uid][ch_id].cancel()
        del spamming[uid][ch_id]
    if uid in user_channels and ch_id in user_channels[uid]:
        del user_channels[uid][ch_id]
        save_channels(user_channels)
    kb, _ = channels_kb(uid)
    await call.message.edit_text("📢 *Ваши каналы:*", parse_mode="Markdown", reply_markup=kb)
    await call.answer("🗑 Канал удалён")


@dp.callback_query_handler(lambda c: c.data.startswith("chanstart_"))
async def chan_start_cb(call: types.CallbackQuery):
    uid = call.from_user.id
    ch_id = int(call.data.split("_", 1)[1])
    info = user_channels.get(uid, {}).get(ch_id)
    if not info:
        await call.answer("❌ Канал не найден", show_alert=True)
        return

    delay = user_cd.get(uid, 1.0)
    is_turbo_chan = turbo_mode.get(uid, False) and uid in premium_users

    if not is_turbo_chan and delay < 2.0 and uid not in premium_users:
        await call.answer("🔒 Задержка менее 2 сек. — только для премиум!", show_alert=True)
        await call.message.answer(
            "⚠️ Задержка менее 2 секунд доступна только в *премиум*!\n"
            "Без премиума минимальная задержка — 2 секунды.\n\n"
            "Премиум стоит всего 10 звёзд навсегда",
            parse_mode="Markdown",
            reply_markup=premium_kb(),
        )
        return

    if uid not in spamming:
        spamming[uid] = {}
    if ch_id in spamming[uid] and not spamming[uid][ch_id].done():
        await call.answer("⚡️ Снос уже идёт!", show_alert=True)
        return

    discussion_id = info.get("discussion_id")

    # Проверяем доступ к группе обсуждений перед запуском
    if discussion_id:
        can_post, group_title, group_username = await check_discussion_access(discussion_id)
        user_channels[uid][ch_id]["can_post"] = can_post
        save_channels(user_channels)
        if not can_post:
            link = f"@{group_username}" if group_username else f"ID: `{discussion_id}`"
            await call.answer("❌ Нет доступа к комментариям!", show_alert=True)
            await call.message.answer(
                f"❌ *Бот не в группе обсуждений канала*\n\n"
                f"Группа обсуждений: *{group_title}* ({link})\n\n"
                f"📋 *Как исправить:*\n"
                f"1. Найди группу обсуждений канала\n"
                f"2. Добавь @grupposnos\\_robot туда как участника\n"
                f"3. Вернись и нажми 🔍 Проверить доступ\n"
                f"4. После подтверждения доступа нажми 🚀 Начать снос",
                parse_mode="Markdown",
            )
            return
        target_id = discussion_id
        mode = "комментариях"
    else:
        target_id = ch_id
        mode = "канале"

    is_turbo = turbo_mode.get(uid, False) and uid in premium_users

    if is_turbo:
        task = asyncio.create_task(turbo_spam_task(uid, target_id))
        mode_label = f"🚀 ТУРБО ({TURBO_BURST_SIZE} сообщ. каждые {TURBO_PAUSE}с)"
    else:
        task = asyncio.create_task(spam_task(uid, target_id, delay))
        mode_label = f"задержка: {delay} сек."

    spamming[uid][ch_id] = task

    await call.answer("✅ Снос запущен!")
    await call.message.answer(
        f"🚀 Снос запущен в *{info['title']}* ({mode}, {mode_label})",
        parse_mode="Markdown",
    )


@dp.callback_query_handler(lambda c: c.data.startswith("chanstop_"))
async def chan_stop_cb(call: types.CallbackQuery):
    uid = call.from_user.id
    ch_id = int(call.data.split("_", 1)[1])
    info = user_channels.get(uid, {}).get(ch_id, {})
    if uid in spamming and ch_id in spamming[uid]:
        spamming[uid][ch_id].cancel()
        del spamming[uid][ch_id]
        await call.answer("⏹ Снос остановлен!")
        await call.message.answer(
            f"⏹ Снос в *{info.get('title', 'канале')}* остановлен.",
            parse_mode="Markdown",
        )
    else:
        await call.answer("❌ Снос не запущен", show_alert=True)


# --- 📨 Спам поста (конкретный пост по ссылке) ---
@dp.callback_query_handler(lambda c: c.data.startswith("chanpost_"))
async def chan_post_cb(call: types.CallbackQuery):
    uid = call.from_user.id
    ch_id = int(call.data.split("_", 1)[1])
    info = user_channels.get(uid, {}).get(ch_id)
    if not info:
        await call.answer("❌ Канал не найден", show_alert=True)
        return

    discussion_id = info.get("discussion_id")
    if not discussion_id:
        await call.answer("❌ У канала нет группы обсуждений — нельзя спамить под постами", show_alert=True)
        return

    user_pending_chan[uid] = {"ch_id": ch_id, "action": "post_link"}
    user_state[uid] = "waiting_post_link"
    await call.message.edit_text(
        "📨 *Спам под постом*\n\n"
        "Введи ссылку на пост канала:\n"
        "Пример: `https://t.me/channelname/123`",
        parse_mode="Markdown",
    )
    await call.answer()


# --- 🔥 Спам всех постов (только премиум) ---
@dp.callback_query_handler(lambda c: c.data == "chanallpostslocked")
async def chan_all_posts_locked_cb(call: types.CallbackQuery):
    await call.answer("🔒 Только для премиум!", show_alert=True)
    await call.message.answer(
        "⚠️ Спам всех постов доступен только в *премиум*!\n"
        "Премиум стоит всего 10 звёзд навсегда",
        parse_mode="Markdown",
        reply_markup=premium_kb(),
    )


@dp.callback_query_handler(lambda c: c.data.startswith("chanallposts_"))
async def chan_all_posts_cb(call: types.CallbackQuery):
    uid = call.from_user.id
    ch_id = int(call.data.split("_", 1)[1])

    if uid not in premium_users:
        await call.answer("🔒 Только для премиум!", show_alert=True)
        return

    info = user_channels.get(uid, {}).get(ch_id)
    if not info:
        await call.answer("❌ Канал не найден", show_alert=True)
        return

    discussion_id = info.get("discussion_id")
    if not discussion_id:
        await call.answer("❌ У канала нет группы обсуждений — нельзя спамить под постами", show_alert=True)
        return

    can_post = info.get("can_post")
    if can_post is False:
        await call.answer("❌ Нет доступа к группе обсуждений!", show_alert=True)
        return

    if uid not in spamming:
        spamming[uid] = {}
    if ch_id in spamming[uid] and not spamming[uid][ch_id].done():
        spamming[uid][ch_id].cancel()

    delay = user_cd.get(uid, 1.0)
    task = asyncio.create_task(all_posts_spam_task(uid, discussion_id, delay))
    spamming[uid][ch_id] = task

    await call.answer("🔥 Запущено!")
    await call.message.answer(
        f"🔥 *Спам всех постов запущен!*\n"
        f"Канал: *{info['title']}*\n\n"
        f"Бот автоматически определит последний пост и начнёт проверку...\n"
        f"_Пришлю уведомление когда найду посты._\n\n"
        f"_Для остановки: /stop или кнопка ⏹ Остановить снос_",
        parse_mode="Markdown",
    )


# --- Получить текущее слово для пользователя ---
def _get_word(uid: int) -> str:
    mode = word_mode.get(uid, "promo")
    if mode == "random_normal":
        return random.choice(NORMAL_WORDS)
    elif mode == "random_mat":
        if uid in premium_users:
            return random.choice(MAT_WORDS)
        return random.choice(NORMAL_WORDS)
    elif mode == "custom":
        if uid in premium_users:
            return user_words.get(uid, PROMO_TEXT)
        return PROMO_TEXT
    return PROMO_TEXT


# --- Задача спама (обычный режим) ---
async def spam_task(uid: int, chat_id: int, delay: float, reply_to: int = None, thread_id: int = None):
    kwargs = {}
    if thread_id:
        kwargs["message_thread_id"] = thread_id
    if reply_to:
        kwargs["reply_to_message_id"] = reply_to
    while True:
        try:
            await bot.send_message(chat_id, _get_word(uid), **kwargs)
        except RetryAfter as e:
            logging.warning(f"FloodWait [{uid} -> {chat_id}]: ждём {e.timeout}с")
            await asyncio.sleep(e.timeout)
            continue
        except Exception as e:
            logging.warning(f"Spam error [{uid} -> {chat_id}]: {e}")
        await asyncio.sleep(delay)


# --- Задача спама (турбо-режим, только премиум) ---
async def turbo_spam_task(uid: int, chat_id: int, reply_to: int = None, thread_id: int = None):
    kwargs = {}
    if thread_id:
        kwargs["message_thread_id"] = thread_id
    if reply_to:
        kwargs["reply_to_message_id"] = reply_to
    while True:
        for _ in range(TURBO_BURST_SIZE):
            try:
                await bot.send_message(chat_id, _get_word(uid), **kwargs)
            except RetryAfter as e:
                logging.warning(f"Turbo FloodWait [{uid} -> {chat_id}]: ждём {e.timeout}с")
                await asyncio.sleep(e.timeout)
                continue
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logging.warning(f"Turbo spam error [{uid} -> {chat_id}]: {e}")
            await asyncio.sleep(TURBO_BURST_DELAY)
        await asyncio.sleep(TURBO_PAUSE)


# --- Авто-определение диапазона постов в группе обсуждений ---
async def _detect_max_post_id(discussion_id: int) -> int:
    """Отправляет тестовое сообщение, берёт его ID как верхний предел, удаляет."""
    try:
        temp = await bot.send_message(discussion_id, ".")
        max_id = temp.message_id - 1
        await bot.delete_message(discussion_id, temp.message_id)
        return max(max_id, 1)
    except Exception as e:
        logging.warning(f"detect_max_post_id error: {e}")
        return 500  # fallback


# --- Задача спама под всеми постами канала (только премиум) ---
async def all_posts_spam_task(uid: int, discussion_id: int, delay: float):
    # Определяем верхний предел автоматически
    try:
        await bot.send_message(uid, "🔍 Определяю последний пост канала...", parse_mode="Markdown")
    except Exception:
        pass

    max_post_id = await _detect_max_post_id(discussion_id)

    # Проверяем все посты от последнего вниз (до 300 назад)
    valid_ids = []
    probe_end = max(1, max_post_id - 299)

    for post_id in range(max_post_id, probe_end - 1, -1):
        try:
            sent = await bot.send_message(
                discussion_id, _get_word(uid),
                message_thread_id=post_id,
                reply_to_message_id=post_id,
            )
            valid_ids.append(post_id)
        except RetryAfter as e:
            await asyncio.sleep(e.timeout)
        except asyncio.CancelledError:
            raise
        except Exception:
            pass  # Пост не существует или недоступен
        await asyncio.sleep(0.15)

    count = len(valid_ids)
    try:
        await bot.send_message(
            uid,
            f"✅ Проверка завершена! Найдено *{count}* постов.\n"
            f"Начинаю непрерывный спам под всеми ними...",
            parse_mode="Markdown",
        )
    except Exception:
        pass

    if not valid_ids:
        return

    while True:
        for post_id in valid_ids:
            try:
                await bot.send_message(
                    discussion_id, _get_word(uid),
                    message_thread_id=post_id,
                    reply_to_message_id=post_id,
                )
            except RetryAfter as e:
                await asyncio.sleep(e.timeout)
                continue
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logging.warning(f"All posts spam error [{uid}]: {e}")
            await asyncio.sleep(delay)


@dp.callback_query_handler(lambda c: c.data.startswith("startsend_"))
async def start_send_cb(call: types.CallbackQuery):
    uid = call.from_user.id
    chat_id = int(call.data.split("_", 1)[1])

    delay = user_cd.get(uid, 1.0)
    is_turbo = turbo_mode.get(uid, False) and uid in premium_users

    if not is_turbo and delay < 2.0 and uid not in premium_users:
        await call.answer("🔒 Задержка менее 2 сек. — только для премиум!", show_alert=True)
        await call.message.answer(
            "⚠️ Задержка менее 2 секунд доступна только в *премиум*!\n"
            "Без премиума минимальная задержка — 2 секунды.\n\n"
            "Премиум стоит всего 10 звёзд навсегда",
            parse_mode="Markdown",
            reply_markup=premium_kb(),
        )
        return

    if uid not in spamming:
        spamming[uid] = {}

    if chat_id in spamming[uid] and not spamming[uid][chat_id].done():
        await call.answer("⚡️ Рассылка уже идёт!", show_alert=True)
        return

    if is_turbo:
        task = asyncio.create_task(turbo_spam_task(uid, chat_id))
        mode_label = f"🚀 ТУРБО ({TURBO_BURST_SIZE} сообщ. каждые {TURBO_PAUSE}с)"
    else:
        task = asyncio.create_task(spam_task(uid, chat_id, delay))
        mode_label = f"задержка: {delay} сек."

    spamming[uid][chat_id] = task

    await call.answer("✅ Рассылка запущена!")
    title = user_groups.get(uid, {}).get(chat_id, f"Группа {chat_id}")
    await call.message.answer(f"🚀 Рассылка запущена в *{title}* ({mode_label})", parse_mode="Markdown")


@dp.callback_query_handler(lambda c: c.data.startswith("stopsend_"))
async def stop_send_cb(call: types.CallbackQuery):
    uid = call.from_user.id
    chat_id = int(call.data.split("_", 1)[1])

    if uid in spamming and chat_id in spamming[uid]:
        spamming[uid][chat_id].cancel()
        del spamming[uid][chat_id]
        await call.answer("⏹ Рассылка остановлена!")
        title = user_groups.get(uid, {}).get(chat_id, f"Группа {chat_id}")
        await call.message.answer(f"⏹ Рассылка в *{title}* остановлена.", parse_mode="Markdown")
    else:
        await call.answer("❌ Рассылка не запущена", show_alert=True)


# --- /stop ---
@dp.message_handler(commands=["stop"])
async def cmd_stop(msg: types.Message):
    if msg.chat.type != "private":
        return
    uid = msg.from_user.id
    if uid in spamming and spamming[uid]:
        count = len(spamming[uid])
        for task in spamming[uid].values():
            task.cancel()
        spamming[uid] = {}
        await msg.answer(f"⏹ Остановлено рассылок: {count}")
    else:
        await msg.answer("❌ Нет активных рассылок")


# --- /cd ---
@dp.message_handler(commands=["cd"])
async def cmd_cd(msg: types.Message):
    if msg.chat.type != "private":
        return
    uid = msg.from_user.id
    user_state[uid] = "waiting_cd"
    current = user_cd.get(uid, 1.0)
    min_note = "любое значение" if uid in premium_users else "минимум 2 секунды (менее 2 сек. — только премиум)"
    await msg.answer(
        f"⏱ Введи задержку в секундах ({min_note}):\n"
        f"Текущая задержка: *{current} сек.*",
        parse_mode="Markdown",
    )


# --- /turbo ---
@dp.message_handler(commands=["turbo"])
async def cmd_turbo(msg: types.Message):
    if msg.chat.type != "private":
        return
    uid = msg.from_user.id
    if uid not in premium_users:
        await msg.answer(
            "🔒 *Турбо-режим доступен только для премиум!*\n\n"
            f"В турбо-режиме бот отправляет *{TURBO_BURST_SIZE} сообщений* пачками\n"
            f"с паузой *{TURBO_BURST_DELAY}с* внутри пачки и *{TURBO_PAUSE}с* между пачками.\n\n"
            "Это максимально быстрый спам без бана от Telegram.\n\n"
            "Премиум стоит всего 10 звёзд навсегда",
            parse_mode="Markdown",
            reply_markup=premium_kb(),
        )
        return

    current = turbo_mode.get(uid, False)
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ Включить турбо", callback_data="turbo_on"),
        InlineKeyboardButton("❌ Выключить турбо", callback_data="turbo_off"),
    )
    status = "🟢 Включён" if current else "🔴 Выключен"
    await msg.answer(
        f"🚀 *Турбо-режим спама*\n\n"
        f"Текущий статус: {status}\n\n"
        f"📊 Параметры турбо:\n"
        f"• Сообщений за пачку: *{TURBO_BURST_SIZE}*\n"
        f"• Пауза внутри пачки: *{TURBO_BURST_DELAY}с*\n"
        f"• Пауза между пачками: *{TURBO_PAUSE}с*\n\n"
        f"⚡️ Скорость: ~{int(TURBO_BURST_SIZE / (TURBO_BURST_SIZE * TURBO_BURST_DELAY + TURBO_PAUSE))} сообщ/сек\n\n"
        f"_При FloodWait бот автоматически подождёт и продолжит._",
        parse_mode="Markdown",
        reply_markup=kb,
    )


@dp.callback_query_handler(lambda c: c.data in ("turbo_on", "turbo_off"))
async def turbo_toggle_cb(call: types.CallbackQuery):
    uid = call.from_user.id
    if uid not in premium_users:
        await call.answer("🔒 Только для премиум!", show_alert=True)
        return
    enable = call.data == "turbo_on"
    turbo_mode[uid] = enable
    status = "🟢 Включён" if enable else "🔴 Выключен"
    action = "включён" if enable else "выключен"
    await call.answer(f"{'🚀' if enable else '⏹'} Турбо-режим {action}!")
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("✅ Включить турбо", callback_data="turbo_on"),
        InlineKeyboardButton("❌ Выключить турбо", callback_data="turbo_off"),
    )
    await call.message.edit_text(
        f"🚀 *Турбо-режим спама*\n\n"
        f"Текущий статус: {status}\n\n"
        f"📊 Параметры турбо:\n"
        f"• Сообщений за пачку: *{TURBO_BURST_SIZE}*\n"
        f"• Пауза внутри пачки: *{TURBO_BURST_DELAY}с*\n"
        f"• Пауза между пачками: *{TURBO_PAUSE}с*\n\n"
        f"⚡️ Скорость: ~{int(TURBO_BURST_SIZE / (TURBO_BURST_SIZE * TURBO_BURST_DELAY + TURBO_PAUSE))} сообщ/сек\n\n"
        f"_При FloodWait бот автоматически подождёт и продолжит._",
        parse_mode="Markdown",
        reply_markup=kb,
    )


DEV_SESSION_TTL = 86400  # 24 часа в секундах


def is_dev_authed(uid: int) -> bool:
    ts = dev_sessions.get(uid)
    return ts is not None and (time.time() - ts) < DEV_SESSION_TTL


# --- /developer ---
@dp.message_handler(commands=["developer"])
async def cmd_developer(msg: types.Message):
    if msg.chat.type != "private":
        return
    uid = msg.from_user.id
    if is_dev_authed(uid):
        await msg.answer(
            dev_menu_text(uid),
            parse_mode="Markdown",
            reply_markup=dev_menu_kb(uid),
        )
        return
    user_state[uid] = "waiting_dev_password"
    await msg.answer(
        "🔒 *Меню разработчика*\n\n"
        "⚠️ Это меню предназначено только для разработчиков.\n\n"
        "Введите пароль для доступа:",
        parse_mode="Markdown",
    )


# --- /linkjoin ---
@dp.message_handler(commands=["linkjoin"])
async def cmd_linkjoin(msg: types.Message):
    if msg.chat.type != "private":
        return
    user_state[msg.from_user.id] = "waiting_link"
    await msg.answer(
        "🔗 Отправь ссылку-приглашение группы (например: t.me/joinchat/...).\n\n"
        "После этого добавь меня в ту группу, и она появится в /groups"
    )


# --- Обработчики состояний ---
@dp.message_handler(lambda m: m.chat.type == "private" and user_state.get(m.from_user.id) == "waiting_word")
async def save_word_state(msg: types.Message):
    uid = msg.from_user.id
    user_words[uid] = msg.text
    word_mode[uid] = "custom"
    user_state.pop(uid, None)
    await msg.answer(f"✅ Слово установлено: *{msg.text}*", parse_mode="Markdown")


@dp.message_handler(lambda m: m.chat.type == "private" and user_state.get(m.from_user.id) == "waiting_cd")
async def save_cd_state(msg: types.Message):
    uid = msg.from_user.id
    try:
        cd = float(msg.text.replace(",", "."))
        if cd <= 0:
            raise ValueError("must be positive")
        if cd < 2.0 and uid not in premium_users:
            await msg.answer(
                "⚠️ Без премиума нельзя ставить задержку менее 2 секунд!\n"
                "Минимальная задержка без премиума — 2 секунды.\n\n"
                "Премиум стоит всего 10 звёзд навсегда",
                reply_markup=premium_kb(),
            )
            return
        user_cd[uid] = cd
        user_state.pop(uid, None)
        await msg.answer(f"✅ Задержка установлена: *{cd} сек.*", parse_mode="Markdown")
    except ValueError:
        await msg.answer("❌ Введи корректное число, например: 1.5")


def dev_menu_kb(uid: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    if uid in premium_users:
        kb.add(InlineKeyboardButton("🗑 Удалить мой премиум", callback_data="dev_remove_premium"))
    else:
        kb.add(InlineKeyboardButton("👑 Выдать мне премиум", callback_data="dev_give_premium"))
    kb.add(InlineKeyboardButton("🎁 Выдать премиум по @username", callback_data="dev_give_by_username"))
    kb.add(InlineKeyboardButton("🚫 Забрать премиум по @username", callback_data="dev_remove_by_username"))
    kb.add(InlineKeyboardButton("👑 Список премиум пользователей", callback_data="dev_premium_list"))
    kb.add(InlineKeyboardButton("👥 Статистика пользователей", callback_data="dev_users_stats"))
    return kb


def dev_menu_text(uid: int) -> str:
    now = time.time()
    total = len(all_users)
    last_24h = sum(1 for u in all_users.values() if now - u["first_seen"] < 86400)
    last_week = sum(1 for u in all_users.values() if now - u["first_seen"] < 604800)
    last_month = sum(1 for u in all_users.values() if now - u["first_seen"] < 2592000)
    prem_count = len(premium_users)
    status = "👑 Активен" if uid in premium_users else "❌ Не активен"
    return (
        "🛠 *Меню разработчика*\n\n"
        f"Мой премиум: {status}\n\n"
        f"📊 *Статистика:*\n"
        f"👥 Всего пользователей: *{total}*\n"
        f"🕐 За 24 часа: *{last_24h}*\n"
        f"📅 За неделю: *{last_week}*\n"
        f"🗓 За месяц: *{last_month}*\n"
        f"👑 Премиум: *{prem_count}*\n\n"
        "Выбери действие:"
    )


def _esc(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _user_link(uid_str: str, info: dict) -> str:
    name = _esc(info.get("name", "—"))
    username = info.get("username", "")
    prem = " 👑" if int(uid_str) in premium_users else ""
    if username:
        return f'<a href="https://t.me/{username}">{name} (@{_esc(username)})</a>{prem}'
    else:
        return f'<a href="tg://user?id={uid_str}">{name}</a> — ID: <code>{uid_str}</code>{prem}'


@dp.message_handler(lambda m: m.chat.type == "private" and user_state.get(m.from_user.id) == "waiting_dev_password")
async def check_dev_password(msg: types.Message):
    uid = msg.from_user.id
    user_state.pop(uid, None)
    if msg.text.strip() == DEV_PASSWORD:
        dev_sessions[uid] = time.time()
        await msg.answer(
            "✅ *Пароль верный!*\n\n"
            "🔓 Сессия активна 24 часа — пароль больше не нужен.\n\n"
            + dev_menu_text(uid),
            parse_mode="Markdown",
            reply_markup=dev_menu_kb(uid),
        )
    else:
        await msg.answer(
            "❌ *Неверный пароль!*\n\n"
            "Доступ запрещён.",
            parse_mode="Markdown",
        )


@dp.callback_query_handler(lambda c: c.data == "dev_give_premium")
async def dev_give_premium_cb(call: types.CallbackQuery):
    uid = call.from_user.id
    premium_users.add(uid)
    save_premium(premium_users)
    await call.answer("✅ Премиум выдан!", show_alert=False)
    await call.message.edit_text(
        dev_menu_text(uid),
        parse_mode="Markdown",
        reply_markup=dev_menu_kb(uid),
    )


@dp.callback_query_handler(lambda c: c.data == "dev_remove_premium")
async def dev_remove_premium_cb(call: types.CallbackQuery):
    uid = call.from_user.id
    premium_users.discard(uid)
    save_premium(premium_users)
    await call.answer("🗑 Премиум удалён!", show_alert=False)
    await call.message.edit_text(
        dev_menu_text(uid),
        parse_mode="Markdown",
        reply_markup=dev_menu_kb(uid),
    )


@dp.callback_query_handler(lambda c: c.data == "dev_give_by_username")
async def dev_give_by_username_cb(call: types.CallbackQuery):
    uid = call.from_user.id
    user_state[uid] = "dev_waiting_give_username"
    await call.answer()
    await call.message.answer(
        "🎁 Введи @username пользователя, которому хочешь выдать премиум:\n\n"
        "_(Пользователь должен был хотя бы раз запустить бота)_",
        parse_mode="Markdown",
    )


@dp.callback_query_handler(lambda c: c.data == "dev_remove_by_username")
async def dev_remove_by_username_cb(call: types.CallbackQuery):
    uid = call.from_user.id
    user_state[uid] = "dev_waiting_remove_username"
    await call.answer()
    await call.message.answer(
        "🚫 Введи @username пользователя, у которого хочешь забрать премиум:",
        parse_mode="Markdown",
    )


def _find_uid_by_username(username: str):
    username = username.lstrip("@").lower()
    for uid_str, info in all_users.items():
        if info.get("username", "").lower() == username:
            return int(uid_str), info
    return None, None


@dp.message_handler(lambda m: m.chat.type == "private" and user_state.get(m.from_user.id) in ("dev_waiting_give_username", "dev_waiting_remove_username"))
async def dev_username_action(msg: types.Message):
    uid = msg.from_user.id
    action = user_state.pop(uid, None)
    text = msg.text.strip()
    target_uid, info = _find_uid_by_username(text)

    if target_uid is None:
        await msg.answer(
            f"❌ Пользователь *{text}* не найден в базе.\n"
            "Он должен был хотя бы раз написать /start боту.",
            parse_mode="Markdown",
            reply_markup=dev_menu_kb(uid),
        )
        return

    name = info.get("name", "—")
    uname = f"@{info.get('username', '')}"

    if action == "dev_waiting_give_username":
        if target_uid in premium_users:
            await msg.answer(
                f"ℹ️ У *{name}* ({uname}) уже есть премиум.",
                parse_mode="Markdown",
                reply_markup=dev_menu_kb(uid),
            )
        else:
            premium_users.add(target_uid)
            save_premium(premium_users)
            await msg.answer(
                f"✅ Премиум выдан пользователю *{name}* ({uname})",
                parse_mode="Markdown",
                reply_markup=dev_menu_kb(uid),
            )
    else:
        if target_uid not in premium_users:
            await msg.answer(
                f"ℹ️ У *{name}* ({uname}) нет премиума.",
                parse_mode="Markdown",
                reply_markup=dev_menu_kb(uid),
            )
        else:
            premium_users.discard(target_uid)
            save_premium(premium_users)
            await msg.answer(
                f"🚫 Премиум забран у пользователя *{name}* ({uname})",
                parse_mode="Markdown",
                reply_markup=dev_menu_kb(uid),
            )


def _back_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data="dev_back"))
    return kb


@dp.callback_query_handler(lambda c: c.data == "dev_back")
async def dev_back_cb(call: types.CallbackQuery):
    uid = call.from_user.id
    await call.answer()
    await call.message.edit_text(
        dev_menu_text(uid),
        parse_mode="Markdown",
        reply_markup=dev_menu_kb(uid),
    )


@dp.callback_query_handler(lambda c: c.data == "dev_premium_list")
async def dev_premium_list_cb(call: types.CallbackQuery):
    await call.answer()
    if not premium_users:
        text = "👑 <b>Премиум пользователи</b>\n\nСписок пуст."
    else:
        lines = []
        for uid_int in premium_users:
            uid_str = str(uid_int)
            info = all_users.get(uid_str)
            if info:
                lines.append(f"• {_user_link(uid_str, info)}")
            else:
                lines.append(f"• <code>{uid_int}</code>")
        text = f"👑 <b>Премиум пользователи</b> ({len(premium_users)}):\n\n" + "\n".join(lines)
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=_back_kb())


@dp.callback_query_handler(lambda c: c.data == "dev_users_stats")
async def dev_users_stats_cb(call: types.CallbackQuery):
    await call.answer()
    now = time.time()
    total = len(all_users)
    last_24h = sum(1 for u in all_users.values() if now - u["first_seen"] < 86400)
    last_week = sum(1 for u in all_users.values() if now - u["first_seen"] < 604800)
    last_month = sum(1 for u in all_users.values() if now - u["first_seen"] < 2592000)
    text = (
        "👥 *Статистика пользователей*\n\n"
        f"Всего за всё время: *{total}*\n"
        f"За 24 часа: *{last_24h}*\n"
        f"За неделю: *{last_week}*\n"
        f"За месяц: *{last_month}*\n\n"
        "Выбери период для просмотра списка:"
    )
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton(f"Всё время ({total})", callback_data="dev_ulist_all"),
        InlineKeyboardButton(f"24 часа ({last_24h})", callback_data="dev_ulist_24h"),
        InlineKeyboardButton(f"Неделя ({last_week})", callback_data="dev_ulist_week"),
        InlineKeyboardButton(f"Месяц ({last_month})", callback_data="dev_ulist_month"),
    )
    kb.add(InlineKeyboardButton("⬅️ Назад", callback_data="dev_back"))
    await call.message.edit_text(text, parse_mode="Markdown", reply_markup=kb)


def _users_list_text(title: str, filtered: list) -> str:
    if not filtered:
        return f"👥 <b>{_esc(title)}</b>\n\nНет пользователей за этот период."
    lines = []
    for uid_str, info in filtered:
        lines.append(f"• {_user_link(uid_str, info)}")
    text = f"👥 <b>{_esc(title)}</b> ({len(filtered)}):\n\n" + "\n".join(lines)
    if len(text) > 4000:
        text = text[:3950] + "\n\n<i>...список обрезан</i>"
    return text


@dp.callback_query_handler(lambda c: c.data.startswith("dev_ulist_"))
async def dev_ulist_cb(call: types.CallbackQuery):
    await call.answer()
    period = call.data.split("dev_ulist_")[1]
    now = time.time()
    limits = {"all": None, "24h": 86400, "week": 604800, "month": 2592000}
    titles = {"all": "Все пользователи", "24h": "Пользователи за 24 часа",
               "week": "Пользователи за неделю", "month": "Пользователи за месяц"}
    limit = limits.get(period)
    title = titles.get(period, "Пользователи")
    if limit is None:
        filtered = list(all_users.items())
    else:
        filtered = [(k, v) for k, v in all_users.items() if now - v["first_seen"] < limit]
    text = _users_list_text(title, filtered)
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=_back_kb())


def _parse_post_id(text: str):
    """Извлекает ID поста из ссылки вида https://t.me/channel/123"""
    try:
        return int(text.strip().rstrip("/").split("/")[-1])
    except (ValueError, IndexError):
        return None


@dp.message_handler(lambda m: m.chat.type == "private" and user_state.get(m.from_user.id) == "waiting_post_link")
async def save_post_link_state(msg: types.Message):
    uid = msg.from_user.id
    user_state.pop(uid, None)
    pending = user_pending_chan.pop(uid, None)
    if not pending:
        return

    ch_id = pending["ch_id"]
    info = user_channels.get(uid, {}).get(ch_id)
    if not info:
        await msg.answer("❌ Канал не найден")
        return

    discussion_id = info.get("discussion_id")
    if not discussion_id:
        await msg.answer("❌ У канала нет группы обсуждений")
        return

    post_id = _parse_post_id(msg.text)
    if not post_id:
        await msg.answer(
            "❌ Неверный формат ссылки.\n"
            "Пример: `https://t.me/channelname/123`",
            parse_mode="Markdown",
        )
        return

    if uid not in spamming:
        spamming[uid] = {}
    if ch_id in spamming[uid] and not spamming[uid][ch_id].done():
        spamming[uid][ch_id].cancel()

    delay = user_cd.get(uid, 1.0)
    is_turbo = turbo_mode.get(uid, False) and uid in premium_users

    if is_turbo:
        task = asyncio.create_task(turbo_spam_task(uid, discussion_id, reply_to=post_id, thread_id=post_id))
        mode_label = f"🚀 ТУРБО"
    else:
        task = asyncio.create_task(spam_task(uid, discussion_id, delay, reply_to=post_id, thread_id=post_id))
        mode_label = f"задержка: {delay}с"

    spamming[uid][ch_id] = task
    await msg.answer(
        f"🚀 Спам под постом *#{post_id}* запущен!\n"
        f"Канал: *{info['title']}*\n"
        f"Режим: {mode_label}\n\n"
        f"_Для остановки: /stop или кнопка ⏹ Остановить снос_",
        parse_mode="Markdown",
    )




@dp.message_handler(lambda m: m.chat.type == "private" and user_state.get(m.from_user.id) == "waiting_channel")
async def save_channel_state(msg: types.Message):
    uid = msg.from_user.id
    text = msg.text.strip()
    user_state.pop(uid, None)

    # Извлечь юзернейм из ссылки
    if text.startswith("https://t.me/") or text.startswith("http://t.me/"):
        username = "@" + text.split("t.me/")[-1].strip("/")
    elif text.startswith("t.me/"):
        username = "@" + text.split("t.me/")[-1].strip("/")
    elif not text.startswith("@"):
        username = "@" + text.lstrip("@")
    else:
        username = text

    try:
        chat = await bot.get_chat(username)
    except Exception as e:
        kb, _ = channels_kb(uid)
        await msg.answer(
            f"❌ Не удалось найти канал *{username}*\n\n"
            f"Причина: {e}\n\n"
            f"Убедись что юзернейм правильный и бот добавлен в канал.",
            parse_mode="Markdown",
            reply_markup=kb,
        )
        return

    if chat.type not in ("channel",):
        await msg.answer("❌ Это не канал. Введи юзернейм именно канала, не группы.")
        return

    discussion_id = getattr(chat, "linked_chat_id", None)

    if uid not in user_channels:
        user_channels[uid] = {}
    user_channels[uid][chat.id] = {
        "title": chat.title or username,
        "username": username,
        "discussion_id": discussion_id,
    }

    can_post = None
    if discussion_id:
        can_post, group_title, group_username = await check_discussion_access(discussion_id)
        if can_post:
            mode_info = f"💬 Найдена группа обсуждений *{group_title}* — ✅ доступ есть, спам пойдёт в комментарии."
        else:
            link = f"@{group_username}" if group_username else f"ID: `{discussion_id}`"
            mode_info = (
                f"💬 Найдена группа обсуждений *{group_title}* ({link})\n"
                f"❌ Бот не в группе обсуждений!\n\n"
                f"📋 *Как исправить:*\n"
                f"1. Найди группу обсуждений канала\n"
                f"2. Добавь @grupposnos\\_robot туда как участника\n"
                f"3. В меню канала нажми 🔍 Проверить доступ"
            )
    else:
        mode_info = "📨 Группа обсуждений не найдена — спам пойдёт напрямую в *канал* (бот должен быть там админом)."

    user_channels[uid][chat.id]["can_post"] = can_post
    save_channels(user_channels)

    kb, _ = channels_kb(uid)
    await msg.answer(
        f"✅ Канал *{chat.title}* добавлен!\n\n"
        f"{mode_info}",
        parse_mode="Markdown",
        reply_markup=kb,
    )


@dp.message_handler(lambda m: m.chat.type == "private" and user_state.get(m.from_user.id) == "waiting_link")
async def save_link_state(msg: types.Message):
    uid = msg.from_user.id
    link = msg.text.strip()
    user_state.pop(uid, None)

    if "t.me/" in link or "telegram.me/" in link:
        await msg.answer(
            f"✅ Ссылка получена!\n\n"
            f"Теперь добавь меня в группу вручную или перейди по этой ссылке и добавь бота:\n"
            f"{link}\n\n"
            f"После добавления группа автоматически появится в /groups"
        )
    else:
        await msg.answer(
            "❌ Это не похоже на ссылку Telegram.\n"
            "Попробуй снова — /linkjoin\n\n"
            "Пример ссылки: https://t.me/joinchat/AbCdEfG"
        )


if __name__ == "__main__":
    executor.start_polling(dp, skip_updates=True)
