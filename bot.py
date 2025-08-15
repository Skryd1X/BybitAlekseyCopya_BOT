import os, asyncio, time, uuid, logging
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from dotenv import load_dotenv
from pybit.unified_trading import WebSocket, HTTP
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes
from motor.motor_asyncio import AsyncIOMotorClient

load_dotenv()
TOKEN=os.getenv("TELEGRAM_TOKEN","")
BYBIT_KEY=os.getenv("BYBIT_API_KEY","")
BYBIT_SECRET=os.getenv("BYBIT_API_SECRET","")
NETWORK=os.getenv("NETWORK","mainnet").lower()
MONGO_URI=os.getenv("MONGO_URI","")
DB_NAME=os.getenv("DB_NAME","bybit_bot")
BYBIT_SETTLE=os.getenv("BYBIT_SETTLE","USDT").upper()
LOG_LEVEL=os.getenv("LOG_LEVEL","INFO").upper()

logging.basicConfig(level=getattr(logging,LOG_LEVEL,logging.INFO),
                    format="%(asctime)s | %(levelname)s | %(message)s")

msg_queue:asyncio.Queue=asyncio.Queue()
deal_seq=80000
db=None
coll_pos=None
coll_ev=None
coll_cfg=None
coll_subs=None
MAIN_LOOP: asyncio.AbstractEventLoop | None = None   # главный event loop

SUPPORT_URL="https://t.me/bexruz2281488"

# кэш последней цены исполнения по символу (для fallback номинала)
LAST_EXEC_PRICE: dict[str, Decimal] = {}

# ------------------ форматтеры чисел ------------------

def _to_decimal(val):
    if val is None or val == "":
        return None
    try:
        return Decimal(str(val))
    except (InvalidOperation, ValueError, TypeError):
        return None

def fmt_qty(val, max_dec: int = 6) -> str:
    """Количество/размер без экспоненты и с обрезкой лишних нулей"""
    d = _to_decimal(val)
    if not d:
        return "0"
    s = f"{d.normalize():f}"  # убираем экспоненту
    if "." in s:
        i, f = s.split(".")
        f = f.rstrip("0")[:max_dec]
        s = i if not f else f"{i}.{f}"
    return s

def fmt_price(val) -> str | None:
    """Цена с динамической точностью. None -> не выводим строку."""
    d = _to_decimal(val)
    if not d or d == 0:
        return None
    # динамическая точность: чем меньше цена, тем больше знаков
    dec = 2 if d >= 1 else 4 if d >= Decimal("0.1") else 6
    q = Decimal(10) ** -dec
    return f"{d.quantize(q, rounding=ROUND_DOWN):f}"

def fmt_usd(val) -> str | None:
    d = _to_decimal(val)
    if not d or d == 0:
        return None
    return f"${d:,.2f}".replace(",", " ")

def fmt_pct(val) -> str:
    d = _to_decimal(val) or Decimal("0")
    return f"{d.quantize(Decimal('0.01'), rounding=ROUND_DOWN):f}"

def fmt_lev(val) -> str | None:
    """Плечо: xN, если N>0. Иначе скрываем."""
    d = _to_decimal(val)
    if d and d > 0:
        try:
            return f"x{int(d)}"
        except Exception:
            return f"x{d.normalize():f}"
    return None

def line(caption: str, value) -> str:
    """Вернуть 'Ключ: Значение\n' или пустую строку, если value пустое."""
    return f"{caption}: {value}\n" if value not in (None, "", "—") else ""

def notional_from_row(row: dict) -> tuple[Decimal | None, bool]:
    """
    Возвращает (номинал, approx_flag).
    approx_flag=True, если это приблизительный расчёт (без positionValue).
    Приоритет:
      1) positionValue
      2) |size| * avgPrice
      3) |size| * markPrice
    """
    pv = _to_decimal(row.get("positionValue") or row.get("position_value"))
    if pv and pv > 0:
        return pv.quantize(Decimal("0.01"), rounding=ROUND_DOWN), False

    size = _to_decimal(row.get("size"))
    avg  = _to_decimal(row.get("avgPrice") or row.get("avg_price"))
    if size and avg and size != 0 and avg > 0:
        val = (abs(size) * avg).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        return val, True

    mark = _to_decimal(row.get("markPrice") or row.get("mark_price"))
    if size and mark and size != 0 and mark > 0:
        val = (abs(size) * mark).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        return val, True

    return None, False

# ------------------ UI ------------------

def kb(enabled: bool):
    t = "🔕 Отключить сигналы" if enabled else "🔔 Включить сигналы"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🆘 Поддержка", url=SUPPORT_URL)],
        [InlineKeyboardButton(t, callback_data=("notify_off" if enabled else "notify_on"))]
    ])

async def broadcast(app:Application, text:str):
    cur = coll_subs.find({"enabled":True})
    async for sub in cur:
        try:
            await app.bot.send_message(chat_id=sub["chat_id"], text=text, reply_markup=kb(True))
        except Exception as e:
            logging.warning("Broadcast failed: %s", e)

# === WS callbacks (вызываются в другом потоке) ===
def _put_from_thread(item):
    if MAIN_LOOP is None:
        logging.error("MAIN_LOOP is not set, drop WS message")
        return
    MAIN_LOOP.call_soon_threadsafe(msg_queue.put_nowait, item)

def ws_pos(msg):
    logging.debug("WS position: %s", msg)
    _put_from_thread(("position", msg))

def ws_order(msg):
    logging.debug("WS order: %s", msg)
    _put_from_thread(("order", msg))

def ws_exec(msg):
    logging.debug("WS execution: %s", msg)
    _put_from_thread(("execution", msg))

# ------------------ telegram ------------------

async def cmd_start(update:Update, context:ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await coll_subs.update_one(
        {"chat_id":chat_id},
        {"$setOnInsert":{"created_at":int(time.time())},"$set":{"enabled":True}},
        upsert=True
    )
    text = ("Привет! Это копия бота Алексея, собранная за пару часов.\n"
            "Я читаю позиции мастера Bybit в реальном времени и отправляю сигналы: открытие, частичное и полное закрытие, плечо, средняя цена и размер.\n"
            "Сигналы включены. Если нужно — можешь отключить их кнопкой ниже.")
    await update.message.reply_text(text, reply_markup=kb(True))

async def on_toggle(update:Update, context:ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    chat_id = q.message.chat_id
    sub = await coll_subs.find_one({"chat_id":chat_id}) or {"enabled":True}
    if q.data=="notify_off":
        await coll_subs.update_one({"chat_id":chat_id},{"$set":{"enabled":False}},upsert=True)
        await q.edit_message_reply_markup(reply_markup=kb(False))
        await q.message.reply_text("Сигналы выключены. Нажми «🔔 Включить сигналы», чтобы снова получать уведомления.", reply_markup=kb(False))
    elif q.data=="notify_on":
        await coll_subs.update_one({"chat_id":chat_id},{"$set":{"enabled":True}},upsert=True)
        await q.edit_message_reply_markup(reply_markup=kb(True))
        await q.message.reply_text("Сигналы включены. Буду присылать уведомления о сделках мастера.", reply_markup=kb(True))

# ------------------ обработка очереди ------------------

async def queue_consumer(app:Application,http:HTTP):
    logging.info("Queue consumer started")
    while True:
        try:
            topic,msg=await msg_queue.get()
            if topic=="position":
                await on_position(app,msg)
            elif topic=="execution":
                # запоминаем последнюю execPrice по символам (для fallback номинала)
                try:
                    data = msg.get("data", [])
                    if isinstance(data, dict):
                        data = [data]
                    symbols = set()
                    for r in data:
                        s = r.get("symbol")
                        p = _to_decimal(r.get("execPrice") or r.get("price"))
                        if s and p:
                            LAST_EXEC_PRICE[s] = p
                        if s:
                            symbols.add(s)
                    # при исполнении ордеров догружаем точный срез по символам (если HTTP доступен)
                    for s in symbols:
                        await fetch_symbol_and_process(app,http,s)
                except Exception as e:
                    logging.warning("Execution follow-up failed: %s", e)
        except Exception as e:
            logging.error("Queue consumer error: %s", e)

# ------------------ хранилище событий ------------------

async def save_event(kind,symbol,side,size,avg,lev,deal_id,percent=None):
    doc={"_id":str(uuid.uuid4()),"t":int(time.time()),"kind":kind,"symbol":symbol,"side":side,
         "size":float(_to_decimal(size) or 0),"avg":float(_to_decimal(avg) or 0),"leverage":str(lev),"deal":int(deal_id)}
    if percent is not None: doc["percent"]=float(_to_decimal(percent) or 0)
    await coll_ev.insert_one(doc)

# ------------------ основной обработчик позиций ------------------

async def on_position(app:Application,msg:dict):
    global deal_seq
    if "data" not in msg: return
    rows=msg.get("data",[])
    if isinstance(rows,dict): rows=[rows]
    for r in rows:
        symbol=r.get("symbol")
        if not symbol: continue
        side=str(r.get("side","")).upper()

        size = _to_decimal(r.get("size","0")) or Decimal("0")
        avg  = _to_decimal(r.get("avgPrice", r.get("avg_price","0") or "0")) or Decimal("0")
        lev  = str(r.get("leverage", r.get("leverageEr","")))

        prev=await coll_pos.find_one({"_id":symbol}) or {"size":0.0,"avg":0.0,"side":"","deal":0}
        prev_size=_to_decimal(prev.get("size",0.0)) or Decimal("0")
        prev_side=str(prev.get("side",""))

        opened      = (prev_size==0 and size!=0)
        closed_full = (prev_size!=0 and size==0)
        partial     = (prev_size!=0 and size!=0 and abs(size)<abs(prev_size))

        if opened:
            deal_seq+=1
            await coll_pos.update_one({"_id":symbol},{"$set":{
                "size":float(size),"avg":float(avg),"side":side,"deal":int(deal_seq),"lev":lev
            }},upsert=True)

            # Номинал: positionValue -> size*avg -> size*mark (≈)
            nt_val, approx = notional_from_row(r)

            # Доп. fallback: если HTTP/mark/avg недоступны, считаем по последней цене исполнения из WS
            if nt_val is None:
                lp = LAST_EXEC_PRICE.get(symbol)
                if lp:
                    nt_val = (abs(size) * lp).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
                    approx = True

            nt_str = fmt_usd(nt_val)
            nt_str = f"≈ {nt_str}" if nt_str and approx else nt_str

            txt = (
                f"Сделка №{deal_seq}\n"
                f"🟢 Открытие позиции\n\n"
                f"{side} {symbol}\n"
                f"Размер: {fmt_qty(size)}\n"
                f"{line('Плечо', fmt_lev(lev))}"
                f"{line('Средняя цена входа', fmt_price(avg))}"
                f"{line('Номинал', nt_str)}"
            )
            await save_event("open",symbol,side,size,avg,lev,deal_seq)
            await broadcast(app,txt)
            continue

        if partial:
            left = (abs(size) / abs(prev_size)) if prev_size != 0 else Decimal("0")
            closed_pct = (Decimal("1") - left) * Decimal("100")

            deal_id=int(prev.get("deal",deal_seq+1) or deal_seq+1)
            await coll_pos.update_one({"_id":symbol},{"$set":{
                "size":float(size),"avg":float(avg),"side":side,"deal":deal_id,"lev":lev
            }},upsert=True)

            txt = (
                f"Сделка №{deal_id}\n"
                f"🟧 Частичное закрытие\n\n"
                f"{side} {symbol}\n"
                f"Закрыто: {fmt_pct(closed_pct)}%\n"
                f"Осталось: {fmt_qty(size)}\n"
                f"{line('Средняя цена входа', fmt_price(avg))}"
                f"{line('Плечо', fmt_lev(lev))}"
            )
            await save_event("partial",symbol,side,size,avg,lev,deal_id,percent=closed_pct)
            await broadcast(app,txt)
            continue

        if closed_full:
            deal_id=int(prev.get("deal",deal_seq+1) or deal_seq+1)
            await coll_pos.update_one({"_id":symbol},{"$set":{
                "size":0.0,"avg":0.0,"side":"","deal":deal_id,"lev":lev
            }},upsert=True)
            txt=(f"Сделка №{deal_id}\n"
                 f"⬛ Полное закрытие позиции\n\n"
                 f"{prev_side} {symbol}\n"
                 f"Позиция закрыта полностью")
            await save_event("close",symbol,prev_side,Decimal("0"),avg,lev,deal_id)
            await broadcast(app,txt)
            continue

        # просто обновление полей
        await coll_pos.update_one({"_id":symbol},{"$set":{
            "size":float(size),"avg":float(avg),"side":side,"lev":lev
        }},upsert=True)

# ------------------ догрузка среза по символу ------------------

async def fetch_symbol_and_process(app:Application,http:HTTP,symbol:str):
    try:
        r=http.get_positions(category="linear", symbol=symbol, settleCoin=BYBIT_SETTLE)
        lst=r.get("result",{}).get("list",[]) or []
        rows=[]
        for x in lst:
            if x.get("symbol")==symbol:
                rows.append({
                    "symbol":x.get("symbol"),
                    "side":x.get("side"),
                    "size":x.get("size"),
                    "avgPrice":x.get("avgPrice"),
                    "leverage":x.get("leverage"),
                    "markPrice":x.get("markPrice"),
                    "positionValue":x.get("positionValue")
                })
        if rows:
            await on_position(app,{"topic":"position","data":rows})
    except Exception as e:
        logging.warning("Fetch positions for %s failed: %s", symbol, e)

# ------------------ жизненный цикл приложения ------------------

async def post_init(app:Application):
    global MAIN_LOOP
    MAIN_LOOP = asyncio.get_running_loop()
    logging.info("Starting post_init...")

    client=AsyncIOMotorClient(MONGO_URI,uuidRepresentation="standard")
    global db, coll_pos, coll_ev, coll_cfg, coll_subs
    db=client[DB_NAME]
    coll_pos=db["positions"]
    coll_ev=db["events"]
    coll_cfg=db["config"]
    coll_subs=db["subscribers"]
    await db.command("ping")
    logging.info("Mongo connected: db=%s", DB_NAME)

    bot=await app.bot.get_me()
    logging.info("Telegram bot authorized: @%s id=%s", bot.username, bot.id)

    http=HTTP(testnet=(NETWORK!="mainnet"),api_key=BYBIT_KEY,api_secret=BYBIT_SECRET)
    logging.info("Bybit HTTP ready network=%s", NETWORK)

    # индексы
    await coll_pos.create_index("side")
    await coll_ev.create_index([("t",1)])
    await coll_subs.create_index("chat_id", unique=True)
    logging.info("Mongo indexes ready")

    # стартовый снимок активных позиций по USDT-контрактам
    try:
        r=http.get_positions(category="linear", settleCoin=BYBIT_SETTLE)
        lst=r.get("result",{}).get("list",[]) or []
        cnt=0
        for x in lst:
            symbol=x.get("symbol")
            if not symbol: continue
            size=_to_decimal(x.get("size","0")) or Decimal("0")
            if size==0: continue
            avg=_to_decimal(x.get("avgPrice", x.get("avg_price","0") or "0")) or Decimal("0")
            side=str(x.get("side","")).upper()
            lev=str(x.get("leverage",""))
            global deal_seq
            deal_seq+=1
            await coll_pos.update_one({"_id":symbol},{"$set":{
                "size":float(size),"avg":float(avg),"side":side,"deal":int(deal_seq),"lev":lev
            }},upsert=True)
            await save_event("detected",symbol,side,size,avg,lev,deal_seq)
            cnt+=1
        logging.info("HTTP bootstrap positions settle=%s count=%s", BYBIT_SETTLE, cnt)
    except Exception as e:
        logging.warning("HTTP bootstrap failed: %s", e)

    # консюмер очереди
    consumer=asyncio.create_task(queue_consumer(app,http))
    app.bot_data["consumer_task"]=consumer

    # приватный WS на unified v5
    ws=WebSocket(channel_type="private",testnet=(NETWORK!="mainnet"),
                 api_key=BYBIT_KEY,api_secret=BYBIT_SECRET,domain="bybit")
    ws.position_stream(callback=ws_pos)
    ws.order_stream(callback=ws_order)
    ws.execution_stream(callback=ws_exec)
    app.bot_data["ws"]=ws
    logging.info("Bybit WS subscribed: position, order, execution")

async def post_stop(app:Application):
    ws = app.bot_data.get("ws")
    if ws:
        try: ws.exit()
        except Exception: pass
    t = app.bot_data.get("consumer_task")
    if t:
        t.cancel()
    logging.info("Application stopped")

def main():
    logging.info("Launching application...")
    app = (
        Application.builder()
        .token(TOKEN)
        .post_init(post_init)
        .post_stop(post_stop)
        .build()
    )
    app.add_handler(CommandHandler("start",cmd_start))
    app.add_handler(CallbackQueryHandler(on_toggle, pattern="^(notify_on|notify_off)$"))
    app.run_polling(allowed_updates=None, close_loop=False)

if __name__=="__main__":
    main()
