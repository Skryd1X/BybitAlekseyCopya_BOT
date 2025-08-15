import os, asyncio, time, uuid, logging
from decimal import Decimal, ROUND_HALF_UP
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
MAIN_LOOP: asyncio.AbstractEventLoop | None = None   # <— главный event loop

SUPPORT_URL="https://t.me/bexruz2281488"

def d2(x):
    try: return Decimal(str(x)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except: return Decimal("0.00")

def d4(x):
    try: return Decimal(str(x)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    except: return Decimal("0.0000")

def qf(x):
    try: return f"{d4(x).normalize()}"
    except: return str(x)

def pf(x):
    try: return f"{d2(x).normalize()}"
    except: return str(x)

def usd(x):
    try: return f"${d2(x).normalize()}"
    except: return f"${x}"

def notional(avg,size):
    try: return Decimal(str(abs(size)))*Decimal(str(avg))
    except: return Decimal("0")

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
    # планируем put_nowait в главный луп потокобезопасно
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

async def queue_consumer(app:Application,http:HTTP):
    logging.info("Queue consumer started")
    while True:
        try:
            topic,msg=await msg_queue.get()
            if topic=="position":
                await on_position(app,msg)
            elif topic=="execution":
                # при исполнении ордеров догружаем точный срез по символам
                try:
                    data=msg.get("data",[])
                    if isinstance(data,dict): data=[data]
                    symbols=set()
                    for r in data:
                        s=r.get("symbol")
                        if s: symbols.add(s)
                    for s in symbols:
                        await fetch_symbol_and_process(app,http,s)
                except Exception as e:
                    logging.warning("Execution follow-up failed: %s", e)
        except Exception as e:
            logging.error("Queue consumer error: %s", e)

async def save_event(kind,symbol,side,size,avg,lev,deal_id,percent=None):
    doc={"_id":str(uuid.uuid4()),"t":int(time.time()),"kind":kind,"symbol":symbol,"side":side,
         "size":float(size),"avg":float(avg),"leverage":str(lev),"deal":int(deal_id)}
    if percent is not None: doc["percent"]=float(percent)
    await coll_ev.insert_one(doc)

async def on_position(app:Application,msg:dict):
    global deal_seq
    if "data" not in msg: return
    rows=msg.get("data",[])
    if isinstance(rows,dict): rows=[rows]
    for r in rows:
        symbol=r.get("symbol")
        if not symbol: continue
        side=str(r.get("side","")).upper()
        try: size=Decimal(r.get("size","0"))
        except: size=Decimal("0")
        try: avg=Decimal(r.get("avgPrice",r.get("avg_price","0") or "0"))
        except: avg=Decimal("0")
        lev=str(r.get("leverage",r.get("leverageEr","—")))
        prev=await coll_pos.find_one({"_id":symbol}) or {"size":0.0,"avg":0.0,"side":"","deal":0}
        prev_size=Decimal(str(prev.get("size",0.0)))
        prev_side=str(prev.get("side",""))
        opened=(prev_size==0 and size!=0)
        closed_full=(prev_size!=0 and size==0)
        partial=(prev_size!=0 and size!=0 and abs(size)<abs(prev_size))
        if opened:
            deal_seq+=1
            await coll_pos.update_one({"_id":symbol},{"$set":{
                "size":float(size),"avg":float(avg),"side":side,"deal":int(deal_seq),"lev":lev
            }},upsert=True)
            nt=notional(avg,size)
            txt=(f"Сделка №{deal_seq}\n"
                 f"🟢 Открытие позиции\n\n"
                 f"{side} {symbol}\n"
                 f"Размер: {qf(size)}\n"
                 f"Плечо: x{lev}\n"
                 f"Средняя цена входа: {pf(avg)}\n"
                 f"Нотионал: {usd(nt)}")
            await save_event("open",symbol,side,size,avg,lev,deal_seq)
            await broadcast(app,txt)
            continue
        if partial:
            left=float(abs(size))/float(abs(prev_size)) if prev_size!=0 else 0.0
            closed=(1.0-left)*100.0
            deal_id=int(prev.get("deal",deal_seq+1) or deal_seq+1)
            await coll_pos.update_one({"_id":symbol},{"$set":{
                "size":float(size),"avg":float(avg),"side":side,"deal":deal_id,"lev":lev
            }},upsert=True)
            txt=(f"Сделка №{deal_id}\n"
                 f"🟧 Частичное закрытие\n\n"
                 f"{side} {symbol}\n"
                 f"Закрыто: {pf(closed)}%\n"
                 f"Осталось: {qf(size)}\n"
                 f"Средняя цена входа: {pf(avg)}\n"
                 f"Плечо: x{lev}")
            await save_event("partial",symbol,side,size,avg,lev,deal_id,percent=closed)
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
                    "leverage":x.get("leverage")
                })
        if rows:
            await on_position(app,{"topic":"position","data":rows})
    except Exception as e:
        logging.warning("Fetch positions for %s failed: %s", symbol, e)

async def post_init(app:Application):
    global MAIN_LOOP
    MAIN_LOOP = asyncio.get_running_loop()  # <— сохраняем главный event loop
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
            size=Decimal(str(x.get("size","0")))
            if size==0: continue
            avg=Decimal(str(x.get("avgPrice",x.get("avg_price","0") or "0")))
            side=str(x.get("side","")).upper()
            lev=str(x.get("leverage","—"))
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
    # подписки
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
