import os, asyncio, time, uuid, logging
from decimal import Decimal, ROUND_DOWN, InvalidOperation
from datetime import datetime, timedelta, timezone as dt_tz
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
STATS_TZ_HOURS=int(os.getenv("STATS_TZ_HOURS","3"))  # МСК по умолчанию

logging.basicConfig(level=getattr(logging,LOG_LEVEL,logging.INFO),
                    format="%(asctime)s | %(levelname)s | %(message)s")

msg_queue:asyncio.Queue=asyncio.Queue()
deal_seq=80000

db=None
coll_pos=None
coll_ev=None
coll_cfg=None
coll_subs=None
coll_deals=None

MAIN_LOOP: asyncio.AbstractEventLoop | None = None

SUPPORT_URL="https://t.me/bexruz2281488"

# Кэш цены из последнего исполнения — как запасной вариант для exit_price/номинала
LAST_EXEC_PRICE: dict[str, Decimal] = {}
# Буфер исполнений до появления deal_id
PENDING_EXEC: dict[str, dict] = {}

# ───────────────────────── helpers/format ─────────────────────────

def _to_decimal(val):
    if val is None or val == "":
        return None
    try:
        return Decimal(str(val))
    except (InvalidOperation, ValueError, TypeError):
        return None

def fmt_qty(val, max_dec: int = 6) -> str:
    d = _to_decimal(val)
    if not d:
        return "0"
    s = f"{d.normalize():f}"
    if "." in s:
        i, f = s.split(".")
        f = f.rstrip("0")[:max_dec]
        s = i if not f else f"{i}.{f}"
    return s

def fmt_price(val) -> str | None:
    d = _to_decimal(val)
    if not d or d == 0:
        return None
    dec = 2 if d >= 1 else 4 if d >= Decimal("0.1") else 6
    q = Decimal(10) ** -dec
    return f"{d.quantize(q, rounding=ROUND_DOWN):f}"

def fmt_usd(val) -> str | None:
    d = _to_decimal(val)
    if not d or d == 0:
        return None
    return f"${d:,.2f}".replace(",", " ")

def fmt_usd_signed(val) -> str | None:
    d = _to_decimal(val)
    if d is None:
        return None
    s = "-" if d < 0 else ""
    return f"{s}${abs(d):,.2f}".replace(",", " ")

def fmt_pct(val) -> str:
    d = _to_decimal(val) or Decimal("0")
    return f"{d.quantize(Decimal('0.01'), rounding=ROUND_DOWN):f}"

def fmt_lev(val) -> str | None:
    d = _to_decimal(val)
    if d and d > 0:
        try:
            return f"x{int(d)}"
        except Exception:
            return f"x{d.normalize():f}"
    return None

def line(caption: str, value) -> str:
    return f"{caption}: {value}\n" if value not in (None, "", "—") else ""

def notional_from_row(row: dict) -> tuple[Decimal | None, bool]:
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

async def _wait_exec_notional(symbol: str, size: Decimal, tries: int = 5, delay: float = 0.2):
    for _ in range(tries):
        p = LAST_EXEC_PRICE.get(symbol)
        if p:
            return (abs(size) * p).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        await asyncio.sleep(delay)
    return None

def _avg_price(total_val: Decimal | None, total_qty: Decimal | None) -> Decimal | None:
    tv, tq = _to_decimal(total_val), _to_decimal(total_qty)
    if tv and tq and tq > 0:
        return (tv / tq).quantize(Decimal("0.00000001"), rounding=ROUND_DOWN)
    return None

def _deal_dir_from_side(side_str: str) -> str:
    return "Long" if (side_str or "").upper() == "BUY" else "Short"

# ───────────────────────── UI ─────────────────────────

def kb(enabled: bool):
    t = "🔕 Отключить сигналы" if enabled else "🔔 Включить сигналы"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🆘 Поддержка", url=SUPPORT_URL),
         InlineKeyboardButton("📊 Статистика", callback_data="stats")],
        [InlineKeyboardButton(t, callback_data=("notify_off" if enabled else "notify_on"))]
    ])

async def broadcast(app:Application, text:str):
    cur = coll_subs.find({"enabled":True})
    async for sub in cur:
        try:
            await app.bot.send_message(chat_id=sub["chat_id"], text=text, reply_markup=kb(True))
        except Exception as e:
            logging.warning("Broadcast failed: %s", e)

# WS → очередь
def _put_from_thread(item):
    if MAIN_LOOP is None:
        logging.error("MAIN_LOOP is not set, drop WS message")
        return
    MAIN_LOOP.call_soon_threadsafe(msg_queue.put_nowait, item)
def ws_pos(msg):  _put_from_thread(("position", msg))
def ws_order(msg): _put_from_thread(("order", msg))
def ws_exec(msg): _put_from_thread(("execution", msg))

# ───────────────────────── telegram ─────────────────────────

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

# ───────────── суточная статистика (как в примере) ─────────────

def _fmt_price_usdt(p: Decimal | None) -> str:
    s = fmt_price(p)
    return f"{s} USDT" if s else "—"

def _calc_pnl_by_prices(dir_: str, entry_price: Decimal | None, exit_price: Decimal | None,
                        qty: Decimal | None, fees: Decimal | None) -> Decimal | None:
    e, x, q, f = _to_decimal(entry_price), _to_decimal(exit_price), _to_decimal(qty), (_to_decimal(fees) or Decimal("0"))
    if e and x and q and q > 0:
        pnl = (x - e) * q if dir_ == "Long" else (e - x) * q
        return (pnl - f).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
    return None

async def _build_daily_stats_text(tz_hours: int = 3) -> str:
    tz = dt_tz(timedelta(hours=tz_hours))
    now_local = datetime.now(tz)
    start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    end_local = start_local + timedelta(days=1)
    start_ts_utc = int(start_local.astimezone(dt_tz.utc).timestamp())
    end_ts_utc   = int(end_local.astimezone(dt_tz.utc).timestamp())

    cur = coll_deals.find({
        "status": "closed",
        "end_ts": {"$gte": start_ts_utc, "$lt": end_ts_utc}
    }).sort("end_ts", 1)

    deals = await cur.to_list(length=200)
    if not deals:
        return "🟢 Статистика закрытых сделок за сутки:\n\nНет закрытых сделок."

    lines = ["🟢 Статистика закрытых сделок за сутки:\n"]
    total_pnl = Decimal("0")
    idx = 0
    for d in deals:
        idx += 1
        symbol = d.get("symbol","UNKNOWN")
        side_open = d.get("side","")
        dir_ = _deal_dir_from_side(side_open)

        buy_q  = _to_decimal(d.get("buy_qty",0))  or Decimal("0")
        sell_q = _to_decimal(d.get("sell_qty",0)) or Decimal("0")
        buy_v  = _to_decimal(d.get("buy_val",0))  or Decimal("0")
        sell_v = _to_decimal(d.get("sell_val",0)) or Decimal("0")
        fees   = _to_decimal(d.get("fees",0))     or Decimal("0")
        entry_price = _to_decimal(d.get("entry_price"))
        entry_qty   = _to_decimal(d.get("entry_qty"))

        # средние цены по фактическим ногам
        avg_buy  = _avg_price(buy_v, buy_q)
        avg_sell = _avg_price(sell_v, sell_q)

        if dir_ == "Long":
            final_entry = entry_price or avg_buy
            final_exit  = avg_sell or LAST_EXEC_PRICE.get(symbol)
            closed_qty  = sell_q or entry_qty
        else:  # Short
            final_entry = entry_price or avg_sell
            final_exit  = avg_buy or LAST_EXEC_PRICE.get(symbol)
            closed_qty  = buy_q or entry_qty

        stored_pnl = _to_decimal(d.get("pnl"))
        calc_pnl   = _calc_pnl_by_prices(dir_, final_entry, final_exit, closed_qty, fees)

        # если в базе нет PnL или он около нуля — используем рассчитанный
        if stored_pnl is None or abs(stored_pnl) < Decimal("0.005"):
            pnl = calc_pnl or Decimal("0")
        else:
            pnl = stored_pnl

        # если смогли посчитать и заметно отличается — подправим запись
        if calc_pnl is not None and (stored_pnl is None or abs(calc_pnl - stored_pnl) >= Decimal("0.01")):
            await coll_deals.update_one({"deal": d.get("deal")}, {"$set": {"pnl": float(calc_pnl)}})

        total_pnl += pnl

        type_str = "Buy (лонг)" if dir_=="Long" else "Sell (шорт)"
        lines += [
            f"{idx} {symbol}",
            f"• Тип: {type_str}",
            f"• Кол-во: {fmt_qty(closed_qty)}",
            f"• Цена входа: {_fmt_price_usdt(final_entry)}",
            f"• Цена выхода: {_fmt_price_usdt(final_exit)}",
            f"• {'Прибыль' if pnl>=0 else 'Убыток'} (PnL): {fmt_usd_signed(pnl)}",
            ""
        ]

    lines += [
        "— Итого по закрытым сделкам:",
        f"• Сделок: {idx}",
        f"• Общий PnL: {fmt_usd_signed(total_pnl)}"
    ]
    return "\n".join(lines)

async def on_stats(update:Update, context:ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    text = await _build_daily_stats_text(STATS_TZ_HOURS)
    await q.message.reply_text(text, reply_markup=kb(True))

# ───────────────────────── internal helpers ─────────────────────────

async def _apply_pending_to_deal(symbol:str, deal_id:int):
    buf = PENDING_EXEC.pop(symbol, None)
    if not buf:
        return
    incs = {}
    for k in ("buy_qty","sell_qty","buy_val","sell_val","fees"):
        v = _to_decimal(buf.get(k))
        if v and v != 0:
            incs[k] = float(v)
    if incs:
        await coll_deals.update_one({"deal":deal_id},{"$inc":incs})

# ───────────────────────── queue consumer ─────────────────────────

async def queue_consumer(app:Application,http:HTTP):
    logging.info("Queue consumer started")
    while True:
        try:
            topic,msg=await msg_queue.get()
            if topic=="position":
                await on_position(app,msg)
            elif topic=="execution":
                try:
                    data = msg.get("data", [])
                    if isinstance(data, dict): data = [data]
                    symbols=set()
                    for r in data:
                        sym = r.get("symbol")
                        if sym: symbols.add(sym)

                        price = (_to_decimal(r.get("execPrice")) or
                                 _to_decimal(r.get("orderPrice")) or
                                 _to_decimal(r.get("price")))
                        value = _to_decimal(r.get("execValue"))
                        fee   = _to_decimal(r.get("execFee")) or Decimal("0")
                        qty   = _to_decimal(r.get("execQty"))
                        side  = str(r.get("side","")).title()  # 'Buy'/'Sell'

                        if sym and price:
                            LAST_EXEC_PRICE[sym] = price
                        if value is None and qty and price:
                            value = (qty * price).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
                        if not (sym and value and value>0):
                            continue

                        pos = await coll_pos.find_one({"_id":sym})
                        deal_id = int((pos or {}).get("deal",0))

                        if deal_id:
                            await coll_deals.update_one({"deal":deal_id},{"$setOnInsert":{
                                "deal":deal_id,"symbol":sym,"side":(pos or {}).get("side",""),
                                "start_ts":int(time.time()),
                                "buy_qty":0.0,"buy_val":0.0,"sell_qty":0.0,"sell_val":0.0,
                                "fees":0.0,"status":"open"
                            }},upsert=True)
                            incs={}
                            if side=="Buy":
                                incs={"buy_val": float(value)}
                                if qty: incs["buy_qty"]=float(qty)
                            elif side=="Sell":
                                incs={"sell_val": float(value)}
                                if qty: incs["sell_qty"]=float(qty)
                            if fee and fee>0:
                                incs["fees"]=float(fee)
                            if incs:
                                await coll_deals.update_one({"deal":deal_id},{"$inc":incs})
                        else:
                            buf = PENDING_EXEC.setdefault(sym, {"buy_qty":Decimal("0"),"sell_qty":Decimal("0"),
                                                                "buy_val":Decimal("0"),"sell_val":Decimal("0"),
                                                                "fees":Decimal("0")})
                            if side=="Buy":
                                buf["buy_val"] += value
                                if qty: buf["buy_qty"] += qty
                            elif side=="Sell":
                                buf["sell_val"] += value
                                if qty: buf["sell_qty"] += qty
                            if fee and fee>0:
                                buf["fees"] += fee

                    for s in symbols:
                        await fetch_symbol_and_process(app,http,s)
                except Exception as e:
                    logging.warning("Execution follow-up failed: %s", e)
        except Exception as e:
            logging.error("Queue consumer error: %s", e)

# ───────────────────────── events ─────────────────────────

async def save_event(kind,symbol,side,size,avg,lev,deal_id,percent=None):
    doc={"_id":str(uuid.uuid4()),"t":int(time.time()),"kind":kind,"symbol":symbol,"side":side,
         "size":float(_to_decimal(size) or 0),"avg":float(_to_decimal(avg) or 0),
         "leverage":str(lev),"deal":int(deal_id)}
    if percent is not None: doc["percent"]=float(_to_decimal(percent) or 0)
    await coll_ev.insert_one(doc)

# ───────────────────────── positions handler ─────────────────────────

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
        increased   = (prev_size!=0 and size!=0 and abs(size)>abs(prev_size))

        if opened:
            deal_seq+=1
            await coll_pos.update_one({"_id":symbol},{"$set":{
                "size":float(size),"avg":float(avg),"side":side,"deal":int(deal_seq),"lev":lev
            }},upsert=True)

            await coll_deals.update_one({"deal":int(deal_seq)},{"$setOnInsert":{
                "deal": int(deal_seq),
                "symbol": symbol,
                "side": side,
                "start_ts": int(time.time()),
                "buy_qty": 0.0, "buy_val": 0.0,
                "sell_qty": 0.0, "sell_val": 0.0,
                "fees": 0.0,
                "entry_price": float(avg) if avg else None,
                "entry_qty": float(abs(size)) if size else None,
                "status": "open"
            }},upsert=True)
            await _apply_pending_to_deal(symbol, int(deal_seq))

            nt_val, approx = notional_from_row(r)
            if nt_val is None:
                w = await _wait_exec_notional(symbol, size)
                if w is not None:
                    nt_val, approx = w, True
            nt_str = fmt_usd(nt_val)
            if nt_str and approx:
                nt_str = f"≈ {nt_str}"

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

        if increased:
            # усреднили/добрали — обновим среднюю входа и текущий размер
            deal_id=int(prev.get("deal",deal_seq+1) or deal_seq+1)
            await coll_deals.update_one({"deal":deal_id},{
                "$set":{"entry_price":float(avg) if avg else None,
                        "entry_qty":float(abs(size)) if size else None}
            })

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
            await _apply_pending_to_deal(symbol, deal_id)

            d = await coll_deals.find_one({"deal":deal_id}) or {}
            dir_ = _deal_dir_from_side(prev_side)
            buy_q  = _to_decimal(d.get("buy_qty",0))  or Decimal("0")
            sell_q = _to_decimal(d.get("sell_qty",0)) or Decimal("0")
            buy_v  = _to_decimal(d.get("buy_val",0))  or Decimal("0")
            sell_v = _to_decimal(d.get("sell_val",0)) or Decimal("0")
            fees   = _to_decimal(d.get("fees",0))     or Decimal("0")
            entry_price = _to_decimal(d.get("entry_price")) or _to_decimal(prev.get("avg"))

            avg_buy  = _avg_price(buy_v, buy_q)
            avg_sell = _avg_price(sell_v, sell_q)

            if dir_=="Long":
                exit_price = avg_sell or LAST_EXEC_PRICE.get(symbol)
                closed_qty = sell_q or abs(_to_decimal(prev.get("size")) or 0)
            else:
                exit_price = avg_buy or LAST_EXEC_PRICE.get(symbol)
                closed_qty = buy_q or abs(_to_decimal(prev.get("size")) or 0)

            pnl_calc = _calc_pnl_by_prices(dir_, entry_price, exit_price, closed_qty, fees)

            upd = {"status":"closed","end_ts":int(time.time())}
            if pnl_calc is not None:
                upd["pnl"] = float(pnl_calc)
            if exit_price:
                upd["exit_price"] = float(exit_price)
            if entry_price and not d.get("entry_price"):
                upd["entry_price"] = float(entry_price)
            if closed_qty and not d.get("entry_qty"):
                upd["entry_qty"] = float(closed_qty)

            await coll_deals.update_one({"deal":deal_id},{"$set":upd})

            await coll_pos.update_one({"_id":symbol},{"$set":{
                "size":0.0,"avg":0.0,"side":"","deal":deal_id,"lev":lev
            }},upsert=True)

            txt=(f"Сделка №{deal_id}\n"
                 f"⬛ Полное закрытие позиции\n\n"
                 f"{prev_side} {symbol}\n"
                 f"Позиция закрыта полностью\n"
                 f"{line('PNL', fmt_usd_signed(pnl_calc) if pnl_calc is not None else '—')}")
            await save_event("close",symbol,prev_side,Decimal("0"),avg,lev,deal_id)
            await broadcast(app,txt)
            continue

        # обычное обновление позиции
        await coll_pos.update_one({"_id":symbol},{"$set":{
            "size":float(size),"avg":float(avg),"side":side,"lev":lev
        }},upsert=True)

# ───────────────────────── fetch symbol snapshot ─────────────────────────

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

# ───────────────────────── lifecycle ─────────────────────────

async def _init_deal_seq():
    global deal_seq
    try:
        last1 = await coll_deals.find().sort("deal",-1).limit(1).to_list(length=1)
        last2 = await coll_pos.find().sort("deal",-1).limit(1).to_list(length=1)
        max_deal = deal_seq
        if last1: max_deal = max(max_deal, int(last1[0].get("deal",deal_seq)))
        if last2: max_deal = max(max_deal, int(last2[0].get("deal",deal_seq)))
        deal_seq = max_deal
        logging.info("Deal sequence initialized: %s", deal_seq)
    except Exception as e:
        logging.warning("Init deal_seq failed: %s", e)

async def post_init(app:Application):
    global MAIN_LOOP
    MAIN_LOOP = asyncio.get_running_loop()
    logging.info("Starting post_init...")

    client=AsyncIOMotorClient(MONGO_URI,uuidRepresentation="standard")
    global db, coll_pos, coll_ev, coll_cfg, coll_subs, coll_deals
    db=client[DB_NAME]
    coll_pos=db["positions"]
    coll_ev=db["events"]
    coll_cfg=db["config"]
    coll_subs=db["subscribers"]
    coll_deals=db["deals"]

    await db.command("ping")
    logging.info("Mongo connected: db=%s", DB_NAME)

    bot=await app.bot.get_me()
    logging.info("Telegram bot authorized: @%s id=%s", bot.username, bot.id)

    http=HTTP(testnet=(NETWORK!="mainnet"),api_key=BYBIT_KEY,api_secret=BYBIT_SECRET)
    logging.info("Bybit HTTP ready network=%s", NETWORK)

    await coll_pos.create_index("side")
    await coll_ev.create_index([("t",1)])
    await coll_subs.create_index("chat_id", unique=True)
    await coll_deals.create_index([("status",1),("end_ts",-1)])
    await coll_deals.create_index("deal", unique=True)
    logging.info("Mongo indexes ready")

    await _init_deal_seq()

    # стартовый снимок активных позиций
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
            await coll_deals.update_one({"deal":int(deal_seq)},{"$setOnInsert":{
                "deal":int(deal_seq),"symbol":symbol,"side":side,"start_ts":int(time.time()),
                "buy_qty":0.0,"buy_val":0.0,"sell_qty":0.0,"sell_val":0.0,"fees":0.0,
                "entry_price": float(avg) if avg else None,
                "entry_qty": float(abs(size)) if size else None,
                "status":"open"
            }},upsert=True)
            await save_event("detected",symbol,side,size,avg,lev,deal_seq)
            cnt+=1
        logging.info("HTTP bootstrap positions settle=%s count=%s", BYBIT_SETTLE, cnt)
    except Exception as e:
        logging.warning("HTTP bootstrap failed: %s", e)

    consumer=asyncio.create_task(queue_consumer(app,http))
    app.bot_data["consumer_task"]=consumer

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
    app.add_handler(CallbackQueryHandler(on_stats, pattern="^stats$"))
    app.run_polling(allowed_updates=None, close_loop=False)

if __name__=="__main__":
    main()
