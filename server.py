# Сервер-посредник для онлайн-игры (комнаты на двоих + быстрый поиск).
# Он НЕ знает правил игр: только принимает «тапы» игроков, нумерует их и рассылает обоим.
# Оба телефона применяют одни и те же тапы в одном и том же порядке, поэтому игра у них одинаковая.
# Весь ход матча хранится в журнале (log), поэтому после потери интернета игрок
# возвращается в комнату и догоняет пропущенные тапы.
#
# Запуск:   pip install aiohttp   и   python server.py
# Порт берётся из переменной окружения PORT (так делают Render/Railway/Fly), по умолчанию 8080.

import asyncio
import json
import os
import random
import secrets
import time

from aiohttp import WSMsgType, web

GAMES = {"snake", "zov", "blast", "quantum", "hole"}
ROOM_IDLE_SEC = 600        # комната без игроков живёт 10 минут
WAIT_ROOM_SEC = 900        # комната, где никто так и не присоединился
MAX_LOG = 20000

rooms = {}    # code -> room
queues = {}   # ключ (игра+опции) -> (ws, ctx)


def new_room(game, opts):
    for _ in range(100):
        code = "%04d" % random.randint(0, 9999)
        if code not in rooms:
            break
    room = {
        "code": code, "game": game, "opts": opts,
        "slots": [None] * opts["players"],   # у каждого: {"token", "ws", "ctx"}
        "started": False, "log": [], "born": time.time(), "empty_since": None,
    }
    rooms[code] = room
    return room


def clean_opts(d):
    d = d if isinstance(d, dict) else {}
    mode = int(d.get("mode", 1))
    return {"mode": mode if mode in (1, 2, 3) else 1,
            "bomb": bool(d.get("bomb", False)), "full": bool(d.get("full", False)),
            "grow": bool(d.get("grow", False)),
            "players": 3 if int(d.get("players", 2)) == 3 else 2}


async def send(ws, data):
    if ws is not None and not ws.closed:
        try:
            await ws.send_str(json.dumps(data))
        except Exception:
            pass


def others(room, me):
    return [s for i, s in enumerate(room["slots"]) if s is not None and i != me]


def filled(room):
    return sum(1 for s in room["slots"] if s is not None)


async def lobby_update(room):
    for slot in room["slots"]:
        if slot:
            await send(slot["ws"], {"t": "lobby", "have": filled(room), "need": len(room["slots"])})


async def start_room(room):
    room["started"] = True
    for me in range(len(room["slots"])):
        slot = room["slots"][me]
        slot["ctx"]["room"] = room
        slot["ctx"]["me"] = me
        await send(slot["ws"], {"t": "start", "code": room["code"], "token": slot["token"], "me": me,
                                "game": room["game"], "opts": room["opts"]})


def drop_queue(ctx):
    key = ctx.get("queue_key")
    if key and key in queues:
        queues[key] = [e for e in queues[key] if e[1] is not ctx]
        if not queues[key]:
            del queues[key]
    ctx["queue_key"] = None


async def handle(ws, ctx, d):
    t = d.get("t")
    if t == "ping":
        await send(ws, {"t": "pong"})

    elif t == "create":
        game = d.get("game")
        if game not in GAMES or ctx.get("room"):
            await send(ws, {"t": "err", "code": "bad"})
            return
        room = new_room(game, clean_opts(d.get("opts")))
        token = secrets.token_hex(8)
        room["slots"][0] = {"token": token, "ws": ws, "ctx": ctx}
        ctx["room"] = room
        ctx["me"] = 0
        await send(ws, {"t": "created", "code": room["code"], "token": token, "me": 0})
        await lobby_update(room)

    elif t == "join":
        room = rooms.get(str(d.get("code", "")))
        if room is None:
            await send(ws, {"t": "err", "code": "no_room"})
            return
        free = [i for i, sl in enumerate(room["slots"]) if sl is None]
        if not free or room["started"] or ctx.get("room"):
            await send(ws, {"t": "err", "code": "full"})
            return
        me = free[0]
        room["slots"][me] = {"token": secrets.token_hex(8), "ws": ws, "ctx": ctx}
        ctx["room"] = room
        ctx["me"] = me
        if not free[1:]:
            await start_room(room)
        else:
            await lobby_update(room)

    elif t == "queue":
        game = d.get("game")
        if game not in GAMES or ctx.get("room"):
            await send(ws, {"t": "err", "code": "bad"})
            return
        opts = clean_opts(d.get("opts"))
        key = game + json.dumps(opts, sort_keys=True)
        drop_queue(ctx)
        waiting = [e for e in queues.get(key, []) if not e[0].closed]
        waiting.append((ws, ctx))
        ctx["queue_key"] = key
        if len(waiting) >= opts["players"]:
            group = waiting[:opts["players"]]
            rest = waiting[opts["players"]:]
            if rest:
                queues[key] = rest
            else:
                queues.pop(key, None)
            room = new_room(game, opts)
            for i, (qws, qctx) in enumerate(group):
                qctx["queue_key"] = None
                room["slots"][i] = {"token": secrets.token_hex(8), "ws": qws, "ctx": qctx}
            await start_room(room)
        else:
            queues[key] = waiting
            await send(ws, {"t": "queued"})

    elif t == "cancel":
        drop_queue(ctx)
        room = ctx.get("room")
        if room and not room["started"]:
            me = ctx["me"]
            if me == 0:   # хост отменил — комната закрывается
                for sl in others(room, 0):
                    await send(sl["ws"], {"t": "left"})
                    sl["ctx"]["room"] = None
                rooms.pop(room["code"], None)
            else:
                room["slots"][me] = None
                await lobby_update(room)
            ctx["room"] = None

    elif t == "tap":
        room = ctx.get("room")
        if not room or not room["started"] or len(room["log"]) >= MAX_LOG:
            return
        item = {"i": len(room["log"]), "p": ctx["me"], "x": float(d.get("x", 0)), "y": float(d.get("y", 0)),
                "f": 1 if d.get("f") else 0}
        room["log"].append(item)
        msg = dict(item)
        msg["t"] = "tap"
        for slot in room["slots"]:
            if slot:
                await send(slot["ws"], msg)

    elif t == "resume":
        room = rooms.get(str(d.get("code", "")))
        me = None
        if room:
            for i, slot in enumerate(room["slots"]):
                if slot and slot["token"] == d.get("token"):
                    me = i
        if room is None or me is None:
            await send(ws, {"t": "err", "code": "gone"})
            return
        slot = room["slots"][me]
        slot["ws"] = ws
        slot["ctx"] = ctx
        ctx["room"] = room
        ctx["me"] = me
        room["empty_since"] = None
        start = max(0, int(d.get("from", 0)))
        await send(ws, {"t": "log", "items": room["log"][start:]})
        for i, sl in enumerate(room["slots"]):
            if sl is not None and i != me:
                up = sl["ws"] is not None and not sl["ws"].closed
                await send(ws, {"t": "peer", "p": i, "online": up})
                await send(sl["ws"], {"t": "peer", "p": me, "online": True})

    elif t == "leave":
        room = ctx.get("room")
        if room:
            for sl in others(room, ctx["me"]):
                await send(sl["ws"], {"t": "left"})
            rooms.pop(room["code"], None)
            ctx["room"] = None
        drop_queue(ctx)


async def on_disconnect(ws, ctx):
    drop_queue(ctx)
    room = ctx.get("room")
    if not room:
        return
    me = ctx["me"]
    slot = room["slots"][me]
    if slot and slot["ws"] is ws:
        slot["ws"] = None
        if not room["started"]:
            if me == 0:   # хост ушёл, пока ждал игроков
                for sl in others(room, 0):
                    await send(sl["ws"], {"t": "left"})
                rooms.pop(room["code"], None)
            else:
                room["slots"][me] = None
                await lobby_update(room)
            return
        for sl in others(room, me):
            await send(sl["ws"], {"t": "peer", "p": me, "online": False})
        if all(sl is None or sl["ws"] is None for sl in room["slots"]):
            room["empty_since"] = time.time()


async def ws_handler(request):
    ws = web.WebSocketResponse(heartbeat=20)
    await ws.prepare(request)
    ctx = {"room": None, "me": None, "queue_key": None}
    async for msg in ws:
        if msg.type == WSMsgType.TEXT:
            try:
                d = json.loads(msg.data)
                if isinstance(d, dict):
                    await handle(ws, ctx, d)
            except Exception as e:   # плохое сообщение не должно ронять сервер
                print("bad message:", e)
    await on_disconnect(ws, ctx)
    return ws


async def cleaner(app):
    async def loop():
        while True:
            await asyncio.sleep(60)
            now = time.time()
            for code, room in list(rooms.items()):
                gone = room["empty_since"] and now - room["empty_since"] > ROOM_IDLE_SEC
                stale = not room["started"] and now - room["born"] > WAIT_ROOM_SEC
                if gone or stale:
                    rooms.pop(code, None)
    task = asyncio.create_task(loop())
    yield
    task.cancel()


async def health(request):
    return web.Response(text="ok: rooms=%d queues=%d" % (len(rooms), len(queues)))


app = web.Application()
app.router.add_get("/", health)
app.router.add_get("/ws", ws_handler)
app.cleanup_ctx.append(cleaner)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
