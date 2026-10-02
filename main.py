from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from dataclasses import dataclass, field
from typing import Dict, List
from pydantic import BaseModel
import secrets
import time
import json
import asyncio
from pathlib import Path

app = FastAPI()


@dataclass
class Room:
    id: str
    name: str
    expires_at: float
    clients: Dict[WebSocket, str] = field(default_factory=dict)  # websocket -> username
    history: List[dict] = field(default_factory=list)


rooms: Dict[str, Room] = {}

ALLOWED_DURATIONS = [5, 10, 15, 20, 30, 45, 60]


class CreateRoomRequest(BaseModel):
    name: str
    minutes: int


@app.post("/room/create-room")
async def create_room(req: CreateRoomRequest):
    if not req.name.strip():
        return JSONResponse({"error": "Room name required"}, status_code=400)
    if req.minutes not in ALLOWED_DURATIONS:
        return JSONResponse({"error": "Invalid duration"}, status_code=400)

    room_id = secrets.token_hex(4)  # short 8 char code
    rooms[room_id] = Room(
        id=room_id,
        name=req.name.strip()[:50],
        expires_at=time.time() + req.minutes * 60,
    )
    asyncio.create_task(expire_room(room_id, req.minutes * 60))  # start countdown

    return {
        "roomId": room_id,
        "name": req.name,
        "expiresAt": rooms[room_id].expires_at * 1000,
    }


async def expire_room(room_id: str, delay: int):
    await asyncio.sleep(delay)
    await close_room(room_id, "Time is up! The room has self-destructed.")


async def close_room(room_id: str, reason: str):
    room = rooms.pop(room_id, None)
    if not room:
        return
    await broadcast(room, {"type": "room-closed", "reason": reason})
    for ws in list(room.clients):
        try:
            await ws.close()
        except Exception:
            pass


async def broadcast(room: Room, obj: dict, exclude: WebSocket = None):
    data = json.dumps(obj)
    for conn in list(room.clients):
        if conn is exclude:
            continue
        try:
            await conn.send_text(data)
        except Exception:
            pass


@app.websocket("/ws/{room_id}")
async def chat_ws(ws: WebSocket, room_id: str, name: str = "Anonymous"):
    await ws.accept()

    room = rooms.get(room_id)
    if not room or time.time() > room.expires_at:
        await ws.send_text(json.dumps({"type": "error", "message": "Room not found or expired!"}))
        await ws.close()
        return

    name = name.strip()[:30] or "Anonymous"
    room.clients[ws] = name

    # welcome: room state + history for the new member
    await ws.send_text(json.dumps({
        "type": "joined",
        "roomId": room.id,
        "name": room.name,
        "expiresAt": room.expires_at * 1000,
        "history": room.history,
        "online": len(room.clients),
    }))
    await broadcast(room, {"type": "system", "text": f"{name} joined the chat", "online": len(room.clients)}, exclude=ws)

    try:
        while True:
            raw = await ws.receive_text()
            msg = json.loads(raw)
            if msg.get("type") == "message":
                entry = {
                    "name": name,
                    "text": str(msg.get("text", ""))[:500],
                    "time": int(time.time() * 1000),
                }
                room.history.append(entry)
                if len(room.history) > 200:
                    room.history.pop(0)
                await broadcast(room, {"type": "message", **entry})
    except WebSocketDisconnect:
        pass
    finally:
        if ws in room.clients:
            room.clients.pop(ws)
            if room_id in rooms:
                await broadcast(room, {"type": "system", "text": f"{name} left the chat.", "online": len(room.clients)})


HTML_FILE = Path(__file__).parent / "index.html"

@app.get("/", response_class=HTMLResponse)
async def home():
    return HTMLResponse(HTML_FILE.read_text(encoding="utf-8"))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)