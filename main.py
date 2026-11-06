import asyncio
import importlib
import subprocess
import sys

HOST = "0.0.0.0"
PORT = 8000
REQUIRED_PACKAGES = ["fastapi", "uvicorn[standard]"]

try:
    import fastapi  # noqa: F401
    import uvicorn
except ImportError:
    print("Installing missing dependencies (fastapi, uvicorn)...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", *REQUIRED_PACKAGES])
    importlib.invalidate_caches()
    import uvicorn

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

PIECES_PER_PLAYER = 3
RESTART_DELAY = 2.5
MAX_NAME_LEN = 20
MAX_ROOM_LEN = 30
WIN_LINES = [
    (0, 1, 2), (3, 4, 5), (6, 7, 8),
    (0, 3, 6), (1, 4, 7), (2, 5, 8),
    (0, 4, 8), (2, 4, 6),
]

app = FastAPI()


def other(symbol):
    return "O" if symbol == "X" else "X"


def valid_cell(cell):
    return type(cell) is int and 0 <= cell < 9


class Player:
    def __init__(self, ws, name, symbol):
        self.ws = ws
        self.name = name
        self.symbol = symbol


class Room:
    def __init__(self, room_id):
        self.id = room_id
        self.players = []
        self.scores = {"X": 0, "O": 0}
        self.starter = "X"
        self.round = 0
        self.restart_task = None
        self.new_round()

    def new_round(self):
        self.board = [None] * 9
        self.placed = {"X": 0, "O": 0}
        self.turn = self.starter
        self.winner = None
        self.win_line = None
        self.round += 1

    def reset_match(self):
        self.scores = {"X": 0, "O": 0}
        self.starter = "X"
        self.new_round()

    @property
    def ready(self):
        return len(self.players) == 2

    def state(self):
        return {
            "type": "state",
            "board": self.board,
            "turn": self.turn,
            "placed": self.placed,
            "scores": self.scores,
            "players": {p.symbol: p.name for p in self.players},
            "winner": self.winner,
            "winLine": self.win_line,
            "ready": self.ready,
        }

    async def broadcast(self, msg):
        for p in list(self.players):
            try:
                await p.ws.send_json(msg)
            except Exception:
                pass

    def apply(self, symbol, msg):
        """Validate and apply a move; returns True if the board changed."""
        if not self.ready or self.winner or symbol != self.turn:
            return False

        if self.placed[symbol] < PIECES_PER_PLAYER:
            cell = msg.get("cell")
            if msg.get("type") != "place" or not valid_cell(cell) or self.board[cell] is not None:
                return False
            self.board[cell] = symbol
            self.placed[symbol] += 1
        else:
            src, dst = msg.get("from"), msg.get("to")
            if (msg.get("type") != "move" or not valid_cell(src) or not valid_cell(dst)
                    or self.board[src] != symbol or self.board[dst] is not None):
                return False
            self.board[src] = None
            self.board[dst] = symbol

        line = next((l for l in WIN_LINES if all(self.board[i] == symbol for i in l)), None)
        if line:
            self.winner = symbol
            self.win_line = line
            self.scores[symbol] += 1
        else:
            self.turn = other(symbol)
        return True

    async def restart_later(self, round_no):
        await asyncio.sleep(RESTART_DELAY)
        # Skip if a player left (round was reset) during the delay.
        if self.round == round_no and self.winner and self.ready:
            self.starter = other(self.starter)
            self.new_round()
            await self.broadcast(self.state())


rooms: dict[str, Room] = {}


@app.get("/")
async def index():
    return HTMLResponse(INDEX_HTML)


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    room = None
    player = None
    try:
        join = await ws.receive_json()
        if not isinstance(join, dict) or join.get("type") != "join":
            return
        name = str(join.get("name", "")).strip()[:MAX_NAME_LEN]
        room_id = str(join.get("room", "")).strip()[:MAX_ROOM_LEN]
        if not name or not room_id:
            await ws.send_json({"type": "error", "message": "Name and room ID are required."})
            return

        room = rooms.get(room_id)
        if room is None:
            room = rooms[room_id] = Room(room_id)
        if room.ready:
            room = None
            await ws.send_json({"type": "error", "message": "Room is full."})
            return

        symbol = other(room.players[0].symbol) if room.players else "X"
        player = Player(ws, name, symbol)
        room.players.append(player)
        await ws.send_json({"type": "joined", "symbol": symbol, "room": room_id})
        if room.ready:
            room.reset_match()
        await room.broadcast(room.state())

        while True:
            msg = await ws.receive_json()
            if isinstance(msg, dict) and room.apply(player.symbol, msg):
                await room.broadcast(room.state())
                if room.winner:
                    room.restart_task = asyncio.create_task(room.restart_later(room.round))
    except (WebSocketDisconnect, ValueError):
        pass
    finally:
        if room and player in room.players:
            room.players.remove(player)
            if room.players:
                room.reset_match()
                await room.broadcast(room.state())
            else:
                rooms.pop(room.id, None)
        try:
            await ws.close()
        except (RuntimeError, WebSocketDisconnect):
            pass


INDEX_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Three Men's Morris</title>
<style>
  * { box-sizing: border-box; }
  body {
    font-family: "Segoe UI", system-ui, sans-serif; background: #1f2430; color: #eee;
    display: flex; justify-content: center; align-items: center; min-height: 100vh; margin: 0;
  }
  .card {
    background: #2b3140; padding: 24px 32px; border-radius: 12px; width: 360px;
    box-shadow: 0 8px 24px rgba(0, 0, 0, .4); text-align: center;
  }
  h1 { margin-top: 0; font-size: 1.5rem; }
  input {
    width: 100%; padding: 10px; margin: 6px 0; border-radius: 6px; border: 1px solid #555;
    background: #1f2430; color: #eee; font-size: 1rem;
  }
  button {
    padding: 10px 20px; border: none; border-radius: 6px; background: #1e88e5; color: #fff;
    font-size: 1rem; cursor: pointer; margin-top: 8px;
  }
  .hidden { display: none; }
  .error { color: #ef5350; min-height: 1.2em; }
  .room { color: #aaa; font-size: .9rem; margin-bottom: 8px; }
  .scores { display: flex; gap: 8px; }
  .score { flex: 1; padding: 8px; border-radius: 6px; border: 2px solid transparent; }
  .score.active { border-color: #fbc02d; }
  .points { font-size: 1.8rem; font-weight: bold; }
  .X { color: #42a5f5; }
  .O { color: #ef5350; }
  .board { display: grid; grid-template-columns: repeat(3, 90px); gap: 6px; justify-content: center; margin: 16px 0; }
  .cell {
    width: 90px; height: 90px; background: #3a4256; border-radius: 8px; font-size: 3rem; font-weight: bold;
    display: flex; align-items: center; justify-content: center; cursor: pointer; user-select: none;
    touch-action: none;
  }
  .cell.movable { cursor: grab; }
  .cell.selected { outline: 3px solid #fbc02d; }
  .cell.dragging { opacity: .35; }
  .cell.drop-target { outline: 3px dashed #fbc02d; }
  .cell.win { background: #43a047; color: #fff; }
  .ghost {
    position: fixed; pointer-events: none; transform: translate(-50%, -50%);
    font-size: 3rem; font-weight: bold; z-index: 10; cursor: grabbing;
  }
  #status { min-height: 1.5em; }
</style>
</head>
<body>
<div class="card" id="join-view">
  <h1>Three Men's Morris</h1>
  <form id="join-form">
    <input id="name" placeholder="Your name" maxlength="20" required>
    <input id="room" placeholder="Room ID" maxlength="30" required>
    <button type="submit">Join</button>
  </form>
  <p class="error" id="join-error"></p>
</div>

<div class="card hidden" id="game-view">
  <div class="room">Room: <span id="room-label"></span></div>
  <div class="scores">
    <div class="score" id="score-X"><div class="X" id="name-X"></div><div class="points" id="pts-X">0</div></div>
    <div class="score" id="score-O"><div class="O" id="name-O"></div><div class="points" id="pts-O">0</div></div>
  </div>
  <div class="board" id="board"></div>
  <p id="status"></p>
  <button id="leave">Leave</button>
</div>

<script>
const PIECES = 3;
const $ = (id) => document.getElementById(id);
let ws = null, me = null, state = null, selected = null;
let drag = null, suppressClick = false;

const cells = [];
for (let i = 0; i < 9; i++) {
  const cell = document.createElement("div");
  cell.className = "cell";
  cell.addEventListener("click", () => onCellClick(i));
  cell.addEventListener("pointerdown", (e) => startDrag(e, i));
  $("board").appendChild(cell);
  cells.push(cell);
}

function canMove() {
  const s = state;
  return s && s.ready && !s.winner && s.turn === me && s.placed[me] >= PIECES;
}

function cellAt(x, y) {
  const el = document.elementFromPoint(x, y);
  const cell = el && el.closest(".cell");
  return cell ? cells.indexOf(cell) : -1;
}

function startDrag(e, i) {
  if (canMove() && state.board[i] === me) drag = { from: i, x: e.clientX, y: e.clientY, ghost: null };
}

function endDrag() {
  if (drag && drag.ghost) drag.ghost.remove();
  drag = null;
  cells.forEach((c) => c.classList.remove("dragging", "drop-target"));
}

document.addEventListener("pointermove", (e) => {
  if (!drag) return;
  if (!drag.ghost) {
    // Small movements are treated as a tap, not a drag.
    if (Math.hypot(e.clientX - drag.x, e.clientY - drag.y) < 5) return;
    drag.ghost = document.createElement("div");
    drag.ghost.className = "ghost " + me;
    drag.ghost.textContent = me;
    document.body.appendChild(drag.ghost);
    cells[drag.from].classList.add("dragging");
  }
  drag.ghost.style.left = e.clientX + "px";
  drag.ghost.style.top = e.clientY + "px";
  const target = cellAt(e.clientX, e.clientY);
  cells.forEach((c, j) => c.classList.toggle("drop-target", j === target && state.board[j] === null));
});

document.addEventListener("pointerup", (e) => {
  if (!drag) return;
  const { from, ghost } = drag;
  endDrag();
  if (!ghost) return;
  suppressClick = true;
  setTimeout(() => { suppressClick = false; }, 0);
  const to = cellAt(e.clientX, e.clientY);
  if (to !== -1 && state.board[to] === null && canMove()) {
    selected = null;
    send({ type: "move", from, to });
  }
});

document.addEventListener("pointercancel", endDrag);

$("join-form").addEventListener("submit", (e) => {
  e.preventDefault();
  const name = $("name").value.trim();
  const room = $("room").value.trim();
  if (name && room) connect(name, room);
});

$("leave").addEventListener("click", () => ws && ws.close());

function connect(name, room) {
  $("join-error").textContent = "";
  const proto = location.protocol === "https:" ? "wss" : "ws";
  ws = new WebSocket(`${proto}://${location.host}/ws`);
  ws.onopen = () => send({ type: "join", name, room });
  ws.onmessage = (e) => handle(JSON.parse(e.data));
  ws.onclose = showJoin;
}

function send(msg) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
}

function handle(msg) {
  if (msg.type === "error") {
    $("join-error").textContent = msg.message;
  } else if (msg.type === "joined") {
    me = msg.symbol;
    $("room-label").textContent = msg.room;
    $("join-view").classList.add("hidden");
    $("game-view").classList.remove("hidden");
  } else if (msg.type === "state") {
    state = msg;
    if (selected !== null && state.board[selected] !== me) selected = null;
    render();
  }
}

function showJoin() {
  ws = me = state = selected = null;
  $("game-view").classList.add("hidden");
  $("join-view").classList.remove("hidden");
}

function onCellClick(i) {
  const s = state;
  if (suppressClick || !s || !s.ready || s.winner || s.turn !== me) return;

  if (s.placed[me] < PIECES) {
    if (s.board[i] === null) send({ type: "place", cell: i });
  } else if (s.board[i] === me) {
    selected = selected === i ? null : i;
    render();
  } else if (s.board[i] === null && selected !== null) {
    send({ type: "move", from: selected, to: i });
    selected = null;
  }
}

function render() {
  const s = state;
  const movable = canMove();
  s.board.forEach((v, i) => {
    cells[i].textContent = v || "";
    cells[i].className = "cell" + (v ? " " + v : "")
      + (movable && v === me ? " movable" : "")
      + (i === selected ? " selected" : "")
      + (s.winLine && s.winLine.includes(i) ? " win" : "");
  });
  for (const sym of ["X", "O"]) {
    const name = s.players[sym] || "waiting...";
    $("name-" + sym).textContent = `${sym}: ${name}${sym === me ? " (you)" : ""}`;
    $("pts-" + sym).textContent = s.scores[sym];
    $("score-" + sym).classList.toggle("active", s.ready && !s.winner && s.turn === sym);
  }
  $("status").textContent = statusText();
}

function statusText() {
  const s = state;
  const opponent = me === "X" ? "O" : "X";
  if (!s.ready) return "Waiting for an opponent to join this room...";
  if (s.winner) return `${s.winner === me ? "You win" : s.players[s.winner] + " wins"}! Next round starting...`;
  if (s.turn !== me) return `Waiting for ${s.players[opponent]}...`;
  const left = PIECES - s.placed[me];
  if (left > 0) return `Your turn: place a piece (${left} left)`;
  return selected === null ? "Your turn: drag (or tap) one of your pieces" : "Now pick an empty square";
}
</script>
</body>
</html>
"""


if __name__ == "__main__":
    uvicorn.run(app, host=HOST, port=PORT, ws_max_size=4096)
