"""
Satellite Telemetry API — Hybrid WebSocket (command-driven + auto-stream)
Built with FastAPI | CelesTrak TLE data | Skyfield orbital mechanics
8080
REST Endpoints
──────────────
GET  /                          Health check
GET  /telemetry                 Real-time orbital position (live TLE from CelesTrak)
GET  /tle                       Latest TLE data for any NORAD ID
GET  /groundstations            Deduplicated ground station list from the dataset
GET  /telemetry/frames          Paginated raw hex frames from the dataset
GET  /telemetry/frames/next     Next unread frame (sequential cursor, never repeats)
POST /telemetry/frames/reset    Reset cursor back to frame 1

WebSocket Endpoint
──────────────────
WS   /ws/telemetry              Hybrid: command-driven + optional auto-stream
                                 Query params:
                                   dataset – override CSV path
                                   reset   – "true" to restart from frame 1 on connect

Client → Server commands (JSON)
────────────────────────────────
  { "type": "start_stream",  "interval": <float, default 5> }
  { "type": "stop_stream"  }
  { "type": "get_next_frame" }
  { "type": "reset_cursor"  }

Server → Client messages (JSON)
────────────────────────────────
  { "type": "frame",     "cursor": N, "total_frames": T, "remaining": R,
    "streamed_at": "<iso8601>", "frame": {...} }
  { "type": "exhausted", "total_frames": T, "message": "..." }
  { "type": "ack",       "command": "<cmd>", "message": "..." }
  { "type": "error",     "message": "..." }
"""

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import requests
import json
import os
import asyncio
from datetime import datetime, timezone
from skyfield.api import Loader, EarthSatellite
from typing import Optional
import logging

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ─── Config ──────────────────────────────────────────────────────────────────
DEFAULT_NORAD_ID         = 39444   # FUNcube-1 / AO-73
DATA_FILE_PATH           = "./OTOF-8025-8953-4756-4779-5568-20260220T214057Z-month.csv"
CELESTRAK_URL            = "https://celestrak.org/NORAD/elements/gp.php?CATNR={norad_id}&FORMAT=TLE"
DEFAULT_STREAM_INTERVAL  = 5.0    # seconds between WebSocket frames

# ─── Skyfield ────────────────────────────────────────────────────────────────
loader = Loader("~/skyfield-data")
ts     = loader.timescale()


# ─── Lifespan ────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Satellite Telemetry API starting up…")
    yield
    logger.info("Satellite Telemetry API shutting down.")


# ─── App ─────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Satellite Telemetry API",
    description=(
        "Pull-model REST API + hybrid WebSocket for satellite telemetry. "
        "The WebSocket supports both on-demand single frames and controlled auto-streaming."
    ),
    version="3.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# ══════════════════════════════════════════════════════════════════════════════
# TLE / Orbital helpers
# ══════════════════════════════════════════════════════════════════════════════

def fetch_tle(norad_id: int) -> dict:
    """Fetch TLE lines from CelesTrak for a given NORAD ID."""
    url = CELESTRAK_URL.format(norad_id=norad_id)
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        lines = [l.strip() for l in resp.text.strip().splitlines() if l.strip()]
        if len(lines) < 3:
            raise ValueError(f"Incomplete TLE data received (got {len(lines)} lines).")
        return {"name": lines[0], "line1": lines[1], "line2": lines[2], "source": url}
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"CelesTrak request failed: {exc}")
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


def build_satellite(tle: dict) -> EarthSatellite:
    return EarthSatellite(tle["line1"], tle["line2"], tle["name"], ts)


def compute_position(satellite: EarthSatellite) -> dict:
    t        = ts.now()
    subpoint = satellite.at(t).subpoint()
    return {
        "latitude_deg":  round(subpoint.latitude.degrees, 6),
        "longitude_deg": round(subpoint.longitude.degrees, 6),
        "altitude_km":   round(subpoint.elevation.km, 3),
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
    }


# ══════════════════════════════════════════════════════════════════════════════
# Sequential cursor (persisted to disk so it survives restarts)
# ══════════════════════════════════════════════════════════════════════════════
CURSOR_FILE = "./telemetry_cursor.json"


def _read_cursor() -> int:
    if os.path.exists(CURSOR_FILE):
        try:
            with open(CURSOR_FILE, "r") as f:
                return int(json.load(f).get("next_index", 0))
        except Exception:
            pass
    return 0


def _write_cursor(index: int) -> None:
    with open(CURSOR_FILE, "w") as f:
        json.dump(
            {"next_index": index, "updated_utc": datetime.now(timezone.utc).isoformat()},
            f,
        )


def _reset_cursor() -> None:
    _write_cursor(0)


# ══════════════════════════════════════════════════════════════════════════════
# Dataset parsers
# ══════════════════════════════════════════════════════════════════════════════

def _parse_row(lineno: int, raw: str) -> Optional[dict]:
    """Parse one pipe-delimited CSV row. Returns None if malformed."""
    parts = raw.split("|")
    if len(parts) < 4:
        return None
    captured_at  = parts[0].strip()
    hex_data     = parts[1].strip()
    freq_info    = parts[2].strip() if len(parts) > 2 else ""
    station_info = parts[3].strip()
    station_name = station_info.split(" ")[0]
    grid_locator = station_info.split("-")[-1] if "-" in station_info else "N/A"
    return {
        "frame_index":    lineno,
        "captured_at":    captured_at,
        "station": {
            "name":         station_name,
            "grid_locator": grid_locator,
            "raw_info":     station_info,
        },
        "frequency_info": freq_info,
        "hex_frame":      hex_data,
        "hex_byte_count": len(hex_data) // 2 if hex_data else 0,
    }


def _require_file(filepath: str) -> None:
    if not os.path.exists(filepath):
        raise HTTPException(
            status_code=404,
            detail=f"Dataset file not found: {os.path.abspath(filepath)}",
        )


def load_all_frames(filepath: str) -> list[dict]:
    """Load every valid frame from the dataset."""
    _require_file(filepath)
    frames = []
    with open(filepath, "r", encoding="utf-8") as f:
        for lineno, raw in enumerate(f, start=1):
            raw = raw.strip()
            if not raw:
                continue
            frame = _parse_row(lineno, raw)
            if frame:
                frames.append(frame)
    return frames


def paginate_frames(
    filepath: str,
    limit: int,
    offset: int,
    station_filter: Optional[str],
) -> tuple[list[dict], int]:
    """Return a paginated, optionally-filtered slice of frames + total match count."""
    _require_file(filepath)
    matched = []
    with open(filepath, "r", encoding="utf-8") as f:
        for lineno, raw in enumerate(f, start=1):
            raw = raw.strip()
            if not raw:
                continue
            frame = _parse_row(lineno, raw)
            if frame is None:
                continue
            if station_filter and frame["station"]["name"].lower() != station_filter.lower():
                continue
            matched.append(frame)
    return matched[offset: offset + limit], len(matched)


def parse_ground_stations(filepath: str) -> list[dict]:
    """Return a deduplicated list of ground stations in the dataset."""
    _require_file(filepath)
    stations: dict[str, dict] = {}
    with open(filepath, "r", encoding="utf-8") as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            parts = raw.split("|")
            if len(parts) < 4:
                continue
            station_info = parts[3].strip()
            station_name = station_info.split(" ")[0]
            grid_locator = station_info.split("-")[-1] if "-" in station_info else "N/A"
            if station_name not in stations:
                stations[station_name] = {
                    "name":         station_name,
                    "grid_locator": grid_locator,
                    "raw_info":     station_info,
                }
    return list(stations.values())


# ══════════════════════════════════════════════════════════════════════════════
# REST Routes
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/", summary="API health check")
def root():
    return {
        "status":  "online",
        "message": "Satellite Telemetry API v3 — REST + Hybrid WebSocket",
        "rest_endpoints": [
            "GET  /telemetry",
            "GET  /tle",
            "GET  /groundstations",
            "GET  /telemetry/frames",
            "GET  /telemetry/frames/next",
            "POST /telemetry/frames/reset",
        ],
        "websocket_endpoints": [
            "WS /ws/telemetry  (query params: dataset, reset)",
        ],
        "websocket_commands": [
            '{ "type": "start_stream",  "interval": <seconds> }',
            '{ "type": "stop_stream"  }',
            '{ "type": "get_next_frame" }',
            '{ "type": "reset_cursor"  }',
        ],
        "docs": "/docs",
    }


@app.get("/telemetry", summary="Real-time satellite position")
def get_telemetry(
    norad: int = Query(default=DEFAULT_NORAD_ID, description="NORAD Catalog Number"),
):
    logger.info(f"GET /telemetry  norad={norad}")
    tle       = fetch_tle(norad)
    satellite = build_satellite(tle)
    position  = compute_position(satellite)
    return {
        "norad_id":   norad,
        "satellite":  tle["name"],
        "position":   position,
        "tle_source": tle["source"],
    }


@app.get("/tle", summary="Latest TLE data for a satellite")
def get_tle(
    norad: int = Query(default=DEFAULT_NORAD_ID, description="NORAD Catalog Number"),
):
    logger.info(f"GET /tle  norad={norad}")
    tle = fetch_tle(norad)
    return {
        "norad_id":       norad,
        "satellite_name": tle["name"],
        "tle_line1":      tle["line1"],
        "tle_line2":      tle["line2"],
        "fetched_utc":    datetime.now(timezone.utc).isoformat(),
        "source":         tle["source"],
    }


@app.get("/groundstations", summary="Ground stations found in the telemetry dataset")
def get_ground_stations(
    dataset: Optional[str] = Query(default=None, description="Override path to the CSV file"),
):
    filepath = dataset or DATA_FILE_PATH
    logger.info(f"GET /groundstations  file={filepath}")
    stations = parse_ground_stations(filepath)
    return {
        "total":    len(stations),
        "stations": stations,
        "source":   os.path.abspath(filepath),
    }


@app.get("/telemetry/frames", summary="Paginated raw hex frames from the dataset")
def get_frames(
    limit:   int           = Query(default=5,  ge=1, le=10),
    offset:  int           = Query(default=0,   ge=0),
    station: Optional[str] = Query(default=None),
    dataset: Optional[str] = Query(default=None),
):
    filepath = dataset or DATA_FILE_PATH
    logger.info(f"GET /telemetry/frames  limit={limit} offset={offset} station={station}")
    frames, total = paginate_frames(filepath, limit=limit, offset=offset, station_filter=station)
    return {
        "total_matching": total,
        "returned":       len(frames),
        "offset":         offset,
        "limit":          limit,
        "filters":        {"station": station},
        "source":         os.path.abspath(filepath),
        "frames":         frames,
    }


@app.get("/telemetry/frames/next", summary="Next unread frame — never repeats")
def get_next_frame(
    dataset: Optional[str] = Query(default=None),
):
    filepath = dataset or DATA_FILE_PATH
    frames   = load_all_frames(filepath)
    cursor   = _read_cursor()
    logger.info(f"GET /telemetry/frames/next  cursor={cursor}/{len(frames)}")

    if cursor >= len(frames):
        return {
            "exhausted":    True,
            "message":      "All frames delivered. POST /telemetry/frames/reset to replay.",
            "total_frames": len(frames),
            "cursor":       cursor,
        }

    frame = frames[cursor]
    _write_cursor(cursor + 1)
    return {
        "exhausted":    False,
        "cursor":       cursor,
        "total_frames": len(frames),
        "remaining":    len(frames) - (cursor + 1),
        "frame":        frame,
    }


@app.post("/telemetry/frames/reset", summary="Reset the sequential cursor to frame 1")
def reset_cursor():
    _reset_cursor()
    logger.info("POST /telemetry/frames/reset  cursor -> 0")
    return {
        "message": "Cursor reset. The next /telemetry/frames/next call will return frame 1.",
        "cursor":  0,
    }


# ══════════════════════════════════════════════════════════════════════════════
# WebSocket helpers
# ══════════════════════════════════════════════════════════════════════════════

async def _send_frame(websocket: WebSocket, frames: list[dict]) -> bool:
    """
    Send the next frame from the shared cursor.
    Returns True if a frame was sent, False if the dataset is exhausted.
    Advances the cursor on success.
    """
    cursor = _read_cursor()
    total  = len(frames)

    if cursor >= total:
        await websocket.send_json({
            "type":         "exhausted",
            "total_frames": total,
            "message":      (
                "All frames have been streamed. "
                "Send { \"type\": \"reset_cursor\" } to replay from frame 1."
            ),
        })
        return False

    frame     = frames[cursor]
    remaining = total - (cursor + 1)

    await websocket.send_json({
        "type":         "frame",
        "cursor":       cursor,
        "total_frames": total,
        "remaining":    remaining,
        "streamed_at":  datetime.now(timezone.utc).isoformat(),
        "frame":        frame,
    })
    _write_cursor(cursor + 1)
    return True


async def _stream_loop(
    websocket: WebSocket,
    frames: list[dict],
    interval: float,
    stop_event: asyncio.Event,
    client: str,
) -> None:
    """
    Background coroutine: sends frames at `interval`-second intervals until
    the dataset is exhausted or `stop_event` is set by a stop_stream command.
    """
    logger.info(f"WS [{client}]  stream loop started | interval={interval}s")

    while not stop_event.is_set():
        sent = await _send_frame(websocket, frames)

        if not sent:
            # Dataset exhausted — the exhausted message was already sent inside _send_frame
            logger.info(f"WS [{client}]  stream loop finished (dataset exhausted)")
            break

        cursor = _read_cursor()
        logger.info(
            f"WS [{client}]  streamed frame {cursor}/{len(frames)} "
            f"| remaining={len(frames) - cursor}"
        )

        try:
            # Sleep for `interval` seconds, but wake up immediately if stop_event fires
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            # Normal path: interval elapsed, send the next frame
            pass

    logger.info(f"WS [{client}]  stream loop exited")


# ══════════════════════════════════════════════════════════════════════════════
# WebSocket — hybrid command-driven + auto-stream endpoint
# ══════════════════════════════════════════════════════════════════════════════

@app.websocket("/ws/telemetry")
async def ws_telemetry(
    websocket: WebSocket,
    dataset: Optional[str] = Query(
        default=None,
        description="Override path to the pipe-delimited CSV file",
    ),
    reset: Optional[str] = Query(
        default=None,
        description='Pass "true" to restart the stream from frame 1 before connecting',
    ),
):
    """
    Hybrid WebSocket endpoint: command-driven single frames + controlled auto-streaming.

    After connecting, the client drives behaviour by sending JSON commands.

    ── Commands ─────────────────────────────────────────────────────────────
    start_stream   Begin sending frames automatically.
                   Optional "interval" key sets seconds between frames (default 5, min 1).
                     { "type": "start_stream", "interval": 3 }

    stop_stream    Halt the auto-stream; connection stays open.
                     { "type": "stop_stream" }

    get_next_frame Send exactly one frame right now (like REST /telemetry/frames/next).
                     { "type": "get_next_frame" }

    reset_cursor   Rewind the cursor to frame 1.
                     { "type": "reset_cursor" }

    ── Server responses ─────────────────────────────────────────────────────
    Frame:
      { "type": "frame", "cursor": N, "total_frames": T, "remaining": R,
        "streamed_at": "<iso>", "frame": {...} }

    Exhausted:
      { "type": "exhausted", "total_frames": T, "message": "..." }

    Acknowledgement:
      { "type": "ack", "command": "<cmd>", "message": "..." }

    Error:
      { "type": "error", "message": "..." }
    """
    await websocket.accept()

    filepath = dataset or DATA_FILE_PATH
    client   = str(websocket.client)

    # ── Optional cursor reset on connect ─────────────────────────────────────
    if str(reset).lower() == "true":
        _reset_cursor()
        logger.info(f"WS [{client}]  cursor reset to 0 on connect")

    # ── Load dataset once for this connection ─────────────────────────────────
    try:
        frames = load_all_frames(filepath)
    except HTTPException as exc:
        await websocket.send_json({"type": "error", "message": exc.detail})
        await websocket.close(code=1011)
        return

    total = len(frames)
    logger.info(
        f"WS [{client}]  connected | frames={total} | file={filepath}"
    )

    # ── Per-connection streaming state ────────────────────────────────────────
    # stream_task  : the currently running _stream_loop asyncio.Task, or None
    # stop_event   : set to interrupt the sleep inside _stream_loop immediately
    stream_task: Optional[asyncio.Task] = None
    stop_event  = asyncio.Event()

    # ── Command receive loop ──────────────────────────────────────────────────
    try:
        while True:
            raw_msg = await websocket.receive_text()

            # ── Parse incoming JSON ───────────────────────────────────────────
            try:
                msg = json.loads(raw_msg)
            except json.JSONDecodeError:
                await websocket.send_json({
                    "type":    "error",
                    "message": "Invalid JSON. All commands must be valid JSON objects.",
                })
                continue

            cmd = msg.get("type", "").strip()

            # ════════════════════════════════════════════════════════════════
            # Command: start_stream
            # ════════════════════════════════════════════════════════════════
            if cmd == "start_stream":
                # Cancel any in-progress stream first
                if stream_task and not stream_task.done():
                    stop_event.set()
                    await stream_task   # wait for graceful exit
                    logger.info(f"WS [{client}]  previous stream cancelled by start_stream")

                raw_interval = msg.get("interval", DEFAULT_STREAM_INTERVAL)
                try:
                    interval = max(1.0, float(raw_interval))
                except (TypeError, ValueError):
                    interval = DEFAULT_STREAM_INTERVAL

                # Fresh stop_event for the new loop
                stop_event = asyncio.Event()
                stream_task = asyncio.create_task(
                    _stream_loop(websocket, frames, interval, stop_event, client)
                )

                await websocket.send_json({
                    "type":     "ack",
                    "command":  "start_stream",
                    "message":  f"Streaming started at {interval}s intervals.",
                    "interval": interval,
                })
                logger.info(f"WS [{client}]  start_stream | interval={interval}s")

            # ════════════════════════════════════════════════════════════════
            # Command: stop_stream
            # ════════════════════════════════════════════════════════════════
            elif cmd == "stop_stream":
                if stream_task and not stream_task.done():
                    stop_event.set()
                    await stream_task
                    logger.info(f"WS [{client}]  stream stopped by client command")
                    await websocket.send_json({
                        "type":    "ack",
                        "command": "stop_stream",
                        "message": "Streaming stopped. Connection remains open.",
                    })
                else:
                    await websocket.send_json({
                        "type":    "ack",
                        "command": "stop_stream",
                        "message": "No active stream to stop.",
                    })

            # ════════════════════════════════════════════════════════════════
            # Command: get_next_frame
            # ════════════════════════════════════════════════════════════════
            elif cmd == "get_next_frame":
                if stream_task and not stream_task.done():
                    await websocket.send_json({
                        "type":    "error",
                        "message": (
                            "Cannot use get_next_frame while streaming is active. "
                            "Send stop_stream first."
                        ),
                    })
                    continue

                cursor = _read_cursor()
                logger.info(
                    f"WS [{client}]  get_next_frame | cursor={cursor}/{total}"
                )
                await _send_frame(websocket, frames)

            # ════════════════════════════════════════════════════════════════
            # Command: reset_cursor
            # ════════════════════════════════════════════════════════════════
            elif cmd == "reset_cursor":
                # Stop any active stream before resetting so frames restart cleanly
                if stream_task and not stream_task.done():
                    stop_event.set()
                    await stream_task
                    logger.info(f"WS [{client}]  stream stopped by reset_cursor")

                _reset_cursor()
                logger.info(f"WS [{client}]  cursor reset to 0 by client command")
                await websocket.send_json({
                    "type":         "ack",
                    "command":      "reset_cursor",
                    "message":      "Cursor reset to frame 1.",
                    "cursor":       0,
                    "total_frames": total,
                })

            # ════════════════════════════════════════════════════════════════
            # Unknown command
            # ════════════════════════════════════════════════════════════════
            else:
                await websocket.send_json({
                    "type":    "error",
                    "message": (
                        f"Unknown command type: \"{cmd}\". "
                        "Supported: start_stream | stop_stream | get_next_frame | reset_cursor"
                    ),
                })

    except WebSocketDisconnect:
        logger.info(f"WS [{client}]  client disconnected")
    except Exception as exc:
        logger.error(f"WS [{client}]  unexpected error: {exc}")
        try:
            await websocket.send_json({"type": "error", "message": str(exc)})
            await websocket.close(code=1011)
        except Exception:
            pass
    finally:
        # Always clean up the background stream task on any exit path
        if stream_task and not stream_task.done():
            stop_event.set()
            stream_task.cancel()
            try:
                await stream_task
            except asyncio.CancelledError:
                pass
        logger.info(f"WS [{client}]  connection closed")