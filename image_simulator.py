"""
Image Dataset Simulator API — Hybrid WebSocket (command-driven + auto-stream)
Built with FastAPI | CSV pipe-delimited dataset | Base64 inline image delivery
8084
REST Endpoints
──────────────
GET  /                          Health check
GET  /images/frames             Paginated image frames from the dataset
GET  /images/frames/next        Next unread frame (sequential cursor, never repeats)
POST /images/frames/reset       Reset cursor back to frame 1
GET  /images/labels             Unique label/class list from the dataset
GET  /images/stats              Dataset statistics — totals, label distribution, splits
GET  /images/search             Full-text search across labels and metadata fields

WebSocket Endpoint
──────────────────
WS   /ws/images                 Hybrid: command-driven + optional auto-stream
                                 Query params:
                                   dataset      – override CSV path
                                   reset        – "true" to restart from frame 1 on connect
                                   label_filter – only stream frames matching this label

Client → Server commands (JSON)
────────────────────────────────
  { "type": "start_stream",  "interval": <float, default 3>, "label": "<optional>" }
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

CSV format (pipe-delimited)
────────────────────────────
  filename|label|width|height|bbox_x|bbox_y|bbox_w|bbox_h|split|captured_at
  img_001.jpg|cat|640|480|120|80|200|160|train|2024-01-15T10:30:00Z

  - bbox fields are optional (leave empty if no annotation)
  - split is optional (train / val / test)
  - Images are resolved from IMAGE_DIR; missing files are flagged gracefully
"""

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import json
import os
import asyncio
import base64
import mimetypes
from datetime import datetime, timezone
from typing import Optional
import logging

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ─── Config ──────────────────────────────────────────────────────────────────
DATA_FILE_PATH          = "./image_dataset.csv"   # pipe-delimited CSV index
IMAGE_DIR               = "./images"               # folder containing image files
DEFAULT_STREAM_INTERVAL = 3.0                      # seconds between WebSocket frames
MAX_B64_SIZE_MB         = 5                        # skip inline payload above this size


# ─── Lifespan ────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Image Dataset Simulator API starting up…")
    logger.info(f"Dataset: {os.path.abspath(DATA_FILE_PATH)}")
    logger.info(f"Image dir: {os.path.abspath(IMAGE_DIR)}")
    yield
    logger.info("Image Dataset Simulator API shutting down.")


# ─── App ─────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Image Dataset Simulator API",
    description=(
        "Pull-model REST API + hybrid WebSocket for image dataset simulation. "
        "Sequential cursor-based delivery, paginated browsing, label filtering, "
        "and base64 inline image payloads — same architecture as the Satellite Telemetry API."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)


# ══════════════════════════════════════════════════════════════════════════════
# Sequential cursor (persisted to disk so it survives restarts)
# ══════════════════════════════════════════════════════════════════════════════
CURSOR_FILE = "./image_cursor.json"


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
# Image helpers
# ══════════════════════════════════════════════════════════════════════════════

def _load_image_b64(filename: str) -> dict:
    """
    Attempt to load an image from IMAGE_DIR and return a base64 payload dict.
    Returns a dict with 'data', 'media_type', 'size_bytes', and 'available' flag.
    Gracefully handles missing files without raising.
    """
    filepath = os.path.join(IMAGE_DIR, filename)

    if not os.path.exists(filepath):
        return {
            "available":  False,
            "reason":     f"File not found: {filepath}",
            "data":       None,
            "media_type": None,
            "size_bytes": None,
        }

    size_bytes = os.path.getsize(filepath)
    if size_bytes > MAX_B64_SIZE_MB * 1024 * 1024:
        return {
            "available":  False,
            "reason":     f"File exceeds {MAX_B64_SIZE_MB} MB inline limit ({size_bytes} bytes)",
            "data":       None,
            "media_type": None,
            "size_bytes": size_bytes,
        }

    media_type, _ = mimetypes.guess_type(filename)
    media_type = media_type or "application/octet-stream"

    with open(filepath, "rb") as f:
        encoded = base64.b64encode(f.read()).decode("utf-8")

    return {
        "available":  True,
        "reason":     None,
        "data":       encoded,
        "media_type": media_type,
        "size_bytes": size_bytes,
    }


# ══════════════════════════════════════════════════════════════════════════════
# Dataset parsers
# ══════════════════════════════════════════════════════════════════════════════

# Expected CSV columns (pipe-delimited, no header row required)
# filename | label | width | height | bbox_x | bbox_y | bbox_w | bbox_h | split | captured_at
_COL_FILENAME   = 0
_COL_LABEL      = 1
_COL_WIDTH      = 2
_COL_HEIGHT     = 3
_COL_BBOX_X     = 4
_COL_BBOX_Y     = 5
_COL_BBOX_W     = 6
_COL_BBOX_H     = 7
_COL_SPLIT      = 8
_COL_CAPTURED   = 9


def _safe_int(value: str) -> Optional[int]:
    try:
        return int(value.strip())
    except (ValueError, AttributeError):
        return None


def _safe_float(value: str) -> Optional[float]:
    try:
        return float(value.strip())
    except (ValueError, AttributeError):
        return None


def _parse_row(lineno: int, raw: str) -> Optional[dict]:
    """
    Parse one pipe-delimited CSV row into an image frame record.
    Returns None if the row is malformed (fewer than 2 fields).
    Bounding box, split, and captured_at are all optional.
    """
    parts = raw.split("|")
    if len(parts) < 2:
        return None

    def col(idx: int) -> str:
        return parts[idx].strip() if idx < len(parts) else ""

    filename = col(_COL_FILENAME)
    label    = col(_COL_LABEL)

    # Dimensions
    width  = _safe_int(col(_COL_WIDTH))
    height = _safe_int(col(_COL_HEIGHT))

    # Bounding box — fully optional
    bbox_x = _safe_float(col(_COL_BBOX_X))
    bbox_y = _safe_float(col(_COL_BBOX_Y))
    bbox_w = _safe_float(col(_COL_BBOX_W))
    bbox_h = _safe_float(col(_COL_BBOX_H))

    has_bbox = all(v is not None for v in [bbox_x, bbox_y, bbox_w, bbox_h])
    bounding_box = (
        {"x": bbox_x, "y": bbox_y, "width": bbox_w, "height": bbox_h}
        if has_bbox else None
    )

    split       = col(_COL_SPLIT) or None
    captured_at = col(_COL_CAPTURED) or None

    return {
        "frame_index":   lineno,
        "filename":      filename,
        "label":         label,
        "dimensions":    {"width": width, "height": height},
        "bounding_box":  bounding_box,
        "split":         split,
        "captured_at":   captured_at,
    }


def _require_file(filepath: str) -> None:
    if not os.path.exists(filepath):
        raise HTTPException(
            status_code=404,
            detail=f"Dataset file not found: {os.path.abspath(filepath)}",
        )


def load_all_frames(filepath: str) -> list[dict]:
    """Load every valid frame from the dataset (no image data — metadata only)."""
    _require_file(filepath)
    frames = []
    with open(filepath, "r", encoding="utf-8") as f:
        for lineno, raw in enumerate(f, start=1):
            raw = raw.strip()
            if not raw or raw.startswith("#"):
                continue
            frame = _parse_row(lineno, raw)
            if frame:
                frames.append(frame)
    return frames


def paginate_frames(
    filepath: str,
    limit: int,
    offset: int,
    label_filter: Optional[str],
    split_filter: Optional[str],
) -> tuple[list[dict], int]:
    """Return a paginated, optionally-filtered slice of frames + total match count."""
    _require_file(filepath)
    matched = []
    with open(filepath, "r", encoding="utf-8") as f:
        for lineno, raw in enumerate(f, start=1):
            raw = raw.strip()
            if not raw or raw.startswith("#"):
                continue
            frame = _parse_row(lineno, raw)
            if frame is None:
                continue
            if label_filter and frame["label"].lower() != label_filter.lower():
                continue
            if split_filter and (frame["split"] or "").lower() != split_filter.lower():
                continue
            matched.append(frame)
    return matched[offset: offset + limit], len(matched)


def parse_labels(filepath: str) -> list[dict]:
    """Return a deduplicated label list with per-label counts."""
    _require_file(filepath)
    counts: dict[str, int] = {}
    with open(filepath, "r", encoding="utf-8") as f:
        for raw in f:
            raw = raw.strip()
            if not raw or raw.startswith("#"):
                continue
            parts = raw.split("|")
            if len(parts) < 2:
                continue
            label = parts[_COL_LABEL].strip()
            if label:
                counts[label] = counts.get(label, 0) + 1
    return [{"label": lbl, "count": cnt} for lbl, cnt in sorted(counts.items())]


def compute_stats(filepath: str) -> dict:
    """Compute full dataset statistics: total, label distribution, split breakdown."""
    _require_file(filepath)
    total        = 0
    label_counts: dict[str, int] = {}
    split_counts: dict[str, int] = {}
    annotated    = 0
    missing      = 0

    with open(filepath, "r", encoding="utf-8") as f:
        for raw in f:
            raw = raw.strip()
            if not raw or raw.startswith("#"):
                continue
            frame = _parse_row(0, raw)
            if frame is None:
                continue
            total += 1

            label = frame["label"]
            label_counts[label] = label_counts.get(label, 0) + 1

            split = frame["split"] or "unspecified"
            split_counts[split] = split_counts.get(split, 0) + 1

            if frame["bounding_box"] is not None:
                annotated += 1

            img_path = os.path.join(IMAGE_DIR, frame["filename"])
            if not os.path.exists(img_path):
                missing += 1

    return {
        "total_frames":       total,
        "annotated_frames":   annotated,
        "unannotated_frames": total - annotated,
        "missing_images":     missing,
        "label_distribution": [
            {"label": lbl, "count": cnt, "pct": round(cnt / total * 100, 1) if total else 0}
            for lbl, cnt in sorted(label_counts.items())
        ],
        "split_distribution": [
            {"split": spl, "count": cnt, "pct": round(cnt / total * 100, 1) if total else 0}
            for spl, cnt in sorted(split_counts.items())
        ],
    }


def search_frames(
    filepath: str,
    query: str,
    limit: int,
    offset: int,
) -> tuple[list[dict], int]:
    """Full-text search across filename, label, and split fields."""
    _require_file(filepath)
    q = query.lower()
    matched = []
    with open(filepath, "r", encoding="utf-8") as f:
        for lineno, raw in enumerate(f, start=1):
            raw = raw.strip()
            if not raw or raw.startswith("#"):
                continue
            frame = _parse_row(lineno, raw)
            if frame is None:
                continue
            searchable = " ".join([
                frame["filename"],
                frame["label"],
                frame["split"] or "",
                frame["captured_at"] or "",
            ]).lower()
            if q in searchable:
                matched.append(frame)
    return matched[offset: offset + limit], len(matched)


def enrich_frame_with_image(frame: dict) -> dict:
    """
    Attach base64 image payload to a frame dict.
    Returns a new dict — does not mutate the input.
    """
    enriched = dict(frame)
    enriched["image"] = _load_image_b64(frame["filename"])
    return enriched


# ══════════════════════════════════════════════════════════════════════════════
# REST Routes
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/", summary="API health check")
def root():
    return {
        "status":  "online",
        "message": "Image Dataset Simulator API v1 — REST + Hybrid WebSocket",
        "rest_endpoints": [
            "GET  /images/frames",
            "GET  /images/frames/next",
            "POST /images/frames/reset",
            "GET  /images/labels",
            "GET  /images/stats",
            "GET  /images/search",
        ],
        "websocket_endpoints": [
            "WS /ws/images  (query params: dataset, reset, label_filter)",
        ],
        "websocket_commands": [
            '{ "type": "start_stream",  "interval": <seconds>, "label": "<optional>" }',
            '{ "type": "stop_stream"  }',
            '{ "type": "get_next_frame" }',
            '{ "type": "reset_cursor"  }',
        ],
        "docs": "/docs",
    }


@app.get("/images/frames", summary="Paginated image frames from the dataset")
def get_frames(
    limit:   int           = Query(default=5,    ge=1,  le=50),
    offset:  int           = Query(default=0,    ge=0),
    label:   Optional[str] = Query(default=None, description="Filter by label/class name"),
    split:   Optional[str] = Query(default=None, description="Filter by split: train | val | test"),
    dataset: Optional[str] = Query(default=None, description="Override path to the CSV file"),
    images:  bool          = Query(default=False, description="Include base64 image payloads"),
):
    filepath = dataset or DATA_FILE_PATH
    logger.info(
        f"GET /images/frames  limit={limit} offset={offset} "
        f"label={label} split={split} images={images}"
    )
    frames, total = paginate_frames(
        filepath, limit=limit, offset=offset,
        label_filter=label, split_filter=split,
    )
    if images:
        frames = [enrich_frame_with_image(f) for f in frames]
    return {
        "total_matching": total,
        "returned":       len(frames),
        "offset":         offset,
        "limit":          limit,
        "filters":        {"label": label, "split": split},
        "images_inline":  images,
        "source":         os.path.abspath(filepath),
        "frames":         frames,
    }


@app.get("/images/frames/next", summary="Next unread image frame — never repeats")
def get_next_frame(
    dataset: Optional[str] = Query(default=None),
    images:  bool          = Query(default=True, description="Include base64 image payload"),
):
    filepath = dataset or DATA_FILE_PATH
    frames   = load_all_frames(filepath)
    cursor   = _read_cursor()
    logger.info(f"GET /images/frames/next  cursor={cursor}/{len(frames)}")

    if cursor >= len(frames):
        return {
            "exhausted":    True,
            "message":      "All frames delivered. POST /images/frames/reset to replay.",
            "total_frames": len(frames),
            "cursor":       cursor,
        }

    frame = frames[cursor]
    _write_cursor(cursor + 1)

    if images:
        frame = enrich_frame_with_image(frame)

    return {
        "exhausted":    False,
        "cursor":       cursor,
        "total_frames": len(frames),
        "remaining":    len(frames) - (cursor + 1),
        "frame":        frame,
    }


@app.post("/images/frames/reset", summary="Reset the sequential cursor to frame 1")
def reset_cursor():
    _reset_cursor()
    logger.info("POST /images/frames/reset  cursor -> 0")
    return {
        "message": "Cursor reset. The next /images/frames/next call will return frame 1.",
        "cursor":  0,
    }


@app.get("/images/labels", summary="Unique label/class list with counts")
def get_labels(
    dataset: Optional[str] = Query(default=None),
):
    filepath = dataset or DATA_FILE_PATH
    logger.info(f"GET /images/labels  file={filepath}")
    labels = parse_labels(filepath)
    return {
        "total_labels": len(labels),
        "labels":       labels,
        "source":       os.path.abspath(filepath),
    }


@app.get("/images/stats", summary="Full dataset statistics")
def get_stats(
    dataset: Optional[str] = Query(default=None),
):
    filepath = dataset or DATA_FILE_PATH
    logger.info(f"GET /images/stats  file={filepath}")
    stats = compute_stats(filepath)
    return {
        **stats,
        "source":      os.path.abspath(filepath),
        "computed_utc": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/images/search", summary="Full-text search across labels and metadata")
def search_images(
    q:       str           = Query(..., description="Search query — matched against filename, label, split"),
    limit:   int           = Query(default=10,   ge=1, le=50),
    offset:  int           = Query(default=0,    ge=0),
    dataset: Optional[str] = Query(default=None),
    images:  bool          = Query(default=False, description="Include base64 image payloads"),
):
    filepath = dataset or DATA_FILE_PATH
    logger.info(f"GET /images/search  q={q!r} limit={limit} offset={offset}")
    frames, total = search_frames(filepath, query=q, limit=limit, offset=offset)
    if images:
        frames = [enrich_frame_with_image(f) for f in frames]
    return {
        "query":          q,
        "total_matching": total,
        "returned":       len(frames),
        "offset":         offset,
        "limit":          limit,
        "images_inline":  images,
        "source":         os.path.abspath(filepath),
        "frames":         frames,
    }


# ══════════════════════════════════════════════════════════════════════════════
# WebSocket helpers
# ══════════════════════════════════════════════════════════════════════════════

async def _send_frame(
    websocket: WebSocket,
    frames: list[dict],
    label_filter: Optional[str] = None,
) -> bool:
    """
    Send the next frame from the shared cursor, optionally skipping non-matching labels.
    Returns True if a frame was sent, False if the dataset is exhausted.
    Advances the cursor on every consumed row (matching or not) to stay consistent
    with the REST cursor — pass label_filter to skip non-matching rows transparently.
    """
    total = len(frames)

    while True:
        cursor = _read_cursor()

        if cursor >= total:
            await websocket.send_json({
                "type":         "exhausted",
                "total_frames": total,
                "message":      (
                    "All frames have been streamed. "
                    'Send { "type": "reset_cursor" } to replay from frame 1.'
                ),
            })
            return False

        frame = frames[cursor]
        _write_cursor(cursor + 1)

        # Apply label filter — keep scanning if this row doesn't match
        if label_filter and frame["label"].lower() != label_filter.lower():
            continue

        enriched  = enrich_frame_with_image(frame)
        remaining = total - (cursor + 1)

        await websocket.send_json({
            "type":         "frame",
            "cursor":       cursor,
            "total_frames": total,
            "remaining":    remaining,
            "streamed_at":  datetime.now(timezone.utc).isoformat(),
            "frame":        enriched,
        })
        return True


async def _stream_loop(
    websocket: WebSocket,
    frames: list[dict],
    interval: float,
    stop_event: asyncio.Event,
    label_filter: Optional[str],
    client: str,
) -> None:
    """
    Background coroutine: sends frames at `interval`-second intervals until
    the dataset is exhausted or `stop_event` is set by a stop_stream command.
    """
    logger.info(
        f"WS [{client}]  stream loop started | interval={interval}s | label={label_filter}"
    )

    while not stop_event.is_set():
        sent = await _send_frame(websocket, frames, label_filter=label_filter)

        if not sent:
            logger.info(f"WS [{client}]  stream loop finished (dataset exhausted)")
            break

        cursor = _read_cursor()
        logger.info(
            f"WS [{client}]  streamed frame {cursor}/{len(frames)} "
            f"| remaining={len(frames) - cursor}"
        )

        try:
            # Sleep for `interval` seconds, but wake immediately if stop_event fires
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass

    logger.info(f"WS [{client}]  stream loop exited")


# ══════════════════════════════════════════════════════════════════════════════
# WebSocket — hybrid command-driven + auto-stream endpoint
# ══════════════════════════════════════════════════════════════════════════════

@app.websocket("/ws/images")
async def ws_images(
    websocket: WebSocket,
    dataset: Optional[str] = Query(
        default=None,
        description="Override path to the pipe-delimited CSV file",
    ),
    reset: Optional[str] = Query(
        default=None,
        description='Pass "true" to restart the stream from frame 1 before connecting',
    ),
    label_filter: Optional[str] = Query(
        default=None,
        description="Only deliver frames whose label matches this value",
    ),
):
    """
    Hybrid WebSocket endpoint: command-driven single frames + controlled auto-streaming.
    Each frame payload includes base64 image data, bounding box, label, dimensions, filename.

    After connecting, the client drives behaviour by sending JSON commands.

    ── Commands ─────────────────────────────────────────────────────────────
    start_stream   Begin sending frames automatically.
                   Optional "interval" sets seconds between frames (default 3, min 1).
                   Optional "label" overrides the connection-level label_filter.
                     { "type": "start_stream", "interval": 2, "label": "cat" }

    stop_stream    Halt the auto-stream; connection stays open.
                     { "type": "stop_stream" }

    get_next_frame Send exactly one frame right now.
                     { "type": "get_next_frame" }

    reset_cursor   Rewind the cursor to frame 1.
                     { "type": "reset_cursor" }

    ── Server responses ─────────────────────────────────────────────────────
    Frame:
      { "type": "frame", "cursor": N, "total_frames": T, "remaining": R,
        "streamed_at": "<iso>",
        "frame": {
          "frame_index": N, "filename": "...", "label": "...",
          "dimensions": {"width": W, "height": H},
          "bounding_box": {"x": X, "y": Y, "width": W, "height": H} | null,
          "split": "train" | "val" | "test" | null,
          "captured_at": "<iso>" | null,
          "image": {
            "available": true, "data": "<base64>",
            "media_type": "image/jpeg", "size_bytes": N
          }
        }
      }

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
        f"WS [{client}]  connected | frames={total} | "
        f"file={filepath} | label_filter={label_filter}"
    )

    # ── Per-connection streaming state ────────────────────────────────────────
    stream_task: Optional[asyncio.Task] = None
    stop_event  = asyncio.Event()
    active_label_filter = label_filter   # may be overridden by start_stream command

    # ── Command receive loop ──────────────────────────────────────────────────
    try:
        while True:
            raw_msg = await websocket.receive_text()

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
                if stream_task and not stream_task.done():
                    stop_event.set()
                    await stream_task
                    logger.info(f"WS [{client}]  previous stream cancelled by start_stream")

                raw_interval = msg.get("interval", DEFAULT_STREAM_INTERVAL)
                try:
                    interval = max(1.0, float(raw_interval))
                except (TypeError, ValueError):
                    interval = DEFAULT_STREAM_INTERVAL

                # Command-level label override takes precedence over connect-time filter
                cmd_label = msg.get("label") or label_filter
                active_label_filter = cmd_label

                stop_event = asyncio.Event()
                stream_task = asyncio.create_task(
                    _stream_loop(
                        websocket, frames, interval,
                        stop_event, active_label_filter, client,
                    )
                )

                await websocket.send_json({
                    "type":         "ack",
                    "command":      "start_stream",
                    "message":      (
                        f"Streaming started at {interval}s intervals."
                        + (f" Label filter: {active_label_filter}" if active_label_filter else "")
                    ),
                    "interval":     interval,
                    "label_filter": active_label_filter,
                })
                logger.info(
                    f"WS [{client}]  start_stream | interval={interval}s | "
                    f"label={active_label_filter}"
                )

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
                logger.info(f"WS [{client}]  get_next_frame | cursor={cursor}/{total}")
                # Respect the active label filter for on-demand frames too
                cmd_label = msg.get("label") or active_label_filter
                await _send_frame(websocket, frames, label_filter=cmd_label)

            # ════════════════════════════════════════════════════════════════
            # Command: reset_cursor
            # ════════════════════════════════════════════════════════════════
            elif cmd == "reset_cursor":
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
                        f'Unknown command type: "{cmd}". '
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
        if stream_task and not stream_task.done():
            stop_event.set()
            stream_task.cancel()
            try:
                await stream_task
            except asyncio.CancelledError:
                pass
        logger.info(f"WS [{client}]  connection closed")