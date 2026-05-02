"""
Satellite Command Service
─────────────────────────
FastAPI service that receives satellite commands, executes logic,
and integrates with the Simulator Service at http://localhost:8000.

TABLE 10 — Full Command List
─────────────────────────────────────────────────────────────────
Index  CMD_ID  Name    Description
  1    0x01    HI      Broadcast at subsystem startup
  2    0x02    ACK     Acknowledge reply            (outbound only)
  3    0x03    NACK    No-acknowledge reply         (outbound only)
  4    0x04    PING    Check subsystem status
  5    0x05    STIME   Set satellite time
  6    0x06    SMOD    Set subsystem mode
  7    0x07    GOTLM   Get online telemetry frame
  8    0x08    GSTLM   Get stored telemetry (5 frames)
  9    0x09    SON     Switch ON subsystem
 10    0x0A    SOFF    Switch OFF subsystem
 11    0x0B    —       Reserved
 12    0x0C    CIMG    Capture image
 13    0x0D    DIMG    Delete image
 14    0x0E    GIMG    Get image
 15    0x0F    —       Reserved
─────────────────────────────────────────────────────────────────

Run with:
    python command.py
    -- or --
    uvicorn command:app --reload --port 8002

POST /command
    Body: { "cmd_id": "0x07", "parameters": {} }
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Any
import httpx
import logging
import uuid
from datetime import datetime, timezone

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ─── App ──────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Satellite Command Service",
    description="Full TABLE 10 command implementation. Integrates with the Simulator at http://localhost:8000.",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Simulator config ─────────────────────────────────────────────────────────
SIMULATOR_BASE_URL = "http://localhost:8000"
SIMULATOR_TIMEOUT  = 10       # seconds
GSTLM_FRAME_COUNT  = 5        # how many frames GSTLM fetches

# ══════════════════════════════════════════════════════════════════════════════
# In-memory satellite state
# ══════════════════════════════════════════════════════════════════════════════
current_mode:     int | None   = None   # set by SMOD  (0x06)
satellite_time:   float | None = None   # set by STIME (0x05)
subsystem_status: str          = "OFF"  # toggled by SON/SOFF (0x09 / 0x0A)

# Image store: { image_id: { image_id, captured_at, resolution, mode, size_kb } }
image_store: dict[str, dict] = {}

# ─── Command registry ─────────────────────────────────────────────────────────
SUPPORTED_COMMANDS: dict[str, str] = {
    "0x01": "HI",
    "0x04": "PING",
    "0x05": "STIME",
    "0x06": "SMOD",
    "0x07": "GOTLM",
    "0x08": "GSTLM",
    "0x09": "SON",
    "0x0a": "SOFF",
    "0x0c": "CIMG",
    "0x0d": "DIMG",
    "0x0e": "GIMG",
}

RESERVED_COMMANDS: set[str] = {"0x0b", "0x0f"}

# 0x02 / 0x03 are outbound-only response codes, not dispatchable commands
OUTBOUND_ONLY: set[str] = {"0x02", "0x03"}


# ══════════════════════════════════════════════════════════════════════════════
# Pydantic models
# ══════════════════════════════════════════════════════════════════════════════

class CommandRequest(BaseModel):
    cmd_id:     str
    parameters: dict[str, Any] = {}


class CommandResponse(BaseModel):
    status:        str             # "success" | "error"
    cmd_id:        str
    response_type: str             # "ACK" | "NACK" | "DATA"
    data:          dict[str, Any]
    message:       str


# ══════════════════════════════════════════════════════════════════════════════
# Response builders
# ══════════════════════════════════════════════════════════════════════════════

def _ack(
    cmd_id:  str,
    data:    dict | None = None,
    message: str = "Command executed successfully.",
) -> CommandResponse:
    return CommandResponse(
        status="success",
        cmd_id=cmd_id,
        response_type="ACK",
        data=data or {},
        message=message,
    )


def _nack(cmd_id: str, reason: str) -> CommandResponse:
    return CommandResponse(
        status="error",
        cmd_id=cmd_id,
        response_type="NACK",
        data={},
        message=reason,
    )


def _data(
    cmd_id:  str,
    payload: dict,
    message: str = "Data retrieved successfully.",
) -> CommandResponse:
    return CommandResponse(
        status="success",
        cmd_id=cmd_id,
        response_type="DATA",
        data=payload,
        message=message,
    )


# ══════════════════════════════════════════════════════════════════════════════
# Simulator helpers
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_next_frame() -> dict:
    """GET /telemetry/frames/next from the simulator. Raises on failure."""
    with httpx.Client(timeout=SIMULATOR_TIMEOUT) as client:
        resp = client.get(f"{SIMULATOR_BASE_URL}/telemetry/frames/next")
        resp.raise_for_status()
        return resp.json()


def _reset_simulator_cursor() -> None:
    """POST /telemetry/frames/reset on the simulator."""
    with httpx.Client(timeout=SIMULATOR_TIMEOUT) as client:
        resp = client.post(f"{SIMULATOR_BASE_URL}/telemetry/frames/reset")
        resp.raise_for_status()


# ══════════════════════════════════════════════════════════════════════════════
# Command handlers  (one function per CMD_ID)
# ══════════════════════════════════════════════════════════════════════════════

# ── 0x01  HI ──────────────────────────────────────────────────────────────────
def _handle_hi(cmd_id: str, _params: dict) -> CommandResponse:
    """
    CMD 0x01 - HI
    Broadcast greeting issued at subsystem startup.
    SRC: ALL  DEST: Broadcast
    """
    now = datetime.now(timezone.utc).isoformat()
    logger.info(f"HI broadcast received at {now}")
    return _ack(
        cmd_id,
        data={"broadcast": True, "received_at": now},
        message="HI broadcast acknowledged. Subsystem is online.",
    )


# ── 0x04  PING ────────────────────────────────────────────────────────────────
def _handle_ping(cmd_id: str, _params: dict) -> CommandResponse:
    """
    CMD 0x04 - PING
    Check subsystem status. Returns a full state snapshot.
    SRC: OBC/GCS  DEST: ALL
    """
    try:
        return _ack(
            cmd_id,
            data={
                "server_utc":       datetime.now(timezone.utc).isoformat(),
                "subsystem_status": subsystem_status,
                "current_mode":     current_mode,
                "satellite_time":   satellite_time,
                "images_stored":    len(image_store),
            },
            message="PING acknowledged. Subsystem is alive.",
        )
    except Exception as exc:
        logger.error(f"PING internal error: {exc}")
        return _nack(cmd_id, f"Internal error during PING: {exc}")


# ── 0x05  STIME ───────────────────────────────────────────────────────────────
def _handle_stime(cmd_id: str, params: dict) -> CommandResponse:
    """
    CMD 0x05 - STIME (Set Satellite Time)
    Input:  { "timestamp": <unix epoch float> }
    Effect: stores satellite_time in memory.
    SRC: OBC/GCS  DEST: ALL
    """
    global satellite_time

    if "timestamp" not in params:
        return _nack(cmd_id, "Missing required parameter: 'timestamp'.")

    try:
        satellite_time = float(params["timestamp"])
    except (TypeError, ValueError) as exc:
        return _nack(cmd_id, f"Invalid 'timestamp' value: {exc}")

    readable = datetime.fromtimestamp(satellite_time, tz=timezone.utc).isoformat()
    logger.info(f"STIME -> satellite_time = {satellite_time} ({readable})")

    return _ack(
        cmd_id,
        data={"satellite_time": satellite_time, "readable_utc": readable},
        message=f"Satellite time updated to {readable}.",
    )


# ── 0x06  SMOD ────────────────────────────────────────────────────────────────
def _handle_smod(cmd_id: str, params: dict) -> CommandResponse:
    """
    CMD 0x06 - SMOD (Set Mode of Operation)
    Input:  { "mode": <int> }
    Effect: stores current_mode in memory.
    SRC: OBC/GCS  DEST: ALL
    """
    global current_mode

    if "mode" not in params:
        return _nack(cmd_id, "Missing required parameter: 'mode'.")

    try:
        current_mode = int(params["mode"])
    except (TypeError, ValueError) as exc:
        return _nack(cmd_id, f"Invalid 'mode' value: {exc}")

    logger.info(f"SMOD -> current_mode = {current_mode}")

    return _ack(
        cmd_id,
        data={"current_mode": current_mode},
        message=f"Satellite mode updated to {current_mode}.",
    )


# ── 0x07  GOTLM ───────────────────────────────────────────────────────────────
def _handle_gotlm(cmd_id: str, _params: dict) -> CommandResponse:
    """
    CMD 0x07 - GOTLM (Get Online Telemetry)
    Fetches one live telemetry frame from the simulator.
    SRC: OBC/GCS  DEST: ALL
    """
    try:
        result = _fetch_next_frame()
    except httpx.ConnectError:
        return _nack(cmd_id, "Simulator unreachable at http://localhost:8000.")
    except httpx.HTTPStatusError as exc:
        return _nack(cmd_id, f"Simulator returned HTTP {exc.response.status_code}.")
    except Exception as exc:
        return _nack(cmd_id, f"Unexpected simulator error: {exc}")

    if result.get("exhausted"):
        return _nack(
            cmd_id,
            "Simulator dataset exhausted. Reset cursor via "
            "POST http://localhost:8000/telemetry/frames/reset.",
        )

    frame = result.get("frame", {})
    return _data(
        cmd_id,
        payload={
            "raw_frame": frame.get("hex_frame", ""),
            "source":    "simulator",
            "cursor":    result.get("cursor"),
            "remaining": result.get("remaining"),
            "frame":     frame,
        },
        message="Online telemetry frame retrieved from simulator.",
    )


# ── 0x08  GSTLM ───────────────────────────────────────────────────────────────
def _handle_gstlm(cmd_id: str, _params: dict) -> CommandResponse:
    """
    CMD 0x08 - GSTLM (Get Stored Telemetry)
    Fetches GSTLM_FRAME_COUNT frames sequentially from the simulator.
    Partial results are returned with a warning if the dataset runs out.
    SRC: GCS  DEST: OBC
    """
    frames       = []
    exhausted_at = None

    for i in range(GSTLM_FRAME_COUNT):
        try:
            result = _fetch_next_frame()
        except httpx.ConnectError:
            if not frames:
                return _nack(cmd_id, "Simulator unreachable at http://localhost:8000.")
            exhausted_at = i
            break
        except httpx.HTTPStatusError as exc:
            if not frames:
                return _nack(cmd_id, f"Simulator returned HTTP {exc.response.status_code}.")
            exhausted_at = i
            break
        except Exception as exc:
            if not frames:
                return _nack(cmd_id, f"Unexpected simulator error: {exc}")
            exhausted_at = i
            break

        if result.get("exhausted"):
            exhausted_at = i
            break

        frame = result.get("frame", {})
        frames.append({
            "index":     i + 1,
            "cursor":    result.get("cursor"),
            "raw_frame": frame.get("hex_frame", ""),
            "frame":     frame,
        })

    if not frames:
        return _nack(cmd_id, "No stored frames available. Simulator dataset may be exhausted.")

    message = f"Retrieved {len(frames)} of {GSTLM_FRAME_COUNT} requested stored frames."
    if exhausted_at is not None:
        message += f" Dataset exhausted after frame {exhausted_at}."

    logger.info(f"GSTLM -> fetched {len(frames)} frames")

    return _data(
        cmd_id,
        payload={
            "frames":              frames,
            "frames_returned":     len(frames),
            "frames_requested":    GSTLM_FRAME_COUNT,
            "source":              "simulator",
            "exhausted":           exhausted_at is not None,
        },
        message=message,
    )


# ── 0x09  SON ─────────────────────────────────────────────────────────────────
def _handle_son(cmd_id: str, _params: dict) -> CommandResponse:
    """
    CMD 0x09 - SON (Switch ON Subsystem)
    Sets subsystem_status = "ON".
    SRC: OBC/GCS  DEST: EPS
    """
    global subsystem_status
    subsystem_status = "ON"
    logger.info("SON -> subsystem_status = ON")
    return _ack(
        cmd_id,
        data={"subsystem_status": subsystem_status},
        message="Subsystem switched ON.",
    )


# ── 0x0A  SOFF ────────────────────────────────────────────────────────────────
def _handle_soff(cmd_id: str, _params: dict) -> CommandResponse:
    """
    CMD 0x0A - SOFF (Switch OFF Subsystem)
    Sets subsystem_status = "OFF".
    SRC: OBC/GCS  DEST: EPS
    """
    global subsystem_status
    subsystem_status = "OFF"
    logger.info("SOFF -> subsystem_status = OFF")
    return _ack(
        cmd_id,
        data={"subsystem_status": subsystem_status},
        message="Subsystem switched OFF.",
    )


# ── 0x0C  CIMG ────────────────────────────────────────────────────────────────
def _handle_cimg(cmd_id: str, params: dict) -> CommandResponse:
    """
    CMD 0x0C - CIMG (Capture Image)
    Simulates triggering the payload camera and storing image metadata.
    Optional input: { "resolution": "HD"|"SD"|"4K", "mode": "nadir"|"limb"|"star" }
    Requires subsystem to be ON.
    SRC: OBC/GCS  DEST: PL (Payload)
    """
    if subsystem_status != "ON":
        return _nack(
            cmd_id,
            "Subsystem is OFF. Send SON (0x09) before capturing an image.",
        )

    resolution = params.get("resolution", "HD")
    mode       = params.get("mode", "nadir")

    if resolution not in ("HD", "SD", "4K"):
        return _nack(cmd_id, f"Invalid resolution '{resolution}'. Supported: HD, SD, 4K.")
    if mode not in ("nadir", "limb", "star"):
        return _nack(cmd_id, f"Invalid mode '{mode}'. Supported: nadir, limb, star.")

    image_id    = str(uuid.uuid4())
    captured_at = datetime.now(timezone.utc).isoformat()
    size_kb     = {"HD": 1024, "SD": 512, "4K": 4096}[resolution]

    image_store[image_id] = {
        "image_id":    image_id,
        "captured_at": captured_at,
        "resolution":  resolution,
        "mode":        mode,
        "size_kb":     size_kb,
    }

    logger.info(f"CIMG -> captured image_id={image_id} ({resolution}, {mode})")

    return _ack(
        cmd_id,
        data={
            "image_id":      image_id,
            "captured_at":   captured_at,
            "resolution":    resolution,
            "mode":          mode,
            "size_kb":       size_kb,
            "images_stored": len(image_store),
        },
        message=f"Image captured successfully. ID: {image_id}",
    )


# ── 0x0D  DIMG ────────────────────────────────────────────────────────────────
def _handle_dimg(cmd_id: str, params: dict) -> CommandResponse:
    """
    CMD 0x0D - DIMG (Delete Image)
    Removes an image from the store by image_id, or wipes all images.
    Input:  { "image_id": "<uuid>" }
              -- or --
            { "all": true }
    SRC: OBC/GCS  DEST: PL (Payload)
    """
    # Wipe all images
    if params.get("all") is True:
        count = len(image_store)
        image_store.clear()
        logger.info(f"DIMG -> deleted all {count} images")
        return _ack(
            cmd_id,
            data={"deleted_count": count, "images_remaining": 0},
            message=f"All {count} image(s) deleted.",
        )

    # Delete single image
    if "image_id" not in params:
        return _nack(
            cmd_id,
            "Missing required parameter: 'image_id'. "
            "Provide an image_id, or pass { \"all\": true } to delete everything.",
        )

    image_id = params["image_id"]

    if image_id not in image_store:
        return _nack(
            cmd_id,
            f"Image '{image_id}' not found. "
            f"Stored IDs: {list(image_store.keys()) or 'none'}.",
        )

    del image_store[image_id]
    logger.info(f"DIMG -> deleted image_id={image_id}")

    return _ack(
        cmd_id,
        data={
            "deleted_image_id": image_id,
            "images_remaining": len(image_store),
        },
        message=f"Image '{image_id}' deleted.",
    )


# ── 0x0E  GIMG ────────────────────────────────────────────────────────────────
def _handle_gimg(cmd_id: str, params: dict) -> CommandResponse:
    """
    CMD 0x0E - GIMG (Get Image)
    Retrieves image metadata from the store.
    Input:  { "image_id": "<uuid>" }   -> specific image
              -- or --
            {}                          -> list all images
    SRC: OBC/GCS  DEST: PL (Payload)
    """
    # List all
    if "image_id" not in params:
        images = list(image_store.values())
        logger.info(f"GIMG -> listing all {len(images)} images")
        return _data(
            cmd_id,
            payload={"images": images, "total_images": len(images)},
            message=f"{len(images)} image(s) in store.",
        )

    # Single image
    image_id = params["image_id"]

    if image_id not in image_store:
        return _nack(
            cmd_id,
            f"Image '{image_id}' not found. "
            f"Stored IDs: {list(image_store.keys()) or 'none'}.",
        )

    logger.info(f"GIMG -> retrieved image_id={image_id}")
    return _data(
        cmd_id,
        payload={"image": image_store[image_id]},
        message=f"Image '{image_id}' retrieved.",
    )


# ══════════════════════════════════════════════════════════════════════════════
# Dispatch table  (normalized lowercase hex -> handler)
# ══════════════════════════════════════════════════════════════════════════════
_COMMAND_HANDLERS: dict[str, Any] = {
    "0x01": _handle_hi,
    "0x04": _handle_ping,
    "0x05": _handle_stime,
    "0x06": _handle_smod,
    "0x07": _handle_gotlm,
    "0x08": _handle_gstlm,
    "0x09": _handle_son,
    "0x0a": _handle_soff,
    "0x0c": _handle_cimg,
    "0x0d": _handle_dimg,
    "0x0e": _handle_gimg,
}


# ══════════════════════════════════════════════════════════════════════════════
# Core dispatcher
# ══════════════════════════════════════════════════════════════════════════════

def handle_command(cmd_id: str, parameters: dict) -> CommandResponse:
    """
    Normalises cmd_id, validates it, then delegates to the correct handler.
    """
    normalized = cmd_id.strip().lower()

    # Reserved slots
    if normalized in RESERVED_COMMANDS:
        return _nack(cmd_id, f"Command '{cmd_id}' is reserved and not implemented.")

    # Outbound-only codes cannot be dispatched
    if normalized in OUTBOUND_ONLY:
        return _nack(
            cmd_id,
            f"'{cmd_id}' is an outbound-only response code, not a dispatchable command.",
        )

    handler = _COMMAND_HANDLERS.get(normalized)
    if handler is None:
        supported = ", ".join(SUPPORTED_COMMANDS.keys())
        return _nack(cmd_id, f"Unknown command: '{cmd_id}'. Supported: {supported}.")

    name = SUPPORTED_COMMANDS.get(normalized, normalized.upper())
    logger.info(f"Dispatching {cmd_id} ({name}) | params={parameters}")
    return handler(cmd_id, parameters)


# ══════════════════════════════════════════════════════════════════════════════
# REST Routes
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/", summary="Service health check")
def root():
    return {
        "status":            "online",
        "service":           "Satellite Command Service v2.0",
        "port":              8002,
        "simulator":         SIMULATOR_BASE_URL,
        "state": {
            "current_mode":     current_mode,
            "satellite_time":   satellite_time,
            "subsystem_status": subsystem_status,
            "images_stored":    len(image_store),
        },
        "supported_commands": SUPPORTED_COMMANDS,
        "reserved_commands":  sorted(RESERVED_COMMANDS),
        "docs":               "/docs",
    }


@app.post(
    "/command",
    response_model=CommandResponse,
    summary="Execute a satellite command",
    description=(
        "Send a TABLE 10 command to the satellite command service. "
        "Returns a structured ACK, NACK, or DATA response."
    ),
)
def post_command(request: CommandRequest) -> CommandResponse:
    logger.info(f"POST /command  cmd_id={request.cmd_id}  params={request.parameters}")
    response = handle_command(request.cmd_id, request.parameters)
    logger.info(f"Response -> type={response.response_type}  status={response.status}")
    return response


@app.get("/state", summary="Inspect current in-memory satellite state")
def get_state():
    """Returns all in-memory state variables for diagnostics."""
    return {
        "current_mode":     current_mode,
        "satellite_time":   satellite_time,
        "subsystem_status": subsystem_status,
        "images_stored":    len(image_store),
        "image_ids":        list(image_store.keys()),
    }


# ─── Entrypoint ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("command:app", host="0.0.0.0", port=8002, reload=True)