"""
AO-73 / FUNcube-1 Telemetry Decoder Service
============================================
Source: FUNcube-1 downlink data spec ver1-as_flown Issue 1.0 (AMSAT-UK)

Frame layout
  Byte 0        : Header  → Sat ID (bits 7-6)  +  Frame Type (bits 5-0)
  Bytes 1–55    : RTT payload  (55 bytes)
  Minimum total : 56 bytes  → 112 hex characters

RTT payload byte map
  Offset  0–23  : EPS board     (16 channels: 8× u16 big-endian + 8× u8)
  Offset 24–36  : ASIB board    (10 channels × 10-bit packed into 13 bytes)
  Offset 37–42  : RF board      (6× u8)
  Offset 43–46  : PA board      (4× u8)
  Offset 47–49  : ANTS board    (2× u8 temp + 1× u8 flags)
  Offset 50–55  : SW status     (3-byte seq num + 3× u8 flag bytes)

All values are returned as raw integers — no engineering conversion —
matching the official AMSAT-UK Data Warehouse output.
"""

import struct
import requests
import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, field_validator

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ─── App ──────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="AO-73 Telemetry Decoder Service",
    description=(
        "Decodes raw AO-73 / FUNcube-1 hex telemetry frames into structured JSON. "
        "Board layout and byte offsets follow the official AMSAT-UK spec (ver1-as_flown Issue 1.0). "
        "Raw values match the AMSAT-UK Data Warehouse exactly."
    ),
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

SIMULATOR_API = "http://localhost:8000"

# ─── Sat / Frame lookup tables (from spec) ────────────────────────────────────
SAT_NAMES = {
    0b00: "FUNcube-1 EM",
    0b01: "FUNcube-2 UKube",
    0b10: "FUNcube-1 FM",
    0b11: "Extended",
}

FRAME_KINDS = {
    **{i: "WO" for i in range(1, 13)},   # Whole Orbit
    13: "HR", 17: "HR", 21: "HR",        # High-Resolution
    **{i: "FM" for i in list(range(14, 17)) + list(range(18, 21)) + list(range(22, 25))},
}

# Minimum: 1 header byte + 55 RTT bytes = 56 bytes = 112 hex chars
MIN_BYTES    = 56
MIN_HEX_LEN  = MIN_BYTES * 2


# ══════════════════════════════════════════════════════════════════════════════
# Core Decoder  —  mirrors decode_ao73() from the official AMSAT-UK notebook
# ══════════════════════════════════════════════════════════════════════════════
def decode_frame(hex_frame: str) -> dict:
    """
    Decode a raw hex frame into structured subsystem boards.

    Raises ValueError with a clear message on any validation failure.
    All values are raw integers as per the official spec — no engineering scaling.
    """
    hex_frame = hex_frame.strip()

    # ── Validation ────────────────────────────────────────────────────────────
    if not hex_frame:
        raise ValueError("Empty hex frame.")

    if len(hex_frame) < MIN_HEX_LEN:
        raise ValueError(
            f"Frame too short: {len(hex_frame)} hex chars "
            f"({len(hex_frame) // 2} bytes). "
            f"Minimum is {MIN_HEX_LEN} hex chars ({MIN_BYTES} bytes)."
        )

    try:
        raw = bytes.fromhex(hex_frame)
    except ValueError:
        raise ValueError("Frame contains non-hexadecimal characters.")

    # ── Header byte ───────────────────────────────────────────────────────────
    header     = raw[0]
    sat_id     = (header >> 6) & 0x03
    frame_type = header & 0x3F

    # ── RTT payload: bytes 1–55 ───────────────────────────────────────────────
    d = raw[1:56]
    if len(d) < 55:
        raise ValueError(
            f"RTT payload too short: got {len(d)} bytes after header, need 55."
        )

    def u16(off: int) -> int:
        """Read a big-endian unsigned 16-bit integer from offset into d."""
        return struct.unpack_from(">H", d, off)[0]

    def u8(off: int) -> int:
        """Read one unsigned byte from offset into d."""
        return d[off]

    # ── EPS Board  (offset 0–23, 24 bytes) ───────────────────────────────────
    #   8 channels as u16 big-endian  +  8 channels as u8
    eps = {
        "EPS_Solar_Panel_Voltage_X":  u16(0),
        "EPS_Solar_Panel_Voltage_Y":  u16(2),
        "EPS_Solar_Panel_Voltage_Z":  u16(4),
        "EPS_Total_Photo_Current":    u16(6),
        "EPS_Battery_Voltage":        u16(8),
        "EPS_Total_System_Current":   u16(10),
        "EPS_Reboot_Count":           u16(12),
        "EPS_Software_Errors":        u16(14),
        "EPS_Boost_Conv_Temp_X":      u8(16),
        "EPS_Boost_Conv_Temp_Y":      u8(17),
        "EPS_Boost_Conv_Temp_Z":      u8(18),
        "EPS_Battery_Temp":           u8(19),
        "EPS_Latch_Up_Count_5V":      u8(20),
        "EPS_Latch_Up_Count_3V3":     u8(21),
        "EPS_Reset_Cause":            u8(22),
        "EPS_Power_Point_Track_Mode": u8(23),
    }

    # ── ASIB Board  (offset 24–36, 13 bytes, 10 channels × 10-bit packed) ────
    #   Pack all 13 bytes into a big integer, then extract each 10-bit window.
    asib_bytes = d[24:37]
    bits       = int.from_bytes(asib_bytes, "big")
    total_bits = len(asib_bytes) * 8  # 104 bits

    def get10(n: int) -> int:
        """Extract the nth 10-bit value (0-indexed from the MSB side)."""
        shift = total_bits - 10 * (n + 1)
        return (bits >> shift) & 0x3FF

    asib = {
        "ASIB_Sun_Sensor_X_Plus":        get10(0),
        "ASIB_Sun_Sensor_Y_Plus":        get10(1),
        "ASIB_Sun_Sensor_Z_Plus":        get10(2),
        "ASIB_Solar_Panel_Temp_X_Plus":  get10(3),
        "ASIB_Solar_Panel_Temp_X_Minus": get10(4),
        "ASIB_Solar_Panel_Temp_Y_Plus":  get10(5),
        "ASIB_Solar_Panel_Temp_Y_Minus": get10(6),
        "ASIB_Bus_Voltage_3V3":          get10(7),
        "ASIB_Bus_Current_3V3":          get10(8),
        "ASIB_Bus_Voltage_5V":           get10(9),
    }

    # ── RF Board  (offset 37–42, 6 bytes) ────────────────────────────────────
    rf = {
        "RF_Receiver_Doppler":  u8(37),
        "RF_Receiver_RSSI":     u8(38),
        "RF_Temperature":       u8(39),
        "RF_Receive_Current":   u8(40),
        "RF_TX_Current_3V3":    u8(41),
        "RF_TX_Current_5V":     u8(42),
    }

    # ── PA Board  (offset 43–46, 4 bytes) ────────────────────────────────────
    pa = {
        "PA_Reverse_Power":  u8(43),
        "PA_Forward_Power":  u8(44),
        "PA_Board_Temp":     u8(45),
        "PA_Board_Current":  u8(46),
    }

    # ── ANTS Board  (offset 47–49, 3 bytes) ──────────────────────────────────
    #   2× u8 temperatures + 1× u8 antenna deployment flags (bits 3–0)
    ant_flags = u8(49)
    ants = {
        "ANTS_Temp_0":             u8(47),
        "ANTS_Temp_1":             u8(48),
        "ANTS_Antenna_0_Deployed": bool(ant_flags & 0x08),
        "ANTS_Antenna_1_Deployed": bool(ant_flags & 0x04),
        "ANTS_Antenna_2_Deployed": bool(ant_flags & 0x02),
        "ANTS_Antenna_3_Deployed": bool(ant_flags & 0x01),
    }

    # ── SW Status  (offset 50–55, 6 bytes) ───────────────────────────────────
    #   3-byte big-endian sequence number + 3 flag bytes
    seq_num  = (u8(50) << 16) | (u8(51) << 8) | u8(52)
    sw_byte1 = u8(53)
    sw_byte2 = u8(54)
    sw_byte3 = d[55] if len(d) > 55 else 0

    sw = {
        "SW_Sequence_Number":     seq_num,
        "SW_DTMF_Command_Count":  (sw_byte1 >> 2) & 0x3F,
        "SW_DTMF_Last_Command":   sw_byte1 & 0x1F,
        "SW_DTMF_Success":        bool(sw_byte2 & 0x80),
        "SW_Data_Valid_ASIB":     bool(sw_byte2 & 0x40),
        "SW_Data_Valid_EPS":      bool(sw_byte2 & 0x20),
        "SW_Data_Valid_PA":       bool(sw_byte2 & 0x10),
        "SW_Data_Valid_RF":       bool(sw_byte2 & 0x08),
        "SW_Data_Valid_MSE":      bool(sw_byte2 & 0x04),
        "SW_Data_Valid_ANTS_B":   bool(sw_byte2 & 0x02),
        "SW_Data_Valid_ANTS_A":   bool(sw_byte2 & 0x01),
        "SW_In_Eclipse":          bool(sw_byte3 & 0x80),
        "SW_In_Safe_Mode":        bool(sw_byte3 & 0x40),
        "SW_Hardware_ABF":        bool(sw_byte3 & 0x20),
        "SW_Software_ABF":        bool(sw_byte3 & 0x10),
        "SW_Deployment_Wait":     bool(sw_byte3 & 0x08),
    }

    # ── Assemble result ───────────────────────────────────────────────────────
    return {
        "meta": {
            "decoded_at_utc":   datetime.now(timezone.utc).isoformat(),
            "frame_hex_length": len(hex_frame),
            "frame_bytes":      len(raw),
            "sat_id":           sat_id,
            "sat_name":         SAT_NAMES.get(sat_id, "Unknown"),
            "frame_type":       frame_type,
            "frame_kind":       FRAME_KINDS.get(frame_type, "Unknown"),
        },
        "EPS":  eps,
        "ASIB": asib,
        "RF":   rf,
        "PA":   pa,
        "ANTS": ants,
        "SW":   sw,
    }


# ══════════════════════════════════════════════════════════════════════════════
# Request Models
# ══════════════════════════════════════════════════════════════════════════════
class DecodeRequest(BaseModel):
    hex_frame:   str
    captured_at: Optional[str] = None   # ISO timestamp from the ground station record
    station:     Optional[str] = None   # Ground station name (pass-through context)

    @field_validator("hex_frame")
    @classmethod
    def hex_frame_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("hex_frame must not be blank.")
        return v.strip()


class BatchDecodeRequest(BaseModel):
    frames: list[DecodeRequest]

    @field_validator("frames")
    @classmethod
    def frames_not_empty(cls, v):
        if not v:
            raise ValueError("frames list must not be empty.")
        if len(v) > 500:
            raise ValueError("Maximum 500 frames per batch request.")
        return v


# ══════════════════════════════════════════════════════════════════════════════
# Routes  (unchanged from v1 — only decode_frame() internals changed)
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/", summary="Decoder service health check")
def root():
    return {
        "status":  "online",
        "service": "AO-73 Telemetry Decoder",
        "spec":    "FUNcube-1 downlink data spec ver1-as_flown Issue 1.0 (AMSAT-UK)",
        "frame_structure": {
            "byte_0":           "Header (Sat ID bits 7-6, Frame Type bits 5-0)",
            "bytes_1_to_55":    "RTT payload (55 bytes)",
            "minimum_hex_len":  MIN_HEX_LEN,
            "boards":           ["EPS", "ASIB", "RF", "PA", "ANTS", "SW"],
        },
        "endpoints": {
            "POST /decode":       "Decode a single hex frame",
            "POST /decode/batch": "Decode up to 500 frames in one request",
            "GET  /decode/next":  "Pull the next frame from the simulator and decode it",
            "GET  /boards":       "List all boards, fields, and byte offsets",
        },
    }


@app.post("/decode", summary="Decode a single raw hex frame")
def decode_single(req: DecodeRequest):
    """
    Accepts one raw AO-73 hex frame and returns fully decoded telemetry
    grouped by subsystem board: EPS / ASIB / RF / PA / ANTS / SW.

    Values are raw integers matching the AMSAT-UK Data Warehouse exactly.
    """
    logger.info(
        f"POST /decode  len={len(req.hex_frame)}"
        f"  station={req.station}  captured_at={req.captured_at}"
    )
    try:
        decoded = decode_frame(req.hex_frame)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    return {
        "status":      "ok",
        "captured_at": req.captured_at,
        "station":     req.station,
        "raw_hex":     req.hex_frame,
        **decoded,
    }


@app.post("/decode/batch", summary="Decode up to 500 frames in one request")
def decode_batch(req: BatchDecodeRequest):
    """
    Accepts a batch of raw hex frames and decodes each independently.
    Frames that fail are marked status: error — they do not abort the batch.
    Designed for continuous-feed use where the simulator sends frames in bursts.
    """
    logger.info(f"POST /decode/batch  count={len(req.frames)}")
    results = []

    for item in req.frames:
        try:
            decoded = decode_frame(item.hex_frame)
            results.append({
                "status":      "ok",
                "captured_at": item.captured_at,
                "station":     item.station,
                "raw_hex":     item.hex_frame,
                **decoded,
            })
        except ValueError as exc:
            results.append({
                "status":      "error",
                "captured_at": item.captured_at,
                "station":     item.station,
                "raw_hex":     item.hex_frame,
                "error":       str(exc),
            })

    ok_count  = sum(1 for r in results if r["status"] == "ok")
    err_count = len(results) - ok_count
    logger.info(f"  batch complete: {ok_count} ok, {err_count} errors")

    return {
        "total":   len(results),
        "ok":      ok_count,
        "errors":  err_count,
        "results": results,
    }


@app.get("/decode/next", summary="Pull the next frame from the simulator and decode it")
def decode_next(
    simulator_url: str = Query(
        default=SIMULATOR_API,
        description="Base URL of the simulator API (e.g. http://localhost:8000)",
    ),
):
    """
    Closes the simulator → decoder loop in a single call:
    1. Calls GET {simulator_url}/telemetry/frames/next (advances the cursor).
    2. Immediately decodes the returned hex frame.
    3. Returns the combined result.

    Returns status: exhausted when the simulator dataset is fully consumed.
    """
    next_url = f"{simulator_url.rstrip('/')}/telemetry/frames/next"
    logger.info(f"GET /decode/next  -> {next_url}")

    try:
        resp = requests.get(next_url, timeout=10)
        resp.raise_for_status()
    except requests.RequestException as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Could not reach simulator at {next_url}: {exc}",
        )

    sim_data = resp.json()

    if sim_data.get("exhausted"):
        return {
            "status":       "exhausted",
            "message":      sim_data.get("message"),
            "cursor":       sim_data.get("cursor"),
            "total_frames": sim_data.get("total_frames"),
        }

    frame       = sim_data.get("frame", {})
    hex_frame   = frame.get("hex_frame", "")
    captured_at = frame.get("captured_at")
    station     = frame.get("station", {}).get("name")

    if not hex_frame:
        raise HTTPException(
            status_code=422,
            detail="Simulator returned a frame with no hex_frame field.",
        )

    try:
        decoded = decode_frame(hex_frame)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"Decode failed: {exc}")

    return {
        "status":           "ok",
        "simulator_cursor": sim_data.get("cursor"),
        "remaining":        sim_data.get("remaining"),
        "total_frames":     sim_data.get("total_frames"),
        "captured_at":      captured_at,
        "station":          station,
        "raw_hex":          hex_frame,
        **decoded,
    }


@app.get("/boards", summary="List all boards, fields, and byte offsets")
def list_boards():
    """
    Documents the complete AO-73 RTT payload structure:
    every board, every field, its byte offset inside the RTT payload,
    its data type, and its unit.
    """
    return {
        "spec":   "FUNcube-1 downlink data spec ver1-as_flown Issue 1.0",
        "header": {
            "byte": 0,
            "fields": {
                "sat_id":     "bits 7-6  →  satellite identity (0-3)",
                "frame_type": "bits 5-0  →  frame type / sequence code",
            },
        },
        "rtt_payload_start_byte": 1,
        "boards": {
            "EPS": {
                "offset_in_rtt": "0–23  (24 bytes)",
                "fields": {
                    "EPS_Solar_Panel_Voltage_X":  {"offset": 0,  "type": "u16 big-endian", "unit": "raw ADC"},
                    "EPS_Solar_Panel_Voltage_Y":  {"offset": 2,  "type": "u16 big-endian", "unit": "raw ADC"},
                    "EPS_Solar_Panel_Voltage_Z":  {"offset": 4,  "type": "u16 big-endian", "unit": "raw ADC"},
                    "EPS_Total_Photo_Current":    {"offset": 6,  "type": "u16 big-endian", "unit": "raw ADC"},
                    "EPS_Battery_Voltage":        {"offset": 8,  "type": "u16 big-endian", "unit": "raw ADC"},
                    "EPS_Total_System_Current":   {"offset": 10, "type": "u16 big-endian", "unit": "raw ADC"},
                    "EPS_Reboot_Count":           {"offset": 12, "type": "u16 big-endian", "unit": "count"},
                    "EPS_Software_Errors":        {"offset": 14, "type": "u16 big-endian", "unit": "count"},
                    "EPS_Boost_Conv_Temp_X":      {"offset": 16, "type": "u8",             "unit": "raw ADC"},
                    "EPS_Boost_Conv_Temp_Y":      {"offset": 17, "type": "u8",             "unit": "raw ADC"},
                    "EPS_Boost_Conv_Temp_Z":      {"offset": 18, "type": "u8",             "unit": "raw ADC"},
                    "EPS_Battery_Temp":           {"offset": 19, "type": "u8",             "unit": "raw ADC"},
                    "EPS_Latch_Up_Count_5V":      {"offset": 20, "type": "u8",             "unit": "count"},
                    "EPS_Latch_Up_Count_3V3":     {"offset": 21, "type": "u8",             "unit": "count"},
                    "EPS_Reset_Cause":            {"offset": 22, "type": "u8",             "unit": "enum"},
                    "EPS_Power_Point_Track_Mode": {"offset": 23, "type": "u8",             "unit": "enum"},
                },
            },
            "ASIB": {
                "offset_in_rtt": "24–36  (13 bytes, 10 channels × 10-bit packed)",
                "fields": {
                    "ASIB_Sun_Sensor_X_Plus":        {"bit_index": 0, "type": "10-bit", "unit": "raw ADC"},
                    "ASIB_Sun_Sensor_Y_Plus":        {"bit_index": 1, "type": "10-bit", "unit": "raw ADC"},
                    "ASIB_Sun_Sensor_Z_Plus":        {"bit_index": 2, "type": "10-bit", "unit": "raw ADC"},
                    "ASIB_Solar_Panel_Temp_X_Plus":  {"bit_index": 3, "type": "10-bit", "unit": "raw ADC"},
                    "ASIB_Solar_Panel_Temp_X_Minus": {"bit_index": 4, "type": "10-bit", "unit": "raw ADC"},
                    "ASIB_Solar_Panel_Temp_Y_Plus":  {"bit_index": 5, "type": "10-bit", "unit": "raw ADC"},
                    "ASIB_Solar_Panel_Temp_Y_Minus": {"bit_index": 6, "type": "10-bit", "unit": "raw ADC"},
                    "ASIB_Bus_Voltage_3V3":          {"bit_index": 7, "type": "10-bit", "unit": "raw ADC"},
                    "ASIB_Bus_Current_3V3":          {"bit_index": 8, "type": "10-bit", "unit": "raw ADC"},
                    "ASIB_Bus_Voltage_5V":           {"bit_index": 9, "type": "10-bit", "unit": "raw ADC"},
                },
            },
            "RF": {
                "offset_in_rtt": "37–42  (6 bytes)",
                "fields": {
                    "RF_Receiver_Doppler": {"offset": 37, "type": "u8", "unit": "raw"},
                    "RF_Receiver_RSSI":    {"offset": 38, "type": "u8", "unit": "raw"},
                    "RF_Temperature":      {"offset": 39, "type": "u8", "unit": "raw ADC"},
                    "RF_Receive_Current":  {"offset": 40, "type": "u8", "unit": "raw ADC"},
                    "RF_TX_Current_3V3":   {"offset": 41, "type": "u8", "unit": "raw ADC"},
                    "RF_TX_Current_5V":    {"offset": 42, "type": "u8", "unit": "raw ADC"},
                },
            },
            "PA": {
                "offset_in_rtt": "43–46  (4 bytes)",
                "fields": {
                    "PA_Reverse_Power": {"offset": 43, "type": "u8", "unit": "raw ADC"},
                    "PA_Forward_Power": {"offset": 44, "type": "u8", "unit": "raw ADC"},
                    "PA_Board_Temp":    {"offset": 45, "type": "u8", "unit": "raw ADC"},
                    "PA_Board_Current": {"offset": 46, "type": "u8", "unit": "raw ADC"},
                },
            },
            "ANTS": {
                "offset_in_rtt": "47–49  (3 bytes)",
                "fields": {
                    "ANTS_Temp_0":             {"offset": 47, "type": "u8",         "unit": "raw ADC"},
                    "ANTS_Temp_1":             {"offset": 48, "type": "u8",         "unit": "raw ADC"},
                    "ANTS_Antenna_0_Deployed": {"offset": 49, "type": "bit 3",      "unit": "bool"},
                    "ANTS_Antenna_1_Deployed": {"offset": 49, "type": "bit 2",      "unit": "bool"},
                    "ANTS_Antenna_2_Deployed": {"offset": 49, "type": "bit 1",      "unit": "bool"},
                    "ANTS_Antenna_3_Deployed": {"offset": 49, "type": "bit 0",      "unit": "bool"},
                },
            },
            "SW": {
                "offset_in_rtt": "50–55  (6 bytes)",
                "fields": {
                    "SW_Sequence_Number":   {"offset": "50-52", "type": "u24 big-endian", "unit": "count"},
                    "SW_DTMF_Command_Count":{"offset": 53,      "type": "bits 7-2",       "unit": "count"},
                    "SW_DTMF_Last_Command": {"offset": 53,      "type": "bits 4-0",       "unit": "enum"},
                    "SW_DTMF_Success":      {"offset": 54,      "type": "bit 7",          "unit": "bool"},
                    "SW_Data_Valid_ASIB":   {"offset": 54,      "type": "bit 6",          "unit": "bool"},
                    "SW_Data_Valid_EPS":    {"offset": 54,      "type": "bit 5",          "unit": "bool"},
                    "SW_Data_Valid_PA":     {"offset": 54,      "type": "bit 4",          "unit": "bool"},
                    "SW_Data_Valid_RF":     {"offset": 54,      "type": "bit 3",          "unit": "bool"},
                    "SW_Data_Valid_MSE":    {"offset": 54,      "type": "bit 2",          "unit": "bool"},
                    "SW_Data_Valid_ANTS_B": {"offset": 54,      "type": "bit 1",          "unit": "bool"},
                    "SW_Data_Valid_ANTS_A": {"offset": 54,      "type": "bit 0",          "unit": "bool"},
                    "SW_In_Eclipse":        {"offset": 55,      "type": "bit 7",          "unit": "bool"},
                    "SW_In_Safe_Mode":      {"offset": 55,      "type": "bit 6",          "unit": "bool"},
                    "SW_Hardware_ABF":      {"offset": 55,      "type": "bit 5",          "unit": "bool"},
                    "SW_Software_ABF":      {"offset": 55,      "type": "bit 4",          "unit": "bool"},
                    "SW_Deployment_Wait":   {"offset": 55,      "type": "bit 3",          "unit": "bool"},
                },
            },
        },
    }


# ─── Run directly ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("decoder:app", host="0.0.0.0", port=8001, reload=True)