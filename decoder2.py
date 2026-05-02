"""
EGSACUBE-ED Telemetry Decoder Service
8082
======================================
ICD Reference : EGSACUBE-ED Telemetry ICD v3.2
Data Source   : AO-73 (FUNcube-1) raw hex frames fed through the simulator

Decoding rules are taken verbatim from the ICD v3.2 notebook:
  - Real channels    : extracted directly from AO-73 byte offsets
  - Simulated channels: generated with realistic physics (orbit, sensors)
  - No changes to scaling, bit-packing, or simulation formulas

Frame layout (same physical frame as AO-73):
  Byte 0        : Header  →  Sat ID (bits 7-6)  +  Frame Type (bits 5-0)
  Bytes 1-55    : RTT payload (55 bytes)
  Minimum total : 56 bytes = 112 hex characters

Subsystem boards decoded:
  EPS   (ICD Table 11) — solar voltages/currents, battery, bus rails, temp
  OBC   (ICD Table 12) — RTC, simulated GPS position + velocity, command counts
  ADCS  (ICD Table 13) — sun sensors (real), accel/gyro/magnetometer/RW (sim)
  COMM  (ICD Table 14) — RSSI, RF temp, TX/RX currents (real), frame stats (sim)
  SW                   — eclipse flag, safe-mode flag, ABF flags, sequence number

Run:
  uvicorn decoder_egsacube:app --port 8002 --reload
"""

import struct
import requests
import logging
import numpy as np
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

# Fixed seed — keeps simulated channels reproducible across restarts
np.random.seed(42)

# ─── App ──────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="EGSACUBE-ED Telemetry Decoder Service",
    description=(
        "Decodes raw AO-73 hex frames into EGSACUBE-ED structured telemetry. "
        "Real channels are extracted from the frame bytes; simulated channels "
        "are generated with realistic orbital/sensor physics. "
        "ICD reference: EGSACUBE-ED v3.2."
    ),
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

SIMULATOR_API = "http://localhost:8080"

# ─── Frame type lookup (same as AO-73 spec) ───────────────────────────────────
FRAME_KINDS = {
    **{i: "WO" for i in range(1, 13)},
    13: "HR", 17: "HR", 21: "HR",
    **{i: "FM" for i in list(range(14, 17)) + list(range(18, 21)) + list(range(22, 25))},
}

# ─── Subsystem addresses  (ICD Table 7) ───────────────────────────────────────
ADDR = {
    "OBC":   0xA1,
    "EPS":   0xA2,
    "ADCS":  0xA3,
    "PL":    0xA4,
    "SBAND": 0xA5,
    "UHF":   0xA6,
    "GCS":   0xB0,
}

# ─── Satellite operating modes  (ICD Table 9) ─────────────────────────────────
MODES = {0x01: "Initialization", 0x02: "De-tumbling", 0x03: "Normal"}

MIN_HEX_LEN = 112   # 56 bytes × 2


# ══════════════════════════════════════════════════════════════════════════════
# Simulator connectivity check  (non-blocking, short timeout)
# ══════════════════════════════════════════════════════════════════════════════
def _ping_simulator(base_url: str) -> dict:
    try:
        r = requests.get(f"{base_url.rstrip('/')}/", timeout=3)
        r.raise_for_status()
        return {"reachable": True, "url": base_url}
    except requests.ConnectionError:
        return {"reachable": False, "url": base_url,
                "reason": "Connection refused — is the simulator running? "
                          "(uvicorn main:app --port 8000)"}
    except requests.Timeout:
        return {"reachable": False, "url": base_url,
                "reason": "Request timed out — wrong port or simulator overloaded."}
    except requests.RequestException as exc:
        return {"reachable": False, "url": base_url, "reason": str(exc)}


# ══════════════════════════════════════════════════════════════════════════════
# CRC-16 / IBM-3740  (ICD Section 6.1.1)
# ══════════════════════════════════════════════════════════════════════════════
def crc16_ibm3740(data: bytes) -> int:
    crc = 0xFFFF
    for b in data:
        crc ^= b << 8
        for _ in range(8):
            crc = (crc << 1) ^ 0x1021 if crc & 0x8000 else crc << 1
            crc &= 0xFFFF
    return crc


# ══════════════════════════════════════════════════════════════════════════════
# CSSP frame builder  (ICD Table 4 / cmd 0x07)
# Frame: Flag | DEST | SRC | CMD_ID | LEN | Data | CRC0 | CRC1 | Flag
# ══════════════════════════════════════════════════════════════════════════════
def build_cssp_frame(dest: int, src: int, cmd_id: int, payload: bytes) -> str:
    body  = bytes([dest, src, cmd_id, len(payload)]) + payload
    crc   = crc16_ibm3740(body)
    frame = bytes([0xC0]) + body + bytes([crc & 0xFF, (crc >> 8) & 0xFF, 0xC0])
    return frame.hex().upper()


# ══════════════════════════════════════════════════════════════════════════════
# EPS  (ICD Table 11)
# Real    : Solar_1/2/3_V, VBAT, IBAT, Temp
# Simulated: Solar currents, bus rails, line status
# ══════════════════════════════════════════════════════════════════════════════
def decode_eps(d: bytes, frame_index: int) -> dict:
    u16 = lambda off: struct.unpack_from(">H", d, off)[0]
    u8  = lambda off: d[off]

    # --- REAL values from AO-73 bytes ---
    solar_1_v = u16(0)    # Photo voltage X
    solar_2_v = u16(2)    # Photo voltage Y
    solar_3_v = u16(4)    # Photo voltage Z
    total_i   = u16(6)    # Total photo current
    vbat      = u16(8)    # Battery voltage
    ibat      = u16(10)   # Total system current
    reboot    = u16(12)   # Reboot count (RTC proxy)
    sw_err    = u16(14)   # Software error count
    temp_eps  = u8(19)    # Battery temperature

    sat_mode  = 0x03      # Normal mode (default)

    # --- SIMULATED: solar currents proportional to voltage ---
    solar_1_c = max(0, int(solar_1_v * 0.15 + np.random.normal(0, 5)))
    solar_2_c = max(0, int(solar_2_v * 0.15 + np.random.normal(0, 5)))
    solar_3_c = max(0, int(solar_3_v * 0.15 + np.random.normal(0, 5)))

    # --- SIMULATED: bus voltages (3.3 V and 5 V rails) ---
    bus_1_v    = int(3300 + np.random.normal(0, 20))
    bus_1_c    = max(0, int(total_i * 0.30 + np.random.normal(0, 3)))
    bus_2_v    = int(5000 + np.random.normal(0, 30))
    bus_2_c    = max(0, int(total_i * 0.20 + np.random.normal(0, 2)))
    line_status = 0x01FF  # all nine power lines active

    eps = {
        # header
        "EPS_Address":    hex(ADDR["EPS"]),
        "EPS_Mode":       sat_mode,
        "EPS_Mode_Name":  MODES[sat_mode],
        "EPS_Time":       frame_index * 5,
        "EPS_RTC":        reboot,
        # solar panels [REAL]
        "EPS_Solar_1_V":  solar_1_v,
        "EPS_Solar_1_C":  solar_1_c,
        "EPS_Solar_2_V":  solar_2_v,
        "EPS_Solar_2_C":  solar_2_c,
        "EPS_Solar_3_V":  solar_3_v,
        "EPS_Solar_3_C":  solar_3_c,
        # battery [REAL]
        "EPS_VBAT":       vbat,
        "EPS_IBAT":       ibat,
        # bus rails [SIMULATED]
        "EPS_BUS_1_V":    bus_1_v,
        "EPS_BUS_1_C":    bus_1_c,
        "EPS_BUS_2_V":    bus_2_v,
        "EPS_BUS_2_C":    bus_2_c,
        # status [REAL / SIMULATED]
        "EPS_Line_Status": line_status,
        "EPS_Temp":        temp_eps,
        "EPS_SW_Errors":   sw_err,
    }

    # Build CSSP frame for this telemetry packet
    payload = struct.pack(
        ">BBQIHHHHHHHHHHHHHH",
        ADDR["EPS"], sat_mode, frame_index * 5, reboot,
        solar_1_v, solar_1_c, solar_2_v, solar_2_c,
        solar_3_v, solar_3_c, vbat, ibat,
        bus_1_v, bus_1_c, bus_2_v, bus_2_c,
        line_status, temp_eps,
    )
    eps["EPS_CSSP_Frame"] = build_cssp_frame(ADDR["GCS"], ADDR["EPS"], 0x47, payload)
    return eps


# ══════════════════════════════════════════════════════════════════════════════
# OBC  (ICD Table 12)
# Real    : RTC (reboot count proxy)
# Simulated: GPS position/velocity, command counts, stored records
# ══════════════════════════════════════════════════════════════════════════════
def decode_obc(d: bytes, frame_index: int) -> dict:
    u16 = lambda off: struct.unpack_from(">H", d, off)[0]
    reboot   = u16(12)
    sat_mode = 0x03

    # 600 km sun-synchronous orbit, period ≈ 98 min
    orbit_period = 5880   # seconds
    t     = (frame_index * 5) % orbit_period
    angle = (t / orbit_period) * 2 * np.pi

    latitude  = int(np.sin(angle) * 97.8 * 1e7)
    longitude = int(((frame_index * 5 * 0.5) % 360 - 180) * 1e7)
    vel_n     = int(7500 * np.cos(angle) * 100)
    vel_e     = int(7500 * np.sin(angle) * 100)
    vel_d     = int(np.random.normal(0, 10))
    itow      = frame_index * 5000

    rx_cmd   = frame_index // 10
    tx_cmd   = frame_index // 10
    last_cmd = 0x07   # GOTLM

    eps_rec  = min(frame_index, 1000)
    obc_rec  = min(frame_index, 1000)
    adcs_rec = min(frame_index, 1000)
    comm_rec = min(frame_index, 1000)
    pl_rec   = min(frame_index // 5, 200)

    return {
        "OBC_Address":       hex(ADDR["OBC"]),
        "OBC_Mode":          sat_mode,
        "OBC_Mode_Name":     MODES[sat_mode],
        "OBC_Time":          frame_index * 5,
        "OBC_RTC":           reboot,
        # GPS [SIMULATED - realistic orbit]
        "OBC_Longitude":     longitude,
        "OBC_Latitude":      latitude,
        "OBC_velN":          vel_n,
        "OBC_velE":          vel_e,
        "OBC_velD":          vel_d,
        "OBC_iTOW":          itow,
        # command counters [SIMULATED]
        "OBC_RX_CMD_Count":  rx_cmd,
        "OBC_TX_CMD_Count":  tx_cmd,
        "OBC_Last_CMD":      hex(last_cmd),
        # stored telemetry records [SIMULATED]
        "OBC_EPS_SRecords":  eps_rec,
        "OBC_OBC_SRecords":  obc_rec,
        "OBC_ADCS_SRecords": adcs_rec,
        "OBC_COMM_SRecords": comm_rec,
        "OBC_PL_SRecords":   pl_rec,
    }


# ══════════════════════════════════════════════════════════════════════════════
# ADCS  (ICD Table 13)
# Real    : Sun sensors X/Y/Z (10-bit packed in ASIB section)
# Simulated: Accelerometer, gyroscope, magnetometer, reaction wheel RPM
# ══════════════════════════════════════════════════════════════════════════════
def decode_adcs(d: bytes, frame_index: int) -> dict:
    u16 = lambda off: struct.unpack_from(">H", d, off)[0]
    reboot   = u16(12)
    sat_mode = 0x03

    # Extract 10-bit sun sensor values from ASIB section of AO-73 (bytes 24-36)
    asib    = d[24:37]
    bits    = int.from_bytes(asib, "big")
    total_b = len(asib) * 8
    get10   = lambda n: (bits >> (total_b - 10 * (n + 1))) & 0x3FF
    sun_x, sun_y, sun_z = get10(0), get10(1), get10(2)

    orbit_period = 5880
    t     = (frame_index * 5) % orbit_period
    angle = (t / orbit_period) * 2 * np.pi

    # accelerometer (m/s²) [SIMULATED]
    accel_x = round(float(np.sin(angle) * 9.8  + np.random.normal(0, 0.05)), 4)
    accel_y = round(float(np.cos(angle) * 9.8  + np.random.normal(0, 0.05)), 4)
    accel_z = round(float(np.random.normal(0, 0.10)), 4)

    # gyroscope (rad/s) [SIMULATED]
    spin   = 0.05
    gyro_x = round(float(spin * np.sin(angle) + np.random.normal(0, 0.001)), 6)
    gyro_y = round(float(spin * np.cos(angle) + np.random.normal(0, 0.001)), 6)
    gyro_z = round(float(np.random.normal(0, 0.001)), 6)

    # magnetometer (µT, realistic Earth field at 600 km) [SIMULATED]
    mm_x = round(float(25 * np.sin(angle) + np.random.normal(0, 0.5)), 3)
    mm_y = round(float(25 * np.cos(angle) + np.random.normal(0, 0.5)), 3)
    mm_z = round(float(40 + np.random.normal(0, 0.5)), 3)

    rw_rpm = int(np.random.normal(500, 10))

    return {
        "ADCS_Address":   hex(ADDR["ADCS"]),
        "ADCS_Mode":      sat_mode,
        "ADCS_Mode_Name": MODES[sat_mode],
        "ADCS_Time":      frame_index * 5,
        "ADCS_RTC":       reboot,
        # sun sensors [REAL]
        "ADCS_Sun_X":     sun_x,
        "ADCS_Sun_Y":     sun_y,
        "ADCS_Sun_Z":     sun_z,
        # accelerometer [SIMULATED]
        "ADCS_accel_x":   accel_x,
        "ADCS_accel_y":   accel_y,
        "ADCS_accel_z":   accel_z,
        # gyroscope [SIMULATED]
        "ADCS_Gyro_X":    gyro_x,
        "ADCS_Gyro_Y":    gyro_y,
        "ADCS_Gyro_Z":    gyro_z,
        # magnetometer [SIMULATED]
        "ADCS_MM_X":      mm_x,
        "ADCS_MM_Y":      mm_y,
        "ADCS_MM_Z":      mm_z,
        # reaction wheel [SIMULATED]
        "ADCS_RW_RPM":    rw_rpm,
    }


# ══════════════════════════════════════════════════════════════════════════════
# COMM  (ICD Table 14)
# Real    : RSSI, RF temperature, TX/RX currents
# Simulated: Total/correct RX frames, data rate, RF power
# ══════════════════════════════════════════════════════════════════════════════
def decode_comm(d: bytes, frame_index: int) -> dict:
    u16 = lambda off: struct.unpack_from(">H", d, off)[0]
    u8  = lambda off: d[off]
    reboot   = u16(12)
    sat_mode = 0x03

    # REAL RF values from AO-73 byte offsets
    rssi    = u8(38)
    rf_temp = u8(39)
    rx_i    = u8(40)
    tx_i_3v = u8(41)
    tx_i_5v = u8(42)

    # SIMULATED frame statistics
    total_rx   = max(0, frame_index + int(np.random.normal(0, 2)))
    correct_rx = int(total_rx * 0.97)
    rf_power   = max(0, int(tx_i_3v * 2 + np.random.normal(0, 5)))

    return {
        "COMM_Address":    hex(ADDR["UHF"]),
        "COMM_Mode":       sat_mode,
        "COMM_Mode_Name":  MODES[sat_mode],
        "COMM_Time":       frame_index * 5,
        "COMM_RTC":        reboot,
        # frame counts [SIMULATED]
        "COMM_Total_RX":   total_rx,
        "COMM_Correct_RX": correct_rx,
        "COMM_Last_CMD":   hex(0x07),
        "COMM_Rate":       1200,
        "COMM_RF_Power":   rf_power,
        # RF measurements [REAL]
        "COMM_RSSI":       rssi,
        "COMM_RF_Temp":    rf_temp,
        "COMM_RX_Current": rx_i,
        "COMM_TX_I_3V3":   tx_i_3v,
        "COMM_TX_I_5V":    tx_i_5v,
    }


# ══════════════════════════════════════════════════════════════════════════════
# SW Status  (AO-73 software flags mapped to satellite status)
# ══════════════════════════════════════════════════════════════════════════════
def decode_sw(d: bytes) -> dict:
    u8 = lambda off: d[off]
    seq_num  = (u8(50) << 16) | (u8(51) << 8) | u8(52)
    sw_byte  = u8(54)
    return {
        "SW_Seq_Number":   seq_num,
        "SW_In_Eclipse":   bool(sw_byte & 0x80),
        "SW_In_Safe_Mode": bool(sw_byte & 0x40),
        "SW_HW_ABF":       bool(sw_byte & 0x20),
        "SW_SW_ABF":       bool(sw_byte & 0x10),
    }


# ══════════════════════════════════════════════════════════════════════════════
# Core decoder — entry point per frame
# Mirrors ICD v3.2 decode_frame() exactly, adapted for the FastAPI service
# ══════════════════════════════════════════════════════════════════════════════
def decode_frame(hex_frame: str, frame_index: int) -> dict:
    """
    Decode one raw hex frame into EGSACUBE-ED subsystem boards.
    frame_index is the sequential count of successfully decoded frames
    (used by the simulated physics channels).
    Raises ValueError with a clear message on validation failure.
    """
    hex_frame = "".join(hex_frame.split())

    # ── Validation ────────────────────────────────────────────────────────────
    if not hex_frame:
        raise ValueError("Empty hex frame.")

    if len(hex_frame) < MIN_HEX_LEN:
        raise ValueError(
            f"Frame too short: {len(hex_frame)} hex chars "
            f"({len(hex_frame) // 2} bytes). "
            f"Minimum required: {MIN_HEX_LEN} hex chars (56 bytes)."
        )

    try:
        raw = bytes.fromhex(hex_frame)
    except ValueError:
        raise ValueError("Frame contains non-hexadecimal characters.")

    # ── Header byte ───────────────────────────────────────────────────────────
    header     = raw[0]
    sat_id     = (header >> 6) & 0x03
    frame_type = header & 0x3F

    # ── RTT payload: bytes 1-55 ───────────────────────────────────────────────
    d = raw[1:56]
    if len(d) < 55:
        raise ValueError(
            f"RTT payload too short: {len(d)} bytes after header, need 55."
        )

    # ── Decode all boards ─────────────────────────────────────────────────────
    result = {
        "meta": {
            "decoded_at_utc":   datetime.now(timezone.utc).isoformat(),
            "frame_hex_length": len(hex_frame),
            "frame_bytes":      len(raw),
            "frame_index":      frame_index,
            "sat_id":           sat_id,
            "frame_type":       frame_type,
            "frame_kind":       FRAME_KINDS.get(frame_type, "Unknown"),
        },
        "EPS":  decode_eps(d,  frame_index),
        "OBC":  decode_obc(d,  frame_index),
        "ADCS": decode_adcs(d, frame_index),
        "COMM": decode_comm(d, frame_index),
        "SW":   decode_sw(d),
    }
    return result


# ══════════════════════════════════════════════════════════════════════════════
# Request Models  (same shape as the AO-73 decoder)
# ══════════════════════════════════════════════════════════════════════════════
class DecodeRequest(BaseModel):
    hex_frame:   str
    frame_index: int  = 0   # caller supplies the sequential frame counter
    captured_at: Optional[str] = None
    station:     Optional[str] = None

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
# Routes  —  identical shape to the AO-73 decoder service
# ══════════════════════════════════════════════════════════════════════════════

@app.get("/", summary="Decoder service health check")
def root():
    return {
        "status":  "online",
        "service": "EGSACUBE-ED Telemetry Decoder",
        "icd":     "EGSACUBE-ED v3.2",
        "frame_structure": {
            "byte_0":          "Header (Sat ID bits 7-6, Frame Type bits 5-0)",
            "bytes_1_to_55":   "RTT payload (55 bytes)",
            "minimum_hex_len": MIN_HEX_LEN,
            "boards":          ["EPS", "OBC", "ADCS", "COMM", "SW"],
        },
        "channel_sources": {
            "REAL":      "EPS Solar V, VBAT, IBAT, Temp | ADCS Sun Sensors | COMM RSSI, RF Temp, Currents",
            "SIMULATED": "OBC GPS/Velocity | ADCS Gyro/Accel/Magnetometer/RW | COMM Frame counts | EPS Bus rails/Solar currents",
        },
        "endpoints": {
            "POST /decode":       "Decode a single hex frame",
            "POST /decode/batch": "Decode up to 500 frames in one request",
            "GET  /decode/next":  "Pull the next frame from the simulator and decode it",
            "GET  /boards":       "List all boards and their channel sources",
        },
    }


@app.post("/decode", summary="Decode a single raw hex frame")
def decode_single(req: DecodeRequest):
    """
    Accepts one raw hex frame and returns fully decoded EGSACUBE-ED telemetry
    grouped by board: EPS / OBC / ADCS / COMM / SW.

    Supply frame_index (0-based count of successful frames so far) so the
    simulated physics channels (orbit position, gyro, etc.) stay consistent
    across calls.
    """
    logger.info(
        f"POST /decode  len={len(req.hex_frame)}"
        f"  frame_index={req.frame_index}"
        f"  station={req.station}"
    )
    try:
        decoded = decode_frame(req.hex_frame, req.frame_index)
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
    Each item carries its own frame_index so physics simulation stays correct.
    Frames that fail validation are marked status: error — they do not abort the batch.
    """
    logger.info(f"POST /decode/batch  count={len(req.frames)}")
    results = []

    for item in req.frames:
        try:
            decoded = decode_frame(item.hex_frame, item.frame_index)
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


@app.get("/simulator/status", summary="Check if the simulator API is reachable")
def simulator_status(
    simulator_url: str = Query(default=SIMULATOR_API,
                               description="Base URL of the simulator API"),
):
    """Pings the simulator and tells you clearly whether it is reachable."""
    result = _ping_simulator(simulator_url)
    if not result["reachable"]:
        raise HTTPException(status_code=503, detail=result)
    return {
        "simulator_reachable": True,
        "simulator_url":       simulator_url,
        "tip": "You can now use GET /decode/from-simulator to pull and decode frames.",
    }


@app.get("/decode/from-simulator",
         summary="Pull next frame from simulator then decode it (requires simulator running)")
def decode_from_simulator(
    frame_index: int = Query(
        default=0,
        description="Sequential index of this frame (for physics simulation channels)",
    ),
    simulator_url: str = Query(
        default=SIMULATOR_API,
        description="Base URL of the simulator API (e.g. http://localhost:8000)",
    ),
):
    """
    Requires the simulator to be running. Check first with GET /simulator/status.

    Steps performed:
    1. Pings the simulator — returns a clear 503 (not a timeout) if unreachable.
    2. Calls GET /telemetry/frames/next on the simulator (advances the cursor).
    3. Decodes the hex frame through the EGSACUBE-ED ICD v3.2.
    4. Returns the combined result.

    Pass frame_index so the simulated physics channels (orbit, gyro, etc.)
    stay consistent across successive calls.
    """
    # ── Pre-flight check — fail fast and clearly ──────────────────────────────
    ping = _ping_simulator(simulator_url)
    if not ping["reachable"]:
        raise HTTPException(
            status_code=503,
            detail={
                "error":  "Simulator is not reachable.",
                "reason": ping.get("reason"),
                "fix":    "Start the simulator with:  uvicorn main:app --port 8000 --reload",
                "tip":    "Or use POST /decode and supply the hex_frame directly — "
                          "the simulator is not required for that endpoint.",
            },
        )

    next_url = f"{simulator_url.rstrip('/')}/telemetry/frames/next"
    logger.info(f"GET /decode/from-simulator  frame_index={frame_index}  -> {next_url}")

    try:
        resp = requests.get(next_url, timeout=10)
        resp.raise_for_status()
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"Simulator request failed: {exc}")

    sim_data = resp.json()

    if sim_data.get("exhausted"):
        return {
            "status":       "exhausted",
            "message":      sim_data.get("message"),
            "cursor":       sim_data.get("cursor"),
            "total_frames": sim_data.get("total_frames"),
            "tip":          "POST http://localhost:8000/telemetry/frames/reset to start over.",
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
        decoded = decode_frame(hex_frame, frame_index)
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


@app.get("/boards", summary="List all boards and their channel sources")
def list_boards():
    """
    Documents every EGSACUBE-ED board, its ICD table reference,
    and which channels come from real AO-73 bytes vs. physics simulation.
    """
    return {
        "icd":    "EGSACUBE-ED Telemetry ICD v3.2",
        "boards": {
            "EPS": {
                "icd_table":   "Table 11",
                "rtt_offset":  "0-23 (24 bytes)",
                "real_channels": [
                    "EPS_Solar_1_V (offset 0, u16)",
                    "EPS_Solar_2_V (offset 2, u16)",
                    "EPS_Solar_3_V (offset 4, u16)",
                    "EPS_VBAT      (offset 8, u16)",
                    "EPS_IBAT      (offset 10, u16)",
                    "EPS_Temp      (offset 19, u8)",
                    "EPS_SW_Errors (offset 14, u16)",
                ],
                "simulated_channels": [
                    "EPS_Solar_1/2/3_C (solar current ∝ voltage + noise)",
                    "EPS_BUS_1/2_V     (3.3 V / 5 V rails + noise)",
                    "EPS_BUS_1/2_C     (bus current ∝ total_i + noise)",
                    "EPS_Line_Status   (all lines active: 0x01FF)",
                    "EPS_CSSP_Frame    (CSSP-framed packet, CRC-16/IBM-3740)",
                ],
            },
            "OBC": {
                "icd_table":   "Table 12",
                "real_channels": [
                    "OBC_RTC (reboot count from offset 12, u16)",
                ],
                "simulated_channels": [
                    "OBC_Latitude/Longitude (600 km SSO, period 98 min)",
                    "OBC_velN/velE/velD     (orbital velocity cm/s)",
                    "OBC_iTOW               (GPS time-of-week ms)",
                    "OBC_RX/TX_CMD_Count    (frame_index / 10)",
                    "OBC_*_SRecords         (stored record counts)",
                ],
            },
            "ADCS": {
                "icd_table":   "Table 13",
                "real_channels": [
                    "ADCS_Sun_X (ASIB 10-bit channel 0)",
                    "ADCS_Sun_Y (ASIB 10-bit channel 1)",
                    "ADCS_Sun_Z (ASIB 10-bit channel 2)",
                ],
                "simulated_channels": [
                    "ADCS_accel_x/y/z (m/s², orbital sine/cosine + noise)",
                    "ADCS_Gyro_X/Y/Z  (rad/s, 0.05 rad/s spin + noise)",
                    "ADCS_MM_X/Y/Z    (µT, Earth field at 600 km + noise)",
                    "ADCS_RW_RPM      (reaction wheel, ~500 RPM + noise)",
                ],
            },
            "COMM": {
                "icd_table":   "Table 14",
                "real_channels": [
                    "COMM_RSSI       (offset 38, u8)",
                    "COMM_RF_Temp    (offset 39, u8)",
                    "COMM_RX_Current (offset 40, u8)",
                    "COMM_TX_I_3V3   (offset 41, u8)",
                    "COMM_TX_I_5V    (offset 42, u8)",
                ],
                "simulated_channels": [
                    "COMM_Total_RX   (frame_index + noise)",
                    "COMM_Correct_RX (97% of Total_RX)",
                    "COMM_RF_Power   (TX_I_3V3 × 2 + noise)",
                    "COMM_Rate       (fixed 1200 bps)",
                ],
            },
            "SW": {
                "note": "AO-73 software flags mapped to satellite status",
                "real_channels": [
                    "SW_Seq_Number   (bytes 50-52, 24-bit big-endian)",
                    "SW_In_Eclipse   (byte 54 bit 7)",
                    "SW_In_Safe_Mode (byte 54 bit 6)",
                    "SW_HW_ABF       (byte 54 bit 5)",
                    "SW_SW_ABF       (byte 54 bit 4)",
                ],
            },
        },
    }


# ─── Run directly ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("decoder_egsacube:app", host="0.0.0.0", port=8082, reload=True)