import asyncio
import struct
import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("satellite_radio")

app = FastAPI()

# ══════════════════════════════════════════════════════════════════════════════
# 4. التوجيه الداخلي (Internal Routing) & Hardware State
# ══════════════════════════════════════════════════════════════════════════════
class SatelliteState:
    def __init__(self):
        self.subsystems = {
            0xB0: "ON",  # OBC (الحاسوب الرئيسي)
            0xA1: "ON",  # EPS (الطاقة)
            0xA5: "OFF"  # PL (الحمولة / الكاميرا)
        }
        self.images_count = 0

STATE = SatelliteState()

# ══════════════════════════════════════════════════════════════════════════════
# 3. التحقق من سلامة الإطار (Frame Validation & CRC)
# ══════════════════════════════════════════════════════════════════════════════
def calculate_crc(data: bytes) -> bytes:
    """ محاكاة بسيطة لحساب CRC16 (مجموع البايتات للتوضيح) """
    crc = sum(data) & 0xFFFF
    return struct.pack(">H", crc)

def parse_frame(frame: bytes):
    """
    تفكيك الإطار والتأكد من سلامته.
    البنية المتوقعة: 0xC0 | DEST | SRC | CMD_ID | LEN | DATA... | CRC0 | CRC1 | 0xC0
    """
    if len(frame) < 8:
        raise ValueError("Frame too short")
    
    if frame[0] != 0xC0 or frame[-1] != 0xC0:
        raise ValueError("Invalid Frame Bounds (Missing 0xC0)")

    dest = frame[1]
    src = frame[2]
    cmd_id = frame[3]
    length = frame[4]
    
    data = frame[5 : 5 + length]
    received_crc = frame[-3:-1]
    
    # التحقق من الـ CRC
    expected_crc = calculate_crc(frame[1:-3])
    if received_crc != expected_crc:
        raise ValueError(f"CRC Mismatch! Expected {expected_crc.hex()}, got {received_crc.hex()}")

    return dest, src, cmd_id, data

def build_frame(dest: int, src: int, cmd_id: int, data: bytes = b"") -> bytes:
    """ بناء إطار جديد للإرسال للأرض """
    header_and_data = struct.pack("BBBB", dest, src, cmd_id, len(data)) + data
    crc = calculate_crc(header_and_data)
    return b"\xC0" + header_and_data + crc + b"\xC0"

# ══════════════════════════════════════════════════════════════════════════════
# المهام التشغيلية (Hardware Tasks)
# ══════════════════════════════════════════════════════════════════════════════

async def hardware_task_capture_image(websocket: WebSocket, cmd_id: int):
    """ 2. محاكاة التأخير الزمني للعتاد (Hardware Latency) """
    logger.info("[HARDWARE] Camera sensor warming up... opening lens.")
    await asyncio.sleep(4.0) # تأخير 4 ثوانٍ للتصوير الحقيقي
    
    STATE.images_count += 1
    logger.info(f"[HARDWARE] Image {STATE.images_count} captured and saved.")
    
    # إرسال إشعار للمحطة الأرضية عبر الراديو
    event_frame = build_frame(0x05, 0xB0, cmd_id, b"\x01") # 0x01 = Success
    await websocket.send_bytes(event_frame)

async def hardware_task_gstlm(websocket: WebSocket, cmd_id: int):
    """ 5. الالتزام الصارم بشروط الـ ICD (مثال: GSTLM) """
    logger.info("[RADIO] Starting GSTLM downlink window...")
    
    # إرسال 8 إطارات بالضبط كما ينص الـ ICD
    for i in range(1, 9):
        await asyncio.sleep(0.5) # بطء الإرسال الراديوي
        mock_telemetry_data = struct.pack("B", i) + b"\xAA\xBB\xCC" # بيانات وهمية
        
        # 0x48 هو كود الرد للبيانات المخزنة حسب الجدول
        tlm_frame = build_frame(0x05, 0xB0, 0x48, mock_telemetry_data)
        await websocket.send_bytes(tlm_frame)
        logger.info(f"[RADIO] Transmitted GSTLM Frame {i}/8")

# ══════════════════════════════════════════════════════════════════════════════
# وصلة الراديو الأساسية (WebSocket Radio Link)
# ══════════════════════════════════════════════════════════════════════════════

@app.websocket("/ws/radio")
async def radio_link(websocket: WebSocket):
    await websocket.accept()
    logger.info("[RADIO] Ground Control Station connected.")
    
    try:
        while True:
            # استقبال بايتات خام (Raw Bytes) وليس JSON
            raw_frame = await websocket.receive_bytes()
            logger.info(f"[RADIO] RX Hex: {raw_frame.hex().upper()}")
            
            try:
                # 3. التحقق من الإطار
                dest, src, cmd_id, data = parse_frame(raw_frame)
            except ValueError as e:
                logger.error(f"[RADIO] Frame Dropped: {e}")
                continue # تجاهل الإطار المشوه (لا نرسل NACK إذا كان الـ CRC خطأ)

            logger.info(f"[OBC] Parsed -> DEST: {hex(dest)}, CMD: {hex(cmd_id)}")

            # 4. التوجيه الداخلي (هل النظام الوجهة يعمل؟)
            if dest in STATE.subsystems and STATE.subsystems[dest] == "OFF":
                logger.warning(f"[OBC] Routing failed. Subsystem {hex(dest)} is OFF.")
                nack_frame = build_frame(src, dest, 0x03, bytes([cmd_id])) # 0x03 = NACK
                await websocket.send_bytes(nack_frame)
                continue

            # 1. فصل الـ ACK عن البيانات (إرسال التأكيد فوراً)
            ack_frame = build_frame(src, dest, 0x02, bytes([cmd_id])) # 0x02 = ACK
            await websocket.send_bytes(ack_frame)
            logger.info(f"[RADIO] TX ACK for CMD {hex(cmd_id)}")

            # تنفيذ الأمر في الخلفية بناءً على الـ CMD_ID
            if cmd_id == 0x0C: # CIMG (Capture Image)
                asyncio.create_task(hardware_task_capture_image(websocket, cmd_id))
                
            elif cmd_id == 0x08: # GSTLM (Get Stored Telemetry)
                asyncio.create_task(hardware_task_gstlm(websocket, cmd_id))
                
            elif cmd_id == 0x09: # SON (Switch ON)
                target_sub = data[0] if len(data) > 0 else None
                if target_sub in STATE.subsystems:
                    STATE.subsystems[target_sub] = "ON"
                    logger.info(f"[EPS] Powered ON subsystem {hex(target_sub)}")

    except WebSocketDisconnect:
        logger.info("[RADIO] Ground Control Station disconnected (Signal Lost).")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("command_sim:app", host="0.0.0.0", port=8000, reload=True)