import asyncio
import struct
import logging
import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("OBC_SIMULATOR")

app = FastAPI(title="Satellite Raw Command Simulator")

# ══════════════════════════════════════════════════════════════════════════════
# Satellite State (Memory & Hardware)
# 8081
# ══════════════════════════════════════════════════════════════════════════════
class SatelliteState:
    def __init__(self):
        self.mode = 1
        self.time = 0.0
        self.subsystems = {
            0xB0: "ON",   # OBC
            0xA1: "ON",   # EPS
            0xA5: "OFF"   # PL (Payload/Camera)
        }
        self.images = {}  # Store captured images
        self.next_image_id = 1

STATE = SatelliteState()

# ══════════════════════════════════════════════════════════════════════════════
# Frame Helpers (Strictly matching the ICD)
# ══════════════════════════════════════════════════════════════════════════════
def calculate_crc(data: bytes) -> bytes:
   
    crc = sum(data) & 0xFFFF
    return struct.pack(">H", crc)

def build_frame(dest: int, src: int, cmd_id: int, data: bytes = b"") -> bytes:
    """ 
    0xC0 | DEST | SRC | CMD | LEN | DATA | CRC0 | CRC1 | 0xC0
    """
    if len(data) > 255:
        logger.warning(f"⚠️ Data length ({len(data)}) exceeds 255 bytes. Truncating to fit ICD specs.")
        data = data[:255]
        
    header_and_data = struct.pack("BBBB", dest, src, cmd_id, len(data)) + data
    crc = calculate_crc(header_and_data)
    return b"\xC0" + header_and_data + crc + b"\xC0"

def parse_frame(frame: bytes):
    if len(frame) < 8:
        raise ValueError("Frame too short")
    if frame[0] != 0xC0 or frame[-1] != 0xC0:
        raise ValueError("Invalid Frame Bounds (Must start and end with 0xC0)")

    dest = frame[1]
    src = frame[2]
    cmd_id = frame[3]
    length = frame[4]
    
    if len(frame) != (length + 8):
        raise ValueError(f"Length mismatch: Header specifies {length} bytes of data.")
        
    data = frame[5 : 5 + length]
    return dest, src, cmd_id, data

# ══════════════════════════════════════════════════════════════════════════════
# Main WebSocket Endpoint (The Radio Link)
# ══════════════════════════════════════════════════════════════════════════════
@app.websocket("/ws/radio")
async def radio_link(websocket: WebSocket):
    await websocket.accept()
    logger.info("🛰️ Ground Station Connected via Radio Link.")
    
    try:
        while True:
            raw_frame = await websocket.receive_bytes()
            logger.info(f"📥 RX: {raw_frame.hex().upper()}")
            
            try:
                dest, src, cmd_id, data = parse_frame(raw_frame)
            except ValueError as e:
                logger.error(f"❌ Frame Dropped: {e}")
                continue

            def send_ack():
                return websocket.send_bytes(build_frame(src, dest, 0x02, bytes([cmd_id])))
            
            def send_nack():
                return websocket.send_bytes(build_frame(src, dest, 0x03, bytes([cmd_id])))

            # ════════════════════════════════════════════════════════════════
            # 3. (TABLE 10)
            # ════════════════════════════════════════════════════════════════
            
            # 0x01: HI (Broadcast - No Reply)
            if cmd_id == 0x01:
                logger.info("👋 Received HI Broadcast. No reply needed.")
                continue
                
            # 0x04: PING
            elif cmd_id == 0x04:
                logger.info("🏓 PING received.")
                await send_ack()

            # 0x05: STIME
            elif cmd_id == 0x05:
                logger.info("⏱️ STIME received. Setting time...")
                await send_ack()

            # 0x06: SMODE
            elif cmd_id == 0x06:
                mode = data[0] if len(data) > 0 else 0
                STATE.mode = mode
                logger.info(f"⚙️ SMODE set to {mode}.")
                await send_ack()

            # 0x07: GOTLM (Online Telemetry)
            elif cmd_id == 0x07:
                logger.info("📊 GOTLM received. Fetching real telemetry from internal sensors...")
                
                try:
                    
                    async with httpx.AsyncClient() as client:
                        #response = await client.get("http://telemetry-api:8000/telemetry/frames/next")
                        response = await client.get("http://127.0.0.1:8080/telemetry/frames/next")
                        response.raise_for_status()
                        tlm_data_json = response.json()
                        
                    if tlm_data_json.get("exhausted"):
                        logger.warning("⚠️ No more telemetry frames available from sensors.")
                        await send_nack()
                        continue
                    
                   
                    hex_string = tlm_data_json["frame"]["hex_frame"]
                    
                    
                    real_tlm_bytes = bytes.fromhex(hex_string)

                    if len(real_tlm_bytes) > 255:
                        real_tlm_bytes = real_tlm_bytes[:255]
                    
                    
                    reply_frame = build_frame(src, dest, 0x47, real_tlm_bytes)
                    await websocket.send_bytes(reply_frame)
                    logger.info("✅ Real Telemetry frame sent to Ground Station.")
                    
                except Exception as e:
                    logger.error(f"❌ Failed to fetch telemetry from internal sensors: {e}")
                    await send_nack() 

            # 0x08: GSTLM (Stored Telemetry - 5 Frames)
            elif cmd_id == 0x08:
                logger.info("📁 GSTLM received. Fetching stored telemetry from internal sensors...")
                
                await send_ack()
                
                try:
                    async with httpx.AsyncClient() as client:
                        #response = await client.get("http://telemetry-api:8000/telemetry/frames?limit=7")
                        response = await client.get("http://127.0.0.1:8080/telemetry/frames?limit=7")
                        response.raise_for_status()
                        data_json = response.json()
                        
                    frames_list = data_json.get("frames", [])
                    for i in range(7):
                        await asyncio.sleep(0.2) 
                        
                        if i < len(frames_list):
                            hex_string = frames_list[i]["hex_frame"]
                            real_tlm_bytes = bytes.fromhex(hex_string)
                            if len(real_tlm_bytes) > 255:
                                real_tlm_bytes = real_tlm_bytes[:255]
                        else:
                            real_tlm_bytes = bytes([i+1]) + b"\x00\x00\x00"
                            
                        frame = build_frame(src, dest, 0x48, real_tlm_bytes)
                        await websocket.send_bytes(frame)
                        logger.info(f"📤 Sent Stored TLM Frame {i+1}/7")
                        
                except Exception as e:
                    logger.error(f"❌ Failed to fetch stored telemetry: {e}")

            # 0x09: SON (Switch ON)
            elif cmd_id == 0x09:
                target_system = data[0] if data else 0xA5
                STATE.subsystems[target_system] = "ON"
                logger.info(f"⚡ SON: Subsystem {hex(target_system)} is ON.")
                await send_ack()

            # 0x0A: SOFF (Switch OFF)
            elif cmd_id == 0x0A:
                target_system = data[0] if data else 0xA5
                STATE.subsystems[target_system] = "OFF"
                logger.info(f"🔌 SOFF: Subsystem {hex(target_system)} is OFF.")
                await send_ack()

            # 0x0C: CIMG (Capture Image)
            elif cmd_id == 0x0C:
                if STATE.subsystems.get(0xA5) == "OFF":
                    logger.warning("📸 CIMG Failed: Payload is OFF.")
                    await send_nack()
                else:
                    logger.info("📸 CIMG: Requesting Payload to capture next image...")
                    try:
                        # Fetch the next image from the Image Simulator (running on port 8004)
                        async with httpx.AsyncClient() as client:
                            response = await client.get("http://127.0.0.1:8004/images/frames/next")
                            response.raise_for_status()
                            data_json = response.json()
                            
                        if data_json.get("exhausted"):
                            logger.warning("⚠️ Camera out of storage (dataset exhausted).")
                            await send_nack()
                            continue
                            
                        # Extract the Base64 image data from the response payload
                        img_b64 = data_json["frame"]["image"]["data"]
                        img_id = STATE.next_image_id
                        
                        # Store the captured image in the satellite's internal memory (OBC STATE)
                        STATE.images[img_id] = img_b64
                        STATE.next_image_id += 1
                        
                        await send_ack()
                        logger.info(f"✅ Image {img_id} captured successfully by Payload.")
                        
                    except Exception as e:
                        logger.error(f"❌ Failed to communicate with Image Simulator: {e}")
                        await send_nack()

            # 0x0D: DIMG (Delete Image)
            elif cmd_id == 0x0D:
                # Extract image ID from the data payload (default to 1 if empty)
                img_id = struct.unpack(">H", data[:2])[0] if len(data) >= 2 else 1
                
                if img_id in STATE.images:
                    del STATE.images[img_id]
                    logger.info(f"🗑️ DIMG: Deleted image {img_id} from OBC memory.")
                    await send_ack()
                else:
                    logger.warning(f"⚠️ DIMG Failed: Image {img_id} not found in memory.")
                    await send_nack()

            # 0x0E: GIMG (Get Image)
            elif cmd_id == 0x0E:
                # Extract image ID from the data payload
                img_id = struct.unpack(">H", data[:2])[0] if len(data) >= 2 else 1
                logger.info(f"📡 GIMG: Fetching image {img_id} chunks to Ground Station...")
                
                if img_id not in STATE.images:
                    logger.warning(f"⚠️ GIMG Failed: Image {img_id} not found in memory.")
                    await send_nack()
                    continue
                    
                # Send ACK to confirm the command before starting the heavy transmission
                await send_ack()
                
                img_b64 = STATE.images[img_id]
                if not img_b64:
                    logger.warning("⚠️ Image data is empty.")
                    continue
                    
                import base64
                try:
                    # Decode Base64 into raw bytes to reduce the transmission payload size
                    raw_image_bytes = base64.b64decode(img_b64)
                except Exception:
                    # Fallback in case the data is not valid Base64
                    raw_image_bytes = img_b64.encode('utf-8')
                    
                # Split the image into smaller chunks (200 bytes each) 
                # This ensures we strictly obey the 255-byte limit defined in the ICD
                chunk_size = 200
                chunks = [raw_image_bytes[i:i + chunk_size] for i in range(0, len(raw_image_bytes), chunk_size)]
                
                logger.info(f"📡 Transmitting {len(chunks)} chunks for Image {img_id}...")
                
                # Transmit the chunks sequentially to the Ground Station via WebSocket
                for i, chunk_bytes in enumerate(chunks):
                    await asyncio.sleep(0.05) # Simulate radio transmission delay
                    frame = build_frame(src, dest, 0x0E, chunk_bytes)
                    await websocket.send_bytes(frame)
                    
                logger.info(f"✅ Finished transmitting all chunks for Image {img_id}.")

    except WebSocketDisconnect:
        logger.info("📡 Ground Station Disconnected.")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("command2:app", host="0.0.0.0", port=8081, reload=True)