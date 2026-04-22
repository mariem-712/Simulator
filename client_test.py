import asyncio
import websockets
import struct

def calculate_crc(data: bytes) -> bytes:
    crc = sum(data) & 0xFFFF
    return struct.pack(">H", crc)

def build_frame(dest: int, src: int, cmd_id: int, data: bytes = b"") -> bytes:
    header_and_data = struct.pack("BBBB", dest, src, cmd_id, len(data)) + data
    crc = calculate_crc(header_and_data)
    return b"\xC0" + header_and_data + crc + b"\xC0"

async def run_gcs():
    uri = "ws://127.0.0.1:8002/ws/radio"

    async with websockets.connect(uri) as ws:
        print("✅ Connected to Satellite Simulator\n")

        # 1. إرسال أمر Ping (0x04)
        print("➤ Sending PING (0x04)...")
        ping_frame = build_frame(0xB0, 0xA1, 0x04, b"\x03")
        await ws.send(ping_frame)
        reply = await ws.recv()
        print(f"⬅️ Reply (Hex): {reply.hex().upper()}\n")

        # 2. إرسال أمر تشغيل الكاميرا (0x09 SON)
        print("➤ Sending SON (0x09) to turn on Payload...")
        son_frame = build_frame(0xA1, 0xB0, 0x09, b"\xA5")
        await ws.send(son_frame)
        reply = await ws.recv()
        print(f"⬅️ Reply (Hex): {reply.hex().upper()}\n")

        # 3. أمر التقاط صورة (0x0C CIMG)
        print("➤ Sending CIMG (0x0C) to capture image...")
        cimg_frame = build_frame(0xA5, 0xB0, 0x0C, b"")
        await ws.send(cimg_frame)
        reply = await ws.recv()
        print(f"⬅️ Reply (Hex): {reply.hex().upper()}\n")
        
        # 4. طلب تليميتري مباشر (0x07 GOTLM)
        print("➤ Sending GOTLM (0x07)...")
        gotlm_frame = build_frame(0xB0, 0xA1, 0x07, b"")
        await ws.send(gotlm_frame)
        reply = await ws.recv()
        print(f"⬅️ Reply (Hex): {reply.hex().upper()}\n")

        print("➤ Sending GSTLM (0x08) to fetch stored telemetry window...")
        gstlm_frame = build_frame(0xB0, 0xA1, 0x08, b"")
        await ws.send(gstlm_frame)
        print("⏳ Waiting for 8 frames (Window)...")
        for i in range(8):
            reply = await ws.recv()
            if i == 0:
                print(f"⬅️ Frame 1 (ACK):  {reply.hex().upper()}")
            else:
                print(f"⬅️ Frame {i+1} (Data): {reply.hex().upper()}")
                
        print("\n✅ Finished receiving 8-frame window.")

if __name__ == "__main__":
    asyncio.run(run_gcs())