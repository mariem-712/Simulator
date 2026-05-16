import asyncio
import websockets
import struct
import httpx
import os
import time
import json 

# ══════════════════════════════════════════════════════════════════════════════
# Helper Functions
# ══════════════════════════════════════════════════════════════════════════════
def calculate_crc(data: bytes) -> bytes:
    crc = sum(data) & 0xFFFF
    return struct.pack(">H", crc)

def build_frame(dest: int, src: int, cmd_id: int, data: bytes = b"") -> bytes:
    header_and_data = struct.pack("BBBB", dest, src, cmd_id, len(data)) + data
    crc = calculate_crc(header_and_data)
    return b"\xC0" + header_and_data + crc + b"\xC0"

def extract_data_from_frame(frame: bytes) -> bytes:
    if len(frame) >= 8 and frame[0] == 0xC0 and frame[-1] == 0xC0:
        length = frame[4]
        return frame[5:5+length]
    return b""

# ══════════════════════════════════════════════════════════════════════════════
# Main Image Test Sequence
# ══════════════════════════════════════════════════════════════════════════════
async def test_image_lifecycle():
    ws_uri = "ws://127.0.0.1:8081/ws/radio"
    http_url = "http://127.0.0.1:8081"
    
    image_id_to_test = 1  # Arbitrary image ID for testing

    async with websockets.connect(ws_uri) as ws:
        print("\n🚀 --- STARTING PAYLOAD & IMAGE SUBSYSTEM TEST --- 🚀\n")

        # 1. Turn ON Payload
        print("➤ [1/6] Sending SON (0x09) to turn ON Payload...")
        await ws.send(build_frame(0xA1, 0xB0, 0x09, b"\xA5"))
        print(f"⬅️ Reply: {(await ws.recv()).hex().upper()}\n")

        # 2. Capture Image (CIMG)
        print("➤ [2/6] Sending CIMG (0x0C) to capture an image...")
        await ws.send(build_frame(0xA5, 0xB0, 0x0C, b""))
        print(f"⬅️ Reply: {(await ws.recv()).hex().upper()}\n")
        
        print(f"➤ [3/6] Downloading Image {image_id_to_test} instantly via S-Band (HTTP)...")
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{http_url}/sband/download/{image_id_to_test}")
            if resp.status_code == 200:
                
                timestamp = int(time.time())
                img_filename = f"sband_download_{timestamp}.jpg"
                meta_filename = f"sband_download_{timestamp}_meta.json"
                
                with open(img_filename, "wb") as f:
                    f.write(resp.content)
                
    
                metadata_str = resp.headers.get("X-Image-Metadata", "{}")
                metadata_dict = json.loads(metadata_str)
                
                with open(meta_filename, "w", encoding="utf-8") as f:
                    json.dump(metadata_dict, f, indent=4)
                    
                print(f"✅ S-Band Success!")
                print(f"   📸 Saved Image: '{img_filename}' ({len(resp.content)} bytes)")
                print(f"   📄 Saved Meta:  '{meta_filename}'")
                
                tile_name = metadata_dict.get("tile_name", "Unknown")
                gsd = metadata_dict.get("gsd_m_per_px", "Unknown")
                print(f"   🌍 Data Info -> Tile: {tile_name} | GSD: {gsd} m/px\n")
                
                try:
                    os.startfile(img_filename)
                except AttributeError:
                    pass
            else:
                print(f"❌ S-Band Failed: {resp.text}\n")


        # 4. Slow Download (UHF Chunks)
        print(f"➤ [4/6] Downloading Image {image_id_to_test} via slow UHF Chunks (0x0E)...")
        img_id_bytes = struct.pack(">H", image_id_to_test)
        await ws.send(build_frame(0xB0, 0xA1, 0x0E, img_id_bytes))
        
        chunk_count = 0
        image_bytes = bytearray()
        try:
            while True:
                reply = await asyncio.wait_for(ws.recv(), timeout=2.0)
                if chunk_count == 0:
                    print(f"⬅️ Frame 1 (ACK): {reply.hex().upper()}")
                else:
                    payload = extract_data_from_frame(reply)
                    image_bytes.extend(payload)
                    print(f"   📥 Received Chunk {chunk_count}: {len(payload)} bytes")
                chunk_count += 1
        except asyncio.TimeoutError:
            if len(image_bytes) > 0:
                with open("test_uhf.jpg", "wb") as f:
                    f.write(image_bytes)
                print(f"✅ UHF Success! Assembled {chunk_count-1} chunks.")
                print(f"💾 Saved 'test_uhf.jpg' ({len(image_bytes)} bytes)\n")
            else:
                print("❌ UHF Failed: No data received\n")

        # 5. Delete Image (DIMG)
        print(f"➤ [5/6] Sending DIMG (0x0D) to delete Image {image_id_to_test} from OBC Memory...")
        await ws.send(build_frame(0xB0, 0xA1, 0x0D, img_id_bytes))
        print(f"⬅️ Reply: {(await ws.recv()).hex().upper()}\n")

        # 6. Verify Deletion (Sanity Check)
        print("➤ [6/6] Verifying deletion (Attempting S-Band download again)...")
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{http_url}/sband/download/{image_id_to_test}")
            if resp.status_code == 200 and b"error" not in resp.content:
                print("❌ WARNING: Image was NOT deleted successfully!\n")
            else:
                print("✅ Verified: Image successfully deleted. Memory is clean!\n")
                
        print("🏁 --- TEST COMPLETE --- 🏁\n")

if __name__ == "__main__":
    asyncio.run(test_image_lifecycle())