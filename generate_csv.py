import os
import glob

IMAGE_DIR = "./images"  
CSV_FILE = "image_dataset.csv"


supported_extensions = ('*.jpg', '*.jpeg', '*.png')
image_files = []

for ext in supported_extensions:
    image_files.extend(glob.glob(os.path.join(IMAGE_DIR, ext)))


image_files.sort()

with open(CSV_FILE, "w", encoding="utf-8") as f:
    for filepath in image_files:
        filename = os.path.basename(filepath)
        
        label = filename.split('_')[0] if '_' in filename else "unknown"
        
        f.write(f"{filename}|{label}|||||||test|\n")

print(f"✅created {CSV_FILE}: images {len(image_files)}")