"""
YOLO Object Detection and Image Classification Script

- Scans Telegram images
- Runs YOLOv8 detection
- Records detected objects + confidence
- Classifies images into business categories
- Saves results to CSV
"""

from ultralytics import YOLO
from pathlib import Path
import pandas as pd

# --------------------------------------------------
# Configuration
# --------------------------------------------------

IMAGE_DIR = Path("data/raw/images")
OUTPUT_CSV = Path("data/raw/yolo_image_enrichment.csv")
MODEL_NAME = "yolov8n.pt"

# YOLO product proxy objects
PRODUCT_OBJECTS = {"bottle", "box", "jar", "package", "container"}

# --------------------------------------------------
# Load YOLO model
# --------------------------------------------------

model = YOLO(MODEL_NAME)

# --------------------------------------------------
# Helper functions
# --------------------------------------------------

def classify_image(objects: set) -> str:
    """
    Business classification based on detected objects
    """
    has_person = "person" in objects
    has_product = any(obj in PRODUCT_OBJECTS for obj in objects)

    if has_person and has_product:
        return "promotional"
    elif has_product and not has_person:
        return "product_display"
    elif has_person and not has_product:
        return "lifestyle"
    else:
        return "other"


def run_detection(image_path: Path, channel_name: str) -> dict:
    """
    Run YOLO detection on a single image and return structured result
    """
    results = model(image_path, verbose=False)

    detected_objects = []
    confidence_scores = []

    for r in results:
        for box in r.boxes:
            label = model.names[int(box.cls[0])]
            confidence = float(box.conf[0])

            detected_objects.append(label)
            confidence_scores.append(confidence)

    objects_set = set(detected_objects)

    return {
        "message_id": image_path.stem,
        "channel_name": channel_name,
        "image_path": str(image_path),
        "detected_objects": ",".join(detected_objects),
        "object_count": len(detected_objects),
        "avg_confidence": round(sum(confidence_scores) / len(confidence_scores), 3)
        if confidence_scores else None,
        "has_person": "person" in objects_set,
        "has_product": any(o in PRODUCT_OBJECTS for o in objects_set),
        "image_category": classify_image(objects_set),
    }

# --------------------------------------------------
# Main execution
# --------------------------------------------------

records = []

for channel_dir in IMAGE_DIR.iterdir():
    if not channel_dir.is_dir():
        continue

    channel_name = channel_dir.name

    for image_file in channel_dir.glob("*.jpg"):
        try:
            record = run_detection(image_file, channel_name)
            records.append(record)
        except Exception as e:
            print(f"Error processing {image_file}: {e}")

# --------------------------------------------------
# Save results
# --------------------------------------------------

df = pd.DataFrame(records)
df.to_csv(OUTPUT_CSV, index=False)

print(f"YOLO enrichment complete. Saved {len(df)} rows to {OUTPUT_CSV}")
