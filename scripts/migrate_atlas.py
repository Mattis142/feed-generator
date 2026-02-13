import numpy as np
import struct
from qdrant_client import QdrantClient
from qdrant_client.models import (
    PointStruct, VectorParams, Distance, 
    OptimizersConfigDiff, HnswConfigDiff, 
    ScalarQuantization, ScalarQuantizationConfig, ScalarType
)

# --- CONFIGURATION ---
COLLECTION_NAME = "social_atlas"
BATCH_SIZE = 5000  # High batch size for speed
QDRANT_URL = "http://localhost:6333"

client = QdrantClient(url=QDRANT_URL)

def load_and_upsert():
    print("🚀 Initializing Atlas Migration...")

    # 1. Load Metadata & Binary Data efficiently
    print("Reading points.bin...")
    # points.bin is Float32 [x, y, x, y...] -> Reshape to (N, 2)
    coords = np.fromfile("points.bin", dtype=np.float32).reshape(-1, 2)
    num_points = len(coords)

    print("Reading followers.bin...")
    followers = np.fromfile("followers.bin", dtype=np.uint32)

    print("Reading handles.bin...")
    with open("handles.bin", "rb") as f:
        # First 4 bytes = number of handles (uint32)
        n = struct.unpack('<I', f.read(4))[0]
        # Next (n+1) * 4 bytes = offsets (uint32)
        offsets = np.fromfile(f, dtype=np.uint32, count=n+1)
        # The rest is the UTF-8 string blob
        string_blob = f.read()

    # 2. Create optimized Qdrant Collection
    # Optimization: on_disk=True for RAM efficiency, m=0 to disable indexing during load
    print(f"Configuring collection: {COLLECTION_NAME}")
    client.recreate_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=VectorParams(
            size=2, 
            distance=Distance.EUCLID,
            on_disk=True  
        ),
        optimizers_config=OptimizersConfigDiff(indexing_threshold=0), 
        hnsw_config=HnswConfigDiff(m=0), 
        quantization_config=ScalarQuantization(
            scalar=ScalarQuantizationConfig(
                type=ScalarType.INT8,
                always_ram=True 
            )
        )
    )

    # 3. Batch Ingestion
    print(f"Starting ingestion of {num_points:,} users...")
    
    for i in range(0, num_points, BATCH_SIZE):
        batch_end = min(i + BATCH_SIZE, num_points)
        points = []
        
        for idx in range(i, batch_end):
            # Extract handle from blob using offsets
            start, end = offsets[idx], offsets[idx+1]
            handle = string_blob[start:end].decode('utf-8') if end > start else ""
            
            if not handle: continue

            points.append(PointStruct(
                id=idx,
                vector=coords[idx].tolist(),
                payload={
                    "handle": handle,
                    "followers": int(followers[idx]),
                    "x": float(coords[idx][0]),
                    "y": float(coords[idx][1])
                }
            ))
        
        client.upsert(collection_name=COLLECTION_NAME, points=points)
        if i % 100000 == 0:
            print(f"Indexed {i:,} / {num_points:,} users...")

    # 4. Re-enable Indexing
    print("Finalizing... Re-enabling HNSW indexing.")
    client.update_collection(
        collection_name=COLLECTION_NAME,
        optimizer_config=OptimizersConfigDiff(indexing_threshold=20000),
        hnsw_config=HnswConfigDiff(m=16) # Standard HNSW complexity
    )
    print("✅ Migration Complete! Atlas is now live.")

if __name__ == "__main__":
    load_and_upsert()
