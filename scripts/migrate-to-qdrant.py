import struct
import numpy as np
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct, VectorParams, Distance
import sys
import os

def load_data(base_path="./"):
    print("Loading Points (Coordinates)...")
    # points.bin is a flat list of Float32: [x0, y0, x1, y1, ...]
    # We reshape it into an (N, 2) matrix
    with open(f"{base_path}points.bin", "rb") as f:
        points = np.frombuffer(f.read(), dtype=np.float32).reshape(-1, 2)

    print("Loading Followers (Social Proof)...")
    # followers.bin is a flat list of Uint32
    with open(f"{base_path}followers.bin", "rb") as f:
        followers = np.frombuffer(f.read(), dtype=np.uint32)

    print("Loading Handles (Identity)...")
    # handles.bin is a custom binary format:
    # [Count (uint32)] [Offsets (uint32 array)] [String Blob]
    handles = []
    with open(f"{base_path}handles.bin", "rb") as f:
        data = f.read()
        
        # Read the number of handles (first 4 bytes)
        num_handles = struct.unpack('<I', data[:4])[0]
        
        # Read the offsets (next (N+1) * 4 bytes)
        offset_start = 4
        offset_end = offset_start + (num_handles + 1) * 4
        offsets = np.frombuffer(data[offset_start:offset_end], dtype=np.uint32)
        
        # The rest is the string blob
        str_data = data[offset_end:]
        
        # Decode strings using offsets
        for i in range(num_handles):
            start = offsets[i]
            end = offsets[i+1]
            if end > start:
                handle = str_data[start:end].decode('utf-8')
                handles.append(handle)
            else:
                handles.append("") # Empty handle case

    return handles, points, followers

def migrate_to_qdrant():
    print("Starting migration to Qdrant...")
    
    # Load the data
    handles, coordinates, follower_counts = load_data()
    
    print(f"Loaded {len(handles)} users.")
    print(f"Sample: User {handles[0]} is at {coordinates[0]} with {follower_counts[0]} followers.")
    
    # Connect to Qdrant
    client = QdrantClient("http://localhost:6333")
    
    try:
        # Create a specific collection for the Social Map
        print("Creating 'social_atlas' collection...")
        client.recreate_collection(
            collection_name="social_atlas",
            vectors_config=VectorParams(size=2, distance=Distance.EUCLIDEAN)
        )
        print("Collection created successfully.")
        
        # Batch upload
        batch_size = 1000
        points_batch = []
        valid_users = 0
        
        for i, handle in enumerate(handles):
            # Only index if handle is valid
            if not handle: 
                continue
            
            # Create the payload
            payload = {
                "handle": handle,
                "followers": int(follower_counts[i]),
                # We store the raw 2D coordinate as the vector
                "map_x": float(coordinates[i][0]),
                "map_y": float(coordinates[i][1])
            }
            
            points_batch.append(PointStruct(
                id=i, # Use the index as ID, or hash the handle
                vector=coordinates[i].tolist(),
                payload=payload
            ))
            
            valid_users += 1
            
            # Upload in batches
            if len(points_batch) >= batch_size:
                print(f"Uploading batch of {len(points_batch)} points...")
                client.upsert(collection_name="social_atlas", points=points_batch)
                points_batch = []
        
        # Upload remaining points
        if points_batch:
            print(f"Uploading final batch of {len(points_batch)} points...")
            client.upsert(collection_name="social_atlas", points=points_batch)
        
        print(f"Migration completed! Uploaded {valid_users} valid users to Qdrant.")
        
        # Verify the upload
        collection_info = client.get_collection("social_atlas")
        print(f"Collection now has {collection_info.points_count} points.")
        
    except Exception as e:
        print(f"Error during migration: {e}")
        sys.exit(1)

if __name__ == "__main__":
    migrate_to_qdrant()
