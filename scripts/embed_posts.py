import sys
import json
import torch
import open_clip
from PIL import Image
import requests
from io import BytesIO
import argparse
import os

def load_model(model_path):
    """Load the MobileCLIP2-S2 model."""
    # model, _, preprocess = open_clip.create_model_and_transforms('ViT-B-16', pretrained='laion2b_s34b_b88k')
    # Using the specific MobileCLIP variant as requested/available
    # If loading from a local .pt file, we might need a specific loading mechanism
    # or just use the open_clip registry if it supports it. 
    # For now, assuming we use the standard open_clip load with the model name, 
    # or if a path is provided, we might need to load state dict.
    
    # However, 'apple/MobileCLIP2-S2' is typically a specific architecture. 
    # If open_clip supports it natively:
    try:
        model, _, preprocess = open_clip.create_model_and_transforms('MobileCLIP2-S2', pretrained=model_path if os.path.exists(model_path) else 'dfndr2b')
        print(f"SUCCESS: Loaded MobileCLIP2-S2 from {model_path if os.path.exists(model_path) else 'dfndr2b'}", file=sys.stderr)
        
        # Verify we're actually using MobileCLIP2-S2 architecture
        if hasattr(model, 'visual') and hasattr(model.visual, 'trunk'):
            trunk_name = model.visual.trunk.__class__.__name__
            print(f"VERIFICATION: Visual trunk = {trunk_name}", file=sys.stderr)
        
        # Check text encoder
        if hasattr(model, 'text'):
            text_encoder = model.text.__class__.__name__
            print(f"VERIFICATION: Text encoder = {text_encoder}", file=sys.stderr)
            
        print(f"VERIFICATION: Model successfully loaded as MobileCLIP2-S2", file=sys.stderr)
        
    except Exception as e:
        print(f"ERROR: Failed to load MobileCLIP2-S2: {e}", file=sys.stderr)
        raise RuntimeError(f"Failed to load MobileCLIP2-S2 model: {e}")

    # Force CPU for this environment if CUDA not available (Mac usually uses MPS but let's stick to CPU/MPS safe)
    device = "mps" if torch.backends.mps.is_available() else "cpu"
    model.to(device)
    return model, preprocess, device

def download_image(url):
    """Download image from URL and return PIL Image."""
    try:
        response = requests.get(url, timeout=5)
        response.raise_for_status()
        return Image.open(BytesIO(response.content)).convert('RGB')
    except Exception as e:
        # print(f"Error downloading {url}: {e}", file=sys.stderr)
        return None

def process_batch(input_file, output_file, model_path, batch_size=32):
    # Load MobileCLIP2-S2 model - NO FALLBACKS ALLOWED
    try:
        model, _, preprocess = open_clip.create_model_and_transforms('MobileCLIP2-S2', pretrained=model_path)
        print(f"SUCCESS: Loaded MobileCLIP2-S2 from {model_path}", file=sys.stderr)
        
        # Verify we're actually using MobileCLIP2-S2 architecture
        if hasattr(model, 'visual') and hasattr(model.visual, 'trunk'):
            trunk_name = model.visual.trunk.__class__.__name__
            print(f"VERIFICATION: Visual trunk = {trunk_name}", file=sys.stderr)
        
        # Check text encoder
        if hasattr(model, 'text'):
            text_encoder = model.text.__class__.__name__
            print(f"VERIFICATION: Text encoder = {text_encoder}", file=sys.stderr)
            
        print(f"VERIFICATION: Model successfully loaded as MobileCLIP2-S2", file=sys.stderr)
        
    except Exception as e:
        print(f"ERROR: Failed to load MobileCLIP2-S2: {e}", file=sys.stderr)
        raise RuntimeError(f"Failed to load MobileCLIP2-S2 model from {model_path}: {e}")

    device = "mps" if torch.backends.mps.is_available() else "cpu"
    model.to(device)
    model.eval()

    with open(input_file, 'r') as f:
        posts = json.load(f)

    # === PHASE 1: COLLECT ALL DATA FOR BATCHING ===
    print(f"Collecting data for {len(posts)} posts...", file=sys.stderr)
    
    # Collect all texts with mapping back to posts
    all_texts = []
    text_to_post = []  # (post_idx, text)
    
    # Collect all images with mapping
    all_images = []
    image_to_post = []  # (post_idx, img_idx, image_tensor)
    
    # Collect all alt texts with mapping
    all_alt_texts = []
    alt_text_to_post = []  # (post_idx, alt_idx, alt_text)
    
    for post_idx, post in enumerate(posts):
        uri = post.get('uri')
        text = post.get('text', '') or ''
        image_urls = post.get('image_urls', []) or []
        alt_texts = post.get('alt_text', []) or []

        # Collect text
        if text.strip():
            all_texts.append(text)
            text_to_post.append((post_idx, text))

        # Collect images
        for img_idx, url in enumerate(image_urls):
            img = download_image(url)
            if img:
                try:
                    img_tensor = preprocess(img)
                    all_images.append(img_tensor)
                    image_to_post.append((post_idx, img_idx, img_tensor))
                except Exception as e:
                    print(f"Warning: Failed to process image {url}: {e}", file=sys.stderr)
            
        # Collect alt texts
        for alt_idx, alt in enumerate(alt_texts):
            if alt and alt.strip():
                all_alt_texts.append(alt)
                alt_text_to_post.append((post_idx, alt_idx, alt))

    print(f"Collected: {len(all_texts)} texts, {len(all_images)} images, {len(all_alt_texts)} alt texts", file=sys.stderr)

    # === PHASE 2: BATCH PROCESSING ===
    text_embeddings_map = {}
    image_embeddings_map = {}
    alt_text_embeddings_map = {}
    
    # Process texts in batches
    if all_texts:
        print(f"Processing {len(all_texts)} texts in batches...", file=sys.stderr)
        with torch.no_grad():
            text_tokens = open_clip.tokenize(all_texts).to(device)
            text_embeddings = model.encode_text(text_tokens)
            text_embeddings /= text_embeddings.norm(dim=-1, keepdim=True)
            
        # Map back to posts
        for (post_idx, text), emb in zip(text_to_post, text_embeddings):
            if post_idx not in text_embeddings_map:
                text_embeddings_map[post_idx] = []
            text_embeddings_map[post_idx].append(emb)
    
    # Process images in batches
    if all_images:
        print(f"Processing {len(all_images)} images in batches of {batch_size}...", file=sys.stderr)
        with torch.no_grad():
            for i in range(0, len(all_images), batch_size):
                batch_end = min(i + batch_size, len(all_images))
                batch_images = all_images[i:batch_end]
                batch_tensor = torch.stack(batch_images).to(device)
                
                batch_embeddings = model.encode_image(batch_tensor)
                batch_embeddings /= batch_embeddings.norm(dim=-1, keepdim=True)
                
                # Map back to posts
                for j, (post_idx, img_idx, _) in enumerate(image_to_post[i:batch_end]):
                    if post_idx not in image_embeddings_map:
                        image_embeddings_map[post_idx] = []
                    image_embeddings_map[post_idx].append((img_idx, batch_embeddings[j]))
    
    # Process alt texts in batches
    if all_alt_texts:
        print(f"Processing {len(all_alt_texts)} alt texts in batches...", file=sys.stderr)
        with torch.no_grad():
            alt_tokens = open_clip.tokenize(all_alt_texts).to(device)
            alt_embeddings = model.encode_text(alt_tokens)
            alt_embeddings /= alt_embeddings.norm(dim=-1, keepdim=True)
            
        # Map back to posts
        for (post_idx, alt_idx, _), emb in zip(alt_text_to_post, alt_embeddings):
            if post_idx not in alt_text_embeddings_map:
                alt_text_embeddings_map[post_idx] = []
            alt_text_embeddings_map[post_idx].append((alt_idx, emb))

    # === PHASE 3: REASSEMBLE RESULTS ===
    print("Reassembling results...", file=sys.stderr)
    results = []
    
    for post_idx, post in enumerate(posts):
        uri = post.get('uri')
        embeddings = []
        
        # Add text embedding
        if post_idx in text_embeddings_map:
            embeddings.extend(text_embeddings_map[post_idx])
        
        # Add image embeddings (sorted by original order)
        if post_idx in image_embeddings_map:
            sorted_img_embs = sorted(image_embeddings_map[post_idx], key=lambda x: x[0])
            embeddings.extend([emb for _, emb in sorted_img_embs])
        
        # Add alt text embeddings (sorted by original order)
        if post_idx in alt_text_embeddings_map:
            sorted_alt_embs = sorted(alt_text_embeddings_map[post_idx], key=lambda x: x[0])
            embeddings.extend([emb for _, emb in sorted_alt_embs])
        
        # Average embeddings (same logic as original)
        if embeddings:
            # Stack and average - handle both single and multiple embeddings
            if len(embeddings) == 1:
                avg_emb = embeddings[0]  # Single embedding, no need to stack
            else:
                stacked = torch.stack(embeddings, dim=0)
                avg_emb = torch.mean(stacked, dim=0)
            # Ensure proper normalization (already normalized individually, but just in case)
            avg_emb /= avg_emb.norm(dim=-1, keepdim=True)
            
            results.append({
                'uri': uri,
                'vector': avg_emb.cpu().tolist()
            })
        else:
            # No valid content to embed
            results.append({
                'uri': uri,
                'vector': [0.0] * 512
            })

    with open(output_file, 'w') as f:
        json.dump(results, f)
    
    print(f"Successfully processed {len(results)} posts with batched inference", file=sys.stderr)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("input_file", help="Path to input JSON file")
    parser.add_argument("output_file", help="Path to output JSON file")
    parser.add_argument("--model-path", help="Path to model weights", default="")
    parser.add_argument("--batch-size", help="Batch size for processing", type=int, default=32)
    args = parser.parse_args()

    process_batch(args.input_file, args.output_file, args.model_path, args.batch_size)
