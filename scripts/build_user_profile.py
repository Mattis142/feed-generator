#!/usr/bin/env python3
"""
Build multi-centroid user interest profiles using HDBSCAN clustering.

Usage (file-based):
    python3 build_user_profile.py <input.json> <output.json>

Usage (user DID, fetches from Qdrant):
    python3 build_user_profile.py --user-did <did:plc:...> <output.json>

Input JSON:  [{"vector": [512 floats], "weight": float, "interactionType": "like"|"repost"|"requestMore"|"requestLess"}, ...]
Output JSON: [{"clusterId": int, "centroid": [512 floats], "weight": float, "postCount": int}, ...]

Each centroid represents a distinct area of interest for the user.
Falls back to a single weighted average if fewer than 10 data points.
Caps at 5 centroids (by cluster weight).
"""

import sys
import json
import argparse
import numpy as np
import os
from scipy.spatial.distance import pdist, squareform


def weighted_average(vectors: np.ndarray, weights: np.ndarray) -> np.ndarray:
    """Compute weighted average of vectors, L2-normalized."""
    if len(vectors) == 0:
        return np.zeros(512, dtype=np.float32)
    avg = np.average(vectors, axis=0, weights=weights)
    norm = np.linalg.norm(avg)
    if norm > 0:
        avg = avg / norm
    return avg


def consolidate_centroids(centroids: list, threshold: float = 0.85) -> list:
    """
    Ensure centroids are distinct by merging those that are too similar.
    Implements the 'Magnet' model by preventing redundant/overlapping poles.
    """
    if len(centroids) <= 1:
        return centroids

    final_centroids = []
    # Sort by weight descending to ensure strong magnets stay stable
    centroids.sort(key=lambda c: c['weight'], reverse=True)
    
    while centroids:
        base = centroids.pop(0)
        to_merge_indices = []
        
        for i, other in enumerate(centroids):
            similarity = np.dot(base['centroid'], other['centroid'])
            if similarity > threshold:
                to_merge_indices.append(i)
        
        if to_merge_indices:
            # Merge similar centroids into the base
            print(f"  [Magnet] Consolidating {len(to_merge_indices)} centroids into Cluster {base['clusterId']} (similarity > {threshold})", file=sys.stderr)
            base_vec = np.array(base['centroid']) * base['weight']
            total_weight = base['weight']
            total_posts = base['postCount']
            
            for i in sorted(to_merge_indices, reverse=True):
                m = centroids.pop(i)
                base_vec += np.array(m['centroid']) * m['weight']
                total_weight += m['weight']
                total_posts += m['postCount']
            
            # Re-normalize the new consolidated magnet
            norm = np.linalg.norm(base_vec)
            if norm > 0:
                base_vec = base_vec / norm
            
            base['centroid'] = base_vec.tolist()
            base['weight'] = total_weight
            base['postCount'] = total_posts
            
        final_centroids.append(base)
        
    return final_centroids


def cluster_interests(vectors: np.ndarray, weights: np.ndarray, min_cluster_size: int = 5) -> list:
    """
    Cluster interest vectors using HDBSCAN with cosine distance and return centroids.
    
    Since HDBSCAN doesn't natively support cosine metric, we pre-compute cosine distance
    matrix and pass metric='precomputed' to HDBSCAN.
    
    Returns list of dicts: [{clusterId, centroid, weight, postCount}, ...]
    """
    import hdbscan

    # Pre-compute pairwise cosine distances (HDBSCAN requires this for cosine metric)
    # pdist returns distances in condensed form; squareform converts to full matrix
    print(f"    Computing cosine distance matrix for {len(vectors)} vectors...", file=sys.stderr)
    cosine_distances = squareform(pdist(vectors, metric='cosine'))
    
    # Run HDBSCAN with pre-computed distance matrix
    print(f"    Running HDBSCAN with cosine distance metric (precomputed)...", file=sys.stderr)
    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size,
        min_samples=2,
        metric='precomputed',  # Use pre-computed distance matrix
        cluster_selection_method='eom',
    )
    labels = clusterer.fit_predict(cosine_distances)

    unique_labels = set(labels)
    # Remove noise label (-1)
    unique_labels.discard(-1)

    print(f"    HDBSCAN Result: {len(unique_labels)} clusters found + {int(np.sum(labels == -1))} noise points", file=sys.stderr)

    raw_centroids = []
    if len(unique_labels) == 0:
        # HDBSCAN found no clusters — fall back to single centroid
        print(f"    ⚠️  No clusters detected by HDBSCAN, falling back to single weighted average", file=sys.stderr)
        raw_centroids = [{
            'clusterId': 0,
            'centroid': weighted_average(vectors, weights).tolist(),
            'weight': 1.0,
            'postCount': int(len(vectors)),
        }]
    else:
        # Compute centroid for each cluster
        total_weight = 0.0
        for label in sorted(unique_labels):
            mask = labels == label
            cluster_vectors = vectors[mask]
            cluster_weights = weights[mask]
            cluster_weight_sum = float(np.sum(cluster_weights))
            total_weight += cluster_weight_sum

            centroid = weighted_average(cluster_vectors, cluster_weights)
            raw_centroids.append({
                'clusterId': int(label),
                'centroid': centroid.tolist(),
                'weight': cluster_weight_sum,
                'postCount': int(np.sum(mask)),
            })
            print(f"    Raw Cluster {int(label)}: {int(np.sum(mask))} items ({cluster_weight_sum:.2f} weight)", file=sys.stderr)

    # Consolidate magnets to ensure distinct poles (lowered threshold for more diversity)
    print(f"    Consolidating with 0.70 similarity threshold...", file=sys.stderr)
    centroids = consolidate_centroids(raw_centroids, threshold=0.70)

    # Also include significant noise points as a Low-Pass Miscellaneous cluster
    noise_mask = labels == -1
    noise_count = int(np.sum(noise_mask))
    if noise_count >= 5:
        noise_vectors = vectors[noise_mask]
        noise_weights = weights[noise_mask]
        noise_weight_sum = float(np.sum(noise_weights))
        
        centroid = weighted_average(noise_vectors, noise_weights)
        
        # Check if noise is too similar to any existing magnet
        is_distinct = True
        for c in centroids:
            similarity = np.dot(centroid, c['centroid'])
            if similarity > 0.70:  # Lowered threshold to match consolidation
                is_distinct = False
                print(f"    Noise cluster too similar to Cluster {c['clusterId']} ({similarity:.3f} > 0.70), skipping", file=sys.stderr)
                break
        
        if is_distinct:
            print(f"    Adding noise cluster: {noise_count} items ({noise_weight_sum:.2f} weight)", file=sys.stderr)
            centroids.append({
                'clusterId': max([c['clusterId'] for c in centroids] + [-1]) + 1,
                'centroid': centroid.tolist(),
                'weight': noise_weight_sum,
                'postCount': noise_count,
            })

    # Normalize weights to sum to 1.0
    total_final_weight = sum(c['weight'] for c in centroids)
    if total_final_weight > 0:
        for c in centroids:
            c['weight'] = c['weight'] / total_final_weight

    # Sort by weight descending, keep up to 10 magnets (more diversity)
    centroids.sort(key=lambda c: c['weight'], reverse=True)
    centroids = centroids[:10]

    # Re-normalize after truncation
    weight_sum = sum(c['weight'] for c in centroids)
    if weight_sum > 0:
        for c in centroids:
            c['weight'] = c['weight'] / weight_sum

    return centroids


def fetch_embeddings_for_user(user_did: str) -> list:
    """
    Fetch post embeddings for a specific user from Qdrant.
    Requires QDRANT_URL environment variable.
    
    Returns list of dicts: [{"vector": [...], "weight": 1.0, "interactionType": "like"}, ...]
    """
    try:
        from qdrant_client import QdrantClient
        from qdrant_client.models import PointStruct
    except ImportError:
        print(f"ERROR: qdrant-client not installed. Install with: pip install qdrant-client", file=sys.stderr)
        return []

    qdrant_url = os.getenv('QDRANT_URL', 'http://localhost:6333')
    
    try:
        client = QdrantClient(url=qdrant_url, timeout=30.0)
        print(f"  Connecting to Qdrant at {qdrant_url}...", file=sys.stderr)
        
        # Search for all post embeddings (vectors with small batch size to not timeout)
        collection_name = 'feed_posts'  # Name of the collection storing post embeddings
        
        # Simple query: get all points with a limit
        # We'll use a scroll approach to get embeddings for this user's interactions
        print(f"  Fetching embeddings from Qdrant collection '{collection_name}'...", file=sys.stderr)
        
        # Query: search in the collection with filters if available
        # For now, we'll fetch points and check if they're relevant to the user
        points, _ = client.scroll(
            collection_name=collection_name,
            limit=1000,
            with_vectors=True,
            with_payload=True,
        )
        
        print(f"  Found {len(points)} total points in collection", file=sys.stderr)
        
        # Filter to user-relevant embeddings (this is a placeholder)
        # In production, you'd filter by user interaction metadata
        embeddings = []
        for point in points:
            if point.vector and len(point.vector) == 512:
                embeddings.append({
                    "vector": point.vector,
                    "weight": 1.0,
                    "interactionType": "like"
                })
        
        print(f"  Extracted {len(embeddings)} valid embeddings for user", file=sys.stderr)
        return embeddings
        
    except Exception as e:
        print(f"ERROR fetching embeddings from Qdrant: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return []


def main():
    parser = argparse.ArgumentParser(description='Build multi-centroid user profiles via HDBSCAN')
    
    # Mutually exclusive input source
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument('input_json', nargs='?', default=None,
                             help='Path to input JSON file (file-based mode)')
    input_group.add_argument('--user-did', type=str, dest='user_did',
                             help='User DID to fetch embeddings for (Qdrant mode)')
    
    parser.add_argument('output_json', help='Path to output JSON file')
    parser.add_argument('--min-cluster-size', type=int, default=3,
                        help='HDBSCAN min_cluster_size (default: 3)')
    args = parser.parse_args()

    # Determine input source
    if args.user_did:
        # Qdrant mode: fetch embeddings for user
        print(f"[build_user_profile] Fetching embeddings for user {args.user_did}...", file=sys.stderr)
        data = fetch_embeddings_for_user(args.user_did)
        if not data:
            print(f"ERROR: No embeddings found for user {args.user_did}", file=sys.stderr)
            with open(args.output_json, 'w', encoding='utf-8') as f:
                json.dump([], f)
            sys.exit(1)
    else:
        # File mode: read from input JSON
        try:
            with open(args.input_json, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except Exception as e:
            print(f"ERROR reading input: {e}", file=sys.stderr)
            sys.exit(1)

    if not data:
        with open(args.output_json, 'w', encoding='utf-8') as f:
            json.dump([], f)
        sys.exit(0)

    # Interaction type weights
    INTERACTION_WEIGHTS = {
        'like': 1.0,
        'repost': 1.5,
        'requestMore': 3.0,
        'requestLess': -2.0,
    }

    # Parse vectors and weights
    vectors = []
    weights = []

    for item in data:
        vec = item.get('vector')
        if vec is None or len(vec) != 512:
            continue

        interaction_type = item.get('interactionType', 'like')
        base_weight = INTERACTION_WEIGHTS.get(interaction_type, 1.0)
        custom_weight = item.get('weight', 1.0)

        # For negative interactions (requestLess), we invert the vector direction
        # so the centroid moves AWAY from disliked content
        if base_weight < 0:
            vec = [-v for v in vec]
            base_weight = abs(base_weight)

        vectors.append(vec)
        weights.append(base_weight * custom_weight)

    if not vectors:
        with open(args.output_json, 'w', encoding='utf-8') as f:
            json.dump([], f)
        sys.exit(0)

    vectors_np = np.array(vectors, dtype=np.float32)
    weights_np = np.array(weights, dtype=np.float32)

    print(f"Processing {len(vectors)} interaction vectors...", file=sys.stderr)

    # If fewer than 10 data points, skip clustering and just average
    if len(vectors) < 10:
        print(f"  Too few data points ({len(vectors)}) for clustering, using weighted average", file=sys.stderr)
        centroid = weighted_average(vectors_np, weights_np)
        result = [{
            'clusterId': 0,
            'centroid': centroid.tolist(),
            'weight': 1.0,
            'postCount': len(vectors),
        }]
    else:
        print(f"  Running HDBSCAN clustering (min_cluster_size={args.min_cluster_size})...", file=sys.stderr)
        result = cluster_interests(vectors_np, weights_np, args.min_cluster_size)
        print(f"  Found {len(result)} interest clusters", file=sys.stderr)
        for c in result:
            print(f"    Cluster {c['clusterId']}: {c['postCount']} posts, weight={c['weight']:.3f}", file=sys.stderr)

    # Write output
    with open(args.output_json, 'w', encoding='utf-8') as f:
        json.dump(result, f)

    print(f"Done. Wrote {len(result)} centroids to {args.output_json}", file=sys.stderr)


if __name__ == '__main__':
    main()
