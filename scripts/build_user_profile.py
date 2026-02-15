#!/usr/bin/env python3
"""
Build multi-centroid user interest profiles using HDBSCAN clustering.

Usage: python3 build_user_profile.py <input.json> <output.json>

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


def weighted_average(vectors: np.ndarray, weights: np.ndarray) -> np.ndarray:
    """Compute weighted average of vectors, L2-normalized."""
    if len(vectors) == 0:
        return np.zeros(512, dtype=np.float32)
    avg = np.average(vectors, axis=0, weights=weights)
    norm = np.linalg.norm(avg)
    if norm > 0:
        avg = avg / norm
    return avg


def cluster_interests(vectors: np.ndarray, weights: np.ndarray, min_cluster_size: int = 5) -> list:
    """
    Cluster interest vectors using HDBSCAN and return centroids.
    
    Returns list of dicts: [{clusterId, centroid, weight, postCount}, ...]
    """
    import hdbscan

    # Run HDBSCAN
    clusterer = hdbscan.HDBSCAN(
        min_cluster_size=min_cluster_size,
        min_samples=2,
        metric='euclidean',
        cluster_selection_method='eom',
    )
    labels = clusterer.fit_predict(vectors)

    unique_labels = set(labels)
    # Remove noise label (-1)
    unique_labels.discard(-1)

    if len(unique_labels) == 0:
        # HDBSCAN found no clusters — fall back to single centroid
        centroid = weighted_average(vectors, weights)
        return [{
            'clusterId': 0,
            'centroid': centroid.tolist(),
            'weight': 1.0,
            'postCount': int(len(vectors)),
        }]

    # Compute centroid for each cluster
    centroids = []
    total_weight = 0.0

    for label in sorted(unique_labels):
        mask = labels == label
        cluster_vectors = vectors[mask]
        cluster_weights = weights[mask]
        cluster_weight_sum = float(np.sum(cluster_weights))
        total_weight += cluster_weight_sum

        centroid = weighted_average(cluster_vectors, cluster_weights)
        centroids.append({
            'clusterId': int(label),
            'centroid': centroid.tolist(),
            'weight': cluster_weight_sum,
            'postCount': int(np.sum(mask)),
        })

    # Also include noise points as a low-weight "miscellaneous" cluster
    noise_mask = labels == -1
    noise_count = int(np.sum(noise_mask))
    if noise_count >= 3:
        noise_vectors = vectors[noise_mask]
        noise_weights = weights[noise_mask]
        noise_weight_sum = float(np.sum(noise_weights))
        total_weight += noise_weight_sum

        centroid = weighted_average(noise_vectors, noise_weights)
        centroids.append({
            'clusterId': max(c['clusterId'] for c in centroids) + 1,
            'centroid': centroid.tolist(),
            'weight': noise_weight_sum,
            'postCount': noise_count,
        })

    # Normalize weights to sum to 1.0
    if total_weight > 0:
        for c in centroids:
            c['weight'] = c['weight'] / total_weight

    # Sort by weight descending, keep top 5
    centroids.sort(key=lambda c: c['weight'], reverse=True)
    centroids = centroids[:5]

    # Re-normalize after truncation
    weight_sum = sum(c['weight'] for c in centroids)
    if weight_sum > 0:
        for c in centroids:
            c['weight'] = c['weight'] / weight_sum

    return centroids


def main():
    parser = argparse.ArgumentParser(description='Build multi-centroid user profiles via HDBSCAN')
    parser.add_argument('input_json', help='Path to input JSON file')
    parser.add_argument('output_json', help='Path to output JSON file')
    parser.add_argument('--min-cluster-size', type=int, default=5,
                        help='HDBSCAN min_cluster_size (default: 5)')
    args = parser.parse_args()

    # Read input
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
