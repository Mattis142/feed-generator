#!/usr/bin/env python3
"""
Extract keywords using YAKE + TF-IDF filtering.

Usage: python3 extract_keywords.py <liked_corpus.txt> <background_corpus.txt>

Output: keyword<TAB>score (one per line)
"""

import sys
import re
from collections import Counter
import math

try:
    import yake
except ImportError:
    print("ERROR: yake library not installed. Install with: pip install yake", file=sys.stderr)
    sys.exit(1)


def read_corpus(filepath):
    """Read corpus file and return list of documents."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        # Split by double newlines (documents separated by blank lines)
        docs = [doc.strip() for doc in content.split('\n\n') if doc.strip()]
        return docs
    except Exception as e:
        print(f"ERROR reading {filepath}: {e}", file=sys.stderr)
        sys.exit(1)


def compute_tf_idf(keywords, liked_docs, background_docs):
    """
    Compute TF-IDF scores for keywords.
    
    Args:
        keywords: List of keyword strings from YAKE
        liked_docs: List of documents from liked posts
        background_docs: List of documents from background corpus
    
    Returns:
        Dict mapping keyword -> TF-IDF score
    """
    all_docs = liked_docs + background_docs
    n_docs = len(all_docs)
    
    # Normalize keywords and documents for matching
    keyword_lower = [kw.lower() for kw in keywords]
    liked_lower = [doc.lower() for doc in liked_docs]
    all_lower = [doc.lower() for doc in all_docs]
    
    # Count document frequency (DF) for each keyword
    df_counts = Counter()
    for keyword in keyword_lower:
        for doc in all_lower:
            if keyword in doc:
                df_counts[keyword] += 1
                break  # Count once per document
    
    # Compute TF-IDF scores
    keyword_scores = {}
    
    for i, keyword in enumerate(keyword_lower):
        # Term Frequency (TF) in liked documents
        tf = sum(1 for doc in liked_lower if keyword in doc)
        if tf == 0:
            continue
        
        # Document Frequency (DF)
        df = df_counts.get(keyword, 1)
        
        # Inverse Document Frequency (IDF)
        # Use log((n_docs + 1) / (df + 1)) to avoid division by zero
        idf = math.log((n_docs + 1) / (df + 1))
        
        # TF-IDF score (normalized by number of liked docs)
        tf_idf = (tf / len(liked_docs)) * idf if liked_docs else 0
        
        # Store original keyword case
        keyword_scores[keywords[i]] = tf_idf
    
    return keyword_scores


def main():
    if len(sys.argv) != 3:
        print("Usage: python3 extract_keywords.py <liked_corpus.txt> <background_corpus.txt>", file=sys.stderr)
        sys.exit(1)
    
    liked_corpus_path = sys.argv[1]
    background_corpus_path = sys.argv[2]
    
    # Read corpora
    liked_docs = read_corpus(liked_corpus_path)
    background_docs = read_corpus(background_corpus_path)
    
    if not liked_docs:
        print("ERROR: No documents found in liked corpus", file=sys.stderr)
        sys.exit(1)
    
    if not background_docs:
        print("WARNING: No documents found in background corpus", file=sys.stderr)
        background_docs = []
    
    # Combine liked docs into single text for YAKE
    liked_text = '\n\n'.join(liked_docs)
    
    # Extract keywords using YAKE
    # Configure YAKE: max 3 words per phrase, top 50 keywords
    kw_extractor = yake.KeywordExtractor(
        lan="en",
        n=3,  # Max n-gram size
        dedupLim=0.7,  # Deduplication threshold
        top=50,  # Top 50 keywords
        features=None
    )
    
    keywords_raw = kw_extractor.extract_keywords(liked_text)
    
    # Extract just the keyword strings (YAKE returns (keyword, score) tuples)
    keywords = [kw[0] for kw in keywords_raw]  # kw[0] is the keyword string
    
    if not keywords:
        # No keywords extracted
        sys.exit(0)
    
    # Compute TF-IDF scores
    keyword_scores = compute_tf_idf(keywords, liked_docs, background_docs)
    
    # Filter: only keep keywords with TF-IDF > 0.01 (threshold to filter out common words)
    # Sort by TF-IDF score descending
    filtered_keywords = [
        (kw, score) for kw, score in keyword_scores.items()
        if score > 0.01
    ]
    filtered_keywords.sort(key=lambda x: x[1], reverse=True)
    
    # Output: keyword<TAB>score (one per line)
    for keyword, score in filtered_keywords:
        print(f"{keyword}\t{score}")


if __name__ == '__main__':
    main()
