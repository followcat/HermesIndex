#!/usr/bin/env python3
"""
Check vector store source distribution.
Usage: python AIthink/check_vector_sources.py
"""

import argparse
import urllib.parse
import urllib.request
import json


def fetch(url: str, token: str | None = None) -> dict:
    req = urllib.request.Request(url)
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(req, timeout=60) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://127.0.0.1:8000")
    ap.add_argument("--q", required=True, help="Search query")
    ap.add_argument("--topk", type=int, default=100)
    ap.add_argument("--token", help="Bearer token for auth")
    args = ap.parse_args()

    params = {
        "q": args.q,
        "topk": str(args.topk),
        "page_size": str(args.topk),
        "cursor": "0",
        "lite": "true",
        "debug": "true",
        "exclude_nsfw": "false",
        "tmdb_expand": "false",
    }
    url = f"{args.base}/search?{urllib.parse.urlencode(params)}"
    data = fetch(url, token=args.token)
    
    dbg = data.get("_debug", {})
    print("=== Vector Search Results ===")
    print(f"Query: {dbg.get('cleaned_query')}")
    print(f"Final query: {dbg.get('final_query')}")
    print(f"Vector store size: {dbg.get('vector_store_size')}")
    print(f"Raw vector hits: {dbg.get('raw_vector_hits')}")
    print(f"Filtered vector hits: {dbg.get('filtered_vector_hits')}")
    print()
    print("=== IDs by Source ===")
    for source, count in dbg.get("ids_by_source", {}).items():
        print(f"  {source}: {count}")
    print()
    print("=== PG Sources ===")
    for src in dbg.get("pg_sources", []):
        print(f"  {src.get('source')}: ids={src.get('ids')}, rows={src.get('rows')}, pg_fetch_ms={src.get('pg_fetch_ms')}")
    print()
    print("=== Timing ===")
    for key, val in dbg.get("timing_ms", {}).items():
        print(f"  {key}: {val}ms")
    print()
    print(f"Results count: {data.get('count')}")
    
    # Show first few results with scores
    print()
    print("=== Top Results (with source) ===")
    for i, r in enumerate(data.get("results", [])[:10]):
        title = r.get("title", "")[:60]
        print(f"  {i+1}. [{r.get('source')}] score={r.get('score'):.4f} {title}")


if __name__ == "__main__":
    main()
