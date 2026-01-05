#!/usr/bin/env python3

import argparse
import statistics
import time
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
    ap.add_argument("--q", required=True)
    ap.add_argument("--n", type=int, default=5)
    ap.add_argument("--topk", type=int, default=50)
    ap.add_argument("--page_size", type=int, default=50)
    ap.add_argument("--token", help="Bearer token for auth (optional)")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--lite", dest="lite", action="store_true", default=True)
    g.add_argument("--no-lite", dest="lite", action="store_false")
    args = ap.parse_args()

    timings = []
    for i in range(args.n):
        params = {
            "q": args.q,
            "topk": str(args.topk),
            "page_size": str(args.page_size),
            "cursor": "0",
            "lite": "true" if args.lite else "false",
            "debug": "true",
        }
        url = f"{args.base}/search?{urllib.parse.urlencode(params)}"
        t0 = time.perf_counter()
        data = fetch(url, token=args.token)
        dt = (time.perf_counter() - t0) * 1000.0
        dbg = data.get("_debug", {})
        tms = (dbg.get("timing_ms") or {})
        timings.append(dt)
        print(
            f"[{i+1}/{args.n}] http_total_ms={dt:.1f} timing_ms={tms} count={data.get('count')}"
        )

    if timings:
        print(
            "summary_ms:",
            {
                "min": min(timings),
                "p50": statistics.median(timings),
                "avg": sum(timings) / len(timings),
                "max": max(timings),
            },
        )


if __name__ == "__main__":
    main()
