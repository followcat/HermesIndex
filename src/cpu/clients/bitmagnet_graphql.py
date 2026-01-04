from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

import httpx


class BitmagnetGraphQLClient:
    def __init__(self, endpoint: str, timeout: float = 15.0) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.timeout = float(timeout)

    def _post(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        last_exc: Exception | None = None
        last_status: int | None = None
        for attempt in range(3):
            try:
                with httpx.Client(timeout=self.timeout, trust_env=False) as client:
                    resp = client.post(
                        self.endpoint,
                        json={"query": query, "variables": variables},
                        headers={"Content-Type": "application/json"},
                    )
                if resp.status_code == 422:
                    # Some GraphQL servers return 422 for parse/validation errors.
                    raise RuntimeError(f"HTTP 422 from Bitmagnet GraphQL: {resp.text}")
                if resp.status_code in {502, 503, 504}:
                    last_status = int(resp.status_code)
                    last_exc = RuntimeError(f"HTTP {resp.status_code} from Bitmagnet GraphQL")
                    time.sleep(0.3 * (attempt + 1))
                    continue
                resp.raise_for_status()
                payload = resp.json()
                if payload.get("errors"):
                    raise RuntimeError(f"Bitmagnet GraphQL errors: {payload['errors']}")
                return payload
            except Exception as exc:
                last_exc = exc
                time.sleep(0.3 * (attempt + 1))
        suffix = f" status={last_status}" if last_status is not None else ""
        raise RuntimeError(f"Bitmagnet GraphQL request failed{suffix} error={last_exc}") from last_exc

    def search_torrents(self, query_string: str, limit: int = 50) -> Dict[str, Any]:
        gql = """
        query SearchTorrents($query: String!, $limit: Int!) {
          torrents(query: { queryString: $query }, limit: $limit) {
            totalCount
            edges {
              node {
                infoHash
                name
                size
                filesCount
                seeders
                leechers
                publishedAt
                content {
                  type
                  title
                  releaseYear
                  collections { name type }
                  attributes { key value }
                }
              }
            }
          }
        }
        """
        return self._post(gql, {"query": query_string, "limit": int(limit)})

    @staticmethod
    def extract_torrent_nodes(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        data = payload.get("data") or {}
        torrents = data.get("torrents") or {}
        edges = torrents.get("edges") or []
        nodes: List[Dict[str, Any]] = []
        for edge in edges:
            if not isinstance(edge, dict):
                continue
            node = edge.get("node")
            if isinstance(node, dict):
                nodes.append(node)
        return nodes

    @staticmethod
    def total_count(payload: Dict[str, Any]) -> Optional[int]:
        data = payload.get("data") or {}
        torrents = data.get("torrents") or {}
        total = torrents.get("totalCount")
        try:
            return int(total) if total is not None else None
        except (TypeError, ValueError):
            return None
