# Common helpers for CPU node
import hashlib
import re
from typing import Iterable, List


def text_hash(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()


def chunked(iterable: List, size: int) -> Iterable[List]:
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


def normalize_title_text(text: str) -> str:
    if not text:
        return ""
    cleaned = text
    cleaned = re.sub(r"[\\[\\]{}()]", " ", cleaned)
    cleaned = re.sub(r"[._\\-]+", " ", cleaned)
    noise_pattern = re.compile(
        r"\b(\d{3,4}p|4k|8k|uhd|hdr|hdr10|dolby|dv|x264|x265|h\.?(264|265)|hevc|avc|"
        r"bluray|blu\-?ray|web\-?dl|web\-?rip|brrip|dvdrip|hdrip|remux|"
        r"aac|dts|truehd|atmos|flac|mp3|mkv|mp4|avi|ts|m2ts|srt|ass|vtt|sub|"
        r"torrent|seed|complete|proper|repack|extended|uncut|multi|dual|subs?)\b",
        re.IGNORECASE,
    )
    cleaned = noise_pattern.sub(" ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned
