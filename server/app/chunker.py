from __future__ import annotations
import json
from typing import Iterable, Dict, Any, List, Tuple

def iter_chunked_items(
    items: Iterable[Dict[str, Any]],
    chunk_bytes: int = 90_000,
    envelope: bool = True,
) -> Iterable[Tuple[bytes, int, int]]:
    """Yield JSON chunks (<= chunk_bytes) as bytes.

    Returns tuples: (payload_bytes, item_count, approx_bytes)

    If envelope=True, payload is a full JSON object:
        { "items": [...], "next": null, "approx_bytes": N }
    The caller can edit `next` afterwards if needed.

    If envelope=False, payload is a bytes of an NDJSON line.
    """
    batch: List[Dict[str, Any]] = []
    size = 0  # approx payload size in bytes

    def estimate(obj: Any) -> int:
        # A simple size estimator on UTF-8 bytes
        return len(json.dumps(obj, ensure_ascii=False).encode("utf-8"))

    for item in items:
        item_size = estimate(item)
        # if single item is larger than chunk_bytes, we still send it alone
        if not batch:
            projected = estimate({"items": [item], "next": None, "approx_bytes": item_size}) if envelope else item_size
            if projected > chunk_bytes:
                payload = (
                    json.dumps({"items": [item], "next": None, "approx_bytes": projected}, ensure_ascii=False)
                    .encode("utf-8")
                    if envelope
                    else (json.dumps(item, ensure_ascii=False) + "\n").encode("utf-8")
                )
                yield payload, 1, projected
                continue
        projected = estimate({"items": batch + [item], "next": None, "approx_bytes": size + item_size}) if envelope else size + item_size
        if projected <= chunk_bytes:
            batch.append(item)
            size += item_size
        else:
            # flush current
            if batch:
                payload = (
                    json.dumps({"items": batch, "next": None, "approx_bytes": size}, ensure_ascii=False).encode("utf-8")
                    if envelope
                    else ("\n".join(json.dumps(x, ensure_ascii=False) for x in batch) + "\n").encode("utf-8")
                )
                yield payload, len(batch), size
            # start new batch with current item
            batch = [item]
            size = item_size

    if batch:
        payload = (
            json.dumps({"items": batch, "next": None, "approx_bytes": size}, ensure_ascii=False).encode("utf-8")
            if envelope
            else ("\n".join(json.dumps(x, ensure_ascii=False) for x in batch) + "\n").encode("utf-8")
        )
        yield payload, len(batch), size
