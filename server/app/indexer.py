# Project: Confluence Evidence API  Component: indexer  Version: v1.3.2
from __future__ import annotations
import argparse
import json
import os
import re
import sys
from pathlib import Path
from typing import Iterator, Tuple, Optional, Dict

from bs4 import BeautifulSoup

from .repository import connect, upsert_doc, add_attachment
from .indexer_ado import index_ado_cache


def eprint(*a, **kw):
    print(*a, file=sys.stderr, **kw)


def iter_ndjson(path: Path) -> Iterator[dict]:
    """
    Stream NDJSON records from a file. Skips blank/invalid lines.
    """
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                yield json.loads(s)
            except Exception as e:
                eprint(f"[WARN] Bad JSON in {path}: {e}")
                continue


ID_RE = re.compile(r"(\d{3,})")  # capture long digit sequences as content IDs


def html_to_text(html: str) -> str:
    """
    Convert (Confluence) HTML storage to plain text while removing script/style.
    """
    if not html:
        return ""
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style"]):
        tag.decompose()
    return soup.get_text(" ", strip=True)


def build_storage_index(storage_dir: Path) -> Dict[str, str]:
    """
    Build a {content_id -> absolute_path} index for page_storage/ to avoid
    repeated '**' globbing. Matches any filename that contains a numeric id (>=3 digits).
    """
    idx: Dict[str, str] = {}
    if not storage_dir.exists():
        return idx
    rx = re.compile(r"(\d{3,})")
    for p in storage_dir.rglob("*"):
        if not p.is_file():
            continue
        m = rx.search(p.name)
        if m:
            idx[m.group(1)] = str(p)
    return idx


def find_storage_by_id(storage_dir: Path, cid: str, idx: Optional[Dict[str, str]] = None) -> Optional[str]:
    """
    Resolve HTML storage by content id:
      - If an index dict is provided (fast path), use that directly.
      - Otherwise fallback to legacy '**' glob (slow path).
    """
    if not cid:
        return None

    # Fast path: prebuilt index
    if idx is not None:
        path = idx.get(cid)
        if path:
            p = Path(path)
            if p.is_file():
                try:
                    return p.read_text(encoding="utf-8", errors="ignore")
                except Exception:
                    try:
                        return p.read_bytes().decode("utf-8", errors="ignore")
                    except Exception:
                        return None
        # fall through to slow path if not found

    # Slow path
    if not storage_dir.exists():
        return None
    for p in storage_dir.glob(f"**/*{cid}*"):
        if p.is_file():
            try:
                return p.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                try:
                    return p.read_bytes().decode("utf-8", errors="ignore")
                except Exception:
                    continue
    return None


def try_extract_text(space_dir: Path, item: dict, storage_idx: Optional[Dict[str, str]]) -> str:
    """
    Extract page/blog/comment body as plain text.
    Prefer 'page_storage' HTML (fast lookup via storage_idx),
    fallback to NDJSON body.storage.value.
    """
    cid = str(item.get("id", "")).strip()
    storage_dir = space_dir / "page_storage"
    text: Optional[str] = None

    if cid:
        html = find_storage_by_id(storage_dir, cid, idx=storage_idx)
        if html:
            text = html_to_text(html)

    if not text:
        body = item.get("body", {})
        storage = body.get("storage", {}) if isinstance(body, dict) else {}
        val = storage.get("value") if isinstance(storage, dict) else None
        if isinstance(val, str) and val:
            text = html_to_text(val)

    return text or ""


def index_space(conn, space_dir: Path) -> Tuple[int, int]:
    """
    Index a single space:
      - pages/blogposts/comments text (prefers page_storage/ HTML if present)
      - attachments derived from /data/attachments/<space>/**/<name> and a folder-name suffix '_<id>'

    CHANGE (v1.3.2):
      When a directory name matches '..._<digits>', we now link *all files under that subtree*
      to that content_id, not just the files at that directory level. This enables cases like:
        /attachments/RFP/RFP_MASTER_99000001/<deep trees>/files...
      to be linked to content_id 99000001 as well.
    """
    space_key = space_dir.name
    pages_nd = space_dir / "page.ndjson"
    blogs_nd = space_dir / "blogpost.ndjson"
    n_docs = 0

    # Build a one-time storage index for this space (fast O(1) lookups)
    storage_dir = space_dir / "page_storage"
    storage_idx = build_storage_index(storage_dir)

    # Documents
    for nd, doctype in ((pages_nd, "page"), (blogs_nd, "blogpost")):
        if nd.exists():
            for obj in iter_ndjson(nd):
                cid = str(obj.get("id", "")).strip()
                title = (obj.get("title") or "").strip()
                if not cid or not title:
                    continue
                doc = {
                    "id": cid,
                    "space": space_key,
                    "type": doctype,
                    "title": title,
                    "storage_relpath": None,
                    "created": None,
                    "updated": None,
                    "version": None,
                }
                text = try_extract_text(space_dir, obj, storage_idx)
                upsert_doc(conn, doc, text)
                n_docs += 1

    # Comments NDJSON (optional)
    comments_nd = space_dir / "page_comments.ndjson"
    if comments_nd.exists():
        for obj in iter_ndjson(comments_nd):
            cid = str(obj.get("id", "")).strip()
            title = (obj.get("title") or obj.get("containerTitle") or f"Comment {cid}").strip()
            if not cid:
                continue
            text = try_extract_text(space_dir, obj, storage_idx)
            doc = {
                "id": cid,
                "space": space_key,
                "type": "comment",
                "title": title,
                "storage_relpath": None,
                "created": None,
                "updated": None,
                "version": None,
            }
            upsert_doc(conn, doc, text)
            n_docs += 1

    # Attachments
    attachments_root = Path(os.getenv("DATA_ROOT", "/data")) / "attachments"
    n_att = 0
    if attachments_root.exists():
        base_space = attachments_root / space_key
        if base_space.exists():
            # Walk the entire space's attachments tree
            for root, dirs, files in os.walk(base_space):
                base = os.path.basename(root)
                m = re.search(r"_(\d{3,})$", base)
                if not m:
                    continue
                content_id = m.group(1)

                # NEW: link *all* files under this matched subtree to 'content_id'
                for r2, d2, f2 in os.walk(root):
                    for name in f2:
                        fullp = os.path.join(r2, name)
                        relpath = os.path.relpath(fullp, start=attachments_root).replace("\\", "/")
                        try:
                            size = os.path.getsize(fullp)
                        except Exception:
                            size = 0
                        add_attachment(conn, content_id, name, relpath, size or 0, None)
                        n_att += 1

    return n_docs, n_att


def build_index(data_root: str, index_path: str) -> None:
    """
    Full build across all spaces under /data/spaces, then optional ADO cache.
    """
    root = Path(data_root)
    spaces_root = root / "spaces"
    if not spaces_root.exists():
        raise SystemExit(f"No spaces/ folder under {root}. Mount your export at {root}.")
    conn = connect(index_path)
    total_docs = 0
    total_att = 0
    for space_dir in sorted(spaces_root.iterdir()):
        if not space_dir.is_dir():
            continue
        n_docs, n_att = index_space(conn, space_dir)
        total_docs += n_docs
        total_att += n_att
        conn.commit()
        eprint(f"[OK] Indexed space {space_dir.name}: {n_docs} docs, {n_att} attachments")

    # Optional: ADO cache indexing, if ADO_ROOT is set or present under data_root
    ado_root = os.getenv("ADO_ROOT")
    if not ado_root:
        default = root / ".ado_cache" / "8127d161517e897b"
        if default.exists():
            ado_root = str(default)
    if ado_root and os.path.exists(ado_root):
        n_docs, n_att = index_ado_cache(ado_root, conn, space_key="ADO")
        total_docs += n_docs
        total_att += n_att
        conn.commit()
        eprint(f"[OK] Indexed ADO cache: {n_docs} workitems, {n_att} attachments")

    eprint(f"[DONE] Total: {total_docs} docs, {total_att} attachments")


def main():
    ap = argparse.ArgumentParser(
        description="Build SQLite FTS index from exported Confluence tree (+ optional ADO cache)."
    )
    ap.add_argument("--data-root", default=os.getenv("DATA_ROOT", "/data"))
    ap.add_argument("--index-path", default=os.getenv("INDEX_PATH", "/index/docs.db"))
    ap.add_argument(
        "--ado-root",
        default=os.getenv("ADO_ROOT", None),
        help="Optional ADO cache root, e.g. /mnt/disks/data/fetch_ado_items/.ado_cache/8127..."
    )
    args = ap.parse_args()
    if args.ado_root:
        os.environ["ADO_ROOT"] = args.ado_root
    build_index(args.data_root, args.index_path)


if __name__ == "__main__":
    main()
