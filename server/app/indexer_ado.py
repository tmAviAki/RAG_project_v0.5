# Project:Confluence Evidence API  Component:indexer_ado  Version:v1.2.0
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Tuple, Optional, Iterable, Dict, Any, List
import re
import sys

from .repository import upsert_doc, add_attachment

def eprint(*a, **kw):
    print(*a, file=sys.stderr, **kw)

# ---------- helpers ----------
def _read_text(path: Path) -> str:
    try:
        if path.suffix == ".gz":
            import gzip
            return gzip.decompress(path.read_bytes()).decode("utf-8", errors="ignore")
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        try:
            return path.read_bytes().decode("utf-8", errors="ignore")
        except Exception:
            return ""

def _extract_path_like(d: Dict[str, Any]) -> tuple[Optional[str], Optional[int], Optional[str], Optional[str]]:
    candidates = ["path","relpath","relativePath","localPath","fullPath","filePath","attachmentPath"]
    name = d.get("name") or d.get("fileName") or d.get("filename")
    size = d.get("size")
    sha = d.get("sha256") or d.get("sha") or None
    path_val = None
    for k in candidates:
        v = d.get(k)
        if isinstance(v, str) and v.strip():
            path_val = v.strip()
            break
    if not path_val:
        for nest in ("file","attachment","artifact"):
            obj = d.get(nest)
            if isinstance(obj, Dict):
                for k in candidates:
                    v = obj.get(k)
                    if isinstance(v, str) and v.strip():
                        path_val = v.strip()
                        break
                if not name:
                    name = obj.get("name") or obj.get("fileName") or obj.get("filename")
            if path_val:
                break
    return path_val, (int(size or 0) if size is not None else None), (sha or None), (name or None)

def _normalize_public_rel(ado_root: str, rel: str) -> str:
    # normalize any input to be served under /attachments/ADO/<...>
    rel = str(Path(rel)).replace("\\", "/").lstrip("/")
    if rel.startswith("attachments/"):
        rel = rel[len("attachments/") :]
    # ensure "ADO/..." prefix as our API serves /attachments/ADO/<rel>
    return f"ADO/{rel}"

def _load_attachments_index(attachments_index_path: Path) -> Dict[str, List[dict]]:
    """
    Try to load mapping: workItemId -> list of {name, relpath, size, sha256}
    Supports:
      - dict: { "475":[ "attachments/475_x.txt", {"path":"attachments/475_y"} ] }
      - list: [ {"workItemId":475,"path":"attachments/..."} ]
      - gz variant if .json missing
    """
    txt = ""
    if attachments_index_path.exists():
        txt = _read_text(attachments_index_path)
    elif attachments_index_path.with_suffix(".json.gz").exists():
        txt = _read_text(attachments_index_path.with_suffix(".json.gz"))
    if not txt:
        return {}

    try:
        data = json.loads(txt)
    except Exception:
        return {}

    norm: Dict[str, List[dict]] = {}

    if isinstance(data, dict):
        for k, arr in data.items():
            wid = str(k).strip()
            if not isinstance(arr, list):
                continue
            files: List[dict] = []
            for it in arr:
                if isinstance(it, str):
                    p = it
                    files.append({"name": os.path.basename(p), "relpath": p, "size": 0, "sha256": None})
                elif isinstance(it, dict):
                    p, size, sha, name = _extract_path_like(it)
                    if p:
                        files.append({"name": name or os.path.basename(p), "relpath": p, "size": int(size or 0), "sha256": sha})
            if files:
                norm[wid] = files
        return norm

    if isinstance(data, list):
        for it in data:
            if not isinstance(it, dict):
                continue
            wid = it.get("workItemId") or it.get("id") or it.get("work_item_id")
            if wid is None:
                continue
            p, size, sha, name = _extract_path_like(it)
            if p:
                wid = str(wid)
                norm.setdefault(wid, []).append(
                    {"name": name or os.path.basename(p), "relpath": p, "size": int(size or 0), "sha256": sha}
                )
        return norm

    return {}

# ---------- main API ----------
def index_ado_cache(ado_root: str, conn, space_key: str = "ADO") -> Tuple[int, int]:
    """
    Index ADO work items + link attachments.

    Expected tree:
      <ADO_ROOT>/
        items/                   (work item JSON/NDJSON/TXT; optional)
        attachments/             (raw files; filenames often start with <workItemId>_)
        attachments_index.json   (optional; if absent we fallback to filename inference)
    """
    root = Path(ado_root)
    if not root.exists():
        raise SystemExit(f"ADO cache root not found: {ado_root}")

    # 1) Upsert work items from <ADO_ROOT>/items (best-effort)
    items_dir = root / "items"
    n_docs = 0
    if items_dir.exists():
        for p in sorted(items_dir.rglob("*")):
            if not (p.is_file() and p.suffix.lower() in (".json", ".ndjson", ".txt")):
                continue
            txt = _read_text(p)
            if not txt:
                continue
            try:
                obj = json.loads(txt)
                if not isinstance(obj, dict):
                    raise ValueError
            except Exception:
                # key: value lines fallback
                obj = {}
                for line in txt.splitlines():
                    if ":" in line:
                        k, v = line.split(":", 1)
                        obj[k.strip()] = v.strip()

            # ID
            wid = None
            for k in ("id", "workItemId", "work_item_id", "ID"):
                v = obj.get(k)
                if v is not None and str(v).strip():
                    wid = str(v).strip()
                    break
            if not wid:
                continue

            # title
            title = None
            for k in ("title", "System.Title", "fields.System.Title", "name", "Name"):
                v = obj.get(k)
                if isinstance(v, str) and v.strip():
                    title = v.strip()
                    break
            title = title or f"ADO Work Item {wid}"

            # type
            wtype = None
            for k in ("System.WorkItemType", "fields.System.WorkItemType", "type", "Type"):
                v = obj.get(k)
                if isinstance(v, str) and v.strip():
                    wtype = v.strip().lower()
                    break
            wtype = wtype or "ado"

            # text best-effort
            text = ""
            for k in ("System.Description", "fields.System.Description", "description", "Description"):
                v = obj.get(k)
                if isinstance(v, str) and v.strip():
                    text = v
                    break

            doc = {
                "id": f"ADO:{wid}",
                "space": space_key,
                "type": wtype,
                "title": title,
                "storage_relpath": None,
                "created": None,
                "updated": None,
                "version": None,
            }
            upsert_doc(conn, doc, text or "")
            n_docs += 1

    # 2) Attachments: prefer attachments_index.json, otherwise infer from filenames
    att_index = _load_attachments_index(root / "attachments_index.json")
    n_att = 0

    if att_index:
        for wid, files in att_index.items():
            for it in files:
                raw_rel = it.get("relpath") or it.get("path") or ""
                if not raw_rel:
                    continue
                public_rel = _normalize_public_rel(str(root), raw_rel)
                size = int(it.get("size") or 0)
                sha = it.get("sha256") or None
                name = it.get("name") or os.path.basename(public_rel)
                add_attachment(conn, f"ADO:{wid}", name, public_rel, size, sha)
                n_att += 1
        eprint(f"[OK] ADO index: linked {n_att} attachments via attachments_index.json")
    else:
        # Fallback: infer workItemId from filenames like "<digits>_<...>"
        att_root = root / "attachments"
        wid_re = re.compile(r"^(\d+)_")
        if att_root.exists():
            for fn in sorted(att_root.iterdir()):
                if not fn.is_file():
                    continue
                m = wid_re.match(fn.name)
                if not m:
                    continue
                wid = m.group(1)
                # relpath served under /attachments/ADO/<file>
                rel = f"ADO/{fn.name}"
                try:
                    size = fn.stat().st_size
                except Exception:
                    size = 0
                add_attachment(conn, f"ADO:{wid}", fn.name, rel, int(size), None)
                n_att += 1
            eprint(f"[OK] ADO index (fallback): linked {n_att} attachments by filename inference")
        else:
            eprint("[WARN] ADO attachments folder not found; no ADO attachments linked")

    try:
        print(f"[INFO] ADO: indexed {n_docs} workitems, linked {n_att} attachments from {root}", flush=True)
    except Exception:
        pass
    return n_docs, n_att
