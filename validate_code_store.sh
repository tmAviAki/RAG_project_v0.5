#!/usr/bin/env bash
set -euo pipefail

ROOT="/mnt/disks/data/source-code"
OUT="/var/tmp/src_audit_$(date +%F-%H%M%S)"
mkdir -p "$OUT"

echo "[INFO] Auditing $ROOT → $OUT"

# 1) Size & structure
sudo du -h --max-depth=2 "$ROOT" | sort -h | tee "$OUT/du_top2.txt"
{ echo -e "size\tdir"; sudo du -sh "$ROOT"/* 2>/dev/null | sort -h; } | tee "$OUT/top1_sizes.txt"

# 2) Extensions histogram (lowercased; <noext> for extensionless files)
sudo find "$ROOT" -type f -printf '%f\n' \
| awk -F. '{if (NF>1) print tolower($NF); else print "<noext>"}' \
| sort | uniq -c | sort -nr | tee "$OUT/ext_histogram.txt"

# 3) MIME sample of 200 files to estimate text vs binary composition
if command -v shuf >/dev/null 2>&1; then SAMPLE='shuf -n 200'; else SAMPLE='sed -n "1,200p"'; fi
sudo find "$ROOT" -type f -print0 \
| xargs -0 -r -I{} file -bi '{}' \
| eval "$SAMPLE" \
| sort | uniq -c | sort -nr | tee "$OUT/mime_sample.txt"

# 4) Top 50 largest files
sudo find "$ROOT" -type f -printf '%12s\t%p\n' | sort -nr | head -50 | tee "$OUT/top50_largest.txt"

# 5) Unwanted sources: AppleDouble, VCS/IDE dirs, common build dirs
sudo find "$ROOT" -regex '.*/\._[^/]+$' -print | tee "$OUT/dot_underscore.list"
sudo find "$ROOT" -type d \( -name .git -o -name .hg -o -name .svn -o -name .idea -o -name .vscode \) -prune -print \
| tee "$OUT/vcs_ide_dirs.txt"
sudo find "$ROOT" -type d -iregex '.*/(build|bin|obj|out|target|dist|\.vs)(/.*)?' -print \
| tee "$OUT/build_dirs.txt"

# 6) Staging dirs (just to know if they exist; not assumed junk)
sudo find "$ROOT" -type d -iregex '.*\.staging(/.*)?' -print | tee "$OUT/staging_dirs.txt"

echo "[OK] Audit done: $OUT"
