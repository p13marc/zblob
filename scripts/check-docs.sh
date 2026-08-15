#!/usr/bin/env bash
#
# Validate the Markdown docs and their mermaid diagrams.
#
#   1. Always: an offline mermaid gotcha-linter (scripts/mermaid_lint.py) —
#      no dependencies beyond python3, catches the mistakes that have actually
#      broken these diagrams. This is the hard gate.
#   2. If `mmdc` is on PATH: the authoritative check — render every mermaid
#      block through @mermaid-js/mermaid-cli and fail on any parse/render
#      error. CI installs mmdc so this always runs there; locally it is
#      skipped with a note unless you have it.
#
# Usage: scripts/check-docs.sh
# Exit non-zero on any error. Run from the repo root (or anywhere; it cd's).

set -euo pipefail
cd "$(dirname "$0")/.."

# Every tracked Markdown file (git keeps target/ etc. out for us).
mapfile -t MD < <(git ls-files '*.md')
if [[ ${#MD[@]} -eq 0 ]]; then
  echo "no tracked .md files found"; exit 0
fi

echo "== offline mermaid lint (${#MD[@]} markdown files) =="
python3 scripts/mermaid_lint.py "${MD[@]}"

# --- authoritative render check via mermaid-cli, when available -------------
MMDC=""
if command -v mmdc >/dev/null 2>&1; then
  MMDC="mmdc"
elif command -v npx >/dev/null 2>&1 && npx --no-install mmdc --version >/dev/null 2>&1; then
  MMDC="npx --no-install mmdc"
fi

if [[ -z "$MMDC" ]]; then
  echo
  echo "== mmdc not found — skipping the authoritative render check =="
  echo "   (install with: npm i -g @mermaid-js/mermaid-cli; CI does this)"
  exit 0
fi

echo
echo "== mmdc render check ($MMDC) =="
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
printf '{"args":["--no-sandbox","--disable-gpu"]}' > "$tmp/puppeteer.json"

# Extract each ```mermaid block to its own .mmd file, tagged by source.
python3 - "$tmp" "${MD[@]}" <<'PY'
import sys, re, pathlib
out = pathlib.Path(sys.argv[1])
for md in sys.argv[2:]:
    text = pathlib.Path(md).read_text(encoding="utf-8")
    stem = md.replace("/", "__").removesuffix(".md")
    for i, b in enumerate(re.findall(r"```mermaid\n(.*?)\n```", text, re.S)):
        (out / f"{stem}##{i}.mmd").write_text(b + "\n", encoding="utf-8")
PY

rc=0
shopt -s nullglob
for f in "$tmp"/*.mmd; do
  label="$(basename "${f%.mmd}" | sed 's/##/ block /; s/__/\//g')"
  if $MMDC -q -p "$tmp/puppeteer.json" -i "$f" -o "$f.svg" >/dev/null 2>"$f.err"; then
    echo "  ok    $label"
  else
    echo "  FAIL  $label"
    sed 's/^/        /' "$f.err"
    rc=1
  fi
done

if [[ $rc -ne 0 ]]; then
  echo
  echo "mermaid render check failed"
fi
exit $rc
