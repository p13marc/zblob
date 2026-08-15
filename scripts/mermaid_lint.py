#!/usr/bin/env python3
"""Offline linter for the mermaid blocks in Markdown files.

Not a full mermaid parser — the authoritative check is `mmdc` (see
`scripts/check-docs.sh`, which runs this first and then `mmdc` when it is
installed). This catches, fast and with no dependencies, the specific mistakes
that have actually broken zblob's diagrams:

  * a `;` inside a sequenceDiagram line (mermaid treats it as a statement
    separator, so the text after it parses as a new statement and throws
    "expecting arrow, got NEWLINE");
  * `direction` inside a subgraph (unsupported on older mermaid builds);
  * an edge whose endpoint is a *subgraph* id rather than a node (renders
    wrong or errors — link nodes, not subgraphs);
  * an unbalanced `"` on a line.

Usage: mermaid_lint.py <file.md> [more.md ...]   # exit 1 on any error
"""
import re
import sys

ARROW = re.compile(r"-\.?-+>|-\.?->|==+>|--[xo]|<-->|x--x|o--o")
EDGE_LABEL = re.compile(r"\|[^|]*\|")
NODE_DECL = re.compile(r"\b(\w+)\s*[\[\(\{]")
SUBGRAPH = re.compile(r"^\s*subgraph\s+([A-Za-z0-9_]+)")


def blocks(md_text):
    """Yield (start_line, body) for each ```mermaid fenced block."""
    lines = md_text.splitlines()
    i = 0
    while i < len(lines):
        if lines[i].strip() == "```mermaid":
            start = i + 1
            body = []
            i += 1
            while i < len(lines) and lines[i].strip() != "```":
                body.append(lines[i])
                i += 1
            yield start + 1, body  # 1-based line of first body line
        i += 1


def lint_block(path, first_line, body):
    errs, warns = [], []
    kind = body[0].strip() if body else ""
    is_seq = kind.startswith("sequenceDiagram")
    is_flow = kind.startswith(("flowchart", "graph"))

    # Pass 1: collect subgraph ids (edges must not point at them).
    subgraph_ids = set()
    for ln in body:
        m = SUBGRAPH.match(ln)
        if m:
            subgraph_ids.add(m.group(1))

    depth = 0
    for off, raw in enumerate(body):
        ln_no = first_line + off
        s = raw.strip()
        loc = f"{path}:{ln_no}"

        if SUBGRAPH.match(s):
            depth += 1
        elif s == "end" and depth:
            depth -= 1

        if s.count('"') % 2:
            errs.append(f"{loc}: unbalanced '\"' in {s!r}")

        if is_seq:
            stmt = s.startswith((
                "Note ", "loop ", "alt ", "opt ", "par ", "and ",
                "else ", "rect ", "critical ", "break ",
            )) or ARROW.search(s) or "-)" in s
            if stmt and ";" in s:
                errs.append(
                    f"{loc}: ';' in a sequenceDiagram line — mermaid splits "
                    f"on it (use ',' or 'then'): {s!r}"
                )
            if s.startswith("Note") and "<br/>" in s:
                warns.append(f"{loc}: '<br/>' in a sequence Note is unreliable: {s!r}")

        if is_flow:
            if depth and s.startswith("direction "):
                errs.append(f"{loc}: 'direction' inside a subgraph breaks older mermaid: {s!r}")
            if ARROW.search(s):
                stripped = EDGE_LABEL.sub("", s)
                for m in ARROW.finditer(stripped):
                    left = stripped[: m.start()].strip().split()[-1:] or [""]
                    right = stripped[m.end():].strip().split()[:1] or [""]
                    for tok in (left[0], right[0]):
                        tok = re.match(r"[A-Za-z0-9_]+", tok)
                        tok = tok.group(0) if tok else ""
                        if tok and tok in subgraph_ids:
                            errs.append(
                                f"{loc}: edge endpoint '{tok}' is a subgraph, "
                                f"not a node — link nodes instead: {s!r}"
                            )
    return errs, warns


def main(argv):
    all_errs, all_warns, n_blocks = [], [], 0
    for path in argv:
        try:
            text = open(path, encoding="utf-8").read()
        except OSError as e:
            all_errs.append(f"{path}: cannot read ({e})")
            continue
        for first_line, body in blocks(text):
            n_blocks += 1
            e, w = lint_block(path, first_line, body)
            all_errs += e
            all_warns += w
    for w in all_warns:
        print(f"warn:  {w}")
    for e in all_errs:
        print(f"ERROR: {e}")
    print(f"\nmermaid_lint: {n_blocks} block(s), {len(all_errs)} error(s), {len(all_warns)} warning(s)")
    return 1 if all_errs else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
