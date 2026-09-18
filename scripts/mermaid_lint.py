#!/usr/bin/env python3
"""Offline linter for the mermaid blocks in Markdown files.

Not a full mermaid parser — the authoritative check is `mmdc` (see
`scripts/check-docs.sh`, which runs this first and then `mmdc` when it is
installed). This catches, fast and with no dependencies, the specific mistakes
that have actually broken zblob's diagrams:

  * a `;` inside a sequenceDiagram line (mermaid treats it as a statement
    separator, so the text after it parses as a new statement and throws
    "expecting arrow, got NEWLINE").  Semicolons inside HTML entities
    (`&lt;`, `&#39;`) are not separators and are excluded;
  * `direction` inside a subgraph (unsupported on older mermaid builds);
  * an edge whose endpoint is a *subgraph* id rather than a node (renders
    wrong or errors — link nodes, not subgraphs);
  * an unbalanced `"` on a line.

Usage: mermaid_lint.py <file.md> [more.md ...]   # exit 1 on any error
       mermaid_lint.py --self-test               # check the rules themselves
"""
import re
import sys

# Mermaid arrows, flowchart AND sequence.  Every alternative here used to
# require at least two dashes, so the single-dash SEQUENCE forms -- `A->>B`,
# `A->B`, `A-)B`, `A-xB` -- never matched.  `->>` is the arrow a
# sequenceDiagram actually uses, so the ";" check below (the rule this linter
# exists for) was silently off on the common case.
ARROW = re.compile(
    r"--?\.?-*>>?"      # -> --> ->> -->> -.-> -.->>
    r"|==+>"            # ==>
    r"|--?[xo]"         # -x -o --x --o
    r"|--?\)"           # -) --)
    r"|<-->|x--x|o--o"
)
# `&lt;` / `&gt;` / `&#39;` are HTML entities and END IN A SEMICOLON.  Mermaid
# handles them; a naive `";" in s` reads every one as a statement separator.
ENTITY = re.compile(r"&(?:[A-Za-z][A-Za-z0-9]*|#[0-9]+|#[xX][0-9A-Fa-f]+);")
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
            bare = ENTITY.sub("", s)          # see ENTITY above
            if stmt and ";" in bare:
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


# --- self-test ---------------------------------------------------------------
#
# CI only ever runs these rules over docs that PASS, so a rule that quietly
# stops firing looks exactly like a rule with nothing to report.  That is not
# hypothetical: `ARROW` required two dashes, so the `;` check -- the rule this
# file exists for -- never ran on `->>`, the arrow a sequenceDiagram actually
# uses, and it went unnoticed until someone fed it a line it should have
# rejected.  Every case below is a line that must be judged a specific way, and
# both directions matter: the ones expecting 0 are as load-bearing as the rest,
# because a rule that fires on everything gets switched off.
#
# (name, block body, expected errors, expected warnings)
CASES = [
    # --- the `;` rule, across the arrow forms that gate it -------------------
    ("seq ';' with ->>",
     "sequenceDiagram\n  A->>B: one; two", 1, 0),
    ("seq ';' with -->>",
     "sequenceDiagram\n  A-->>B: one; two", 1, 0),
    ("seq ';' with ->",
     "sequenceDiagram\n  A->B: one; two", 1, 0),
    ("seq ';' with -)",
     "sequenceDiagram\n  A-)B: one; two", 1, 0),
    ("seq ';' in a Note",
     "sequenceDiagram\n  Note over A: one; two", 1, 0),
    ("seq clean",
     "sequenceDiagram\n  A->>B: one, two", 0, 0),

    # --- HTML entities end in ';' and are not separators ---------------------
    ("seq entity only, on an arrow line",
     "sequenceDiagram\n  A->>B: logs &lt;node&gt;.log", 0, 0),
    ("seq entity only, in a Note",
     "sequenceDiagram\n  Note over A: &#39;quoted&#39;", 0, 0),
    ("seq entity AND a real ';'",
     "sequenceDiagram\n  A->>B: &lt;x&gt; one; two", 1, 0),

    # --- the remaining rules, so a refactor cannot drop one silently ---------
    ("seq <br/> in a Note is a warning, not an error",
     "sequenceDiagram\n  Note over A: one<br/>two", 0, 1),
    ("flow direction inside a subgraph",
     "flowchart TB\n  subgraph g\n    direction LR\n    N[a]\n  end", 1, 0),
    ("flow direction outside a subgraph is fine",
     "flowchart TB\n  direction LR\n  N[a]", 0, 0),
    ("flow edge into a subgraph id",
     "flowchart LR\n  subgraph g\n    N1[a]\n  end\n  N2 --> g", 1, 0),
    ("flow edge between nodes",
     "flowchart LR\n  subgraph g\n    N1[a]\n  end\n  N2 --> N1", 0, 0),
    ("unbalanced quote",
     'flowchart LR\n  A["oops] --> B', 1, 0),
    ("balanced quotes",
     'flowchart LR\n  A["fine"] --> B', 0, 0),
]


def self_test():
    """Check every rule against a line it must judge a specific way."""
    bad = []
    for name, body, want_e, want_w in CASES:
        errs, warns = lint_block("<self-test>", 1, body.split("\n"))
        if len(errs) != want_e or len(warns) != want_w:
            bad.append(
                f"  {name}: expected {want_e} error(s) / {want_w} warning(s), "
                f"got {len(errs)} / {len(warns)}"
                + "".join(f"\n      {x}" for x in errs + warns)
            )
    for b in bad:
        print(f"SELF-TEST FAILED:\n{b}")
    print(f"mermaid_lint --self-test: {len(CASES)} case(s), {len(bad)} failure(s)")
    return 1 if bad else 0


def main(argv):
    if argv and argv[0] == "--self-test":
        return self_test()
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
