#!/usr/bin/env python3
"""Generate knowledge base for the DQDL bot from repository source.

Usage (from repo root):
    python3 scripts/generate_kb.py > kb.md
"""

import os
from pathlib import Path

REPO_ROOT = Path(".")
SRC_DIR = REPO_ROOT / "src"
TST_DIR = REPO_ROOT / "tst"
CONFIG_DIR = REPO_ROOT / "configuration"
README = REPO_ROOT / "README.md"
POM = REPO_ROOT / "pom.xml"

MAX_FILE_CHARS = 8000
MAX_TOTAL_CHARS = 500000


def read_safe(path, max_chars=None):
    try:
        text = path.read_text(errors="replace")
        if max_chars and len(text) > max_chars:
            text = text[:max_chars] + "\n... (truncated)"
        return text
    except Exception:
        return ""


def main():
    parts = []
    total = 0

    # README
    if README.exists():
        content = read_safe(README, MAX_FILE_CHARS)
        parts.append(f"# DQDL Knowledge Base\n\n## README\n\n{content}")
        total += len(content)

    # pom.xml excerpt
    if POM.exists():
        content = read_safe(POM, 3000)
        parts.append(f"## Build Configuration (pom.xml excerpt)\n\n```xml\n{content}\n```")
        total += len(content)

    # ANTLR Grammar files
    if CONFIG_DIR.exists():
        parts.append("## ANTLR Grammar\n")
        for g4_file in sorted(CONFIG_DIR.rglob("*.g4")) + sorted(CONFIG_DIR.rglob("*.json")):
            if total >= MAX_TOTAL_CHARS:
                break
            rel = g4_file.relative_to(REPO_ROOT)
            content = read_safe(g4_file, MAX_FILE_CHARS)
            if content.strip():
                section = f"### `{rel}`\n\n```\n{content}\n```\n"
                parts.append(section)
                total += len(section)

    # Java source files
    if SRC_DIR.exists():
        parts.append("## Source Code Reference\n")
        for java_file in sorted(SRC_DIR.rglob("*.java")):
            if total >= MAX_TOTAL_CHARS:
                parts.append("\n... (KB size limit reached)")
                break
            if "test" in [p.lower() for p in java_file.parts]:
                continue  # Skip files in test directories
            rel = java_file.relative_to(REPO_ROOT)
            content = read_safe(java_file, MAX_FILE_CHARS)
            if content.strip():
                section = f"### `{rel}`\n\n```java\n{content}\n```\n"
                parts.append(section)
                total += len(section)

    # Test file listing
    if TST_DIR.exists():
        parts.append("## Test Files\n")
        for test_file in sorted(TST_DIR.rglob("*.java")):
            if total >= MAX_TOTAL_CHARS:
                break
            rel = test_file.relative_to(REPO_ROOT)
            lines = len(test_file.read_text(errors="replace").splitlines())
            parts.append(f"- `{rel}` ({lines} lines)")

    print("\n\n".join(parts))


if __name__ == "__main__":
    main()
