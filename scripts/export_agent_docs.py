#!/usr/bin/env python3
"""Generate literal Markdown and llms indexes; no third-party dependencies."""
import argparse
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BASE = "https://dstockton.github.io/qdrant-es-gateway"


def split_document(text):
    match = re.match(r"\A---\r?\n(.*?)\r?\n---\r?\n(.*)\Z", text, re.S)
    if not match:
        return {}, text
    metadata = {}
    for line in match[1].splitlines():
        if ":" in line:
            key, value = line.split(":", 1)
            metadata[key] = value.strip().strip('\"\'')
    return metadata, match[2].lstrip()


def render(doc):
    metadata, body = split_document(doc.read_text())
    title = metadata.get("title", doc.stem)
    # Site-root links must keep their meaning when read as a raw text asset.
    body = re.sub(r"{{\s*['\"]([^'\"]+)['\"]\s*\|\s*relative_url\s*}}",
                  lambda m: BASE + "/" + m[1].lstrip("/"), body)
    body = body.replace("{{ site.baseurl }}", BASE).replace("{{site.baseurl}}", BASE)
    body = re.sub(r"{%\s*post_url\s+([^\s%]+)\s*%}",
                  lambda m: BASE + "/markdown/" + m[1] + ".md", body)
    # Resolve ordinary relative Markdown links against the original page path.
    from urllib.parse import urljoin
    page_base = BASE + "/"
    body = re.sub(r"(\]\()([^\s)]+)(\))",
                  lambda m: m[1] + (urljoin(page_base, m[2])
                                    if not m[2].startswith(("#", "http:", "https:", "mailto:"))
                                    else m[2]) + m[3], body)
    return title, f"# {title}\n\n" + body.rstrip() + "\n"


def outputs(root=ROOT):
    docs = root / "docs"
    generated = {}
    entries = []
    for path in sorted((docs / "_posts").glob("*.md")):
        title, content = render(path)
        generated[docs / "markdown" / path.name] = content
        entries.append((title, BASE + "/markdown/" + path.name, content))
    index = "# Qdrant ES Gateway\n\n> Elasticsearch-compatible application search backed by Qdrant. Compatibility is intentionally scoped.\n\n"
    index += "## Start here\n\n"
    full = index
    primary = [docs / (name + ".md") for name in ("compatibility", "architecture", "production", "migration")]
    guides = primary + [p for p in sorted(docs.glob("*.md")) if p not in primary and p.name not in ("blog.md", "index.md")]
    for path in guides:
        title, content = render(path)
        generated[docs / "markdown" / path.name] = content
        index += f"- [{title}]({BASE}/markdown/{path.name})\n"
        full += f"\nSource: {BASE}/markdown/{path.name}\n\n{content}\n"
    index += "\n## Articles (Markdown)\n\n"
    for title, url, content in reversed(entries):
        index += f"- [{title}]({url})\n"
        full += f"\n---\n\nSource: {url}\n\n{content}\n"
    index += f"\n## Complete text\n\n- [All documentation and articles]({BASE}/llms-full.txt)\n"
    generated[docs / "llms.txt"] = index
    generated[docs / "llms-full.txt"] = full
    return generated


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    expected = outputs()
    stale = set((ROOT / "docs/markdown").glob("*.md")) - set(expected)
    changed = [p for p, content in expected.items() if not p.exists() or p.read_text() != content]
    if args.check:
        for path in sorted(stale | set(changed)):
            print(f"Stale generated documentation: {path.relative_to(ROOT)}")
        return int(bool(stale or changed))
    for path in stale:
        path.unlink()
    for path, content in expected.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    print(f"Exported {len(expected)} documentation assets")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
