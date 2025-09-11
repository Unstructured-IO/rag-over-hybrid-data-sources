#!/usr/bin/env python3
import re
import sys
from pathlib import Path
import yaml
import subprocess

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / 'hybrid_rag_pipeline.py'
OUT = ROOT / 'hybrid_rag_pipeline_enriched.py'
MD_MAP = Path(__file__).resolve().parent / 'markdown_blocks.yaml'

HANDLE_RE = re.compile(r"\[\[MD:([A-Z0-9_]+)\]\]")

def load_blocks():
    with open(MD_MAP, 'r') as f:
        return yaml.safe_load(f)


def _normalize_block(block: str) -> str:
    # Remove surrounding triple quotes if present
    lines = block.strip('\n').splitlines()
    if lines and lines[0].strip() == '"""' and lines[-1].strip() == '"""':
        lines = lines[1:-1]
    return '\n'.join(lines)


def _to_percent_markdown_cell(block: str) -> str:
    content = _normalize_block(block)
    md_lines = ["# %% [markdown]"]
    for ln in content.splitlines():
        md_lines.append(f"# {ln}")
    # Follow markdown cell with a code cell marker so subsequent content is code
    md_lines.append("# %%")
    return '\n'.join(md_lines)


def enrich_file(strict: bool = True):
    blocks = load_blocks()
    lines = SRC.read_text().splitlines()
    enriched_lines = []
    for line in lines:
        m = HANDLE_RE.search(line)
        if m:
            key = m.group(1)
            block = blocks.get(key)
            if block is None:
                print(f"[WARN] No markdown block found for handle: {key}")
                enriched_lines.append(line)
            else:
                print(f"[INFO] Replacing handle: {key}")
                enriched_lines.append(_to_percent_markdown_cell(block))
        else:
            enriched_lines.append(line)
    enriched_text = '\n'.join(enriched_lines) + '\n'
    if strict and '[[MD:' in enriched_text:
        remaining = [ln for ln in enriched_text.splitlines() if '[[MD:' in ln]
        raise SystemExit(f"[ERROR] Unreplaced handles remain: {remaining[:5]}")
    OUT.write_text(enriched_text)
    return OUT


def convert_with_jupytext(py_path: Path):
    # Convert enriched python to notebook using jupytext
    nb_path = py_path.with_suffix('.ipynb')
    cmd = [sys.executable, '-m', 'jupytext', '--to', 'ipynb', str(py_path)]
    subprocess.check_call(cmd)
    return nb_path


def main():
    enriched = enrich_file(strict=True)
    nb = convert_with_jupytext(enriched)
    print(f"✅ Enriched: {enriched}")
    print(f"✅ Notebook: {nb}")

if __name__ == '__main__':
    main()
