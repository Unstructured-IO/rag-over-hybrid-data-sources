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

HANDLE_RE = re.compile(r"^\s*# \[\[MD:([A-Z_]+)\]\]\s*$")

def load_blocks():
    with open(MD_MAP, 'r') as f:
        return yaml.safe_load(f)


def enrich_file():
    blocks = load_blocks()
    lines = SRC.read_text().splitlines()
    enriched = []
    for line in lines:
        m = HANDLE_RE.match(line)
        if m:
            key = m.group(1)
            block = blocks.get(key)
            if block is None:
                enriched.append(line)
            else:
                enriched.append(block.rstrip('\n'))
        else:
            enriched.append(line)
    OUT.write_text('\n'.join(enriched) + '\n')
    return OUT


def convert_with_jupytext(py_path: Path):
    # Convert enriched python to notebook using jupytext
    nb_path = py_path.with_suffix('.ipynb')
    cmd = [sys.executable, '-m', 'jupytext', '--to', 'ipynb', str(py_path)]
    subprocess.check_call(cmd)
    return nb_path


def main():
    enriched = enrich_file()
    nb = convert_with_jupytext(enriched)
    print(f"✅ Enriched: {enriched}")
    print(f"✅ Notebook: {nb}")

if __name__ == '__main__':
    main()
