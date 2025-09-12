#!/usr/bin/env python3
import re
import sys
import argparse
from pathlib import Path
import yaml
import subprocess

ROOT = Path(__file__).resolve().parents[1]
MD_MAP = Path(__file__).resolve().parent / 'markdown_blocks.yaml'
CODE_MAP = Path(__file__).resolve().parent / 'code_blocks.yaml'
CODE_SCRIPTS_DIR = Path(__file__).resolve().parent / 'code-scripts'

MD_HANDLE_RE = re.compile(r"# \[\[MD:([A-Z0-9_]+)\]\]")
CODE_HANDLE_RE = re.compile(r"# \[\[CODE:([A-Z0-9_]+)\]\]")

def load_markdown_blocks():
    with open(MD_MAP, 'r') as f:
        return yaml.safe_load(f)

def load_code_blocks():
    if CODE_MAP.exists():
        with open(CODE_MAP, 'r') as f:
            return yaml.safe_load(f)
    return {}

def load_code_script(script_path: str) -> str:
    """Load code from a script file."""
    script_file = Path(__file__).resolve().parent / script_path
    if script_file.exists():
        return script_file.read_text()
    else:
        print(f"[WARN] Code script not found: {script_path}")
        return f"# Code script not found: {script_path}"

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
    return '\n'.join(md_lines)

def _to_percent_code_cell(code: str) -> str:
    """Convert code to a percent code cell format."""
    lines = ["# %%"]
    lines.extend(code.splitlines())
    return '\n'.join(lines)

def enrich_file(source_file: Path, output_file: Path, strict: bool = True):
    md_blocks = load_markdown_blocks()
    code_blocks = load_code_blocks()
    
    lines = source_file.read_text().splitlines()
    enriched_lines = []
    
    for line in lines:
        # Check for markdown handles
        md_match = MD_HANDLE_RE.search(line)
        if md_match:
            key = md_match.group(1)
            block = md_blocks.get(key)
            if block is None:
                print(f"[WARN] No markdown block found for handle: {key}")
                enriched_lines.append(line)
            else:
                print(f"[INFO] Replacing markdown handle: {key}")
                enriched_lines.append(_to_percent_markdown_cell(block))
            continue
        
        # Check for code handles
        code_match = CODE_HANDLE_RE.search(line)
        if code_match:
            key = code_match.group(1)
            script_path = code_blocks.get(key)
            if script_path is None:
                print(f"[WARN] No code block found for handle: {key}")
                enriched_lines.append(line)
            else:
                print(f"[INFO] Replacing code handle: {key} with {script_path}")
                code_content = load_code_script(script_path)
                enriched_lines.append(_to_percent_code_cell(code_content))
            continue
        
        # Regular line - keep as is
        enriched_lines.append(line)
    
    enriched_text = '\n'.join(enriched_lines) + '\n'
    
    if strict:
        # Check for any remaining handles
        remaining_md = [ln for ln in enriched_text.splitlines() if '[[MD:' in ln]
        remaining_code = [ln for ln in enriched_text.splitlines() if '[[CODE:' in ln]
        
        if remaining_md:
            raise SystemExit(f"[ERROR] Unreplaced MD handles remain: {remaining_md[:5]}")
        if remaining_code:
            raise SystemExit(f"[ERROR] Unreplaced CODE handles remain: {remaining_code[:5]}")
    
    output_file.write_text(enriched_text)
    return output_file

def convert_with_jupytext(py_path: Path):
    # Convert enriched python to notebook using jupytext
    nb_path = py_path.with_suffix('.ipynb')
    cmd = [sys.executable, '-m', 'jupytext', '--to', 'ipynb', str(py_path)]
    subprocess.check_call(cmd)
    return nb_path

def main():
    parser = argparse.ArgumentParser(description='Enrich and convert pipeline to notebook')
    parser.add_argument('--source', choices=['original', 'modular'], default='modular',
                       help='Source pipeline to use (default: modular)')
    parser.add_argument('--output-suffix', default='enriched',
                       help='Suffix for output files (default: enriched)')
    
    args = parser.parse_args()
    
    # Determine source and output files
    if args.source == 'modular':
        source_file = ROOT / 'hybrid_rag_pipeline_modular.py'
    else:
        source_file = ROOT / 'hybrid_rag_pipeline.py'
    
    output_file = ROOT / f'hybrid_rag_pipeline_{args.output_suffix}.py'
    
    # Check if source file exists
    if not source_file.exists():
        print(f"❌ Source file not found: {source_file}")
        sys.exit(1)
    
    print(f"📄 Source: {source_file.name}")
    print(f"📄 Output: {output_file.name}")
    print(f"🔧 Mode: {args.source}")
    print()
    
    enriched = enrich_file(source_file, output_file, strict=True)
    nb = convert_with_jupytext(enriched)
    print(f"✅ Enriched: {enriched}")
    print(f"✅ Notebook: {nb}")

if __name__ == '__main__':
    main()
