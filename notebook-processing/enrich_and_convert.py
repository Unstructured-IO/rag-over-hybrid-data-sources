#!/usr/bin/env python3
import re
import sys
import argparse
import base64
from pathlib import Path
import yaml
import subprocess
from PIL import Image
import io

ROOT = Path(__file__).resolve().parents[1]
MD_MAP = Path(__file__).resolve().parent / 'markdown_blocks.yaml'
CODE_MAP = Path(__file__).resolve().parent / 'code_blocks.yaml'
IMG_MAP = Path(__file__).resolve().parent / 'image_blocks.yaml'
CODE_SCRIPTS_DIR = Path(__file__).resolve().parent / 'code-scripts'
IMAGES_DIR = Path(__file__).resolve().parent / 'images'

MD_HANDLE_RE = re.compile(r"# \[\[MD:([A-Z0-9_]+)\]\]")
CODE_HANDLE_RE = re.compile(r"# \[\[CODE:([A-Z0-9_]+)\]\]")
IMG_HANDLE_RE = re.compile(r"# \[\[IMG:([A-Z0-9_]+)\]\]")

def load_markdown_blocks():
    with open(MD_MAP, 'r') as f:
        return yaml.safe_load(f)

def load_code_blocks():
    if CODE_MAP.exists():
        with open(CODE_MAP, 'r') as f:
            return yaml.safe_load(f)
    return {}

def load_image_blocks():
    if IMG_MAP.exists():
        with open(IMG_MAP, 'r') as f:
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

def resize_image_if_needed(image_path: Path, max_width: int = 800) -> bytes:
    """Resize image if it's wider than max_width, maintaining aspect ratio."""
    try:
        with Image.open(image_path) as img:
            # Convert to RGB if necessary (handles RGBA, P mode images)
            if img.mode in ('RGBA', 'P'):
                # Create a white background for transparent images
                background = Image.new('RGB', img.size, (255, 255, 255))
                if img.mode == 'P':
                    img = img.convert('RGBA')
                background.paste(img, mask=img.split()[-1] if img.mode == 'RGBA' else None)
                img = background
            elif img.mode != 'RGB':
                img = img.convert('RGB')
            
            # Resize if needed
            if img.width > max_width:
                # Calculate new height maintaining aspect ratio
                aspect_ratio = img.height / img.width
                new_height = int(max_width * aspect_ratio)
                img = img.resize((max_width, new_height), Image.Resampling.LANCZOS)
                print(f"[INFO] Resized {image_path.name} from {img.width}x{img.height} to {max_width}x{new_height}")
            
            # Save to bytes
            img_bytes = io.BytesIO()
            img.save(img_bytes, format='PNG', optimize=True)
            return img_bytes.getvalue()
            
    except Exception as e:
        print(f"[WARN] Error resizing image {image_path}: {e}")
        # Fallback to original file
        return image_path.read_bytes()

def load_image_as_base64(image_path: str) -> str:
    """Load image file, resize if needed, and convert to base64 for embedding."""
    image_file = Path(__file__).resolve().parent / image_path
    if not image_file.exists():
        print(f"[WARN] Image file not found: {image_path}")
        return f"# Image not found: {image_path}"
    
    try:
        # Resize image if needed (max 800px wide)
        image_data = resize_image_if_needed(image_file, max_width=800)
        
        # Determine MIME type based on file extension
        ext = image_file.suffix.lower()
        mime_types = {
            '.png': 'image/png',
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.gif': 'image/gif',
            '.svg': 'image/svg+xml',
            '.webp': 'image/webp'
        }
        mime_type = mime_types.get(ext, 'image/png')
        
        # Encode as base64
        base64_data = base64.b64encode(image_data).decode('utf-8')
        
        # Get filename for alt text
        filename = image_file.stem
        
        return f"![{filename}](data:{mime_type};base64,{base64_data})"
        
    except Exception as e:
        print(f"[WARN] Error loading image {image_path}: {e}")
        return f"# Error loading image: {image_path}"

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

def enrich_file(source_file: Path, output_file: Path, strict: bool = True, include_images: bool = False):
    md_blocks = load_markdown_blocks()
    code_blocks = load_code_blocks()
    img_blocks = load_image_blocks() if include_images else {}
    
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
        
        # Check for image handles
        img_match = IMG_HANDLE_RE.search(line)
        if img_match:
            key = img_match.group(1)
            if not include_images:
                print(f"[INFO] Skipping image handle: {key} (images disabled)")
                enriched_lines.append(f"# [[IMG:{key}]]  # Image disabled - use --include-images to enable")
                continue
                
            image_path = img_blocks.get(key)
            if image_path is None:
                print(f"[WARN] No image block found for handle: {key}")
                enriched_lines.append(line)
            else:
                print(f"[INFO] Replacing image handle: {key} with {image_path}")
                image_content = load_image_as_base64(image_path)
                enriched_lines.append(_to_percent_markdown_cell(image_content))
            continue
        
        # Regular line - keep as is
        enriched_lines.append(line)
    
    enriched_text = '\n'.join(enriched_lines) + '\n'
    
    if strict:
        # Check for any remaining handles
        remaining_md = [ln for ln in enriched_text.splitlines() if '[[MD:' in ln]
        remaining_code = [ln for ln in enriched_text.splitlines() if '[[CODE:' in ln]
        remaining_img = [ln for ln in enriched_text.splitlines() if '[[IMG:' in ln and 'Image disabled' not in ln]
        
        if remaining_md:
            raise SystemExit(f"[ERROR] Unreplaced MD handles remain: {remaining_md[:5]}")
        if remaining_code:
            raise SystemExit(f"[ERROR] Unreplaced CODE handles remain: {remaining_code[:5]}")
        if remaining_img:
            raise SystemExit(f"[ERROR] Unreplaced IMG handles remain: {remaining_img[:5]}")
    
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
    parser.add_argument('--include-images', action='store_true',
                       help='Include images in the output (default: False)')
    
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
    print(f"🖼️ Images: {'Enabled' if args.include_images else 'Disabled'}")
    print()
    
    enriched = enrich_file(source_file, output_file, strict=True, include_images=args.include_images)
    nb = convert_with_jupytext(enriched)
    print(f"✅ Enriched: {enriched}")
    print(f"✅ Notebook: {nb}")

if __name__ == '__main__':
    main()
