#!/usr/bin/env python3
"""
Script to remove embedded images (base64 data URLs) from converted Jupyter notebook Python files.
This script specifically targets the markdown image patterns like:
![Screenshot N](data:image/png;base64,...)
"""

import re
import sys
import os
from pathlib import Path


def remove_images_from_py_file(input_file: str, output_file: str = None) -> None:
    """
    Remove base64 encoded images from a Python file converted from Jupyter notebook.
    
    Args:
        input_file (str): Path to the input .py file
        output_file (str, optional): Path to the output file. If None, overwrites input file.
    """
    
    # Read the input file
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            content = f.read()
    except FileNotFoundError:
        print(f"Error: Input file '{input_file}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading file '{input_file}': {e}")
        sys.exit(1)
    
    # Pattern to match markdown images with base64 data URLs
    # Matches patterns like: ![Screenshot 1](data:image/png;base64,iVBORw0KGgoAAA...)
    image_pattern = r'!\[.*?\]\(data:image/[^;]+;base64,[A-Za-z0-9+/=]+\)'
    
    # Count matches before removal
    matches = re.findall(image_pattern, content)
    num_images = len(matches)
    
    if num_images == 0:
        print("No embedded images found in the file.")
        return
    
    print(f"Found {num_images} embedded image(s) to remove.")
    
    # Remove the images
    cleaned_content = re.sub(image_pattern, '', content)
    
    # Clean up any extra empty lines that might be left behind
    # Replace multiple consecutive empty lines with at most 2 empty lines
    cleaned_content = re.sub(r'\n\s*\n\s*\n+', '\n\n\n', cleaned_content)
    
    # Determine output file
    if output_file is None:
        output_file = input_file
    
    # Write the cleaned content
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(cleaned_content)
        
        if output_file == input_file:
            print(f"Successfully removed {num_images} image(s) from '{input_file}'")
        else:
            print(f"Successfully removed {num_images} image(s) and saved cleaned file as '{output_file}'")
            
    except Exception as e:
        print(f"Error writing to file '{output_file}': {e}")
        sys.exit(1)


def main():
    """Main function to handle command line arguments and execute the image removal."""
    
    if len(sys.argv) < 2:
        print("Usage: python remove_images.py <input_file.py> [output_file.py]")
        print("Example: python remove_images.py notebook.py")
        print("Example: python remove_images.py notebook.py cleaned_notebook.py")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    # Validate input file exists and is a .py file
    if not os.path.exists(input_file):
        print(f"Error: Input file '{input_file}' does not exist.")
        sys.exit(1)
    
    if not input_file.endswith('.py'):
        print("Warning: Input file does not have a .py extension.")
    
    # Execute the image removal
    remove_images_from_py_file(input_file, output_file)


if __name__ == "__main__":
    main() 