#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Quick Start Script - PDF Extractor Tool
Provides an interactive interface for users to quickly extract PDF content
"""
import os
import sys
import argparse
from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich import print as rprint

# Add project path to sys.path
current_dir = Path(__file__).parent.absolute()
sys.path.insert(0, str(current_dir)) 

# Change to script directory to ensure consistent working directory (regardless of where it's run from)
# This ensures consistent behavior whether running start.py directly or through start.bat
os.chdir(current_dir)

from pdf_extractor.main import main as cli_main

console = Console()


def print_banner():
    """Print welcome banner"""
    banner = """
    ╔══════════════════════════════════════════════════╗
    ║      PDF Extractor CLI - Quick Start            ║
    ╚══════════════════════════════════════════════════╝
    """
    console.print(Panel.fit(banner, style="bold blue"))


def get_pdf_file():
    """Get PDF file path"""
    console.print("\n[bold cyan]Step 1: Select PDF File[/bold cyan]")
    
    while True:
        file_path = Prompt.ask(
            "\nEnter PDF file path (or drag and drop file here)",
            default=""
        ).strip().strip('"').strip("'")
        
        if not file_path:
            console.print("[yellow]No file path entered, please try again[/yellow]")
            continue
        
        # Convert to absolute path
        file_path = os.path.abspath(file_path)
        
        if not os.path.exists(file_path):
            console.print(f"[red]Error: File does not exist: {file_path}[/red]")
            if not Confirm.ask("Retry?", default=True):
                sys.exit(1)
            continue
        
        if not file_path.lower().endswith('.pdf'):
            console.print("[yellow]Warning: File is not a PDF format[/yellow]")
            if not Confirm.ask("Continue?", default=False):
                continue
        
        return file_path


def get_extraction_options():
    """Get extraction options"""
    console.print("\n[bold cyan]Step 2: Select Extraction Options[/bold cyan]")
    console.print("\nSelect content to extract (multiple selections allowed):")
    
    options = {
        'text': False,
        'tables': False,
        'images': False,
        'ocr': False
    }
    
    options['text'] = Confirm.ask("  [1] Extract Text?", default=True)
    options['tables'] = Confirm.ask("  [2] Extract Tables?", default=True)
    options['images'] = Confirm.ask("  [3] Extract Images?", default=False)
    options['ocr'] = Confirm.ask("  [4] OCR Processing?", default=False)
    
    if not any(options.values()):
        console.print("[red]Error: At least one extraction option must be selected[/red]")
        return get_extraction_options()
    
    return options


def get_page_range():
    """Get page range"""
    console.print("\n[bold cyan]Step 3: Select Page Range (Optional)[/bold cyan]")
    
    extract_all = Confirm.ask(
        "Extract all pages?",
        default=True
    )
    
    if extract_all:
        return None
    
    while True:
        page_input = Prompt.ask(
            "Enter page range (e.g., 1-3,5,7-9)",
            default=""
        ).strip()
        
        if not page_input:
            return None
        
        # Validate page range format
        try:
            parts = page_input.split(',')
            for part in parts:
                if '-' in part:
                    start, end = map(int, part.split('-'))
                    if start <= 0 or end <= 0 or start > end:
                        raise ValueError
                else:
                    if int(part) <= 0:
                        raise ValueError
            return page_input
        except ValueError:
            console.print("[red]Error: Invalid page range format, please try again[/red]")
            if not Confirm.ask("Retry?", default=True):
                return None


def get_output_dir():
    """Get output directory"""
    console.print("\n[bold cyan]Step 4: Set Output Directory (Optional)[/bold cyan]")
    
    # Get script directory to ensure default output directory is based on project root
    script_dir = Path(__file__).parent.absolute()
    default_output_dir = str(script_dir / "output")
    
    use_default = Confirm.ask(
        f"Use default output directory ({default_output_dir})?",
        default=True
    )
    
    if use_default:
        return default_output_dir
    
    while True:
        output_dir = Prompt.ask(
            "Enter output directory path",
            default=default_output_dir
        ).strip().strip('"').strip("'")
        
        if not output_dir:
            output_dir = default_output_dir
        
        # If relative path, convert to absolute path based on script directory
        if not os.path.isabs(output_dir):
            output_dir = str(script_dir / output_dir)
        
        try:
            os.makedirs(output_dir, exist_ok=True)
            return output_dir
        except Exception as e:
            console.print(f"[red]Error: Cannot create directory: {e}[/red]")
            if not Confirm.ask("Retry?", default=True):
                return default_output_dir


def get_table_format():
    """Get table format"""
    console.print("\n[bold cyan]Step 5: Select Table Format (Optional)[/bold cyan]")
    
    format_choice = Prompt.ask(
        "Select table format",
        choices=["csv", "json"],
        default="csv"
    )
    
    return format_choice


def interactive_mode():
    """Interactive mode"""
    print_banner()
    
    # Get all options
    pdf_file = get_pdf_file()
    options = get_extraction_options()
    pages = get_page_range()
    output_dir = get_output_dir()
    
    table_format = "csv"
    if options['tables']:
        table_format = get_table_format()
    
    # Display configuration summary
    console.print("\n" + "="*60)
    console.print("[bold green]Configuration Summary:[/bold green]")
    console.print(f"  PDF File: {pdf_file}")
    console.print(f"  Extraction Options:")
    console.print(f"    - Text: {'✓' if options['text'] else '✗'}")
    console.print(f"    - Tables: {'✓' if options['tables'] else '✗'}")
    console.print(f"    - Images: {'✓' if options['images'] else '✗'}")
    console.print(f"    - OCR:  {'✓' if options['ocr'] else '✗'}")
    console.print(f"  Page Range: {pages if pages else 'All'}")
    console.print(f"  Output Directory: {output_dir}")
    if options['tables']:
        console.print(f"  Table Format: {table_format}")
    console.print("="*60)
    
    if not Confirm.ask("\nConfirm to start extraction?", default=True):
        console.print("[yellow]Cancelled[/yellow]")
        return 0
    
    # Build command line arguments
    sys.argv = ["pdf_extractor_cli.py"]
    sys.argv.append("--file")
    sys.argv.append(pdf_file)
    
    if options['text']:
        sys.argv.append("--text")
    if options['tables']:
        sys.argv.append("--tables")
    if options['images']:
        sys.argv.append("--images")
    if options['ocr']:
        sys.argv.append("--ocr")
    
    if pages:
        sys.argv.append("--pages")
        sys.argv.append(pages)
    
    sys.argv.append("--output")
    sys.argv.append(output_dir)
    
    if options['tables']:
        sys.argv.append("--table-format")
        sys.argv.append(table_format)
    
    # Run main program
    try:
        console.print("\n[bold cyan]Starting extraction...[/bold cyan]")
        result = cli_main()
        return result
    except KeyboardInterrupt:
        console.print("\n[yellow]User interrupted operation[/yellow]")
        return 1
    except Exception as e:
        console.print(f"\n[red]Error occurred: {e}[/red]")
        import traceback
        console.print(f"[dim]{traceback.format_exc()}[/dim]")
        return 1


def simple_mode(pdf_file=None):
    """Simple mode - Extract text and tables with default configuration"""
    print_banner()
    
    # Get script directory to ensure output directory is based on project root
    script_dir = Path(__file__).parent.absolute()
    default_output_dir = str(script_dir / "output")
    
    # Validate and get PDF file path
    if pdf_file:
        pdf_file = os.path.abspath(pdf_file)
        if not os.path.exists(pdf_file):
            console.print(f"[red]Error: File does not exist: {pdf_file}[/red]")
            pdf_file = None
        elif not pdf_file.lower().endswith('.pdf'):
            console.print("[yellow]Warning: File is not a PDF format[/yellow]")
            if not Confirm.ask("Continue?", default=False):
                pdf_file = None
    
    if not pdf_file:
        while True:
            file_path = Prompt.ask(
                "\n[bold cyan]Enter PDF file path (or drag and drop file here)[/bold cyan]",
                default=""
            ).strip().strip('"').strip("'")
            
            if not file_path:
                console.print("[yellow]No file path entered, please try again[/yellow]")
                continue
            
            file_path = os.path.abspath(file_path)
            
            if not os.path.exists(file_path):
                console.print(f"[red]Error: File does not exist: {file_path}[/red]")
                if not Confirm.ask("Retry?", default=True):
                    sys.exit(1)
                continue
            
            if not file_path.lower().endswith('.pdf'):
                console.print("[yellow]Warning: File is not a PDF format[/yellow]")
                if not Confirm.ask("Continue?", default=False):
                    continue
            
            pdf_file = file_path
            break
    
    console.print(f"\n[bold green]Processing: {os.path.basename(pdf_file)}[/bold green]")
    console.print(f"[dim]Default configuration: Extract text and tables, all pages, output to {default_output_dir}[/dim]")
    
    # Build command line arguments - default: extract text and tables
    sys.argv = ["pdf_extractor_cli.py", "--file", pdf_file, "--text", "--tables", "--output", default_output_dir]
    
    try:
        console.print("\n[bold cyan]Starting extraction...[/bold cyan]")
        result = cli_main()
        return result
    except KeyboardInterrupt:
        console.print("\n[yellow]User interrupted operation[/yellow]")
        return 1
    except Exception as e:
        console.print(f"\n[red]Error occurred: {e}[/red]")
        import traceback
        console.print(f"[dim]{traceback.format_exc()}[/dim]")
        return 1


def quick_mode(pdf_file=None):
    """Quick mode - Extract tables with default configuration (kept for backward compatibility)"""
    return simple_mode(pdf_file)


def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="PDF Extractor Tool - Quick Start Script (Simplified: Just enter file path to extract text and tables)",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--file", "-f",
        type=str,
        help="PDF file path (can be specified directly in command line)"
    )
    parser.add_argument(
        "--interactive", "-i",
        action="store_true",
        help="Use interactive mode (detailed configuration options)"
    )
    parser.add_argument(
        "--quick", "-q",
        action="store_true",
        help="(Deprecated, same as default mode)"
    )
    
    args = parser.parse_args()
    
    if args.interactive:
        return interactive_mode()
    
    return simple_mode(args.file)


if __name__ == "__main__":
    # main.py already handles Windows console encoding, no need to handle it here
    # Avoid stream conflicts that cause "I/O operation on closed file" errors
    sys.exit(main())
