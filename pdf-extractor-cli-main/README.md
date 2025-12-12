# PDF Extractor CLI

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

A powerful Python command-line tool that extracts data from PDF files with a clean, modular architecture.

![PDF Extractor CLI Demo](https://raw.githubusercontent.com/yourusername/pdf-extractor-cli/main/docs/images/demo.png)

## Features

- **Text Extraction**: Extract plain text from PDFs using PyMuPDF (fitz)
- **Table Extraction**: Extract tables as CSV or JSON using pdfplumber
- **Image Extraction**: Extract embedded images from PDFs
- **OCR Processing**: Apply OCR to image-based PDFs using pytesseract

## Installation

### Prerequisites

- Python 3.11 or higher
- [Tesseract OCR](https://github.com/tesseract-ocr/tesseract) (for OCR capabilities)
  - Windows: Download and install from [here](https://github.com/UB-Mannheim/tesseract/wiki)
  - macOS: `brew install tesseract`
  - Ubuntu/Debian: `sudo apt install tesseract-ocr`

### Installation Steps

```bash
# Clone the repository
git clone https://github.com/sfkbstnc/pdf-extractor-cli.git
cd pdf-extractor-cli

# Install dependencies
pip install -r requirements.txt

# Make the CLI executable (Linux/macOS)
chmod +x pdf_extractor_cli.py
```

## Usage

```bash
python pdf_extractor_cli.py --file [PDF_FILE] [OPTIONS]
```

### Options

| Option | Description |
|--------|-------------|
| `--file` | Path to the PDF file (required) |
| `--text` | Extract plain text |
| `--tables` | Extract tables |
| `--images` | Extract images |
| `--ocr` | Apply OCR to images |
| `--pages` | Page numbers or ranges to process (e.g. `1-3,5,7`) |
| `--output` | Directory to save outputs (default: `output/`) |
| `--table-format` | Format for table output (`csv` or `json`, default: `csv`) |
| `--verbose` | Enable verbose logging |

### Examples

#### Extract text from a PDF:
```bash
python pdf_extractor_cli.py --file document.pdf --text
```

#### Extract tables from pages 1-3 and save as JSON:
```bash
python pdf_extractor_cli.py --file document.pdf --tables --pages 1-3 --table-format json
```

#### Extract images from specific pages:
```bash
python pdf_extractor_cli.py --file document.pdf --images --pages 5-10
```

#### Apply OCR to extract text from an image-based PDF:
```bash
python pdf_extractor_cli.py --file scanned_document.pdf --ocr
```

#### Extract everything from a PDF:
```bash
python pdf_extractor_cli.py --file document.pdf --text --tables --images --ocr
```

## Output Examples

### Text Extraction
```
--- Page 1 ---
Annual Report 2023
Company XYZ

Our mission is to provide the best services...
```

### Table Extraction (CSV)
```csv
Name,Age,Position
John Doe,35,CEO
Jane Smith,32,CTO
Mike Johnson,28,Developer
```

### Image Extraction
Images are saved to the `output/[filename]_images/` directory with filenames like `page1_image1.png`.

## Project Structure

```
pdf-extractor-cli/
├── pdf_extractor/
│   ├── __init__.py
│   ├── main.py
│   ├── utils.py
│   ├── text_extractor.py
│   ├── table_extractor.py
│   ├── image_extractor.py
│   └── ocr_extractor.py
├── examples/
│   └── README.txt
├── output/
├── pdf_extractor_cli.py
├── requirements.txt
├── pyproject.toml
└── README.md
```

## Troubleshooting

### OCR Issues

#### Tesseract Not Found Error

If you encounter the following error:
```
Error: tesseract is not installed or it's not in your PATH. See README file for more information.
```

Solutions:
1. **Ensure Tesseract is installed**: Follow the installation instructions for your OS in the Prerequisites section.
2. **Add Tesseract to PATH**: 
   - Windows: Add the Tesseract installation directory (e.g., `C:\Program Files\Tesseract-OCR`) to your system's PATH variable.
   - Linux/macOS: Verify the installation with `which tesseract`.
3. **Specify Tesseract Path directly**:
   - If you know where tesseract is installed but can't modify PATH, you can set it in your code:
   ```python
   import pytesseract
   pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'  # Windows example
   ```

### Table Extraction Issues

If no tables are found in your PDF, verify that:
1. Your PDF actually contains tables (not just spaces/tabs formatting text)
2. The tables have clear borders or structural elements that pdfplumber can identify

## Dependencies

- [PyMuPDF (fitz)](https://github.com/pymupdf/PyMuPDF): For text and image extraction
- [pdfplumber](https://github.com/jsvine/pdfplumber): For table extraction
- [pandas](https://pandas.pydata.org/): For data handling
- [pytesseract](https://github.com/madmaze/pytesseract): For OCR
- [Pillow (PIL)](https://python-pillow.org/): For image processing
- [rich](https://rich.readthedocs.io/): For colorful terminal output

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details. 