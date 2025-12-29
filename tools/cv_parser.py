# tools/cv_parser.py
"""
CV text extraction from PDF, DOCX, and TXT files.
"""
import os
from typing import Optional

try:
    import PyPDF2
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False
    print("Warning: PyPDF2 not installed. PDF parsing unavailable.")

try:
    from docx import Document
    DOCX_AVAILABLE = True
except ImportError:
    DOCX_AVAILABLE = False
    print("Warning: python-docx not installed. DOCX parsing unavailable.")


def extract_text_from_pdf(file_path: str) -> str:
    """Extract text from PDF file."""
    if not PDF_AVAILABLE:
        raise ImportError("PyPDF2 not installed. Run: pip install PyPDF2")
    
    text = []
    try:
        with open(file_path, 'rb') as file:
            pdf_reader = PyPDF2.PdfReader(file)
            for page in pdf_reader.pages:
                page_text = page.extract_text()
                if page_text:
                    text.append(page_text)
        
        return '\n'.join(text)
    except Exception as e:
        raise Exception(f"Error extracting text from PDF: {e}")


def extract_text_from_docx(file_path: str) -> str:
    """Extract text from DOCX file."""
    if not DOCX_AVAILABLE:
        raise ImportError("python-docx not installed. Run: pip install python-docx")
    
    try:
        doc = Document(file_path)
        text = []
        for paragraph in doc.paragraphs:
            if paragraph.text.strip():
                text.append(paragraph.text)
        
        return '\n'.join(text)
    except Exception as e:
        raise Exception(f"Error extracting text from DOCX: {e}")


def extract_text_from_txt(file_path: str) -> str:
    """Extract text from TXT file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.read()
    except UnicodeDecodeError:
        # Try with latin-1 encoding
        with open(file_path, 'r', encoding='latin-1') as file:
            return file.read()
    except Exception as e:
        raise Exception(f"Error reading TXT file: {e}")


def extract_cv_text(file_path: str) -> str:
    """
    Extract text from CV file (auto-detects PDF/DOCX/TXT).
    
    Args:
        file_path: Path to CV file
        
    Returns:
        Extracted text content
        
    Raises:
        ValueError: If file format not supported
        Exception: If extraction fails
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    file_ext = os.path.splitext(file_path)[1].lower()
    
    if file_ext == '.pdf':
        return extract_text_from_pdf(file_path)
    elif file_ext in ['.docx', '.doc']:
        return extract_text_from_docx(file_path)
    elif file_ext == '.txt':
        return extract_text_from_txt(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_ext}. Supported: .pdf, .docx, .txt")


def clean_cv_text(text: str) -> str:
    """Clean and normalize CV text."""
    # Remove excessive whitespace
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    text = '\n'.join(lines)
    
    # Remove multiple consecutive newlines
    while '\n\n\n' in text:
        text = text.replace('\n\n\n', '\n\n')
    
    return text


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        cv_path = sys.argv[1]
        print(f"Extracting text from: {cv_path}")
        
        try:
            text = extract_cv_text(cv_path)
            text = clean_cv_text(text)
            
            print(f"\nExtracted {len(text)} characters")
            print(f"Preview (first 500 chars):\n{'-'*50}")
            print(text[:500])
            print(f"{'-'*50}")
            
        except Exception as e:
            print(f"Error: {e}")
    else:
        print("Usage: python cv_parser.py <path_to_cv>")
        print("Example: python cv_parser.py resume.pdf")
