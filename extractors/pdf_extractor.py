"""PDF text extraction using PyPDF2 and pdfplumber."""

import io
from typing import Optional
import PyPDF2
import pdfplumber


class PDFExtractionError(Exception):
    """Error during PDF text extraction."""
    pass


class PDFExtractor:
    """Extract text from machine-readable PDFs."""

    @staticmethod
    def extract_with_pypdf2(file_data: bytes) -> str:
        """
        Extract text using PyPDF2.

        Args:
            file_data: PDF file contents as bytes

        Returns:
            Extracted text
        """
        try:
            reader = PyPDF2.PdfReader(io.BytesIO(file_data))
            text_parts = []
            for page in reader.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
            return "\n\n".join(text_parts)
        except Exception as e:
            raise PDFExtractionError(f"PyPDF2 extraction failed: {e}") from e

    @staticmethod
    def extract_with_pdfplumber(file_data: bytes) -> str:
        """
        Extract text using pdfplumber.

        Args:
            file_data: PDF file contents as bytes

        Returns:
            Extracted text
        """
        try:
            text_parts = []
            with pdfplumber.open(io.BytesIO(file_data)) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)
            return "\n\n".join(text_parts)
        except Exception as e:
            raise PDFExtractionError(f"pdfplumber extraction failed: {e}") from e

    @classmethod
    def extract(cls, file_data: bytes, method: str = "auto") -> str:
        """
        Extract text from PDF using specified method.

        Args:
            file_data: PDF file contents as bytes
            method: Extraction method - "pypdf2", "pdfplumber", or "auto"

        Returns:
            Extracted text
        """
        if method == "pypdf2":
            return cls.extract_with_pypdf2(file_data)
        elif method == "pdfplumber":
            return cls.extract_with_pdfplumber(file_data)
        elif method == "auto":
            # Try pdfplumber first (usually better quality), fall back to PyPDF2
            try:
                text = cls.extract_with_pdfplumber(file_data)
                if text.strip():
                    return text
            except PDFExtractionError:
                pass

            try:
                return cls.extract_with_pypdf2(file_data)
            except PDFExtractionError:
                pass

            return ""
        else:
            raise ValueError(f"Unknown extraction method: {method}")

    @classmethod
    def extract_multiple(
        cls,
        files_data: list[tuple[str, bytes]],
        separator: str = "\n\n---\n\n"
    ) -> tuple[str, list[str], list[str]]:
        """
        Extract text from multiple PDF files.

        Args:
            files_data: List of (filename, file_data) tuples
            separator: Text separator between files

        Returns:
            Tuple of (combined_text, processed_files, errors)
        """
        texts = []
        processed = []
        errors = []

        for filename, file_data in files_data:
            try:
                text = cls.extract(file_data)
                if text.strip():
                    texts.append(text)
                    processed.append(filename)
                else:
                    errors.append(f"{filename}: Empty text extracted")
            except PDFExtractionError as e:
                errors.append(f"{filename}: {e}")

        combined_text = separator.join(texts) if texts else ""
        return combined_text, processed, errors if errors else None
