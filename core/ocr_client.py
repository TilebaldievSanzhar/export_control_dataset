"""OCR API client for document text extraction."""

import asyncio
from typing import Optional
import httpx

from config.settings import settings
from utils.retry import retry_async


class OCRError(Exception):
    """OCR API error."""
    pass


class OCRTimeoutError(OCRError):
    """OCR processing timeout."""
    pass


class OCRClient:
    """OCR API client with async support."""

    def __init__(
        self,
        api_url: Optional[str] = None,
        timeout: Optional[int] = None,
    ):
        self._api_url = (api_url or settings.ocr.api_url).rstrip("/")
        self._timeout = timeout or settings.ocr.timeout

    def test_connection(self) -> bool:
        """Test if OCR API is accessible."""
        try:
            with httpx.Client(timeout=10) as client:
                response = client.get(f"{self._api_url}/")
                return response.status_code in (200, 404)
        except Exception:
            return False

    @retry_async(max_attempts=3, delay=5, backoff=2, exceptions=(httpx.TimeoutException, httpx.ConnectError))
    async def process_file(self, file_data: bytes, filename: str = "document.pdf") -> str:
        """
        Submit file to OCR API and return extracted text.

        Args:
            file_data: PDF file contents as bytes
            filename: Name of the file

        Returns:
            Extracted text from data.text field
        """
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            files = {"file": (filename, file_data, "application/pdf")}
            response = await client.post(
                f"{self._api_url}/ocr/process",
                files=files,
            )
            response.raise_for_status()
            result = response.json()

            status = result.get("status", "").lower()
            if status == "completed":
                return result.get("data", {}).get("text", "")

            error = result.get("error")
            raise OCRError(f"OCR failed with status '{status}': {error}")

    def process_file_sync(self, file_data: bytes, filename: str = "document.pdf") -> str:
        """
        Synchronous wrapper for process_file.

        Args:
            file_data: PDF file contents as bytes
            filename: Name of the file

        Returns:
            Extracted text
        """
        return asyncio.run(self.process_file(file_data, filename))
