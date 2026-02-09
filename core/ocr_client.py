"""OCR API client for document text extraction."""

import asyncio
import time
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
        secret_key: Optional[str] = None,
        poll_interval: Optional[int] = None,
        timeout: Optional[int] = None,
    ):
        self._api_url = (api_url or settings.ocr.api_url).rstrip("/")
        self._secret_key = secret_key or settings.ocr.secret_key
        self._poll_interval = poll_interval or settings.ocr.poll_interval
        self._timeout = timeout or settings.ocr.timeout

    @property
    def headers(self) -> dict[str, str]:
        return {"SECRET_KEY": self._secret_key}

    def test_connection(self) -> bool:
        """Test if OCR API is accessible."""
        try:
            with httpx.Client(timeout=10) as client:
                response = client.get(f"{self._api_url}/", headers=self.headers)
                return response.status_code in (200, 404)  # 404 is OK, just means no endpoint at root
        except Exception:
            return False

    @retry_async(max_attempts=3, delay=5, backoff=2, exceptions=(httpx.TimeoutException, httpx.ConnectError))
    async def submit_file(self, file_data: bytes, filename: str = "document.pdf") -> str:
        """
        Submit file to OCR API.

        Args:
            file_data: PDF file contents as bytes
            filename: Name of the file

        Returns:
            Request ID for polling
        """
        async with httpx.AsyncClient(timeout=30) as client:
            files = {"files_list": (filename, file_data, "application/pdf")}
            data = {"test_request": "True"}
            response = await client.post(
                f"{self._api_url}/request/",
                headers=self.headers,
                files=files,
                data=data,
            )
            response.raise_for_status()
            result = response.json()
            return result["request_id"]

    async def get_result(self, request_id: str) -> dict:
        """
        Get OCR result by request ID.

        Args:
            request_id: Request ID from submit_file

        Returns:
            OCR result dictionary
        """
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                f"{self._api_url}/request/{request_id}/",
                headers=self.headers,
            )
            response.raise_for_status()
            return response.json()

    async def wait_for_result(self, request_id: str) -> str:
        """
        Poll OCR API until result is ready.

        Args:
            request_id: Request ID from submit_file

        Returns:
            Extracted text

        Raises:
            OCRTimeoutError: If processing takes longer than timeout
            OCRError: If OCR processing fails
        """
        start_time = time.time()

        while True:
            elapsed = time.time() - start_time
            if elapsed > self._timeout:
                raise OCRTimeoutError(f"OCR timeout after {self._timeout}s for request {request_id}")

            result = await self.get_result(request_id)
            status = result.get("status", "").upper()

            if status == "COMPLETED":
                # Extract raw_text from result
                tasks = result.get("tasks", [])
                if tasks:
                    ocr_result = tasks[0].get("ocr_result", {})
                    return ocr_result.get("raw_text", "")
                return ""

            if status in ("FAILED", "ERROR"):
                raise OCRError(f"OCR failed for request {request_id}: {result}")

            await asyncio.sleep(self._poll_interval)

    async def process_file(self, file_data: bytes, filename: str = "document.pdf") -> str:
        """
        Submit file and wait for OCR result.

        Args:
            file_data: PDF file contents as bytes
            filename: Name of the file

        Returns:
            Extracted text
        """
        request_id = await self.submit_file(file_data, filename)
        return await self.wait_for_result(request_id)

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
