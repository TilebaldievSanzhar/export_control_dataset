"""OCR-based text extraction for scanned documents."""

import asyncio
from typing import Optional

from core.minio_client import MinIOClient
from core.ocr_client import OCRClient, OCRError, OCRTimeoutError
from config.settings import settings


class OCRExtractor:
    """Extract text from scanned PDFs using OCR API."""

    def __init__(
        self,
        minio_client: Optional[MinIOClient] = None,
        ocr_client: Optional[OCRClient] = None,
        max_concurrent: Optional[int] = None,
    ):
        self._minio = minio_client or MinIOClient()
        self._ocr = ocr_client or OCRClient()
        self._max_concurrent = max_concurrent or settings.ocr.max_concurrent
        self._semaphore: Optional[asyncio.Semaphore] = None

    @property
    def semaphore(self) -> asyncio.Semaphore:
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self._max_concurrent)
        return self._semaphore

    async def process_file(self, object_name: str) -> tuple[str, Optional[str]]:
        """
        Process single file through OCR.

        Args:
            object_name: MinIO object path

        Returns:
            Tuple of (extracted_text, error_message)
        """
        async with self.semaphore:
            try:
                file_data = self._minio.download_file(object_name)
                filename = object_name.split("/")[-1]
                text = await self._ocr.process_file(file_data, filename)
                return text, None
            except (OCRError, OCRTimeoutError) as e:
                return "", str(e)
            except Exception as e:
                return "", f"Unexpected error: {e}"

    async def process_saf_files(
        self,
        saf_number: str,
        directory: str = "specs",
        separator: str = "\n\n---\n\n",
    ) -> tuple[str, list[str], list[str]]:
        """
        Process all files for a SAF number through OCR.

        Args:
            saf_number: SAF number
            directory: MinIO directory (specs, permit, license)
            separator: Text separator between files

        Returns:
            Tuple of (combined_text, processed_files, errors)
        """
        files = self._minio.get_files_for_saf(saf_number, directory)

        if not files:
            return "", [], None

        # Process files concurrently
        tasks = [self.process_file(f) for f in files]
        results = await asyncio.gather(*tasks)

        texts = []
        processed = []
        errors = []

        for file_path, (text, error) in zip(files, results):
            filename = file_path.split("/")[-1]
            if error:
                errors.append(f"{filename}: {error}")
            elif text.strip():
                texts.append(text)
                processed.append(filename)
            else:
                errors.append(f"{filename}: Empty OCR result")

        combined_text = separator.join(texts) if texts else ""
        return combined_text, processed, errors if errors else None

    def process_saf_files_sync(
        self,
        saf_number: str,
        directory: str = "specs",
        separator: str = "\n\n---\n\n",
    ) -> tuple[str, list[str], list[str]]:
        """
        Synchronous wrapper for process_saf_files.

        Args:
            saf_number: SAF number
            directory: MinIO directory (specs, permit, license)
            separator: Text separator between files

        Returns:
            Tuple of (combined_text, processed_files, errors)
        """
        return asyncio.run(self.process_saf_files(saf_number, directory, separator))
