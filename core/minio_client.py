"""MinIO (S3-compatible) storage client."""

import io
from pathlib import Path
from typing import Optional
from minio import Minio
from minio.error import S3Error

from config.settings import settings


class MinIOClient:
    """MinIO storage client for document operations."""

    def __init__(
        self,
        endpoint: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        bucket: Optional[str] = None,
        secure: Optional[bool] = None,
    ):
        self._endpoint = endpoint or settings.minio.endpoint
        self._access_key = access_key or settings.minio.access_key
        self._secret_key = secret_key or settings.minio.secret_key
        self._bucket = bucket or settings.minio.bucket
        self._secure = secure if secure is not None else settings.minio.secure
        self._client: Optional[Minio] = None

    @property
    def client(self) -> Minio:
        if self._client is None:
            self._client = Minio(
                self._endpoint,
                access_key=self._access_key,
                secret_key=self._secret_key,
                secure=self._secure,
            )
        return self._client

    def test_connection(self) -> bool:
        """Test if MinIO connection is working."""
        try:
            self.client.bucket_exists(self._bucket)
            return True
        except Exception:
            return False

    def list_files(self, prefix: str) -> list[str]:
        """
        List files in MinIO with given prefix.

        Args:
            prefix: Path prefix (e.g., "specs/2024-00001/")

        Returns:
            List of file paths
        """
        try:
            objects = self.client.list_objects(self._bucket, prefix=prefix, recursive=True)
            return [obj.object_name for obj in objects if not obj.is_dir]
        except S3Error:
            return []

    def get_files_for_saf(self, saf_number: str, directory: str) -> list[str]:
        """
        Get list of files for a SAF number in a specific directory.

        Args:
            saf_number: SAF number
            directory: Directory name (specs, permit, license)

        Returns:
            List of file paths
        """
        prefix = f"{directory}/{saf_number}/"
        return self.list_files(prefix)

    def download_file(self, object_name: str) -> bytes:
        """
        Download file from MinIO.

        Args:
            object_name: Full object path in bucket

        Returns:
            File contents as bytes
        """
        response = self.client.get_object(self._bucket, object_name)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def download_file_to_path(self, object_name: str, local_path: Path) -> None:
        """
        Download file from MinIO to local path.

        Args:
            object_name: Full object path in bucket
            local_path: Local file path to save to
        """
        self.client.fget_object(self._bucket, object_name, str(local_path))

    def get_file_stream(self, object_name: str) -> io.BytesIO:
        """
        Get file as BytesIO stream.

        Args:
            object_name: Full object path in bucket

        Returns:
            BytesIO stream with file contents
        """
        data = self.download_file(object_name)
        return io.BytesIO(data)

    def file_exists(self, object_name: str) -> bool:
        """Check if file exists in MinIO."""
        try:
            self.client.stat_object(self._bucket, object_name)
            return True
        except S3Error:
            return False

    def get_all_saf_numbers_with_files(self, directory: str) -> set[str]:
        """
        Get all SAF numbers that have files in a directory.

        Args:
            directory: Directory name (specs, permit, license)

        Returns:
            Set of SAF numbers
        """
        prefix = f"{directory}/"
        saf_numbers = set()
        try:
            objects = self.client.list_objects(self._bucket, prefix=prefix, recursive=False)
            for obj in objects:
                if obj.is_dir:
                    # Extract SAF number from path like "specs/2024-00001/"
                    saf_number = obj.object_name.rstrip("/").split("/")[-1]
                    saf_numbers.add(saf_number)
        except S3Error:
            pass
        return saf_numbers
