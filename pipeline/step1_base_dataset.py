"""Step 1: Create base dataset from PostgreSQL."""

import json
from pathlib import Path
from typing import Optional
import pandas as pd

from core.database import Database
from core.minio_client import MinIOClient
from config.settings import settings
from utils.logger import get_step_logger


class Step1BaseDataset:
    """Create base dataset and document mapping."""

    def __init__(
        self,
        database: Optional[Database] = None,
        minio_client: Optional[MinIOClient] = None,
        output_dir: Optional[Path] = None,
        state_dir: Optional[Path] = None,
    ):
        self._db = database or Database()
        self._minio = minio_client or MinIOClient()
        self._output_dir = output_dir or settings.paths.output_dir
        self._state_dir = state_dir or settings.paths.state_dir
        self._logger = get_step_logger("step1_base_dataset")

    def _ensure_dirs(self) -> None:
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._state_dir.mkdir(parents=True, exist_ok=True)

    def run(self, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Run step 1: create base dataset.

        Args:
            limit: Optional limit on number of records

        Returns:
            Base dataset DataFrame
        """
        self._ensure_dirs()
        self._logger.info("Starting Step 1: Base dataset creation")

        # Get base dataset from database
        self._logger.info("Fetching base dataset from PostgreSQL...")
        df = self._db.get_base_dataset()

        if limit:
            df = df.head(limit)
            self._logger.info(f"Limited to {limit} records")

        self._logger.info(f"Retrieved {len(df)} product records")
        self._logger.info(f"Unique SAF numbers: {df['saf_number'].nunique()}")

        # Save base dataset
        output_path = self._output_dir / "step1_base_dataset.parquet"
        df.to_parquet(output_path, index=False)
        self._logger.info(f"Saved base dataset to {output_path}")

        # Create document mapping
        self._create_document_mapping(df["saf_number"].unique().tolist())

        self._logger.info("Step 1 completed successfully")
        return df

    def _create_document_mapping(self, saf_numbers: list[str]) -> dict:
        """
        Create mapping of SAF numbers to document files.

        Args:
            saf_numbers: List of SAF numbers to process

        Returns:
            Document mapping dictionary
        """
        self._logger.info("Creating document mapping from MinIO...")

        mapping = {
            "specs": {},
            "permit": {},
            "license": {},
        }

        for directory in mapping.keys():
            self._logger.info(f"Scanning {directory}/ directory...")
            saf_with_files = self._minio.get_all_saf_numbers_with_files(directory)

            for saf_number in saf_numbers:
                if saf_number in saf_with_files:
                    files = self._minio.get_files_for_saf(saf_number, directory)
                    if files:
                        mapping[directory][saf_number] = files

            self._logger.info(
                f"Found {len(mapping[directory])} SAF numbers with files in {directory}/"
            )

        # Save mapping
        mapping_path = self._state_dir / "document_mapping.json"
        with open(mapping_path, "w", encoding="utf-8") as f:
            json.dump(mapping, f, ensure_ascii=False, indent=2)
        self._logger.info(f"Saved document mapping to {mapping_path}")

        return mapping

    def load_document_mapping(self) -> dict:
        """Load existing document mapping."""
        mapping_path = self._state_dir / "document_mapping.json"
        if mapping_path.exists():
            with open(mapping_path, "r", encoding="utf-8") as f:
                return json.load(f)
        return {"specs": {}, "permit": {}, "license": {}}
