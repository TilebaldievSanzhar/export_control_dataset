"""Step 3: Extract text from permit and license documents."""

import json
from pathlib import Path
from typing import Optional
import pandas as pd

from core.minio_client import MinIOClient
from extractors.pdf_extractor import PDFExtractor
from config.settings import settings
from utils.logger import get_step_logger
from utils.progress import StateManager, ProgressTracker


class Step3PermitLicense:
    """Extract text from permit and license PDFs."""

    def __init__(
        self,
        minio_client: Optional[MinIOClient] = None,
        output_dir: Optional[Path] = None,
        state_dir: Optional[Path] = None,
        batch_size: Optional[int] = None,
    ):
        self._minio = minio_client or MinIOClient()
        self._output_dir = output_dir or settings.paths.output_dir
        self._state_dir = state_dir or settings.paths.state_dir
        self._batch_size = batch_size or settings.batch_size
        self._logger = get_step_logger("step3_permit_license")
        self._state_manager = StateManager("step3_permit_license", self._state_dir)

    def _ensure_dirs(self) -> None:
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._state_dir.mkdir(parents=True, exist_ok=True)

    def _load_document_mapping(self) -> dict:
        """Load document mapping from step 1."""
        mapping_path = self._state_dir / "document_mapping.json"
        if not mapping_path.exists():
            raise FileNotFoundError(
                f"Document mapping not found at {mapping_path}. Run step 1 first."
            )
        with open(mapping_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _extract_from_directory(
        self,
        saf_number: str,
        directory: str,
        file_paths: list[str],
    ) -> tuple[Optional[str], list[str], Optional[list[str]]]:
        """
        Extract text from all files in a directory for a SAF number.

        Args:
            saf_number: SAF number
            directory: Directory name (permit or license)
            file_paths: List of file paths

        Returns:
            Tuple of (combined_text, processed_files, errors)
        """
        if not file_paths:
            return None, [], None

        files_data = []
        for file_path in file_paths:
            try:
                file_data = self._minio.download_file(file_path)
                filename = file_path.split("/")[-1]
                files_data.append((filename, file_data))
            except Exception as e:
                self._logger.warning(f"Failed to download {file_path}: {e}")

        if not files_data:
            return None, [], ["All files failed to download"]

        return PDFExtractor.extract_multiple(files_data)

    def run(
        self,
        resume: bool = False,
        limit: Optional[int] = None,
        incremental: bool = False,
    ) -> pd.DataFrame:
        """
        Run step 3: extract text from permit and license documents.

        Args:
            resume: Whether to resume from previous state
            limit: Optional limit on number of SAF numbers
            incremental: Only process SAF numbers with new files

        Returns:
            DataFrame with extracted permit/license texts
        """
        self._ensure_dirs()
        self._logger.info("Starting Step 3: Permit/License text extraction")

        # Load document mapping
        mapping = self._load_document_mapping()
        permit_mapping = mapping.get("permit", {})
        license_mapping = mapping.get("license", {})

        # Get all unique SAF numbers
        all_saf_numbers = set(permit_mapping.keys()) | set(license_mapping.keys())

        if not all_saf_numbers:
            self._logger.warning("No permit/license files found in document mapping")
            return pd.DataFrame()

        saf_numbers = sorted(all_saf_numbers)

        if limit:
            saf_numbers = saf_numbers[:limit]
            self._logger.info(f"Limited to {limit} SAF numbers")

        # Load or create state
        if (resume or incremental) and self._state_manager.exists():
            self._state_manager.load()
            processed = self._state_manager.get_processed()
            self._logger.info(f"Loaded state with {len(processed)} processed items")
        else:
            self._state_manager.reset()
            processed = set()

        # In incremental mode, find SAF numbers with new files
        if incremental:
            # Combine permit and license mappings for checking new files
            combined_mapping = {}
            for saf in all_saf_numbers:
                combined_mapping[saf] = (
                    permit_mapping.get(saf, []) + license_mapping.get(saf, [])
                )
            saf_numbers_to_process = self._state_manager.get_saf_numbers_with_new_files(
                combined_mapping
            )
            # Filter to only those in our saf_numbers list (respects limit)
            saf_numbers_to_process = [s for s in saf_numbers_to_process if s in saf_numbers]
            self._logger.info(
                f"Incremental mode: {len(saf_numbers_to_process)} SAF numbers with new files"
            )
            saf_numbers = saf_numbers_to_process
            # processed remains from state - allows resume within incremental run

        self._state_manager.set_total(len(saf_numbers))

        # Process SAF numbers
        results = self._load_partial_results() if resume else []

        with ProgressTracker(
            "Processing permit/license",
            len(saf_numbers),
            self._state_manager,
        ) as progress:
            for i, saf_number in enumerate(saf_numbers):
                if saf_number in processed:
                    continue

                try:
                    # Process permit files
                    permit_files = permit_mapping.get(saf_number, [])
                    permit_text, permit_processed, permit_errors = self._extract_from_directory(
                        saf_number, "permit", permit_files
                    )

                    # Process license files
                    license_files = license_mapping.get(saf_number, [])
                    license_text, license_processed, license_errors = self._extract_from_directory(
                        saf_number, "license", license_files
                    )

                    result = {
                        "saf_number": saf_number,
                        "permit_text": permit_text,
                        "license_text": license_text,
                        "permit_files_processed": permit_processed,
                        "license_files_processed": license_processed,
                    }
                    results.append(result)

                    self._state_manager.mark_processed(saf_number)
                    # Track which files were processed for incremental updates
                    self._state_manager.mark_files_processed(
                        saf_number, permit_files + license_files
                    )

                    self._logger.info(
                        f"Processed saf_number={saf_number}, "
                        f"permit_chars={len(permit_text or '')}, "
                        f"license_chars={len(license_text or '')}"
                    )

                except Exception as e:
                    self._logger.error(f"Failed {saf_number}: {e}")
                    self._state_manager.mark_failed(saf_number, str(e))

                progress.advance()

                # Checkpoint
                if (i + 1) % self._batch_size == 0:
                    self._state_manager.update_batch(i + 1)
                    self._state_manager.save()
                    self._save_partial_results(results)
                    self._logger.info(
                        f"Checkpoint: {len(self._state_manager.get_processed())}/{len(saf_numbers)}"
                    )

        # Final save
        self._state_manager.save()
        df_new = pd.DataFrame(results)
        output_path = self._output_dir / "step3_permit_license.parquet"

        # In incremental mode, merge with existing results
        if incremental and output_path.exists() and not df_new.empty:
            df_existing = pd.read_parquet(output_path)
            # Remove old entries for SAF numbers we just reprocessed
            reprocessed_saf = set(df_new["saf_number"].tolist())
            df_existing = df_existing[~df_existing["saf_number"].isin(reprocessed_saf)]
            # Combine
            df = pd.concat([df_existing, df_new], ignore_index=True)
            self._logger.info(
                f"Merged {len(df_new)} new/updated records with {len(df_existing)} existing"
            )
        else:
            df = df_new

        df.to_parquet(output_path, index=False)
        self._logger.info(f"Saved permit/license data to {output_path} ({len(df)} total records)")

        self._logger.info("Step 3 completed successfully")
        return df

    def _save_partial_results(self, results: list[dict]) -> None:
        """Save partial results to file."""
        if results:
            df = pd.DataFrame(results)
            partial_path = self._output_dir / "step3_permit_license_partial.parquet"
            df.to_parquet(partial_path, index=False)

    def _load_partial_results(self) -> list[dict]:
        """Load partial results from file."""
        partial_path = self._output_dir / "step3_permit_license_partial.parquet"
        if partial_path.exists():
            df = pd.read_parquet(partial_path)
            return df.to_dict("records")
        return []
