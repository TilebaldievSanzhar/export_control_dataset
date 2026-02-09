"""Step 2: OCR extraction for technical specifications."""

import asyncio
import json
from pathlib import Path
from typing import Optional
import pandas as pd

from core.minio_client import MinIOClient
from core.ocr_client import OCRClient
from extractors.ocr_extractor import OCRExtractor
from config.settings import settings
from utils.logger import get_step_logger
from utils.progress import StateManager, ProgressTracker


class Step2TechSpecs:
    """Extract text from technical specifications using OCR."""

    def __init__(
        self,
        minio_client: Optional[MinIOClient] = None,
        ocr_client: Optional[OCRClient] = None,
        output_dir: Optional[Path] = None,
        state_dir: Optional[Path] = None,
        batch_size: Optional[int] = None,
    ):
        self._minio = minio_client or MinIOClient()
        self._ocr = ocr_client or OCRClient()
        self._extractor = OCRExtractor(self._minio, self._ocr)
        self._output_dir = output_dir or settings.paths.output_dir
        self._state_dir = state_dir or settings.paths.state_dir
        self._batch_size = batch_size or settings.batch_size
        self._logger = get_step_logger("step2_tech_specs")
        self._state_manager = StateManager("step2_tech_specs", self._state_dir)

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

    def run(
        self,
        resume: bool = False,
        limit: Optional[int] = None,
        incremental: bool = False,
    ) -> pd.DataFrame:
        """
        Run step 2: OCR extraction for specs.

        Args:
            resume: Whether to resume from previous state
            limit: Optional limit on number of SAF numbers
            incremental: Only process SAF numbers with new files

        Returns:
            DataFrame with extracted tech descriptions
        """
        self._ensure_dirs()
        self._logger.info("Starting Step 2: Technical specifications OCR")

        # Load document mapping
        mapping = self._load_document_mapping()
        specs_mapping = mapping.get("specs", {})

        if not specs_mapping:
            self._logger.warning("No specs files found in document mapping")
            return pd.DataFrame()

        saf_numbers = list(specs_mapping.keys())

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
            saf_numbers_to_process = self._state_manager.get_saf_numbers_with_new_files(specs_mapping)
            # Filter to only those in our saf_numbers list (respects limit)
            saf_numbers_to_process = [s for s in saf_numbers_to_process if s in saf_numbers]
            self._logger.info(
                f"Incremental mode: {len(saf_numbers_to_process)} SAF numbers with new files"
            )
            saf_numbers = saf_numbers_to_process
            # processed remains from state - allows resume within incremental run

        self._state_manager.set_total(len(saf_numbers))

        # Clean up old chunks on fresh start
        if not (resume or incremental):
            self._cleanup_partial_results()

        # Process SAF numbers
        results = []

        with ProgressTracker(
            "Processing specs",
            len(saf_numbers),
            self._state_manager,
        ) as progress:
            for i, saf_number in enumerate(saf_numbers):
                if saf_number in processed:
                    continue

                try:
                    self._logger.info(
                        f"Processing saf_number={saf_number}, "
                        f"files={len(specs_mapping[saf_number])}"
                    )

                    text, files_processed, errors = asyncio.run(
                        self._extractor.process_saf_files(saf_number, "specs")
                    )

                    result = {
                        "saf_number": saf_number,
                        "tech_description": text,
                        "tech_files_processed": files_processed,
                        "tech_ocr_errors": errors,
                    }
                    results.append(result)

                    self._state_manager.mark_processed(saf_number)
                    # Track which files were processed for incremental updates
                    self._state_manager.mark_files_processed(
                        saf_number, specs_mapping.get(saf_number, [])
                    )

                    if text:
                        self._logger.info(
                            f"OCR completed for saf_number={saf_number}, chars={len(text)}"
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
                    results.clear()
                    self._logger.info(
                        f"Checkpoint: {len(self._state_manager.get_processed())}/{len(saf_numbers)}"
                    )

        # Final save
        self._state_manager.save()
        if results:
            self._save_partial_results(results)
            results.clear()

        df_new = self._load_all_partial_results()
        self._cleanup_partial_results()
        output_path = self._output_dir / "step2_tech_specs.parquet"

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
        self._logger.info(f"Saved tech specs to {output_path} ({len(df)} total records)")

        self._logger.info("Step 2 completed successfully")
        return df

    def _save_partial_results(self, results: list[dict]) -> None:
        """Save a batch of results as a numbered chunk file."""
        if not results:
            return
        df = pd.DataFrame(results)
        chunk_num = 0
        while (self._output_dir / f"step2_tech_specs_chunk_{chunk_num}.parquet").exists():
            chunk_num += 1
        chunk_path = self._output_dir / f"step2_tech_specs_chunk_{chunk_num}.parquet"
        df.to_parquet(chunk_path, index=False)

    def _load_all_partial_results(self) -> pd.DataFrame:
        """Load and combine all chunk files into a single DataFrame."""
        chunk_files = sorted(self._output_dir.glob("step2_tech_specs_chunk_*.parquet"))
        if not chunk_files:
            return pd.DataFrame()
        chunks = [pd.read_parquet(f) for f in chunk_files]
        return pd.concat(chunks, ignore_index=True)

    def _cleanup_partial_results(self) -> None:
        """Remove all chunk files and old-style partial file."""
        for f in self._output_dir.glob("step2_tech_specs_chunk_*.parquet"):
            f.unlink()
        partial_path = self._output_dir / "step2_tech_specs_partial.parquet"
        if partial_path.exists():
            partial_path.unlink()
