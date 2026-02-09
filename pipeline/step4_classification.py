"""Step 4: Determine license_need based on extracted texts."""

from pathlib import Path
from typing import Optional
import pandas as pd

from config.patterns import determine_license_need
from config.settings import settings
from utils.logger import get_step_logger


class Step4Classification:
    """Combine datasets and determine license_need."""

    def __init__(
        self,
        output_dir: Optional[Path] = None,
    ):
        self._output_dir = output_dir or settings.paths.output_dir
        self._logger = get_step_logger("step4_classification")

    def _ensure_dirs(self) -> None:
        self._output_dir.mkdir(parents=True, exist_ok=True)

    def _load_step_output(self, filename: str) -> pd.DataFrame:
        """Load output from a previous step."""
        path = self._output_dir / filename
        if not path.exists():
            raise FileNotFoundError(f"Step output not found: {path}")
        return pd.read_parquet(path)

    def run(self, output_format: str = "parquet") -> pd.DataFrame:
        """
        Run step 4: combine datasets and classify.

        Args:
            output_format: Output format ("parquet" or "csv")

        Returns:
            Final dataset DataFrame
        """
        self._ensure_dirs()
        self._logger.info("Starting Step 4: Classification")

        # Load previous step outputs
        self._logger.info("Loading step outputs...")

        df_base = self._load_step_output("step1_base_dataset.parquet")
        self._logger.info(f"Loaded base dataset: {len(df_base)} records")

        try:
            df_specs = self._load_step_output("step2_tech_specs.parquet")
            self._logger.info(f"Loaded tech specs: {len(df_specs)} records")
        except FileNotFoundError:
            self._logger.warning("Tech specs not found, continuing without")
            df_specs = pd.DataFrame(columns=["saf_number", "tech_description"])

        try:
            df_permit_license = self._load_step_output("step3_permit_license.parquet")
            self._logger.info(f"Loaded permit/license: {len(df_permit_license)} records")
        except FileNotFoundError:
            self._logger.warning("Permit/license not found, continuing without")
            df_permit_license = pd.DataFrame(
                columns=["saf_number", "permit_text", "license_text"]
            )

        # Merge datasets
        self._logger.info("Merging datasets...")

        # Select only needed columns
        specs_cols = ["saf_number", "tech_description"]
        if "tech_description" in df_specs.columns:
            df_specs = df_specs[specs_cols]
        else:
            df_specs = pd.DataFrame(columns=specs_cols)

        permit_cols = ["saf_number", "permit_text", "license_text"]
        for col in permit_cols:
            if col not in df_permit_license.columns:
                df_permit_license[col] = None
        df_permit_license = df_permit_license[permit_cols]

        # Merge on saf_number
        df = df_base.merge(df_specs, on="saf_number", how="left")
        df = df.merge(df_permit_license, on="saf_number", how="left")

        self._logger.info(f"Merged dataset: {len(df)} records")

        # Determine license_need
        self._logger.info("Determining license_need...")

        df["license_need"] = df.apply(
            lambda row: determine_license_need(
                row.get("permit_text"),
                row.get("license_text"),
            ),
            axis=1,
        )

        # Statistics
        license_need_counts = df["license_need"].value_counts(dropna=False)
        self._logger.info(f"License need distribution:\n{license_need_counts}")

        # Compare with database values
        if "license_need_db" in df.columns and "license_need" in df.columns:
            df_comparison = df.dropna(subset=["license_need"])
            if len(df_comparison) > 0:
                agreement = (
                    df_comparison["license_need"] == df_comparison["license_need_db"]
                ).mean()
                self._logger.info(f"Agreement with database: {agreement:.2%}")

        # Select final columns
        final_columns = [
            "saf_number",
            "hs_code",
            "product_description",
            "tech_description",
            "permit_text",
            "license_text",
            "license_need",
            "license_need_db",
        ]
        df_final = df[[col for col in final_columns if col in df.columns]]

        # Save output
        if output_format == "csv":
            output_path = self._output_dir / "final_dataset.csv"
            df_final.to_csv(output_path, index=False)
        else:
            output_path = self._output_dir / "final_dataset.parquet"
            df_final.to_parquet(output_path, index=False)

        self._logger.info(f"Saved final dataset to {output_path}")
        self._logger.info("Step 4 completed successfully")

        return df_final

    def get_statistics(self) -> dict:
        """Get statistics about the final dataset."""
        df = self._load_step_output("final_dataset.parquet")

        stats = {
            "total_records": len(df),
            "unique_saf_numbers": df["saf_number"].nunique(),
            "license_need_true": len(df[df["license_need"] == True]),
            "license_need_false": len(df[df["license_need"] == False]),
            "license_need_null": df["license_need"].isna().sum(),
            "with_tech_description": df["tech_description"].notna().sum(),
            "with_permit_text": df["permit_text"].notna().sum(),
            "with_license_text": df["license_text"].notna().sum(),
        }

        return stats
