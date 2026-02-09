"""
Diagnostic script to identify where SAF numbers are lost
between PostgreSQL and MinIO in the pipeline.
"""

import json
from pathlib import Path
from collections import Counter

from sqlalchemy import create_engine, text
from config.settings import settings
from core.minio_client import MinIOClient


def get_db_saf_numbers(engine):
    """Get SAF numbers from different DB tables/queries."""

    # 1. All SAF numbers in saf_product_index
    q1 = "SELECT DISTINCT saf_number FROM saf_product_index"
    with engine.connect() as conn:
        saf_product = {row[0] for row in conn.execute(text(q1))}

    # 2. All SAF numbers in saf
    q2 = "SELECT DISTINCT saf_number FROM saf"
    with engine.connect() as conn:
        saf_main = {row[0] for row in conn.execute(text(q2))}

    # 3. Result of the JOIN (what Step 1 actually uses)
    q3 = """
        SELECT DISTINCT p.saf_number
        FROM saf_product_index p
        JOIN saf s ON p.saf_number = s.saf_number
    """
    with engine.connect() as conn:
        saf_joined = {row[0] for row in conn.execute(text(q3))}

    return saf_product, saf_main, saf_joined


def get_minio_saf_numbers(minio: MinIOClient):
    """Get SAF numbers from MinIO directories."""
    specs = minio.get_all_saf_numbers_with_files("specs")
    permit = minio.get_all_saf_numbers_with_files("permit")
    license_ = minio.get_all_saf_numbers_with_files("license")
    return specs, permit, license_


def check_format_mismatches(db_safs: set, minio_safs: set, label: str):
    """Check if SAF number format differences cause mismatches."""
    # Normalize: strip, lower, remove leading/trailing whitespace
    db_normalized = {s.strip().lower(): s for s in db_safs}
    minio_normalized = {s.strip().lower(): s for s in minio_safs}

    # Find cases where normalized form matches but original doesn't
    mismatches = []
    for norm_key, minio_orig in minio_normalized.items():
        if norm_key in db_normalized:
            db_orig = db_normalized[norm_key]
            if db_orig != minio_orig:
                mismatches.append((db_orig, minio_orig))

    if mismatches:
        print(f"\n  !! FORMAT MISMATCHES ({label}): {len(mismatches)} found")
        for db_v, minio_v in mismatches[:10]:
            print(f"     DB: '{db_v}' vs MinIO: '{minio_v}'")
        if len(mismatches) > 10:
            print(f"     ... and {len(mismatches) - 10} more")
    else:
        print(f"\n  No format mismatches detected ({label})")


def load_existing_mapping(state_dir: Path):
    """Load document_mapping.json if exists."""
    mapping_path = state_dir / "document_mapping.json"
    if mapping_path.exists():
        with open(mapping_path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def main():
    print("=" * 70)
    print("DIAGNOSTIC: Pipeline SAF Number Coverage Analysis")
    print("=" * 70)

    # --- Database ---
    print("\n[1] Connecting to PostgreSQL...")
    engine = create_engine(settings.database.connection_string)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("  OK: Connected")
    except Exception as e:
        print(f"  FAILED: {e}")
        return

    saf_product, saf_main, saf_joined = get_db_saf_numbers(engine)

    print(f"\n[2] Database SAF numbers:")
    print(f"  saf_product_index (DISTINCT saf_number):  {len(saf_product):,}")
    print(f"  saf (DISTINCT saf_number):                {len(saf_main):,}")
    print(f"  JOIN result (Step 1 query):               {len(saf_joined):,}")

    lost_in_join = saf_product - saf_main
    print(f"\n  SAF numbers in saf_product_index but NOT in saf table: {len(lost_in_join):,}")
    if lost_in_join:
        examples = sorted(lost_in_join)[:10]
        print(f"  Examples: {examples}")

    # --- MinIO ---
    print(f"\n[3] Connecting to MinIO...")
    minio = MinIOClient()
    try:
        if minio.test_connection():
            print("  OK: Connected")
        else:
            print("  FAILED: Cannot connect")
            return
    except Exception as e:
        print(f"  FAILED: {e}")
        return

    specs_minio, permit_minio, license_minio = get_minio_saf_numbers(minio)

    print(f"\n[4] MinIO SAF directories:")
    print(f"  specs/   directories:  {len(specs_minio):,}")
    print(f"  permit/  directories:  {len(permit_minio):,}")
    print(f"  license/ directories:  {len(license_minio):,}")

    # --- Intersection Analysis ---
    print(f"\n[5] Intersection analysis (specs/):")

    in_both = saf_joined & specs_minio
    in_db_only = saf_joined - specs_minio
    in_minio_only = specs_minio - saf_joined

    print(f"  In DB AND MinIO (= what pipeline processes):  {len(in_both):,}")
    print(f"  In DB only (no specs files):                  {len(in_db_only):,}")
    print(f"  In MinIO only (not in DB):                    {len(in_minio_only):,}")

    if in_minio_only:
        examples = sorted(in_minio_only)[:15]
        print(f"\n  Examples of MinIO-only SAF numbers:")
        for s in examples:
            print(f"    '{s}'")

    # --- Format mismatch check ---
    print(f"\n[6] Format mismatch check:")
    check_format_mismatches(saf_joined, specs_minio, "specs")

    # Also check against the broader saf_product set
    in_product_and_minio = saf_product & specs_minio
    in_product_not_saf = (saf_product & specs_minio) - saf_joined
    if in_product_not_saf:
        print(f"\n  SAF numbers in saf_product_index AND MinIO specs,")
        print(f"  but lost due to JOIN with saf table: {len(in_product_not_saf):,}")
        examples = sorted(in_product_not_saf)[:10]
        print(f"  Examples: {examples}")

    # --- Existing mapping check ---
    print(f"\n[7] Checking existing document_mapping.json...")
    mapping = load_existing_mapping(settings.paths.state_dir)
    if mapping:
        specs_mapped = set(mapping.get("specs", {}).keys())
        permit_mapped = set(mapping.get("permit", {}).keys())
        license_mapped = set(mapping.get("license", {}).keys())

        print(f"  specs  in mapping:   {len(specs_mapped):,}")
        print(f"  permit in mapping:   {len(permit_mapped):,}")
        print(f"  license in mapping:  {len(license_mapped):,}")

        # Check if mapping matches what we'd expect
        diff = in_both - specs_mapped
        if diff:
            print(f"\n  SAF numbers in intersection but MISSING from mapping: {len(diff):,}")
            examples = sorted(diff)[:10]
            print(f"  Examples: {examples}")
        else:
            print(f"\n  Mapping is consistent with DB/MinIO intersection")
    else:
        print("  document_mapping.json not found")

    # --- Step 2 output check ---
    print(f"\n[8] Checking Step 2 output...")
    step2_path = settings.paths.output_dir / "step2_tech_specs.parquet"
    if step2_path.exists():
        import pandas as pd
        df_step2 = pd.read_parquet(step2_path)
        step2_safs = set(df_step2["saf_number"].unique())
        print(f"  Records in step2_tech_specs.parquet: {len(df_step2):,}")
        print(f"  Unique SAF numbers:                  {len(step2_safs):,}")

        # Check for empty tech descriptions
        empty_desc = df_step2["tech_description"].isna().sum()
        empty_desc += (df_step2["tech_description"] == "").sum()
        print(f"  Empty/null tech_description:         {empty_desc:,}")

        # Check errors
        if "tech_ocr_errors" in df_step2.columns:
            has_errors = df_step2["tech_ocr_errors"].apply(
                lambda x: bool(x) if isinstance(x, list) else False
            ).sum()
            print(f"  Records with OCR errors:             {has_errors:,}")
    else:
        print("  step2_tech_specs.parquet not found")

    # --- State file check ---
    print(f"\n[9] Checking Step 2 state...")
    state_path = settings.paths.state_dir / "step2_tech_specs_progress.json"
    if state_path.exists():
        with open(state_path, "r", encoding="utf-8") as f:
            state = json.load(f)
        print(f"  Total items:     {state.get('total_items', '?')}")
        print(f"  Processed items: {state.get('processed_items', '?')}")
        print(f"  Failed items:    {state.get('failed_items', '?')}")
        failed = state.get("failed_saf_numbers", {})
        if failed:
            print(f"  Failed SAF numbers: {len(failed):,}")
            # Show error distribution
            error_types = Counter(str(v)[:80] for v in failed.values())
            print(f"  Error distribution:")
            for err, cnt in error_types.most_common(5):
                print(f"    [{cnt}x] {err}")
    else:
        print("  State file not found")

    # --- Final dataset check ---
    print(f"\n[10] Checking final dataset...")
    final_path = settings.paths.output_dir / "final_dataset.parquet"
    if final_path.exists():
        import pandas as pd
        df_final = pd.read_parquet(final_path)
        print(f"  Total records:                       {len(df_final):,}")
        print(f"  Unique SAF numbers:                  {df_final['saf_number'].nunique():,}")
        has_tech = df_final["tech_description"].notna().sum()
        no_tech = df_final["tech_description"].isna().sum()
        print(f"  With tech_description:               {has_tech:,}")
        print(f"  Without tech_description (null):     {no_tech:,}")
    else:
        print("  final_dataset.parquet not found")

    # --- MinIO-only deep analysis ---
    print(f"\n[11] Deep analysis: MinIO-only SAF numbers ({len(in_minio_only):,})")

    # How many of MinIO-only are in saf_product_index?
    minio_only_in_product = in_minio_only & saf_product
    minio_only_nowhere = in_minio_only - saf_product

    print(f"  In saf_product_index (but not in saf):  {len(minio_only_in_product):,}")
    print(f"  Not in any DB table at all:             {len(minio_only_nowhere):,}")

    if minio_only_in_product:
        print(f"\n  → These {len(minio_only_in_product):,} SAF numbers HAVE product data")
        print(f"    but are lost because saf table has no matching record.")
        print(f"    If saf table were populated for them, potential specs coverage:")

        potential_total = len(in_both) + len(minio_only_in_product)
        print(f"    Current:   {len(in_both):,}")
        print(f"    Potential: {potential_total:,}  (+{len(minio_only_in_product):,})")

        examples = sorted(minio_only_in_product)[:15]
        print(f"\n  Examples of recoverable SAF numbers:")
        for s in examples:
            print(f"    '{s}'")

    if minio_only_nowhere:
        examples = sorted(minio_only_nowhere)[:15]
        print(f"\n  Examples of SAF numbers in MinIO but not in any DB table:")
        for s in examples:
            print(f"    '{s}'")

    # --- Summary ---
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"""
  DB (saf_product_index):          {len(saf_product):>8,} unique SAF numbers
  DB after JOIN with saf:          {len(saf_joined):>8,} unique SAF numbers  (lost {len(saf_product) - len(saf_joined):,} in JOIN)
  MinIO specs/ directories:        {len(specs_minio):>8,}
  Intersection (DB ∩ MinIO):       {len(in_both):>8,}  ← this should be ~2213
  MinIO-only (not in DB):          {len(in_minio_only):>8,}  ← files exist but no DB record
  DB-only (no specs in MinIO):     {len(in_db_only):>8,}  ← DB records but no files

  RECOVERABLE (in saf_product_index + MinIO, missing from saf):
    {len(minio_only_in_product):>8,}  ← could be added if saf table is populated
  NOT IN ANY DB TABLE:
    {len(minio_only_nowhere):>8,}  ← no DB record at all
    """)

    engine.dispose()
    print("Done.")


if __name__ == "__main__":
    main()
