"""Main entry point for export control dataset pipeline."""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

import click
from rich.console import Console
from rich.table import Table

from config.settings import settings
from core.database import Database
from core.minio_client import MinIOClient
from core.ocr_client import OCRClient
from pipeline import Step1BaseDataset, Step2TechSpecs, Step3PermitLicense, Step4Classification
from utils.progress import StateManager

console = Console()


@click.group()
def cli():
    """Export Control Dataset Pipeline CLI."""
    pass


@cli.command()
@click.option("--all", "run_all", is_flag=True, help="Run all steps")
@click.option("--step", type=int, help="Run specific step (1-4)")
@click.option("--resume", is_flag=True, help="Resume from previous state")
@click.option("--incremental", is_flag=True, help="Only process SAF numbers with new files (step 2-3)")
@click.option("--limit", type=int, help="Limit number of records (for testing)")
@click.option("--poll-interval", type=int, help="OCR poll interval in seconds")
@click.option("--max-concurrent", type=int, help="Max concurrent OCR requests")
@click.option("--output-format", type=click.Choice(["parquet", "csv"]), default="parquet")
def run(run_all, step, resume, incremental, limit, poll_interval, max_concurrent, output_format):
    """Run pipeline steps."""
    if not run_all and step is None:
        console.print("[red]Error: Specify --all or --step[/red]")
        return

    steps_to_run = [1, 2, 3, 4] if run_all else [step]

    for step_num in steps_to_run:
        console.print(f"\n[bold blue]Running Step {step_num}...[/bold blue]")

        try:
            if step_num == 1:
                runner = Step1BaseDataset()
                runner.run(limit=limit)

            elif step_num == 2:
                runner = Step2TechSpecs()
                runner.run(resume=resume, limit=limit, incremental=incremental)

            elif step_num == 3:
                runner = Step3PermitLicense()
                runner.run(resume=resume, limit=limit, incremental=incremental)

            elif step_num == 4:
                runner = Step4Classification()
                runner.run(output_format=output_format)

            console.print(f"[green]Step {step_num} completed successfully![/green]")

        except Exception as e:
            console.print(f"[red]Step {step_num} failed: {e}[/red]")
            if not run_all:
                raise


@cli.command()
def status():
    """Show pipeline status."""
    table = Table(title="Pipeline Status")
    table.add_column("Step", style="cyan")
    table.add_column("Status", style="green")
    table.add_column("Progress", style="yellow")
    table.add_column("Failed", style="red")

    output_dir = settings.paths.output_dir
    state_dir = settings.paths.state_dir

    # Step 1
    step1_output = output_dir / "step1_base_dataset.parquet"
    if step1_output.exists():
        import pandas as pd
        df = pd.read_parquet(step1_output)
        table.add_row("Step 1: Base Dataset", "Completed", f"{len(df)} records", "-")
    else:
        table.add_row("Step 1: Base Dataset", "Not started", "-", "-")

    # Step 2
    step2_state = StateManager("step2_tech_specs", state_dir)
    if step2_state.exists():
        step2_state.load()
        processed = len(step2_state.get_processed())
        failed = len(step2_state.get_failed())
        total = step2_state._state.get("total_items", 0)
        status = "Completed" if processed >= total and total > 0 else "In progress"
        table.add_row("Step 2: Tech Specs (OCR)", status, f"{processed}/{total}", str(failed))
    else:
        table.add_row("Step 2: Tech Specs (OCR)", "Not started", "-", "-")

    # Step 3
    step3_state = StateManager("step3_permit_license", state_dir)
    if step3_state.exists():
        step3_state.load()
        processed = len(step3_state.get_processed())
        failed = len(step3_state.get_failed())
        total = step3_state._state.get("total_items", 0)
        status = "Completed" if processed >= total and total > 0 else "In progress"
        table.add_row("Step 3: Permit/License", status, f"{processed}/{total}", str(failed))
    else:
        table.add_row("Step 3: Permit/License", "Not started", "-", "-")

    # Step 4
    step4_output = output_dir / "final_dataset.parquet"
    if step4_output.exists():
        import pandas as pd
        df = pd.read_parquet(step4_output)
        table.add_row("Step 4: Classification", "Completed", f"{len(df)} records", "-")
    else:
        table.add_row("Step 4: Classification", "Not started", "-", "-")

    console.print(table)


@cli.command()
@click.option("--step", type=int, help="Reset specific step")
@click.option("--all", "reset_all", is_flag=True, help="Reset all steps")
@click.confirmation_option(prompt="Are you sure you want to reset?")
def reset(step, reset_all):
    """Reset pipeline state."""
    state_dir = settings.paths.state_dir
    output_dir = settings.paths.output_dir

    if reset_all:
        steps = [1, 2, 3, 4]
    elif step:
        steps = [step]
    else:
        console.print("[red]Error: Specify --step or --all[/red]")
        return

    for step_num in steps:
        console.print(f"Resetting step {step_num}...")

        if step_num == 1:
            files = [
                output_dir / "step1_base_dataset.parquet",
                state_dir / "document_mapping.json",
            ]
        elif step_num == 2:
            files = [
                output_dir / "step2_tech_specs.parquet",
                output_dir / "step2_tech_specs_partial.parquet",
                state_dir / "step2_tech_specs_progress.json",
            ]
        elif step_num == 3:
            files = [
                output_dir / "step3_permit_license.parquet",
                output_dir / "step3_permit_license_partial.parquet",
                state_dir / "step3_permit_license_progress.json",
            ]
        elif step_num == 4:
            files = [
                output_dir / "final_dataset.parquet",
                output_dir / "final_dataset.csv",
            ]

        for f in files:
            if f.exists():
                f.unlink()
                console.print(f"  Deleted {f}")

    console.print("[green]Reset completed![/green]")


@cli.command()
def check():
    """Check connections to all services."""
    table = Table(title="Connection Status")
    table.add_column("Service", style="cyan")
    table.add_column("Status", style="green")
    table.add_column("Details", style="yellow")

    # Database
    console.print("Checking PostgreSQL...")
    db = Database()
    try:
        if db.test_connection():
            table.add_row("PostgreSQL", "[green]Connected[/green]", settings.database.host)
        else:
            table.add_row("PostgreSQL", "[red]Failed[/red]", "Connection test failed")
    except Exception as e:
        table.add_row("PostgreSQL", "[red]Failed[/red]", str(e)[:50])

    # MinIO
    console.print("Checking MinIO...")
    minio = MinIOClient()
    try:
        if minio.test_connection():
            table.add_row("MinIO", "[green]Connected[/green]", settings.minio.endpoint)
        else:
            table.add_row("MinIO", "[red]Failed[/red]", "Connection test failed")
    except Exception as e:
        table.add_row("MinIO", "[red]Failed[/red]", str(e)[:50])

    # OCR API
    console.print("Checking OCR API...")
    ocr = OCRClient()
    try:
        if ocr.test_connection():
            table.add_row("OCR API", "[green]Connected[/green]", settings.ocr.api_url)
        else:
            table.add_row("OCR API", "[red]Failed[/red]", "Connection test failed")
    except Exception as e:
        table.add_row("OCR API", "[red]Failed[/red]", str(e)[:50])

    console.print(table)


@cli.command(name="refresh-mapping")
def refresh_mapping():
    """Refresh document mapping from MinIO without recreating base dataset.

    Use this when new files were added to MinIO but the database hasn't changed.
    After refreshing, run step 2/3 with --incremental to process only new files.
    """
    import pandas as pd

    output_dir = settings.paths.output_dir
    base_dataset_path = output_dir / "step1_base_dataset.parquet"

    if not base_dataset_path.exists():
        console.print("[red]Error: Base dataset not found. Run step 1 first.[/red]")
        return

    console.print("Loading existing base dataset...")
    df = pd.read_parquet(base_dataset_path)
    saf_numbers = df["saf_number"].unique().tolist()
    console.print(f"Found {len(saf_numbers)} unique SAF numbers")

    console.print("Refreshing document mapping from MinIO...")
    runner = Step1BaseDataset()
    mapping = runner._create_document_mapping(saf_numbers)

    total_specs = len(mapping.get("specs", {}))
    total_permit = len(mapping.get("permit", {}))
    total_license = len(mapping.get("license", {}))

    console.print(f"[green]Mapping refreshed![/green]")
    console.print(f"  specs/: {total_specs} SAF numbers with files")
    console.print(f"  permit/: {total_permit} SAF numbers with files")
    console.print(f"  license/: {total_license} SAF numbers with files")
    console.print("\nRun 'python main.py run --step 2 --incremental' to process new files")


@cli.command()
def stats():
    """Show statistics about final dataset."""
    output_path = settings.paths.output_dir / "final_dataset.parquet"

    if not output_path.exists():
        console.print("[red]Final dataset not found. Run step 4 first.[/red]")
        return

    runner = Step4Classification()
    statistics = runner.get_statistics()

    table = Table(title="Dataset Statistics")
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")

    for key, value in statistics.items():
        table.add_row(key.replace("_", " ").title(), str(value))

    console.print(table)


if __name__ == "__main__":
    cli()
