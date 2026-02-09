"""Progress tracking and state management."""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn

from config.settings import settings


class StateManager:
    """Manage pipeline state for resume capability."""

    def __init__(self, step_name: str, state_dir: Optional[Path] = None):
        self._step_name = step_name
        self._state_dir = state_dir or settings.paths.state_dir
        self._state_file = self._state_dir / f"{step_name}_progress.json"
        self._state: dict[str, Any] = {}

    @property
    def state_file(self) -> Path:
        return self._state_file

    def _ensure_dir(self) -> None:
        self._state_dir.mkdir(parents=True, exist_ok=True)

    def load(self) -> dict[str, Any]:
        """Load state from file."""
        if self._state_file.exists():
            with open(self._state_file, "r", encoding="utf-8") as f:
                self._state = json.load(f)
        else:
            self._state = self._create_initial_state()
        return self._state

    def save(self) -> None:
        """Save state to file."""
        self._ensure_dir()
        self._state["updated_at"] = datetime.now().isoformat()
        with open(self._state_file, "w", encoding="utf-8") as f:
            json.dump(self._state, f, ensure_ascii=False, indent=2)

    def _create_initial_state(self) -> dict[str, Any]:
        """Create initial state structure."""
        return {
            "step": self._step_name,
            "started_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "total_items": 0,
            "processed_items": 0,
            "failed_items": 0,
            "current_batch": 0,
            "processed_saf_numbers": [],
            "failed_saf_numbers": {},
            "processed_files": {},  # saf_number -> list of processed file paths
        }

    def get_processed(self) -> set[str]:
        """Get set of processed SAF numbers."""
        return set(self._state.get("processed_saf_numbers", []))

    def get_failed(self) -> dict[str, str]:
        """Get dict of failed SAF numbers with error messages."""
        return dict(self._state.get("failed_saf_numbers", {}))

    def mark_processed(self, saf_number: str) -> None:
        """Mark SAF number as processed."""
        if saf_number not in self._state["processed_saf_numbers"]:
            self._state["processed_saf_numbers"].append(saf_number)
            self._state["processed_items"] = len(self._state["processed_saf_numbers"])

    def mark_failed(self, saf_number: str, error: str) -> None:
        """Mark SAF number as failed with error message."""
        self._state["failed_saf_numbers"][saf_number] = error
        self._state["failed_items"] = len(self._state["failed_saf_numbers"])

    def mark_files_processed(self, saf_number: str, files: list[str]) -> None:
        """Mark specific files as processed for a SAF number."""
        if "processed_files" not in self._state:
            self._state["processed_files"] = {}
        self._state["processed_files"][saf_number] = files

    def get_processed_files(self, saf_number: str) -> list[str]:
        """Get list of processed files for a SAF number."""
        return self._state.get("processed_files", {}).get(saf_number, [])

    def get_all_processed_files(self) -> dict[str, list[str]]:
        """Get all processed files mapping."""
        return dict(self._state.get("processed_files", {}))

    def get_saf_numbers_with_new_files(self, current_mapping: dict[str, list[str]]) -> list[str]:
        """
        Find SAF numbers that have new files compared to what was processed.

        Args:
            current_mapping: Current mapping of saf_number -> list of file paths

        Returns:
            List of SAF numbers that need processing (new or have new files)
        """
        processed_files = self.get_all_processed_files()
        need_processing = []

        for saf_number, files in current_mapping.items():
            if saf_number not in processed_files:
                # New SAF number
                need_processing.append(saf_number)
            else:
                # Check if there are new files
                old_files = set(processed_files[saf_number])
                new_files = set(files)
                if new_files - old_files:
                    # Has new files
                    need_processing.append(saf_number)

        return need_processing

    def set_total(self, total: int) -> None:
        """Set total number of items."""
        self._state["total_items"] = total

    def update_batch(self, batch_num: int) -> None:
        """Update current batch number."""
        self._state["current_batch"] = batch_num

    def reset(self) -> None:
        """Reset state to initial."""
        self._state = self._create_initial_state()
        self.save()

    def exists(self) -> bool:
        """Check if state file exists."""
        return self._state_file.exists()

    def delete(self) -> None:
        """Delete state file."""
        if self._state_file.exists():
            self._state_file.unlink()


class ProgressTracker:
    """Track and display progress with rich library."""

    def __init__(
        self,
        description: str,
        total: int,
        state_manager: Optional[StateManager] = None,
    ):
        self._description = description
        self._total = total
        self._state_manager = state_manager
        self._progress: Optional[Progress] = None
        self._task_id = None

    def __enter__(self):
        self._progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TimeRemainingColumn(),
        )
        self._progress.start()
        self._task_id = self._progress.add_task(self._description, total=self._total)

        # Resume from state if available
        if self._state_manager:
            processed = len(self._state_manager.get_processed())
            if processed > 0:
                self._progress.update(self._task_id, completed=processed)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._progress:
            self._progress.stop()

    def advance(self, amount: int = 1) -> None:
        """Advance progress by amount."""
        if self._progress and self._task_id is not None:
            self._progress.advance(self._task_id, amount)

    def update(self, completed: int) -> None:
        """Set progress to specific value."""
        if self._progress and self._task_id is not None:
            self._progress.update(self._task_id, completed=completed)

    def set_description(self, description: str) -> None:
        """Update progress description."""
        if self._progress and self._task_id is not None:
            self._progress.update(self._task_id, description=description)
