"""Configuration module for export control dataset pipeline."""

from .settings import settings
from .patterns import (
    LICENSE_NOT_REQUIRED_PATTERNS,
    LICENSE_REQUIRED_PATTERNS,
    SOURCE_PRIORITY,
    determine_license_need,
)

__all__ = [
    "settings",
    "LICENSE_NOT_REQUIRED_PATTERNS",
    "LICENSE_REQUIRED_PATTERNS",
    "SOURCE_PRIORITY",
    "determine_license_need",
]
