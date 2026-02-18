"""Configuration settings loaded from environment variables."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


@dataclass
class DatabaseSettings:
    """PostgreSQL database settings."""
    host: str = field(default_factory=lambda: os.getenv("DB_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(os.getenv("DB_PORT", "5432")))
    name: str = field(default_factory=lambda: os.getenv("DB_NAME", "export_control"))
    user: str = field(default_factory=lambda: os.getenv("DB_USER", "user"))
    password: str = field(default_factory=lambda: os.getenv("DB_PASSWORD", "password"))

    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.name}"


@dataclass
class MinIOSettings:
    """MinIO storage settings."""
    endpoint: str = field(default_factory=lambda: os.getenv("MINIO_ENDPOINT", "minio.example.com:9000"))
    access_key: str = field(default_factory=lambda: os.getenv("MINIO_ACCESS_KEY", "access_key"))
    secret_key: str = field(default_factory=lambda: os.getenv("MINIO_SECRET_KEY", "secret_key"))
    bucket: str = field(default_factory=lambda: os.getenv("MINIO_BUCKET", "documents"))
    secure: bool = field(default_factory=lambda: os.getenv("MINIO_SECURE", "true").lower() == "true")


@dataclass
class OCRSettings:
    """OCR API settings."""
    api_url: str = field(default_factory=lambda: os.getenv("OCR_API_URL", "http://192.168.250.44:8088"))
    secret_key: str = field(default_factory=lambda: os.getenv("OCR_SECRET_KEY", ""))
    poll_interval: int = field(default_factory=lambda: int(os.getenv("OCR_POLL_INTERVAL", "5")))
    max_concurrent: int = field(default_factory=lambda: int(os.getenv("OCR_MAX_CONCURRENT", "5")))
    timeout: int = field(default_factory=lambda: int(os.getenv("OCR_TIMEOUT", "300")))


@dataclass
class PathSettings:
    """File path settings."""
    output_dir: Path = field(default_factory=lambda: Path(os.getenv("OUTPUT_DIR", "./output")))
    state_dir: Path = field(default_factory=lambda: Path(os.getenv("STATE_DIR", "./state")))
    logs_dir: Path = field(default_factory=lambda: Path(os.getenv("LOGS_DIR", "./logs")))

    def __post_init__(self):
        self.output_dir = Path(self.output_dir)
        self.state_dir = Path(self.state_dir)
        self.logs_dir = Path(self.logs_dir)


@dataclass
class Settings:
    """Main settings container."""
    database: DatabaseSettings = field(default_factory=DatabaseSettings)
    minio: MinIOSettings = field(default_factory=MinIOSettings)
    ocr: OCRSettings = field(default_factory=OCRSettings)
    paths: PathSettings = field(default_factory=PathSettings)

    batch_size: int = field(default_factory=lambda: int(os.getenv("BATCH_SIZE", "100")))


settings = Settings()
