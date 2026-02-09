"""PostgreSQL database connection and operations."""

from typing import Optional
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config.settings import settings


class Database:
    """PostgreSQL database client."""

    def __init__(self, connection_string: Optional[str] = None):
        self._connection_string = connection_string or settings.database.connection_string
        self._engine: Optional[Engine] = None

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            self._engine = create_engine(self._connection_string)
        return self._engine

    def connect(self) -> None:
        """Establish database connection."""
        _ = self.engine

    def close(self) -> None:
        """Close database connection."""
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None

    def test_connection(self) -> bool:
        """Test if database connection is working."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    def get_base_dataset(self) -> pd.DataFrame:
        """
        Get base dataset from saf and saf_product_index tables.

        Returns:
            DataFrame with columns: saf_number, hs_code, product_description, license_need_db
        """
        query = """
            SELECT
                p.saf_number,
                p.hs_code_10 AS hs_code,
                p.product_description,
                s.lecense_need AS license_need_db
            FROM saf_product_index p
            LEFT JOIN saf s ON p.saf_number = s.saf_number
            ORDER BY p.saf_number, p.id
        """
        return pd.read_sql(query, self.engine)

    def get_document_mapping(self) -> pd.DataFrame:
        """
        Get document mapping from saf_document_index table.

        Returns:
            DataFrame with document information
        """
        query = """
            SELECT
                saf_number,
                document_file_new,
                document_type,
                document_description
            FROM saf_document_index
            WHERE document_file_new IS NOT NULL
            ORDER BY saf_number, id
        """
        return pd.read_sql(query, self.engine)

    def get_unique_saf_numbers(self) -> list[str]:
        """Get list of unique SAF numbers."""
        query = "SELECT DISTINCT saf_number FROM saf_product_index ORDER BY saf_number"
        df = pd.read_sql(query, self.engine)
        return df["saf_number"].tolist()

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
