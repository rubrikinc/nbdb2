"""DatabaseSchemaFactory"""
from typing import Dict, List

from nbdb.schema.schema import Schema


class DatabaseSchemaFactory:
    """Factory to access schema objects for databases"""

    ALL_DATABASES: List[str] = ["default", "batch"]
    DEFAULT_DATABASE: str = "default"
    BATCH_DATABASE: str = "batch"

    def __init__(self):
        self.mapping: Dict[str, Schema] = {}

    def add(self, database: str, schema: Schema) -> None:
        """Add a database to schema mapping"""
        if not DatabaseSchemaFactory.validate_database(database):
            raise ValueError(f"Unknown database {database}")
        self.mapping[database] = schema

    def get(self, database: str) -> Schema:
        """Get schema object for a database"""
        if not DatabaseSchemaFactory.validate_database(database):
            raise ValueError(f"Unknown database {database}")
        return self.mapping[database]

    @staticmethod
    def validate_database(database: str) -> bool:
        """Validate database name"""
        return database in DatabaseSchemaFactory.ALL_DATABASES
