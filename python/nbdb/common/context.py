"""Context"""
from dataclasses import dataclass

from nbdb.schema.schema import Schema


# TODO: Move Settings here
@dataclass
class Context:
    """Essential runtime dependencies."""
    schema: Schema
