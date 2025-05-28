from bson.errors import InvalidId
from pymongo import ASCENDING as ASC
from pymongo import DESCENDING as DESC
from pymongo import GEO2D, GEOSPHERE, HASHED, TEXT
from pymongo.errors import DuplicateKeyError

from morm.orm import (
    AlreadyExists,
    Database,
    DatabaseException,
    DoesNotExist,
    Index,
    Model,
    ObjectId,
)

__version__ = "0.2.0"

__all__ = [
    "Database",
    "Model",
    "Index",
    "ObjectId",
    "DatabaseException",
    "AlreadyExists",
    "DoesNotExist",
    "InvalidId",
    "ASC",
    "DESC",
    "GEO2D",
    "GEOSPHERE",
    "HASHED",
    "TEXT",
    "DuplicateKeyError",
]
