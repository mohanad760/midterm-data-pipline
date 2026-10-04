import sys
from pathlib import Path
from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure

# Add parent directory to path to allow importing config
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import settings


def get_mongo_client():
    """Create and return a MongoDB client with a short timeout for quick failover."""
    try:
        client = MongoClient(settings.MONGODB_URI, serverSelectionTimeoutMS=5000)
        # Verify server is alive
        client.admin.command("ping")
        return client
    except ConnectionFailure as e:
        raise ConnectionFailure(f"Could not connect to MongoDB at {settings.MONGODB_URI}: {e}")


def get_database(client=None):
    """Return the application database handle."""
    if client is None:
        client = get_mongo_client()
    return client[settings.DATABASE_NAME]


def init_database_schema(db=None):
    """
    Ensure all collections and indexes required by the ELT pipeline exist:
    1. orders_raw: Historical raw ingest log. NO unique index on order_id to prevent ingest failure.
    2. orders_validated: Golden records. Enforces UNIQUE index on business key `order_id`.
    3. orders_quarantine: Unrecoverable records. Indexed by run_id and error_codes.
    """
    if db is None:
        client = get_mongo_client()
        db = client[settings.DATABASE_NAME]

    # 1. Raw collection
    raw_col = db[settings.RAW_COLLECTION]
    raw_col.create_index([("run_id", ASCENDING)], background=True)
    raw_col.create_index([("source_row_number", ASCENDING)], background=True)

    # 2. Validated collection (Stable Business Key with Unique Index)
    validated_col = db[settings.VALIDATED_COLLECTION]
    validated_col.create_index([("order_id", ASCENDING)], unique=True, background=True)
    validated_col.create_index([("run_id", ASCENDING)], background=True)
    validated_col.create_index([("quality_status", ASCENDING)], background=True)

    # 3. Quarantine collection
    quarantine_col = db[settings.QUARANTINE_COLLECTION]
    quarantine_col.create_index([("run_id", ASCENDING)], background=True)
    quarantine_col.create_index([("error_codes", ASCENDING)], background=True)

    print("MongoDB indexes and collections initialized successfully.")


def clean_database(db=None, drop_all=False):
    """Utility to clean up or reset database collections."""
    if db is None:
        client = get_mongo_client()
        db = client[settings.DATABASE_NAME]

    if drop_all:
        db.drop_collection(settings.RAW_COLLECTION)
        db.drop_collection(settings.VALIDATED_COLLECTION)
        db.drop_collection(settings.QUARANTINE_COLLECTION)
        print("All pipeline collections dropped.")
        init_database_schema(db)
    else:
        db[settings.RAW_COLLECTION].delete_many({})
        db[settings.VALIDATED_COLLECTION].delete_many({})
        db[settings.QUARANTINE_COLLECTION].delete_many({})
        print("All pipeline collections cleared.")


if __name__ == "__main__":
    init_database_schema()
