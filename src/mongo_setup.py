import argparse
from pymongo import ASCENDING, MongoClient

from config.settings import (
    MONGO_DATABASE,
    MONGO_URI,
    QUARANTINE_COLLECTION,
    RAW_COLLECTION,
    VALIDATED_COLLECTION,
)


VALIDATED_SCHEMA = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": [
            "id_order",
            "customer_id",
            "order_date",
            "currency",
            "total_amount",
            "items",
            "quality_status",
            "source_run_id",
        ],
        "properties": {
            "id_order": {
                "bsonType": "string",
            },
            "order_id": {
                "bsonType": "string",
            },
            "customer_id": {
                "bsonType": "string",
            },
            "order_date": {
                "bsonType": "string",
            },
            "currency": {
                "bsonType": "string",
            },
            "total_amount": {
                "bsonType": [
                    "double",
                    "int",
                    "long",
                    "decimal",
                ],
            },
            "items": {
                "bsonType": "array",
            },
            "quality_status": {
                "enum": [
                    "valid",
                    "corrected",
                ],
            },
            "corrections": {
                "bsonType": "array",
            },
            "source_run_id": {
                "bsonType": "string",
            },
        },
    }
}


def configure_database(database=MONGO_DATABASE):
    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=5000,
    )

    try:
        client.admin.command("ping")

        db = client[database]

        existing = db.list_collection_names()

        # -------------------------------------------------
        # Raw layer
        # -------------------------------------------------

        raw = db[RAW_COLLECTION]

        raw.create_index(
            [
                ("id_run", ASCENDING),
                ("number_row_source", ASCENDING),
            ],
            name="idx_raw_run_row",
        )

        raw.create_index(
            "at_ingested",
            name="idx_raw_ingested_at",
        )

        # -------------------------------------------------
        # Validated layer
        # -------------------------------------------------

        if VALIDATED_COLLECTION not in existing:
            db.create_collection(
                VALIDATED_COLLECTION,
                validator=VALIDATED_SCHEMA,
                validationLevel="strict",
                validationAction="error",
            )
        else:
            db.command(
                {
                    "collMod": VALIDATED_COLLECTION,
                    "validator": VALIDATED_SCHEMA,
                    "validationLevel": "strict",
                    "validationAction": "error",
                }
            )

        validated = db[VALIDATED_COLLECTION]

        old_index_name = "ux_validated_order_id"

        if old_index_name in validated.index_information():
            validated.drop_index(old_index_name)

        validated.create_index(
            "id_order",
            unique=True,
            name="ux_validated_id_order",
        )

        validated.create_index(
            "source_run_id",
            name="idx_validated_source_run",
        )

        # -------------------------------------------------
        # Quarantine layer
        # -------------------------------------------------

        quarantine = db[QUARANTINE_COLLECTION]

        quarantine.create_index(
            [
                ("source_run_id", ASCENDING),
                ("source_row_number", ASCENDING),
            ],
            unique=True,
            name="ux_quarantine_source_row",
        )

        quarantine.create_index(
            "error_codes",
            name="idx_quarantine_error_codes",
        )

        print("MongoDB Setup")
        print("-" * 60)
        print(f"Database   : {database}")
        print(f"Raw        : {RAW_COLLECTION}")
        print(f"Validated  : {VALIDATED_COLLECTION}")
        print(f"Quarantine : {QUARANTINE_COLLECTION}")

        print()
        print("orders_raw indexes:")

        for index in raw.list_indexes():
            print(
                f"  {index['name']} | "
                f"unique={index.get('unique', False)}"
            )

        print()
        print("orders_validated indexes:")

        for index in validated.list_indexes():
            print(
                f"  {index['name']} | "
                f"unique={index.get('unique', False)}"
            )

        print()
        print("quarantine indexes:")

        for index in quarantine.list_indexes():
            print(
                f"  {index['name']} | "
                f"unique={index.get('unique', False)}"
            )

        info = db.command(
            {
                "listCollections": 1,
                "filter": {
                    "name": VALIDATED_COLLECTION
                },
            }
        )

        collection_info = (
            info["cursor"]["firstBatch"][0]
        )

        validator = (
            collection_info
            .get("options", {})
            .get("validator")
        )

        print()
        print(
            "Validated schema validator:",
            "PASS" if validator else "MISSING",
        )

        print(
            "MongoDB configuration: PASS"
        )

    finally:
        client.close()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Create MongoDB collections, validator, and indexes."
    )
    parser.add_argument(
        "--database",
        default=MONGO_DATABASE,
        help="MongoDB database name.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    configure_database(database=args.database)
