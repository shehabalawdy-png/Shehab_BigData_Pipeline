from __future__ import annotations

from typing import Any

from pymongo import (
    ASCENDING,
    DESCENDING,
    MongoClient,
)

from config.settings import (
    MONGO_CONNECT_TIMEOUT_MS,
    MONGO_DATABASE,
    MONGO_URI,
    VALIDATED_COLLECTION,
)


# فهارس Phase 2 منفصلة تمامًا عن فهارس Phase 1.
# هذا يسمح باختبار Explain قبل وبعد بدون حذف
# ux_validated_id_order أو أي فهرس قديم مهم.

PHASE2_INDEXES = [
    {
        "name":
            "idx_p2_city_total",

        "keys": [
            (
                "city",
                ASCENDING,
            ),
            (
                "total_amount",
                DESCENDING,
            ),
        ],

        "reason":
            "يدعم التصفية حسب المدينة "
            "ثم ترتيب النتائج حسب "
            "total_amount تنازليًا.",
    },

    {
        "name":
            "idx_p2_status_date",

        "keys": [
            (
                "status",
                ASCENDING,
            ),
            (
                "order_date",
                DESCENDING,
            ),
        ],

        "reason":
            "يدعم التصفية حسب حالة الطلب "
            "ثم إرجاع أحدث الطلبات أولًا.",
    },

    {
        "name":
            "idx_p2_customer_date",

        "keys": [
            (
                "customer_id",
                ASCENDING,
            ),
            (
                "order_date",
                DESCENDING,
            ),
        ],

        "reason":
            "يدعم استعلام طلبات العميل "
            "مع ترتيبها زمنيًا من الأحدث.",
    },
]


def get_index_definitions() -> list[
    dict[str, Any]
]:
    return [
        dict(item)
        for item
        in PHASE2_INDEXES
    ]


def create_phase2_indexes(
    database: str = MONGO_DATABASE,
) -> dict[str, Any]:

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=
            MONGO_CONNECT_TIMEOUT_MS,
    )

    try:
        client.admin.command(
            "ping"
        )

        collection = (
            client[database]
            [VALIDATED_COLLECTION]
        )

        created = []

        for item in PHASE2_INDEXES:

            name = (
                collection
                .create_index(
                    item["keys"],
                    name=item["name"],
                )
            )

            created.append(
                {
                    "name":
                        name,

                    "keys":
                        item["keys"],

                    "reason":
                        item["reason"],
                }
            )

        return {
            "database":
                database,

            "collection":
                VALIDATED_COLLECTION,

            "created":
                created,
        }

    finally:
        client.close()


def drop_phase2_indexes(
    database: str = MONGO_DATABASE,
) -> dict[str, Any]:

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=
            MONGO_CONNECT_TIMEOUT_MS,
    )

    try:
        client.admin.command(
            "ping"
        )

        collection = (
            client[database]
            [VALIDATED_COLLECTION]
        )

        existing = set(
            collection
            .index_information()
        )

        dropped = []

        for item in PHASE2_INDEXES:

            if item["name"] in existing:

                collection.drop_index(
                    item["name"]
                )

                dropped.append(
                    item["name"]
                )

        return {
            "database":
                database,

            "collection":
                VALIDATED_COLLECTION,

            "dropped":
                dropped,
        }

    finally:
        client.close()


def list_indexes(
    database: str = MONGO_DATABASE,
) -> list[dict[str, Any]]:

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=
            MONGO_CONNECT_TIMEOUT_MS,
    )

    try:
        client.admin.command(
            "ping"
        )

        collection = (
            client[database]
            [VALIDATED_COLLECTION]
        )

        result = []

        for item in (
            collection
            .list_indexes()
        ):

            result.append(
                {
                    "name":
                        item["name"],

                    "key":
                        dict(
                            item["key"]
                        ),

                    "unique":
                        bool(
                            item.get(
                                "unique",
                                False,
                            )
                        ),
                }
            )

        return result

    finally:
        client.close()
