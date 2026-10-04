from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from pymongo import MongoClient, UpdateOne

from config.settings import (
    MONGO_CONNECT_TIMEOUT_MS,
    MONGO_DATABASE,
    MONGO_URI,
    VALIDATED_COLLECTION,
)


DAILY_SALES_VIEW = "daily_sales_summary"
TOP_PRODUCTS_VIEW = "top_products_summary"


def get_materialized_view_names() -> list[str]:
    return [
        DAILY_SALES_VIEW,
        TOP_PRODUCTS_VIEW,
    ]


def _day_from_order(order: dict[str, Any]) -> str | None:
    value = order.get("order_date")

    if not isinstance(value, str) or len(value) < 10:
        return None

    return value[:10]


def _number(value: Any) -> float:
    if isinstance(value, (int, float)):
        return float(value)

    return 0.0


def _product_contributions(
    order: dict[str, Any],
) -> dict[str, dict[str, Any]]:

    totals: dict[str, dict[str, Any]] = {}

    items = order.get("items")

    if not isinstance(items, list):
        return totals

    for item in items:

        if not isinstance(item, dict):
            continue

        sku = str(
            item.get("sku") or ""
        ).strip()

        name = str(
            item.get("name") or ""
        ).strip()

        # SKU هو المفتاح الأفضل.
        # إذا لم يوجد نستخدم الاسم بدلًا منه.
        product_key = sku or name

        if not product_key:
            continue

        if product_key not in totals:
            totals[product_key] = {
                "sku": sku or None,
                "product_name": name or None,
                "quantity": 0.0,
                "sales": 0.0,
            }

        totals[product_key]["quantity"] += _number(
            item.get("qty")
        )

        totals[product_key]["sales"] += _number(
            item.get("total")
        )

    return totals


def _daily_contribution(
    order: dict[str, Any],
) -> dict[str, Any] | None:

    day = _day_from_order(order)

    if not day:
        return None

    return {
        "day": day,
        "order_count": 1,
        "total_sales": _number(
            order.get("total_amount")
        ),
        "delivery_cost": _number(
            order.get("delivery_cost")
        ),
    }


def apply_order_change(
    old_order: dict[str, Any] | None,
    new_order: dict[str, Any] | None,
    database: str = MONGO_DATABASE,
) -> dict[str, Any]:
    """
    تحديث الـMaterialized Views باستخدام الفرق فقط.

    Insert:
        old_order=None
        new_order=<new document>

    Update:
        old_order=<old document>
        new_order=<new document>

    Delete مستقبليًا:
        old_order=<old document>
        new_order=None

    بهذه الطريقة لا نعيد حساب جميع orders_validated.
    """

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=
            MONGO_CONNECT_TIMEOUT_MS,
    )

    try:
        client.admin.command("ping")

        db = client[database]

        daily = db[DAILY_SALES_VIEW]
        products = db[TOP_PRODUCTS_VIEW]

        now = datetime.now(timezone.utc)

        daily_deltas: dict[
            str,
            dict[str, float],
        ] = defaultdict(
            lambda: {
                "order_count": 0,
                "total_sales": 0.0,
                "delivery_cost": 0.0,
            }
        )

        # طرح مساهمة النسخة القديمة عند Update.
        if old_order is not None:

            old_daily = _daily_contribution(
                old_order
            )

            if old_daily:

                day = old_daily["day"]

                daily_deltas[day][
                    "order_count"
                ] -= 1

                daily_deltas[day][
                    "total_sales"
                ] -= old_daily["total_sales"]

                daily_deltas[day][
                    "delivery_cost"
                ] -= old_daily["delivery_cost"]

        # إضافة مساهمة النسخة الجديدة.
        if new_order is not None:

            new_daily = _daily_contribution(
                new_order
            )

            if new_daily:

                day = new_daily["day"]

                daily_deltas[day][
                    "order_count"
                ] += 1

                daily_deltas[day][
                    "total_sales"
                ] += new_daily["total_sales"]

                daily_deltas[day][
                    "delivery_cost"
                ] += new_daily["delivery_cost"]

        daily_operations = []

        for day, delta in daily_deltas.items():

            daily_operations.append(
                UpdateOne(
                    {"_id": day},
                    {
                        "$inc": {
                            "order_count":
                                delta["order_count"],

                            "total_sales":
                                delta["total_sales"],

                            "total_delivery_cost":
                                delta["delivery_cost"],
                        },
                        "$set": {
                            "last_refreshed_at":
                                now,
                        },
                    },
                    upsert=True,
                )
            )

        if daily_operations:

            daily.bulk_write(
                daily_operations,
                ordered=False,
            )

        old_products = (
            _product_contributions(
                old_order
            )
            if old_order
            else {}
        )

        new_products = (
            _product_contributions(
                new_order
            )
            if new_order
            else {}
        )

        product_keys = (
            set(old_products)
            | set(new_products)
        )

        product_operations = []

        for product_key in product_keys:

            old_value = old_products.get(
                product_key,
                {
                    "quantity": 0.0,
                    "sales": 0.0,
                },
            )

            new_value = new_products.get(
                product_key,
                {
                    "quantity": 0.0,
                    "sales": 0.0,
                },
            )

            quantity_delta = (
                new_value.get(
                    "quantity",
                    0.0,
                )
                -
                old_value.get(
                    "quantity",
                    0.0,
                )
            )

            sales_delta = (
                new_value.get(
                    "sales",
                    0.0,
                )
                -
                old_value.get(
                    "sales",
                    0.0,
                )
            )

            metadata = (
                new_products.get(product_key)
                or old_products.get(product_key)
                or {}
            )

            product_operations.append(
                UpdateOne(
                    {
                        "_id":
                            product_key
                    },
                    {
                        "$inc": {
                            "quantity_sold":
                                quantity_delta,

                            "total_sales":
                                sales_delta,
                        },
                        "$set": {
                            "sku":
                                metadata.get(
                                    "sku"
                                ),

                            "product_name":
                                metadata.get(
                                    "product_name"
                                ),

                            "last_refreshed_at":
                                now,
                        },
                    },
                    upsert=True,
                )
            )

        if product_operations:

            products.bulk_write(
                product_operations,
                ordered=False,
            )

        return {
            "mode":
                "incremental",

            "daily_groups_updated":
                len(daily_operations),

            "product_groups_updated":
                len(product_operations),

            "views": get_materialized_view_names(),
        }

    finally:
        client.close()


def rebuild_materialized_views(
    database: str = MONGO_DATABASE,
    source_collection: str = VALIDATED_COLLECTION,
) -> dict[str, Any]:
    """
    Initial Build فقط.

    تستخدم هذه الدالة عند إنشاء الـViews لأول مرة.
    بعد ذلك يجب استخدام apply_order_change للتحديث التزايدي.
    """

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=
            MONGO_CONNECT_TIMEOUT_MS,
    )

    try:
        client.admin.command("ping")

        db = client[database]
        source = db[source_collection]

        # -----------------------------------------------
        # View 1: Daily Sales Summary
        # -----------------------------------------------

        daily_pipeline = [
            {
                "$match": {
                    "order_date": {
                        "$type": "string",
                    },
                    "total_amount": {
                        "$type": "number",
                    },
                }
            },
            {
                "$project": {
                    "day": {
                        "$substrCP": [
                            "$order_date",
                            0,
                            10,
                        ]
                    },
                    "total_amount": 1,
                    "delivery_cost": {
                        "$cond": [
                            {
                                "$isNumber":
                                    "$delivery_cost"
                            },
                            "$delivery_cost",
                            0,
                        ]
                    },
                }
            },
            {
                "$group": {
                    "_id": "$day",
                    "order_count": {
                        "$sum": 1,
                    },
                    "total_sales": {
                        "$sum": "$total_amount",
                    },
                    "total_delivery_cost": {
                        "$sum": "$delivery_cost",
                    },
                }
            },
            {
                "$set": {
                    "last_refreshed_at":
                        "$$NOW",
                }
            },
            {
                "$merge": {
                    "into":
                        DAILY_SALES_VIEW,

                    "on":
                        "_id",

                    "whenMatched":
                        "replace",

                    "whenNotMatched":
                        "insert",
                }
            },
        ]

        list(
            source.aggregate(
                daily_pipeline,
                allowDiskUse=True,
            )
        )

        # -----------------------------------------------
        # View 2: Top Products Summary
        # -----------------------------------------------

        products_pipeline = [
            {
                "$match": {
                    "items": {
                        "$type": "array",
                    }
                }
            },
            {
                "$unwind":
                    "$items"
            },
            {
                "$match": {
                    "items.qty": {
                        "$type": "number",
                    },
                    "items.total": {
                        "$type": "number",
                    },
                }
            },
            {
                "$project": {
                    "product_key": {
                        "$cond": [
                            {
                                "$and": [
                                    {
                                        "$ne": [
                                            "$items.sku",
                                            None,
                                        ]
                                    },
                                    {
                                        "$ne": [
                                            "$items.sku",
                                            "",
                                        ]
                                    },
                                ]
                            },
                            "$items.sku",
                            "$items.name",
                        ]
                    },
                    "sku":
                        "$items.sku",

                    "product_name":
                        "$items.name",

                    "qty":
                        "$items.qty",

                    "sales":
                        "$items.total",
                }
            },
            {
                "$match": {
                    "product_key": {
                        "$nin": [
                            None,
                            "",
                        ]
                    }
                }
            },
            {
                "$group": {
                    "_id":
                        "$product_key",

                    "sku": {
                        "$first":
                            "$sku"
                    },

                    "product_name": {
                        "$first":
                            "$product_name"
                    },

                    "quantity_sold": {
                        "$sum":
                            "$qty"
                    },

                    "total_sales": {
                        "$sum":
                            "$sales"
                    },
                }
            },
            {
                "$set": {
                    "last_refreshed_at":
                        "$$NOW",
                }
            },
            {
                "$merge": {
                    "into":
                        TOP_PRODUCTS_VIEW,

                    "on":
                        "_id",

                    "whenMatched":
                        "replace",

                    "whenNotMatched":
                        "insert",
                }
            },
        ]

        list(
            source.aggregate(
                products_pipeline,
                allowDiskUse=True,
            )
        )

        # Index يساعد عند قراءة أفضل المنتجات.
        db[TOP_PRODUCTS_VIEW].create_index(
            [
                (
                    "total_sales",
                    -1,
                )
            ],
            name="idx_mv_products_sales",
        )

        return {
            "mode":
                "initial_build",

            "source_collection":
                source_collection,

            "views": {
                DAILY_SALES_VIEW:
                    db[
                        DAILY_SALES_VIEW
                    ].estimated_document_count(),

                TOP_PRODUCTS_VIEW:
                    db[
                        TOP_PRODUCTS_VIEW
                    ].estimated_document_count(),
            },
        }

    finally:
        client.close()


def read_materialized_view(
    name: str,
    limit: int = 20,
    database: str = MONGO_DATABASE,
) -> list[dict[str, Any]]:

    if name not in get_materialized_view_names():
        raise KeyError(
            f"Unknown materialized view: {name}"
        )

    if limit < 1 or limit > 200:
        raise ValueError(
            "limit must be between 1 and 200"
        )

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=
            MONGO_CONNECT_TIMEOUT_MS,
    )

    try:
        client.admin.command("ping")

        collection = client[
            database
        ][name]

        if name == TOP_PRODUCTS_VIEW:

            cursor = collection.find(
                {}
            ).sort(
                "total_sales",
                -1,
            )

        else:

            cursor = collection.find(
                {}
            ).sort(
                "_id",
                -1,
            )

        results = list(
            cursor.limit(limit)
        )

        for document in results:
            document.pop("_id", None)

        return results

    finally:
        client.close()
