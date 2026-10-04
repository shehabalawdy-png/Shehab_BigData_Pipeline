from __future__ import annotations

import time
from typing import Any, Callable

from pymongo import MongoClient

from config.settings import (
    MONGO_CONNECT_TIMEOUT_MS,
    MONGO_DATABASE,
    MONGO_URI,
    VALIDATED_COLLECTION,
)


AGGREGATION_DESCRIPTIONS = {
    "sales_by_city":
        "إجمالي المبيعات وعدد الطلبات ومتوسط قيمة الطلب حسب المدينة.",

    "orders_by_status":
        "توزيع الطلبات وإجمالي المبيعات حسب حالة الطلب.",

    "payment_method_summary":
        "ملخص عدد الطلبات وإجمالي المبيعات حسب طريقة الدفع.",

    "delivery_type_summary":
        "ملخص عدد الطلبات وإجمالي المبيعات حسب نوع التوصيل.",

    "monthly_sales_summary":
        "إجمالي المبيعات وعدد الطلبات ومتوسط قيمة الطلب شهريًا.",
}


def _sales_by_city() -> list[dict[str, Any]]:
    return [
        {
            "$match": {
                "city": {
                    "$exists": True,
                    "$nin": [None, ""],
                },
                "total_amount": {
                    "$type": "number",
                },
            }
        },
        {
            "$group": {
                "_id": "$city",
                "order_count": {
                    "$sum": 1,
                },
                "total_sales": {
                    "$sum": "$total_amount",
                },
                "average_order_value": {
                    "$avg": "$total_amount",
                },
            }
        },
        {
            "$sort": {
                "total_sales": -1,
            }
        },
    ]


def _orders_by_status() -> list[dict[str, Any]]:
    return [
        {
            "$match": {
                "status": {
                    "$exists": True,
                    "$nin": [None, ""],
                }
            }
        },
        {
            "$group": {
                "_id": "$status",
                "order_count": {
                    "$sum": 1,
                },
                "total_sales": {
                    "$sum": {
                        "$cond": [
                            {
                                "$isNumber":
                                    "$total_amount"
                            },
                            "$total_amount",
                            0,
                        ]
                    }
                },
            }
        },
        {
            "$sort": {
                "order_count": -1,
            }
        },
    ]


def _payment_method_summary() -> list[dict[str, Any]]:
    return [
        {
            "$match": {
                "payment_method": {
                    "$exists": True,
                    "$nin": [None, ""],
                }
            }
        },
        {
            "$group": {
                "_id": "$payment_method",
                "order_count": {
                    "$sum": 1,
                },
                "total_sales": {
                    "$sum": {
                        "$cond": [
                            {
                                "$isNumber":
                                    "$total_amount"
                            },
                            "$total_amount",
                            0,
                        ]
                    }
                },
                "average_order_value": {
                    "$avg": {
                        "$cond": [
                            {
                                "$isNumber":
                                    "$total_amount"
                            },
                            "$total_amount",
                            None,
                        ]
                    }
                },
            }
        },
        {
            "$sort": {
                "total_sales": -1,
            }
        },
    ]


def _delivery_type_summary() -> list[dict[str, Any]]:
    return [
        {
            "$match": {
                "delivery_type": {
                    "$exists": True,
                    "$nin": [None, ""],
                }
            }
        },
        {
            "$group": {
                "_id": "$delivery_type",
                "order_count": {
                    "$sum": 1,
                },
                "total_sales": {
                    "$sum": {
                        "$cond": [
                            {
                                "$isNumber":
                                    "$total_amount"
                            },
                            "$total_amount",
                            0,
                        ]
                    }
                },
                "total_delivery_cost": {
                    "$sum": {
                        "$cond": [
                            {
                                "$isNumber":
                                    "$delivery_cost"
                            },
                            "$delivery_cost",
                            0,
                        ]
                    }
                },
            }
        },
        {
            "$sort": {
                "order_count": -1,
            }
        },
    ]


def _monthly_sales_summary() -> list[dict[str, Any]]:
    return [
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
                "month": {
                    "$substrCP": [
                        "$order_date",
                        0,
                        7,
                    ]
                },
                "total_amount": 1,
            }
        },
        {
            "$group": {
                "_id": "$month",
                "order_count": {
                    "$sum": 1,
                },
                "total_sales": {
                    "$sum": "$total_amount",
                },
                "average_order_value": {
                    "$avg": "$total_amount",
                },
            }
        },
        {
            "$sort": {
                "_id": 1,
            }
        },
    ]


_AGGREGATIONS: dict[
    str,
    Callable[
        [],
        list[dict[str, Any]],
    ],
] = {
    "sales_by_city":
        _sales_by_city,

    "orders_by_status":
        _orders_by_status,

    "payment_method_summary":
        _payment_method_summary,

    "delivery_type_summary":
        _delivery_type_summary,

    "monthly_sales_summary":
        _monthly_sales_summary,
}


def get_aggregation_names() -> list[str]:
    return list(_AGGREGATIONS)


def get_aggregation_descriptions() -> dict[str, str]:
    return dict(AGGREGATION_DESCRIPTIONS)


def build_aggregation(
    name: str,
) -> list[dict[str, Any]]:

    if name not in _AGGREGATIONS:
        raise KeyError(
            f"Unknown aggregation '{name}'. "
            f"Available: "
            f"{', '.join(get_aggregation_names())}"
        )

    return _AGGREGATIONS[name]()


def run_aggregation(
    name: str,
    limit: int = 20,
    database: str = MONGO_DATABASE,
    collection_name: str = VALIDATED_COLLECTION,
) -> dict[str, Any]:

    if limit < 1 or limit > 200:
        raise ValueError(
            "limit must be between 1 and 200"
        )

    pipeline = build_aggregation(name)

    pipeline.append(
        {
            "$limit": limit,
        }
    )

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=
            MONGO_CONNECT_TIMEOUT_MS,
    )

    start = time.perf_counter()

    try:
        client.admin.command("ping")

        collection = (
            client[database]
            [collection_name]
        )

        results = list(
            collection.aggregate(
                pipeline,
                allowDiskUse=True,
            )
        )

        elapsed = (
            time.perf_counter()
            - start
        )

        return {
            "aggregation_name":
                name,

            "description":
                AGGREGATION_DESCRIPTIONS[name],

            "database":
                database,

            "collection":
                collection_name,

            "execution_time_seconds":
                round(elapsed, 3),

            "returned":
                len(results),

            "results":
                results,
        }

    finally:
        client.close()
