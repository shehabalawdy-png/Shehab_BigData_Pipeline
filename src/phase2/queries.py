from __future__ import annotations

from datetime import date, datetime
from typing import Any, Callable

from bson import ObjectId
from pymongo import DESCENDING, MongoClient

from config.settings import (
    MONGO_CONNECT_TIMEOUT_MS,
    MONGO_DATABASE,
    MONGO_URI,
    VALIDATED_COLLECTION,
)


QUERY_DESCRIPTIONS = {
    "orders_by_city":
        "إرجاع طلبات مدينة محددة مرتبة حسب إجمالي الطلب تنازليًا.",

    "orders_by_status":
        "إرجاع الطلبات حسب الحالة مع عرض الأحدث أولًا.",

    "customer_orders":
        "إرجاع طلبات عميل محدد مع عرض الأحدث أولًا.",

    "orders_by_date_range":
        "إرجاع الطلبات الواقعة داخل فترة زمنية محددة.",

    "high_value_orders":
        "إرجاع الطلبات التي يساوي أو يتجاوز إجماليها حدًا ماليًا محددًا.",
}


def _require(params: dict[str, Any], name: str) -> Any:
    value = params.get(name)

    if value is None or (
        isinstance(value, str)
        and not value.strip()
    ):
        raise ValueError(
            f"Missing required parameter: {name}"
        )

    if isinstance(value, str):
        return value.strip()

    return value


def _limit(params: dict[str, Any]) -> int:
    value = int(
        params.get("limit", 20)
    )

    if value < 1 or value > 200:
        raise ValueError(
            "limit must be between 1 and 200"
        )

    return value


def _orders_by_city(
    params: dict[str, Any]
) -> dict[str, Any]:

    return {
        "filter": {
            "city": _require(
                params,
                "city",
            )
        },
        "sort": [
            (
                "total_amount",
                DESCENDING,
            )
        ],
        "limit": _limit(params),
    }


def _orders_by_status(
    params: dict[str, Any]
) -> dict[str, Any]:

    return {
        "filter": {
            "status": _require(
                params,
                "status",
            )
        },
        "sort": [
            (
                "order_date",
                DESCENDING,
            )
        ],
        "limit": _limit(params),
    }


def _customer_orders(
    params: dict[str, Any]
) -> dict[str, Any]:

    return {
        "filter": {
            "customer_id": _require(
                params,
                "customer_id",
            )
        },
        "sort": [
            (
                "order_date",
                DESCENDING,
            )
        ],
        "limit": _limit(params),
    }


def _orders_by_date_range(
    params: dict[str, Any]
) -> dict[str, Any]:

    start_date = _require(
        params,
        "start_date",
    )

    end_date = _require(
        params,
        "end_date",
    )

    if start_date > end_date:
        raise ValueError(
            "start_date must be less than "
            "or equal to end_date"
        )

    return {
        "filter": {
            "order_date": {
                "$gte": start_date,
                "$lte": end_date,
            }
        },
        "sort": [
            (
                "order_date",
                DESCENDING,
            )
        ],
        "limit": _limit(params),
    }


def _high_value_orders(
    params: dict[str, Any]
) -> dict[str, Any]:

    min_total = float(
        _require(
            params,
            "min_total",
        )
    )

    return {
        "filter": {
            "total_amount": {
                "$gte": min_total,
            }
        },
        "sort": [
            (
                "total_amount",
                DESCENDING,
            )
        ],
        "limit": _limit(params),
    }


_QUERY_BUILDERS: dict[
    str,
    Callable[
        [dict[str, Any]],
        dict[str, Any],
    ],
] = {
    "orders_by_city":
        _orders_by_city,

    "orders_by_status":
        _orders_by_status,

    "customer_orders":
        _customer_orders,

    "orders_by_date_range":
        _orders_by_date_range,

    "high_value_orders":
        _high_value_orders,
}


def get_query_names() -> list[str]:
    return list(
        _QUERY_BUILDERS
    )


def get_query_descriptions() -> dict[str, str]:
    return dict(
        QUERY_DESCRIPTIONS
    )


def build_query(
    name: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:

    if name not in _QUERY_BUILDERS:
        raise KeyError(
            f"Unknown query '{name}'. "
            f"Available: "
            f"{', '.join(get_query_names())}"
        )

    return _QUERY_BUILDERS[name](
        params or {}
    )


def _json_safe(value: Any) -> Any:

    if isinstance(
        value,
        ObjectId,
    ):
        return str(value)

    if isinstance(
        value,
        (datetime, date),
    ):
        return value.isoformat()

    if isinstance(
        value,
        dict,
    ):
        return {
            key: _json_safe(item)
            for key, item
            in value.items()
        }

    if isinstance(
        value,
        list,
    ):
        return [
            _json_safe(item)
            for item in value
        ]

    return value


def run_query(
    name: str,
    params: dict[str, Any] | None = None,
    database: str = MONGO_DATABASE,
) -> dict[str, Any]:

    # جميع استعلامات Phase 2 تعمل على البيانات النهائية فقط.
    spec = build_query(
        name,
        params,
    )

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

        cursor = collection.find(
            spec["filter"]
        )

        if spec.get("sort"):
            cursor = cursor.sort(
                spec["sort"]
            )

        documents = list(
            cursor.limit(
                spec["limit"]
            )
        )

        return {
            "query_name":
                name,

            "description":
                QUERY_DESCRIPTIONS[
                    name
                ],

            "database":
                database,

            "collection":
                VALIDATED_COLLECTION,

            "filter":
                _json_safe(
                    spec["filter"]
                ),

            "sort":
                spec.get(
                    "sort",
                    [],
                ),

            "limit":
                spec["limit"],

            "returned":
                len(documents),

            "documents":
                _json_safe(
                    documents
                ),
        }

    finally:
        client.close()
