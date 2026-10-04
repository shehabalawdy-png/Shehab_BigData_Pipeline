from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pymongo import MongoClient

from config.settings import (
    MONGO_CONNECT_TIMEOUT_MS,
    MONGO_DATABASE,
    MONGO_URI,
    PROJECT_ROOT,
    VALIDATED_COLLECTION,
)

from src.phase2.indexes import (
    create_phase2_indexes,
    drop_phase2_indexes,
)


REPORT_PATH = (
    PROJECT_ROOT
    / "reports"
    / "phase2_explain.json"
)


def _sample_value(
    collection,
    field: str,
) -> Any:

    document = (
        collection
        .find_one(
            {
                field: {
                    "$exists": True,
                    "$nin": [
                        None,
                        "",
                    ],
                }
            },
            {
                field: 1,
                "_id": 0,
            },
        )
    )

    if not document:
        raise RuntimeError(
            "Cannot run Explain "
            f"comparison: no usable "
            f"value found for '{field}'."
        )

    return document[field]


def _targets(
    collection,
) -> list[dict[str, Any]]:

    return [
        {
            "name":
                "orders_by_city",

            "filter": {
                "city":
                    _sample_value(
                        collection,
                        "city",
                    )
            },

            "sort": {
                "total_amount": -1
            },

            "expected_index":
                "idx_p2_city_total",
        },

        {
            "name":
                "orders_by_status",

            "filter": {
                "status":
                    _sample_value(
                        collection,
                        "status",
                    )
            },

            "sort": {
                "order_date": -1
            },

            "expected_index":
                "idx_p2_status_date",
        },

        {
            "name":
                "customer_orders",

            "filter": {
                "customer_id":
                    _sample_value(
                        collection,
                        "customer_id",
                    )
            },

            "sort": {
                "order_date": -1
            },

            "expected_index":
                "idx_p2_customer_date",
        },
    ]


def _find_index_name(
    plan: Any,
) -> str | None:

    if isinstance(
        plan,
        dict,
    ):

        if plan.get(
            "indexName"
        ):
            return str(
                plan["indexName"]
            )

        for value in (
            plan.values()
        ):
            found = (
                _find_index_name(
                    value
                )
            )

            if found:
                return found

    elif isinstance(
        plan,
        list,
    ):

        for value in plan:

            found = (
                _find_index_name(
                    value
                )
            )

            if found:
                return found

    return None


def _explain(
    db,
    collection_name: str,
    target: dict[str, Any],
) -> dict[str, Any]:

    command = {
        "find":
            collection_name,

        "filter":
            target["filter"],

        "sort":
            target["sort"],

        "limit":
            20,
    }

    result = db.command(
        "explain",
        command,
        verbosity=
            "executionStats",
    )

    stats = result.get(
        "executionStats",
        {},
    )

    winning_plan = (
        result
        .get(
            "queryPlanner",
            {},
        )
        .get(
            "winningPlan",
            {},
        )
    )

    return {
        "nReturned":
            stats.get(
                "nReturned",
                0,
            ),

        "totalDocsExamined":
            stats.get(
                "totalDocsExamined",
                0,
            ),

        "totalKeysExamined":
            stats.get(
                "totalKeysExamined",
                0,
            ),

        "executionTimeMillis":
            stats.get(
                "executionTimeMillis",
                0,
            ),

        "indexUsed":
            _find_index_name(
                winning_plan
            ),
    }


def compare_explain(
    database: str = MONGO_DATABASE,
    report_path:
        str | Path = REPORT_PATH,
) -> dict[str, Any]:

    # نحذف فقط فهارس Phase 2 للحصول على Before حقيقي.
    # لا يتم المساس بفهرس id_order أو فهارس Phase 1.
    drop_phase2_indexes(
        database
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

        db = client[
            database
        ]

        collection = db[
            VALIDATED_COLLECTION
        ]

        if (
            collection
            .estimated_document_count()
            == 0
        ):
            raise RuntimeError(
                f"Collection "
                f"'{VALIDATED_COLLECTION}' "
                f"is empty; Explain "
                f"needs test data."
            )

        targets = _targets(
            collection
        )

        before = {
            item["name"]:
                _explain(
                    db,
                    VALIDATED_COLLECTION,
                    item,
                )

            for item
            in targets
        }

    finally:
        client.close()

    # إنشاء الفهارس الثلاثة بعد قياس Before.
    create_phase2_indexes(
        database
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

        db = client[
            database
        ]

        targets = _targets(
            db[
                VALIDATED_COLLECTION
            ]
        )

        comparisons = []

        for item in targets:

            after = _explain(
                db,
                VALIDATED_COLLECTION,
                item,
            )

            before_stats = before[
                item["name"]
            ]

            comparisons.append(
                {
                    "query":
                        item["name"],

                    "filter":
                        item["filter"],

                    "expected_index":
                        item[
                            "expected_index"
                        ],

                    "before":
                        before_stats,

                    "after":
                        after,

                    "docs_examined_reduction":
                        (
                            before_stats[
                                "totalDocsExamined"
                            ]
                            -
                            after[
                                "totalDocsExamined"
                            ]
                        ),
                }
            )

        report = {
            "database":
                database,

            "collection":
                VALIDATED_COLLECTION,

            "verbosity":
                "executionStats",

            "comparisons":
                comparisons,
        }

        path = Path(
            report_path
        )

        path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        path.write_text(
            json.dumps(
                report,
                ensure_ascii=False,
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )

        report[
            "report_path"
        ] = str(path)

        return report

    finally:
        client.close()


if __name__ == "__main__":

    print(
        json.dumps(
            compare_explain(),
            ensure_ascii=False,
            indent=2,
            default=str,
        )
    )
