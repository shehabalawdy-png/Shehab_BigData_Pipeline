from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from bson import ObjectId
from pymongo import MongoClient

from config.settings import (
    MONGO_CONNECT_TIMEOUT_MS,
    MONGO_DATABASE,
    MONGO_URI,
    PROJECT_ROOT,
)

from src.phase2.materialized_views import (
    DAILY_SALES_VIEW,
    TOP_PRODUCTS_VIEW,
)


JOB_LOG_COLLECTION = "phase2_job_runs"

DEFAULT_REPORT_DIR = (
    PROJECT_ROOT
    / "reports"
    / "scheduled"
)


JOB_DEFINITIONS = {
    "daily_sales_report": {
        "description":
            "إنشاء تقرير دوري من daily_sales_summary.",

        "schedule": {
            "type": "cron",
            "hour": 1,
            "minute": 0,
        },
    },

    "top_products_report": {
        "description":
            "إنشاء تقرير دوري لأفضل المنتجات من top_products_summary.",

        "schedule": {
            "type": "cron",
            "hour": 1,
            "minute": 5,
        },
    },
}


def _json_safe(value: Any) -> Any:

    if isinstance(value, ObjectId):
        return str(value)

    if isinstance(
        value,
        (datetime, date),
    ):
        return value.isoformat()

    if isinstance(value, dict):
        return {
            key: _json_safe(item)
            for key, item
            in value.items()
        }

    if isinstance(value, list):
        return [
            _json_safe(item)
            for item in value
        ]

    return value


def get_jobs() -> list[dict[str, Any]]:

    jobs = []

    for name, definition in (
        JOB_DEFINITIONS.items()
    ):

        jobs.append(
            {
                "name":
                    name,

                "description":
                    definition[
                        "description"
                    ],

                "schedule":
                    dict(
                        definition[
                            "schedule"
                        ]
                    ),
            }
        )

    return jobs


def _daily_sales_report(
    database: str,
) -> list[dict[str, Any]]:

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=
            MONGO_CONNECT_TIMEOUT_MS,
    )

    try:
        client.admin.command("ping")

        collection = (
            client[database]
            [DAILY_SALES_VIEW]
        )

        rows = list(
            collection
            .find({})
            .sort("_id", -1)
            .limit(31)
        )

        return _json_safe(rows)

    finally:
        client.close()


def _top_products_report(
    database: str,
) -> list[dict[str, Any]]:

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=
            MONGO_CONNECT_TIMEOUT_MS,
    )

    try:
        client.admin.command("ping")

        collection = (
            client[database]
            [TOP_PRODUCTS_VIEW]
        )

        rows = list(
            collection
            .find({})
            .sort(
                "total_sales",
                -1,
            )
            .limit(20)
        )

        return _json_safe(rows)

    finally:
        client.close()


def _execute_job(
    job_name: str,
    database: str,
) -> list[dict[str, Any]]:

    if job_name == "daily_sales_report":
        return _daily_sales_report(
            database
        )

    if job_name == "top_products_report":
        return _top_products_report(
            database
        )

    raise KeyError(
        f"Unknown job: {job_name}"
    )


def run_job(
    job_name: str,
    database: str = MONGO_DATABASE,
    output_dir: str | Path | None = None,
) -> dict[str, Any]:

    if job_name not in JOB_DEFINITIONS:
        raise KeyError(
            f"Unknown job '{job_name}'. "
            f"Available: "
            f"{', '.join(JOB_DEFINITIONS)}"
        )

    started_at = datetime.now(
        timezone.utc
    )

    output_directory = Path(
        output_dir
        or DEFAULT_REPORT_DIR
    )

    output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=
            MONGO_CONNECT_TIMEOUT_MS,
    )

    log_collection = (
        client[database]
        [JOB_LOG_COLLECTION]
    )

    log_id = None

    try:
        client.admin.command("ping")

        log_document = {
            "job_name":
                job_name,

            "started_at":
                started_at,

            "ended_at":
                None,

            "status":
                "running",

            "database":
                database,
        }

        log_id = (
            log_collection
            .insert_one(
                log_document
            )
            .inserted_id
        )

        rows = _execute_job(
            job_name,
            database,
        )

        ended_at = datetime.now(
            timezone.utc
        )

        result = {
            "job_name":
                job_name,

            "description":
                JOB_DEFINITIONS[
                    job_name
                ][
                    "description"
                ],

            "database":
                database,

            "started_at":
                started_at.isoformat(),

            "ended_at":
                ended_at.isoformat(),

            "status":
                "success",

            "row_count":
                len(rows),

            "results":
                rows,
        }

        filename = (
            f"{job_name}_latest.json"
        )

        output_path = (
            output_directory
            / filename
        )

        output_path.write_text(
            json.dumps(
                result,
                ensure_ascii=False,
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )

        log_collection.update_one(
            {
                "_id":
                    log_id
            },
            {
                "$set": {
                    "ended_at":
                        ended_at,

                    "status":
                        "success",

                    "row_count":
                        len(rows),

                    "output_file":
                        str(
                            output_path
                        ),
                }
            },
        )

        result[
            "output_file"
        ] = str(
            output_path
        )

        return result

    except Exception as exc:

        ended_at = datetime.now(
            timezone.utc
        )

        if log_id is not None:

            log_collection.update_one(
                {
                    "_id":
                        log_id
                },
                {
                    "$set": {
                        "ended_at":
                            ended_at,

                        "status":
                            "failed",

                        "error":
                            str(exc),
                    }
                },
            )

        raise

    finally:
        client.close()


def get_job_runs(
    database: str = MONGO_DATABASE,
    limit: int = 50,
) -> list[dict[str, Any]]:

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=
            MONGO_CONNECT_TIMEOUT_MS,
    )

    try:
        client.admin.command("ping")

        collection = (
            client[database]
            [JOB_LOG_COLLECTION]
        )

        rows = list(
            collection
            .find({})
            .sort(
                "started_at",
                -1,
            )
            .limit(limit)
        )

        return _json_safe(rows)

    finally:
        client.close()
