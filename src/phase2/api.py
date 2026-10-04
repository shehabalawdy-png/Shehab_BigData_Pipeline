from __future__ import annotations

from argparse import Namespace
from typing import Any

from fastapi import (
    FastAPI,
    HTTPException,
    Query,
    Request,
)
from pydantic import BaseModel, Field
from pymongo import MongoClient

from config.settings import (
    BATCH_SIZE,
    MONGO_CONNECT_TIMEOUT_MS,
    MONGO_DATABASE,
    MONGO_URI,
    SMALL_FILE_THRESHOLD_MB,
    SPARK_ELT_SHUFFLE_PARTITIONS,
    VALIDATED_COLLECTION,
)

from src.main import run_pipeline

from src.phase2.aggregations import (
    get_aggregation_descriptions,
    get_aggregation_names,
    run_aggregation,
)

from src.phase2.indexes import (
    create_phase2_indexes,
    list_indexes,
)

from src.phase2.jobs import (
    get_jobs,
    run_job,
)

from src.phase2.materialized_views import (
    DAILY_SALES_VIEW,
    TOP_PRODUCTS_VIEW,
    get_materialized_view_names,
    rebuild_materialized_views,
)

from src.phase2.queries import (
    get_query_descriptions,
    get_query_names,
    run_query,
)


app = FastAPI(
    title="Shehab Big Data Pipeline API",
    description=(
        "Unified API for Big Data Phase 1 and Phase 2. "
        "The ingest endpoint reuses the original hybrid pipeline."
    ),
    version="2.0.0",
)


class IngestRequest(BaseModel):
    input: str

    database: str = MONGO_DATABASE

    threshold_mb: int = Field(
        default=SMALL_FILE_THRESHOLD_MB,
        gt=0,
    )

    batch_size: int = Field(
        default=BATCH_SIZE,
        gt=0,
    )

    write_batch_size: int = Field(
        default=BATCH_SIZE,
        gt=0,
    )

    shuffle_partitions: int = Field(
        default=SPARK_ELT_SHUFFLE_PARTITIONS,
        gt=0,
    )

    validate_only: bool = False
    route_only: bool = False
    raw_only: bool = False


class RefreshMaterializedViewsRequest(
    BaseModel
):
    database: str = MONGO_DATABASE

    source_collection: str = (
        VALIDATED_COLLECTION
    )

    force_rebuild: bool = False


def _http_error(
    exc: Exception,
) -> HTTPException:

    if isinstance(
        exc,
        FileNotFoundError,
    ):
        return HTTPException(
            status_code=404,
            detail=str(exc),
        )

    if isinstance(
        exc,
        KeyError,
    ):
        return HTTPException(
            status_code=404,
            detail=str(exc),
        )

    if isinstance(
        exc,
        ValueError,
    ):
        return HTTPException(
            status_code=400,
            detail=str(exc),
        )

    return HTTPException(
        status_code=500,
        detail=(
            f"{exc.__class__.__name__}: "
            f"{exc}"
        ),
    )


@app.get("/health")
def health(
    database: str = Query(
        default=MONGO_DATABASE
    ),
) -> dict[str, Any]:

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=
            MONGO_CONNECT_TIMEOUT_MS,
    )

    try:
        client.admin.command("ping")

        db = client[database]

        return {
            "status": "ok",
            "mongodb": "connected",
            "database": database,
            "validated_documents":
                db[
                    VALIDATED_COLLECTION
                ].estimated_document_count(),
            "materialized_views": {
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

    except Exception as exc:
        raise _http_error(exc)

    finally:
        client.close()


@app.post("/ingest")
def ingest(
    payload: IngestRequest,
) -> dict[str, Any]:
    """
    يستخدم Pipeline المشروع النصفي نفسه بالكامل.

    File Router -> Python Batch / PySpark
    -> Raw -> Quality -> Validated/Quarantine
    """

    args = Namespace(
        input=payload.input,
        database=payload.database,
        threshold_mb=
            payload.threshold_mb,
        batch_size=
            payload.batch_size,
        write_batch_size=
            payload.write_batch_size,
        shuffle_partitions=
            payload.shuffle_partitions,
        validate_only=
            payload.validate_only,
        route_only=
            payload.route_only,
        raw_only=
            payload.raw_only,
        debug=False,
    )

    try:
        result = run_pipeline(args)

        return {
            "status": "success",
            "pipeline": result,
        }

    except Exception as exc:
        raise _http_error(exc)


@app.post("/indexes")
def indexes(
    database: str = Query(
        default=MONGO_DATABASE
    ),
) -> dict[str, Any]:

    try:
        result = create_phase2_indexes(
            database=database
        )

        return {
            "status": "success",
            "result": result,
            "indexes":
                list_indexes(
                    database=database
                ),
        }

    except Exception as exc:
        raise _http_error(exc)


@app.get("/queries")
def queries() -> dict[str, Any]:

    names = get_query_names()
    descriptions = (
        get_query_descriptions()
    )

    return {
        "count": len(names),
        "queries": [
            {
                "name": name,
                "description":
                    descriptions[name],
            }
            for name in names
        ],
    }


@app.get("/queries/{name}")
def query_by_name(
    name: str,
    request: Request,
    database: str = Query(
        default=MONGO_DATABASE
    ),
) -> dict[str, Any]:

    params = dict(
        request.query_params
    )

    # database خاص بالـAPI وليس بالاستعلام.
    params.pop(
        "database",
        None,
    )

    try:
        return run_query(
            name=name,
            params=params,
            database=database,
        )

    except Exception as exc:
        raise _http_error(exc)


@app.get("/aggregations")
def aggregations() -> dict[str, Any]:

    names = get_aggregation_names()

    descriptions = (
        get_aggregation_descriptions()
    )

    return {
        "count": len(names),
        "aggregations": [
            {
                "name": name,
                "description":
                    descriptions[name],
            }
            for name in names
        ],
    }


@app.get("/aggregations/{name}")
def aggregation_by_name(
    name: str,
    limit: int = Query(
        default=20,
        ge=1,
        le=200,
    ),
    database: str = Query(
        default=MONGO_DATABASE
    ),
) -> dict[str, Any]:

    try:
        return run_aggregation(
            name=name,
            limit=limit,
            database=database,
        )

    except Exception as exc:
        raise _http_error(exc)


@app.post("/refresh-mv")
def refresh_materialized_views(
    payload:
        RefreshMaterializedViewsRequest,
) -> dict[str, Any]:

    client = MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=
            MONGO_CONNECT_TIMEOUT_MS,
    )

    try:
        client.admin.command("ping")

        db = client[
            payload.database
        ]

        current_counts = {
            DAILY_SALES_VIEW:
                db[
                    DAILY_SALES_VIEW
                ].estimated_document_count(),

            TOP_PRODUCTS_VIEW:
                db[
                    TOP_PRODUCTS_VIEW
                ].estimated_document_count(),
        }

    except Exception as exc:
        raise _http_error(exc)

    finally:
        client.close()

    # Initial Build أو Recovery فقط.
    # بعد وجود الـViews يتم تحديثها Incrementally
    # بواسطة incremental_loader.py.
    needs_initial_build = (
        payload.force_rebuild
        or
        any(
            count == 0
            for count
            in current_counts.values()
        )
    )

    if needs_initial_build:

        try:
            result = (
                rebuild_materialized_views(
                    database=
                        payload.database,

                    source_collection=
                        payload.source_collection,
                )
            )

            return {
                "status": "success",
                "refresh_mode":
                    "initial_or_forced_rebuild",
                "result": result,
            }

        except Exception as exc:
            raise _http_error(exc)

    return {
        "status": "success",
        "refresh_mode":
            "incremental_current",

        "message": (
            "Materialized Views already exist. "
            "They are maintained incrementally "
            "by src.incremental_loader. "
            "Set force_rebuild=true only for "
            "initial build or recovery."
        ),

        "views":
            current_counts,

        "available_views":
            get_materialized_view_names(),
    }


@app.get("/jobs")
def jobs() -> dict[str, Any]:

    definitions = get_jobs()

    return {
        "count":
            len(definitions),

        "jobs":
            definitions,
    }


@app.post("/jobs/{name}/run")
def run_named_job(
    name: str,
    database: str = Query(
        default=MONGO_DATABASE
    ),
) -> dict[str, Any]:

    try:
        result = run_job(
            job_name=name,
            database=database,
        )

        return {
            "status": "success",
            "result": result,
        }

    except Exception as exc:
        raise _http_error(exc)
