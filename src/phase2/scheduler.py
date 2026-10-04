from __future__ import annotations

from apscheduler.schedulers.blocking import (
    BlockingScheduler,
)

from src.phase2.jobs import (
    JOB_DEFINITIONS,
    run_job,
)


def _run_safely(
    job_name: str,
) -> None:

    try:
        result = run_job(
            job_name
        )

        print(
            f"[JOB SUCCESS] "
            f"{job_name} | "
            f"rows={result['row_count']}"
        )

    except Exception as exc:

        print(
            f"[JOB FAILED] "
            f"{job_name} | "
            f"{exc}"
        )


def build_scheduler() -> BlockingScheduler:

    scheduler = BlockingScheduler(
        timezone="UTC"
    )

    for job_name, definition in (
        JOB_DEFINITIONS.items()
    ):

        schedule = definition[
            "schedule"
        ]

        scheduler.add_job(
            _run_safely,
            trigger="cron",
            id=job_name,
            name=job_name,
            replace_existing=True,
            kwargs={
                "job_name":
                    job_name
            },
            hour=schedule[
                "hour"
            ],
            minute=schedule[
                "minute"
            ],
        )

    return scheduler


def main() -> None:

    scheduler = build_scheduler()

    print(
        "Phase 2 scheduler started."
    )

    print(
        "Timezone: UTC"
    )

    for job in scheduler.get_jobs():
        print(
            f"- {job.id} | "
            f"{job.trigger}"
        )

    try:
        scheduler.start()

    except (
        KeyboardInterrupt,
        SystemExit,
    ):
        print(
            "Scheduler stopped."
        )


if __name__ == "__main__":
    main()
