import json
from datetime import datetime, timezone
from pathlib import Path

from config.settings import RESULTS_JSON


def _load_results(path: Path) -> dict:
    if not path.exists():
        return {}

    text = path.read_text(encoding="utf-8-sig")
    if not text.strip():
        return {}

    data = json.loads(text)
    if not isinstance(data, dict):
        raise ValueError("reports/results.json must contain a JSON object.")
    return data


def save_runtime_run(report: dict, path=RESULTS_JSON, keep_last=20) -> Path:
    """Append one runtime result while preserving the existing report sections."""
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)

    data = _load_results(destination)
    runtime_runs = data.get("runtime_runs", [])
    if not isinstance(runtime_runs, list):
        runtime_runs = []

    stamped_report = dict(report)
    stamped_report.setdefault(
        "recorded_at",
        datetime.now(timezone.utc).isoformat(),
    )

    runtime_runs.append(stamped_report)
    data["runtime_runs"] = runtime_runs[-keep_last:]

    temp_path = destination.with_suffix(destination.suffix + ".tmp")
    temp_path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    temp_path.replace(destination)
    return destination
