from src.file_router import choose_engine
from src.main import build_parser


def test_main_has_no_manual_engine_override():
    parser = build_parser()
    option_strings = {
        option
        for action in parser._actions
        for option in action.option_strings
    }
    assert "--engine" not in option_strings


def test_router_selects_python_batch_below_threshold(tmp_path):
    path = tmp_path / "small.csv"
    path.write_bytes(b"x" * 1024)

    decision = choose_engine(path, threshold_mb=1)

    assert decision.engine == "python_batch"
    assert "less than or equal" in decision.reason


def test_router_selects_pyspark_above_threshold(tmp_path):
    path = tmp_path / "large.csv"
    # Sparse logical file > 2 MB; avoids allocating a real large test file.
    with path.open("wb") as handle:
        handle.seek((2 * 1024 * 1024) + 1)
        handle.write(b"0")

    decision = choose_engine(path, threshold_mb=1)

    assert decision.engine == "pyspark"
    assert "exceeds" in decision.reason
