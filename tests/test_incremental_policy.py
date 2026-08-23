from src.incremental_loader import decide_version_action, parse_version


def test_version_policy_insert_update_unchanged_and_stale():
    assert decide_version_action(None, None, 1, "A") == "inserted"
    assert decide_version_action(1, "A", 2, "B") == "updated"
    assert decide_version_action(2, "B", 2, "B") == "unchanged"
    assert decide_version_action(2, "B", 1, "A") == "stale"


def test_same_version_different_payload_is_conflict():
    assert decide_version_action(3, "OLD", 3, "NEW") == "version_conflict"


def test_version_must_be_positive_integer():
    assert parse_version("2") == 2
    for value in ("0", "-1", "abc", None):
        try:
            parse_version(value)
        except ValueError:
            pass
        else:
            raise AssertionError(value)
