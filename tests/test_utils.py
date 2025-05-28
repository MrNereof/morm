from morm.utils import recursive_diff


def test_recursive_diff():
    prev = {
        "test": 1,
        "name": "Alex",
        "data": {
            "changed": "nope",
            "custom": "field",
        },
    }

    cur = {
        "test": 1,
        "name": "John",
        "data": {
            "custom": "field",
            "changed": "yep",
            "inner": {
                "new": "flag",
            },
        },
        "newest": "checkme",
    }

    assert recursive_diff(prev, cur) == {
        "data.changed": "yep",
        "data.inner": {"new": "flag"},
        "name": "John",
        "newest": "checkme",
    }
