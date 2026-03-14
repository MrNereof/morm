import typing


def recursive_diff(
    prev: dict[str, typing.Any] | None, current: dict[str, typing.Any]
) -> dict[str, typing.Any]:
    if not isinstance(prev, dict):
        prev = {}
    diff = {}

    for k, v in current.items():
        prev_value = prev.get(k)
        if v != prev_value:
            if isinstance(v, dict):
                prev_nested = prev_value if isinstance(prev_value, dict) else {}
                embedding = recursive_diff(prev_nested, v)
                for ke, ve in embedding.items():
                    new_key = f"{k}.{ke}"
                    diff[new_key] = ve
            else:
                diff[k] = v

    return diff
