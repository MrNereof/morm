import typing


def recursive_diff(
    prev: dict[str, typing.Any], current: dict[str, typing.Any]
) -> dict[str, typing.Any]:
    diff = {}

    for k, v in current.items():
        if v != prev.get(k):
            if isinstance(v, dict) and k in prev:
                diff[k] = recursive_diff(prev[k], v)
            else:
                diff[k] = v

    return diff
