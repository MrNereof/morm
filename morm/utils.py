import typing


def recursive_diff(
    prev: dict[str, typing.Any], current: dict[str, typing.Any]
) -> dict[str, typing.Any]:
    diff = {}

    for k, v in current.items():
        if v != prev.get(k):
            if isinstance(v, dict) and k in prev:
                embedding = recursive_diff(prev[k], v)
                for ke, ve in embedding.items():
                    new_key = f"{k}.{ke}"
                    diff[new_key] = ve
            else:
                diff[k] = v

    return diff
