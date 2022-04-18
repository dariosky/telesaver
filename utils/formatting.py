import datetime
import json


def comprint(o)->str:
    """ Compact print an object """
    if isinstance(o, datetime.datetime):
        return o.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(o, datetime.date):
        return o.strftime("%Y-%m-%d")
    if isinstance(o, dict):
        return "{"+", ".join(
            [f"{k}:{comprint(v)}" for k, v in o.items()]
        )+"}"
    if isinstance(o, str):
        return o
    return json.dumps(o)
