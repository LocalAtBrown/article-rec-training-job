from datetime import datetime


def ms_timestamp(dt: datetime) -> float:
    epoch = datetime.utcfromtimestamp(0)
    return (dt - epoch).total_seconds() * 1000.0
