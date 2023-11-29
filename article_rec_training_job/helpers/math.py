import math


def convert_bytes_to_human_readable(num_bytes: int, max_decimal_points: int = 3) -> str:
    """
    Converts bytes to human readable format.
    """
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"]
    unit_index = int(math.floor(math.log(num_bytes, 1024)))
    size = num_bytes / math.pow(1024, unit_index)
    return f"{round(size, max_decimal_points)} {units[unit_index]}"
