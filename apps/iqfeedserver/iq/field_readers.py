from typing import List
from typing import Tuple
import datetime


def convert_datetime_to_iqfeed_format(date: datetime.datetime) -> str:
    """Converts a python datetime to a format readable by IQFeed.

    Args:
        date: The datetime value to convert.

    Returns:
        The converted datetime in a format readable by IQFeed.
    """
    return "%.4d%.2d%.2d %.2d%.2d%.2d" % (
        date.year, date.month, date.day, date.hour, date.minute, date.second
    )


def convert_datetime_to_iqfeed_date_format(date: datetime.datetime) -> str:
    """Converts a python datetime to a date format readable by IQFeed.

    Args:
        date: The datetime value to convert.

    Returns:
        The converted datetime in a time format readable by IQFeed.
    """
    return date.strftime("%Y%m%d")


def convert_iqfeed_timestamp_to_date_and_time(
    timestamp: str
) -> Tuple[datetime.date, datetime.time]:
    """Converts a timestamp sent by IQFeed to date and time python values.

    Args:
        timestamp: The value sent from IQFeed to convert.

    Returns:
        A tuple containing the converted date and time values.

    Raises:
        ValueError: If the timestamp is an invalid format.
    """
    value = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
    return value.date(), value.time()


def convert_iqfeed_date_to_date(timestamp: str) -> datetime.date:
    """Converts a date sent by IQFeed to a date python value.

    Args:
        timestamp: The value sent from IQFeed to convert.

    Returns:
        The converted python date.

    Raises:
        ValueError: If the timestamp is an invalid format.
    """
    return datetime.datetime.strptime(timestamp, "%Y-%m-%d").date()


def get_field(fields: List[str], index: int) -> str:
    """Gets a field from the list of fields.

    Args:
        fields: The fields to retrieve the specific field from.
        index: The index of the field to retrieve.

    Returns:
        The field at the specified index. Returns an empty string if the field
        doesn't exist.
    """
    try:
        return fields[index]

    except IndexError:
        return ""
