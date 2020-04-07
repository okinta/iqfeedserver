from typing import NamedTuple
import datetime


class Bar(NamedTuple):
    """Represents a bar.
    """
    date: datetime.date
    time: datetime.time
    open_p: float
    high_p: float
    low_p: float
    close_p: float
    tot_vlm: int
    prd_vlm: int
    num_trds: int
    ticker: str


class DailyBar(NamedTuple):
    """Represents a daily bar.
    """
    date: datetime.date
    high_p: float
    low_p: float
    open_p: float
    close_p: float
    prd_vlm: int
    open_int: int
    ticker: str
