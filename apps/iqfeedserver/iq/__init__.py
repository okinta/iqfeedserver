# flake8: noqa: F401

import enum


class IntervalType(enum.Enum):
    """Represents a type of interval.
    """
    SECONDS = "s"
    VOLUME = "v"
    TICKS = "t"


from iqfeedserver.iq.bars import Bar
from iqfeedserver.iq.bars import DailyBar
from iqfeedserver.iq.conn import Conn
from iqfeedserver.iq.conn import ConnectionState
from iqfeedserver.iq.conn import HandlerResult
from iqfeedserver.iq.conn import IQFeedError
from iqfeedserver.iq.conn import NoDataError
from iqfeedserver.iq.conn import TerminationStyle
from iqfeedserver.iq.bar_conn import BarConn
from iqfeedserver.iq.history_conn import HistoryConn
