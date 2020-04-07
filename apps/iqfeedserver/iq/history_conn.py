from typing import Final
from typing import List
import datetime
import logging

from iqfeedserver.iq import Bar
from iqfeedserver.iq import Conn
from iqfeedserver.iq import DailyBar
from iqfeedserver.iq import field_readers
from iqfeedserver.iq import IntervalType
from iqfeedserver.iq import NoDataError


logger = logging.getLogger(__name__)


DAILY_BAR_PREFIX: Final = "D_"
HISTORY_BAR_PREFIX: Final = "H_"


class HistoryConn(Conn):
    """HistoryConn is used to get historical data from IQFeed's lookup socket.
    """

    async def request_bars_in_period(
        self, ticker: str, start: datetime.datetime, end: datetime.datetime,
        interval_len: int, interval_type: IntervalType = IntervalType.SECONDS,
        timeout: int = 30
    ) -> List[Bar]:
        """Retrieves the bars for the given ticker for a specified period.

        Args:
            ticker: The ticker to retrieve the bars for.
            start: The starting period to retrieve the bars for.
            end: The ending period to retrieve the bars for.
            interval_len: The amount of time each bar should represent.
            interval_type: The type of time associated with the given
            interval_len.
            timeout: The maximum amount of seconds to wait retrieving data from
            IQFeed.

        Returns:
            The bars for the given ticker and the given time period. Returns an
            empty list if no bars are retrieved.

        Raises:
            asyncio.TimeoutError: If timeout is reached before retrieving the
            bars from IQFeed.
            NoDataError: If there is no data for the requested ticker and the
            given times.
            IQFeedError: If there is an error sent back from IQFeed.
        """
        req_id = self.get_next_req_id(HISTORY_BAR_PREFIX, ticker)

        command = (
            "HIT,%s,%d,%s,%s,,,,1,%s,,%s," % (
                ticker,
                interval_len,
                field_readers.convert_datetime_to_iqfeed_format(start),
                field_readers.convert_datetime_to_iqfeed_format(end),
                req_id,
                interval_type.value
            )
        )

        bars = await self.wait_for_command(
            command, ticker, req_id, self._handle_historical_bar, timeout
        )

        if not isinstance(bars, list):
            raise ValueError("Got bad result: %s" % str(bars))

        return bars

    async def request_daily_bar_for_date(
        self, ticker: str, day: datetime.datetime, timeout: int = 30
    ) -> DailyBar:
        """Gets the daily bar for the tiven ticker on the given date.

        Args:
            ticker: The ticker to retrieve the daily bar for.
            day: The day to retrieve the daily bar for.
            timeout: The maximum amount of seconds to wait retrieving data from
            IQFeed.

        Returns:
            The daily bar for the given ticker.

        Raises:
            asyncio.TimeoutError: If timeout is reached before retrieving the
            bars from IQFeed.
            NoDataError: If there is no data for the requested ticker and the
            given date.
            IQFeedError: If there is an error sent back from IQFeed.
            ValueError: If bad data was returned by IQFeed.
        """
        req_id = self.get_next_req_id(DAILY_BAR_PREFIX, ticker)

        command = (
            "HDT,%s,%s,%s,,1,%s,," % (
                ticker,
                field_readers.convert_datetime_to_iqfeed_date_format(day),
                field_readers.convert_datetime_to_iqfeed_date_format(day),
                req_id
            )
        )

        bars = await self.wait_for_command(
            command, ticker, req_id, self._handle_daily_bar, timeout
        )

        try:
            assert isinstance(bars, list)
            assert isinstance(bars[-1], DailyBar)
            return bars[-1]

        except (AssertionError, IndexError):
            raise NoDataError("Didn't get valid data for %s" % ticker)

    def _handle_historical_bar(self, fields: List[str]) -> object:
        """Handles a historical bar message.

        Args:
            fields: The fields sent from IQFeed.

        Returns:
            The Bar extracted from the given fields.

        Raises:
            ValueError: If invalid fields were provided.
        """
        (
            ticker, timestamp, high_p, low_p, open_p, close_p, tot_vlm,
            prd_vlm, num_trds
        ) = fields

        date, time = field_readers.convert_iqfeed_timestamp_to_date_and_time(
            timestamp
        )

        return Bar(
            date=date,
            time=time,
            open_p=float(open_p),
            high_p=float(high_p),
            low_p=float(low_p),
            close_p=float(close_p),
            tot_vlm=int(tot_vlm),
            prd_vlm=int(prd_vlm),
            num_trds=int(num_trds),
            ticker=ticker
        )

    def _handle_daily_bar(self, fields: List[str]) -> object:
        """Handles a daily bar message.

        Args:
            fields: The fields sent from IQFeed.

        Returns:
            The DailyBar extracted from the given fields.

        Raises:
            ValueError: If invalid fields were provided.
        """
        (
            ticker, date, high_p, low_p, open_p, close_p, prd_vlm, open_int
        ) = fields

        return DailyBar(
            date=field_readers.convert_iqfeed_date_to_date(date),
            high_p=float(high_p),
            low_p=float(low_p),
            open_p=float(open_p),
            close_p=float(close_p),
            prd_vlm=int(prd_vlm),
            open_int=int(open_int),
            ticker=ticker
        )
