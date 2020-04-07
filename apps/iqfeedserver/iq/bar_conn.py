from typing import Awaitable
from typing import Callable
from typing import Dict
from typing import Final
from typing import List
import datetime
import logging

from iqfeedserver.iq import Bar
from iqfeedserver.iq import Conn
from iqfeedserver.iq import field_readers
from iqfeedserver.iq import HandlerResult
from iqfeedserver.iq import IntervalType
from iqfeedserver.iq.field_readers import get_field


logger = logging.getLogger(__name__)


HISTORY_BAR: Final = "BH"
LIVE_BAR: Final = "BC"
NO_DATA: Final = "n"


class BarConn(Conn):
    """Let's you get live data as interval bar data.
    """

    _history_bar_handlers = []  # type: List[Callable[[Bar], Awaitable[None]]]
    _last_bar = {}  # type: Dict[str, Bar]
    _live_bar_handlers = []  # type: List[Callable[[Bar], Awaitable[None]]]

    def register_history_bar_callback(
        self, callback: Callable[[Bar], Awaitable[None]]
    ) -> None:
        """Registers a callback for processing history bars.

        Args:
            callback: The callback to call when a new history bar is received
            by IQFeed.
        """
        self._history_bar_handlers.append(callback)

    def register_live_bar_callback(
        self, callback: Callable[[Bar], Awaitable[None]]
    ) -> None:
        """Registers a callback for processing live bars.

        Args:
            callback: The callback to call when a new live bar is received
            by IQFeed.
        """
        self._live_bar_handlers.append(callback)

    async def watch(
        self, ticker: str, start: datetime.datetime, interval_len: int = 60,
        interval_type: IntervalType = IntervalType.SECONDS
    ) -> None:
        """Signals to IQFeed that we want to watch a ticker.

        Args:
            ticker: The ticker to watch.
            start: The start time to get backfill data from.
            interval_len: The amount of time each bar should represent.
            interval_type: The type of time associated with the given
            interval_len.
        """
        await self.send_cmd(
            f"BW,{ticker},{interval_len}," +
            field_readers.convert_datetime_to_iqfeed_format(start) +
            f",,,,,,{interval_type.value},,"
        )

    async def unwatch(self, ticker: str) -> None:
        """Unwatch a specific ticker.

        Args:
            ticker: The ticker to unwatch.
        """
        await self.send_cmd("BR,%s" % ticker)

    async def disconnect(self) -> None:
        """Disconnect from the socket to IQFeed. Call this to ensure sockets
        are closed and we exit cleanly.

        Raises:
            RuntimeError: If the writer is not connected.
        """
        await super().disconnect()
        self._history_bar_handlers = []
        self._live_bar_handlers = []

    async def handle_fields(self, fields: List[str]) -> HandlerResult:
        """Called when a message is received from IQFeed. Determines if we've
        received a bar and processes it accordingly.

        Args:
            fields: The fields received by IQFeed.

        Returns:
            HandlerResult.HANDLED if the message was handled successfully.
            HandlerResult.UNKNOWN_MESSAGE otherwise.
        """
        if get_field(fields, 0) == NO_DATA:
            logger.error("No data for %s", get_field(fields, 1))
            return HandlerResult.HANDLED

        bar_type = get_field(fields, 1)
        if bar_type in (HISTORY_BAR, LIVE_BAR):
            bar = None

            try:
                bar = self._get_bar(fields)

            except Exception:
                logger.exception(
                    "Error processing bar with fields: %s", ",".join(fields)
                )

            else:
                if bar_type == HISTORY_BAR:
                    await self._handle_history_bar(bar)

                elif bar_type == LIVE_BAR:
                    await self._handle_live_bar(bar)

                return HandlerResult.HANDLED

        return HandlerResult.UNKNOWN_MESSAGE

    def _get_bar(self, fields: List[str]) -> Bar:
        """Extracts a Bar from the IQFeed fields.

        Args:
            fields: The fields received by IQFeed.

        Returns:
            The Bar instance extracted from the given fields.

        Raises:
            ValueError: If bad data was returned by IQFeed.
        """
        (
            conn_id, bar_type, ticker, timestamp, open_p, high_p, low_p,
            close_p, tot_vlm, prd_vlm, num_trds
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

    async def _handle_history_bar(self, bar: Bar) -> None:
        """Processes a history bar.

        Args:
            bar: The bar to process.
        """
        for handler in self._history_bar_handlers:
            try:
                await handler(bar)

            except Exception:
                logger.exception("Error handling bar for %s", bar.ticker)

    async def _handle_live_bar(self, bar: Bar) -> None:
        """Processes a live bar.

        Args:
            bar: The bar to process.
        """
        if bar != self._last_bar.get(bar.ticker):
            self._last_bar[bar.ticker] = bar

            for handler in self._live_bar_handlers:
                try:
                    await handler(bar)

                except Exception:
                    logger.exception("Error handling bar for %s", bar.ticker)
