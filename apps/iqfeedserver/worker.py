from typing import Final
from typing import List
import datetime
import logging
import os

from iqfeedserver import iq


logger = logging.getLogger(__name__)


DEFAULT_IQFEED_PORT_LOOKUP: Final = 9100
INTERVAL: Final = 60
MARKET_CLOSE_HOUR: Final = 16
MARKET_CLOSE_MINUTE: Final = 0
MARKET_OPEN_HOUR: Final = 9
MARKET_OPEN_MINUTE: Final = 30


async def process_job(ticker: str, date: str) -> List[str]:
    """Pulls information from IQFeed and returns it back to the client.

    Args:
        ticker: The ticker to pull information for.
        date: The date to pull information for.

    Returns:
        The messages to send back to the client.
    """
    day = datetime.datetime(int(date[:4]), int(date[4:6]), int(date[6:]))

    market_open = datetime.datetime(
        day.year, day.month, day.day, MARKET_OPEN_HOUR, MARKET_OPEN_MINUTE
    )
    market_close = market_open.replace(
        hour=MARKET_CLOSE_HOUR, minute=MARKET_CLOSE_MINUTE
    )

    logger.info("Getting bars for %s", ticker)

    conn = iq.HistoryConn()
    await conn.connect(
        os.environ["IQFEED_HOST"],
        int(os.environ.get("IQFEED_PORT_LOOKUP", DEFAULT_IQFEED_PORT_LOOKUP))
    )

    try:
        bars = await conn.request_bars_in_period(
            ticker, market_open, market_close, INTERVAL
        )
        logger.info("Got bars for %s", ticker)

    except Exception:
        logger.exception("Error retrieving bars for %s", ticker)

        return ["n," + ticker]

    else:
        return [
            (
                "%(request_id)s,BC,%(ticker)s,%(date_time)s,%(open)s,%(high)s,"
                "%(low)s,%(last)s,%(cummulative_volume)s,%(interval_volume)s,"
                "%(number_of_trades)s"
            ) % {
                "request_id": "B-%s-0060-s" % ticker,
                "ticker": ticker,
                "date_time": datetime.datetime.combine(bar.date, bar.time),
                "open": bar.open_p,
                "high": bar.high_p,
                "low": bar.low_p,
                "last": bar.close_p,
                "cummulative_volume": bar.tot_vlm,
                "interval_volume": bar.prd_vlm,
                "number_of_trades": bar.num_trds
            } for bar in bars
        ]

    finally:
        await conn.disconnect()


def format_datetime(date: datetime.datetime) -> str:
    """Formats the given datetime value to IQFeed format.

    Args:
        date: The value to format.

    Returns:
        The date in IQFeed format.
    """
    return date.strftime("%Y-%m-%d %H:%M:%S")
