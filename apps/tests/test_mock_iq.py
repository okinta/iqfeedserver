from typing import Final
import asyncio
import datetime
import os

import pytest

from iqfeedserver import iq


MOCK_IQFEED_PORT = 9999


@pytest.mark.asyncio
async def test_capture() -> None:
    day: Final = datetime.datetime(2019, 11, 29, 9, 30)
    price: Final = 267.9
    ticker: Final = "AAPL"
    result = asyncio.get_running_loop().create_future()

    conn = iq.BarConn()

    async def callback(bar: iq.Bar) -> None:
        if (
            bar.ticker == ticker and
            bar.time == bar.time.replace(hour=11, minute=37)
        ):
            result.set_result(bar.close_p)

    conn.register_live_bar_callback(callback)

    await conn.connect(os.environ["IQFEED_HOST"], MOCK_IQFEED_PORT)
    await conn.watch(ticker, day)

    try:
        await asyncio.wait_for(result, timeout=5)
        assert result.result() == price

    finally:
        await conn.unwatch(ticker)
        await conn.disconnect()
