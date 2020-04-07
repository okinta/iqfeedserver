from typing import Final
import asyncio
import logging
import sys

import uvloop

import iqfeedserver.handler


logger = logging.getLogger(__name__)


HOST: Final = "0.0.0.0"
PORT: Final = 9999


def main() -> None:
    """Runs the server.
    """
    logging.basicConfig(
        stream=sys.stdout, level=logging.INFO, format="%(message)s"
    )

    uvloop.install()

    try:
        asyncio.run(run_server())

    except KeyboardInterrupt:
        logger.info("Goodbye")


async def run_server() -> None:
    """Runs the server async.
    """
    handler = iqfeedserver.handler.IQFeedServerHandler()
    server = await asyncio.start_server(handler.handle, HOST, PORT)

    logger.info("Running IQFeed Server")

    try:
        while True:
            await asyncio.sleep(9999999999999)

    except KeyboardInterrupt:
        pass

    finally:
        logger.info("Shutting down IQFeed Server")
        server.close()
        await server.wait_closed()


if __name__ == "__main__":
    main()
