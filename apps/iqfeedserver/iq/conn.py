from typing import Callable
from typing import Dict
from typing import Final
from typing import List
from typing import NamedTuple
from typing import Optional
import asyncio
import enum
import logging
import random
import warnings

from iqfeedserver.iq.field_readers import get_field


logger = logging.getLogger(__name__)


CURRENT_PROTOCOL: Final = "CURRENT PROTOCOL"
END_MSG: Final = "!ENDMSG!"
ERROR: Final = "E"
NO_DATA: Final = "!NO_DATA!"
PROTOCOL: Final = "6.1"
SERVER_CONNECTED: Final = "SERVER CONNECTED"
SYSTEM_MESSAGE: Final = "S"
TIMEOUT: Final = 4


class NoDataError(Exception):
    """Raised when there is no data available for a request.
    """
    pass


class IQFeedError(Exception):
    """Raised when there are too many simultaneous requests to IQFeed.
    """
    pass


class CommandHandler(NamedTuple):
    """Maintains information about an IQFeed command.
    """
    ticker: str
    future: asyncio.Future
    handler: Callable[[List[str]], object]
    result: List[object]


class HandlerResult(enum.Enum):
    """Indicates that status of a message handler.
    """
    HANDLED = enum.auto()
    UNKNOWN_MESSAGE = enum.auto()


class TerminationStyle(enum.Enum):
    """Indicates when the Conn instance should terminate.
    """
    RUN_FOREVER = enum.auto()
    TERMINATE_WHEN_NO_DATA_RECEIVED = enum.auto()


class ConnectionState(enum.Enum):
    """Indicates what state the Conn instance is in.
    """
    NOT_RUNNING = enum.auto()
    READING_MESSAGES = enum.auto()


class Conn:
    """Base async class to pull data from IQFeed.
    """

    def __init__(self) -> None:
        """Instantiates the instance.
        """
        self._commands = {}  # type: Dict[str, CommandHandler]
        self._reader = None  # type: Optional[asyncio.StreamReader]
        self._req_num = 0
        self._runner = None  # type: Optional[asyncio.Task]
        self._state = ConnectionState.NOT_RUNNING
        self._termination_style = TerminationStyle.RUN_FOREVER
        self._writer = None  # type: Optional[asyncio.StreamWriter]

    @property
    def state(self) -> ConnectionState:
        """Gets whether or not this connection is connected.
        """
        return self._state

    async def connect(
        self, host: str, port: int,
        termination_style: TerminationStyle = TerminationStyle.RUN_FOREVER
    ) -> None:
        """Connects to IQFeed.

        Args:
            host: The host to connect to.
            port: The port to connect to.
            termination_style: Indicates how the connection should terminate.
        """
        if self._reader:
            raise RuntimeError("Already connected to IQFeed")

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=DeprecationWarning)

            try:
                self._reader, self._writer = await asyncio.open_connection(
                    host, port
                )

            except Exception:
                logger.error(
                    "Unable to connect to IQFeed at %s:%s", host, port
                )
                raise

        await self.send_cmd("S,SET PROTOCOL,%s" % PROTOCOL)

        self._termination_style = termination_style
        self._state = ConnectionState.READING_MESSAGES
        self._runner = asyncio.get_running_loop().create_task(self._run())

    async def disconnect(self) -> None:
        """Disconnect from the socket to IQFeed. Call this to ensure sockets
        are closed and we exit cleanly.

        Raises:
            RuntimeError: If the writer is not connected.
        """
        if not self._writer or not self._runner:
            raise RuntimeError("Not connected")

        self._runner.cancel()
        await self.send_cmd("S,DISCONNECT")

        self._writer.close()
        await self._writer.wait_closed()
        self._reader = None
        self._runner = None
        self._writer = None

    async def send_cmd(self, cmd: str) -> None:
        """Sends a message to IQFeed.

        Args:
            cmd: The message to send.

        Raises:
            RuntimeError: If IQFeed is not connected.
        """
        if not self._writer:
            raise RuntimeError("Not connected")

        cmd += "\r\n"
        self._writer.write(cmd.encode(encoding="latin-1"))
        await self._writer.drain()

    def get_next_req_id(self, prefix: str, ticker: str) -> str:
        """Gets the next request ID to use for IQFeed.

        Args:
            prefix: The prefix to use for the request.
            ticker: The ticker of the request.

        Returns:
            The next unique request ID.
        """
        req_id = "%s%s%.10d" % (
            prefix, ticker, self._req_num + random.randint(1, 100)
        )
        self._req_num += 1
        return req_id

    async def wait_for_command(
        self, command: str, ticker: str, req_id: str,
        handler: Callable[[List[str]], object], timeout: int
    ) -> object:
        """Waits for the given command to complete and returns the result.

        Args:
            command: The command to send to IQFeed.
            ticker: The ticker this command is related to.
            req_id: The ID to identify this command.
            handler: The function to call when new data related to the command
            is received from IQFeed.
            timeout: The maximum number of seconds to wait before timing out.

        Raises:
            asyncio.TimeoutError: If timeout is reached before retrieving the
            bars from IQFeed.
        """
        result = asyncio.get_running_loop().create_future()
        self._commands[req_id] = CommandHandler(ticker, result, handler, [])

        try:
            await self.send_cmd(command)
            await asyncio.wait_for(result, timeout=timeout)
            return result.result()

        finally:
            del self._commands[req_id]

    async def handle_fields(self, fields: List[str]) -> HandlerResult:
        """Called when a message is received from IQFeed. Subclasses can
        override this method to process messages.

        Args:
            fields: The fields received by IQFeed.

        Returns:
            The status of the handler. Return HandlerResult.HANDLED to signal
            that the message has been handled successfully and does not need
            any further processing.
        """
        return HandlerResult.UNKNOWN_MESSAGE

    async def _run(self) -> None:
        """Continuously listens for messages from IQFeed and sends events when
        received.
        """
        if not self._reader:
            raise RuntimeError("Reader is not connected")

        try:
            while True:
                try:
                    line = await asyncio.wait_for(
                        self._reader.readline(), timeout=TIMEOUT
                    )

                except asyncio.TimeoutError:
                    if (
                        self._termination_style
                        == TerminationStyle.TERMINATE_WHEN_NO_DATA_RECEIVED
                    ):
                        return

                message = line.decode("latin-1").strip()
                if message:
                    await self._handle_message(message)

        except asyncio.CancelledError:
            pass

        finally:
            self._state = ConnectionState.NOT_RUNNING

    async def _handle_message(self, message: str) -> None:
        """Handles the message sent from IQFeed.

        Args:
            message: The message to process.
        """
        fields = message.strip(",").split(",")

        if (
            fields[0] == SYSTEM_MESSAGE and
            get_field(fields, 1) == CURRENT_PROTOCOL
        ):
            self._check_protocol(fields)

        elif (
            fields[0] == SYSTEM_MESSAGE and
            get_field(fields, 1) == SERVER_CONNECTED
        ):
            pass

        elif await self.handle_fields(fields) == HandlerResult.HANDLED:
            pass

        elif fields[0] in self._commands:
            try:
                self._process_future_result(fields, self._commands[fields[0]])

            except ValueError:
                logger.exception(
                    "Could not interpret fields: %s", ",".join(fields)
                )

        else:
            logger.debug("Unknown message: %s", message)

    @staticmethod
    def _process_future_result(
        fields: List[str], command_handler: CommandHandler
    ) -> None:
        """Processes a message from IQFeed by calling the registered callback.

        Args:
            fields: The fields received by IQFeed.
            command_handler: The CommandHandler related to this request.
        """
        if command_handler.future.done():
            return

        # If this is the end of the bars, send back the result
        if get_field(fields, 1) == END_MSG:
            command_handler.future.set_result(command_handler.result)

        # If there is no data, raise an exception
        elif get_field(fields, 2) == NO_DATA:
            command_handler.future.set_exception(
                NoDataError("No data available")
            )

        # If there was an error, raise an exception
        elif get_field(fields, 1) == ERROR:
            command_handler.future.set_exception(
                IQFeedError(get_field(fields, 2))
            )

        # Otherwise, send the data to the callback to be handled
        else:
            command_handler.result.append(command_handler.handler(
                # Replace req_id with ticker
                [command_handler.ticker] + fields[1:]
            ))

    @staticmethod
    def _check_protocol(fields: List[str]) -> None:
        """Checks that the protocol was set correctly.

        Args:
            fields: The fields sent from IQFeed.
        """
        if get_field(fields, 2) != PROTOCOL:
            logger.error("Bad protocol received: %s", ",".join(fields))
