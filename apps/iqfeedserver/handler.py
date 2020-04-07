from typing import List
import asyncio
import logging

from iqfeedserver import worker


logger = logging.getLogger(__name__)


class IQFeedServerHandler:
    """A mock IQFeed server that handles and sends requests.
    """

    async def handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        """Called when a connection to the client is established. Processes all the
        client's requests until the connection is terminnated.

        Args:
            reader: The reader to receive messages from the client.
            writer: The writer to send messages to the client.
        """
        try:
            while True:
                if not await self.process_messages(reader, writer):
                    return

        except Exception:
            logger.exception("Error occurred")

        finally:
            writer.close()

    async def process_messages(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> bool:
        """Processes messages received from the client.

        Args:
            reader: The reader to receive messages from the client.
            writer: The writer to send messages to the client.

        Returns:
            True if the message was processed successfully. False if the client
            has disconnected.
        """
        try:
            message = await self._get_message(reader, writer)

        except BrokenPipeError:
            return False

        if not message:
            return True

        logger.info(message)

        # Connected? Ok
        if message == "S,CONNECT":
            return await self._send(writer, ["S,SERVER CONNECTED"])

        # If the client requests a ticker, add it to the jobs queue
        elif message.startswith("BW,"):
            message_split = message.split(",")
            ticker = message_split[1]
            date = message_split[3].split(" ")[0]

            messages = await worker.process_job(ticker, date)
            return await self._send(writer, messages)

        return True

    @classmethod
    async def _get_message(
        cls, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> str:
        """Gets the next message.

        Args:
            reader: The reader to receive messages from the client.
            writer: The writer to send messages to the client.

        Returns:
            The next message received from the client.
        """
        message = ""

        try:
            line = await asyncio.wait_for(reader.readline(), timeout=5)
            message = line.decode("latin-1").strip()

        except asyncio.TimeoutError:
            pass

        # Check that we're still connected if we didn't get a message
        if not message and not await cls._send(writer, ["S,SERVER CONNECTED"]):
            raise BrokenPipeError("Client disconnected")

        return message

    @staticmethod
    async def _send(
        writer: asyncio.StreamWriter, messages: List[str]
    ) -> bool:
        """Sends a message back to the client.

        Args:
            writer: The writer to send messages to the client.
            messages: The list of messages to send to the client.

        Returns:
            True if the messages were sent successfully. False otherwise.

        Raises:
            RuntimeError: If not connected.
        """
        for message in messages:
            logger.debug("Sending: %s", message)

            encoded_message = (message + "\r\n").encode("latin-1")

            try:
                writer.write(encoded_message)
                await writer.drain()

            except Exception:
                logger.info("Client disconnected")
                return False

        return True
