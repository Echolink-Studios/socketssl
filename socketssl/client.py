import asyncio
import logging
from typing import Awaitable, Callable

from .util import HEADER, Payload, Response

logger = logging.getLogger(__name__)


class Client:

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, *, name: str,
                 callback: Callable[[Response], Awaitable[None]] | None = None):
        self.name = name
        self._callback = callback
        self.reader = reader
        self.writer = writer

    @classmethod
    async def connect(cls, *, host: str, port: int, name: str,
                      callback: Callable[[Response], Awaitable[None]] | None = None):
        try:
            reader, writer = await asyncio.open_connection(host, port)
        except ConnectionRefusedError:
            raise ConnectionRefusedError(f"Could not connect to '{host}:{port}' - Is the server running?")

        client = cls(reader, writer, name=name, callback=callback)

        if await client._has_valid_name():
            logger.info(f"Connected to '{host}:{port}'")
            asyncio.create_task(client._receive())
            return client
        else:
            await client.disconnect()
            raise ConnectionRefusedError(f"Name '{name}' cannot be used as it is already taken.")

    async def send(self, destination: str, message: str):
        try:
            if self.writer:
                data = Payload(source=self.name, destination=destination, data=message).model_dump_json().encode()
                length_header = str(len(data)).encode()
                length_header += b' ' * (HEADER - len(length_header))
                self.writer.write(length_header + data)
                await self.writer.drain()
        except ConnectionResetError:
            await self.disconnect()

    async def disconnect(self):
        if self.writer and not self.writer.is_closing():
            logger.info("Disconnecting...")
            self.writer.close()
            await self.writer.wait_closed()

    def is_connected(self) -> bool:
        """Check if the client is still connected."""
        return self.writer is not None and not self.writer.is_closing()

    async def _receive(self) -> None:
        """Background task for receiving messages from the server."""
        try:
            while True:
                length_bytes = await self.reader.readexactly(HEADER)
                length = int(length_bytes.decode().strip())
                data_bytes = await self.reader.readexactly(length)
                payload = Payload.model_validate_json(data_bytes.decode())
                if self._callback:
                    await self._callback(Response(payload.source, payload.destination, payload.data))
        except (asyncio.CancelledError, asyncio.IncompleteReadError):
            logger.info("Server closed connection or client got terminated.")
        finally:
            await self.disconnect()

    async def _has_valid_name(self):
        await self.send("SERVER", self.name)
        response = await self.reader.readexactly(1)
        return bool(int(response.decode()))
