import asyncio
import logging
from typing import Awaitable, Callable, Coroutine, Self

from .util import HEADER, Payload, Response

logger = logging.getLogger(__name__)


class Client:

    def __init__(self, name: str, *,
                 callback: Callable[[Response], Awaitable[None]] = None,
                 main_routine: Callable[[Self], Coroutine] | Callable[[Self, ...], Coroutine] | None = None,
                 main_routine_args: tuple = (),
                 on_disconnect: Callable = None):
        """
        Initialize a Client instance.

        Parameters:
            name (str): The name to identify this client.
            callback (Callable[[Response], Awaitable[None]], optional):
                An async function to be called when a message is received.
            main_routine (Callable[[Self], Coroutine], optional):
                An async function representing the main routine to run after connecting.
                This should represent the main loop of your application.
                Providing it here guarantees proper error and connection handling.
                Leave empty if you want to handle the main loop manually.
        """
        self._name = name
        self._callback = callback
        self._main_routine = main_routine
        self._main_routine_args = main_routine_args
        self._disconnected = asyncio.Event()
        self._on_disconnect = on_disconnect
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None

    async def connect(self, host: str, port: int) -> Self:
        """Connect to the server at the specified host and port."""
        self._disconnected.clear()
        try:
            self._reader, self._writer = await asyncio.open_connection(host, port)
        except (ConnectionRefusedError, OSError):
            raise ConnectionRefusedError(f"Could not connect to '{host}:{port}' - Is the server running?")

        if not await self._has_valid_name():
            await self.disconnect()
            raise ConnectionAbortedError(f"Name '{self._name}' cannot be used as it is already taken.")

        logger.info(f"Connected to '{host}:{port}'")
        asyncio.create_task(self._receive())

        if self._main_routine:
            disconnect_task = asyncio.create_task(self._wait_for_disconnect())
            send_task = asyncio.create_task(self._main_routine(self, *self._main_routine_args))
            try:
                done, pending = await asyncio.wait(
                    {send_task, disconnect_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
            except asyncio.CancelledError:
                pass

        return self

    async def send(self, destination: str, message: str):
        """Send a message to the specified destination."""
        data = Payload(source=self._name, destination=destination, data=message).model_dump_json().encode()
        length_header = str(len(data)).encode()
        length_header += b' ' * (HEADER - len(length_header))
        try:
            self._writer.write(length_header + data)
            await self._writer.drain()
        except ConnectionResetError:
            logger.warning("Connection is closed. Cannot send message.")

    async def disconnect(self):
        """Disconnect from the server."""
        if self._disconnected.is_set():
            logger.info("Already disconnected.")
            return
        logger.info("Disconnecting...")
        self._disconnected.set()
        if not self._writer.is_closing():
            self._writer.close()
            await self._writer.wait_closed()
        if self._on_disconnect:
            if asyncio.iscoroutinefunction(self._on_disconnect):
                await self._on_disconnect()
            else:
                self._on_disconnect()

    def is_connected(self) -> bool:
        """Check if the client is still connected."""
        return not self._disconnected.is_set()

    async def _wait_for_disconnect(self):
        """Wait until the client is disconnected."""
        await self._disconnected.wait()

    async def _receive(self) -> None:
        """Background task for receiving messages from the server."""
        try:
            while True:
                length_bytes = await self._reader.readexactly(HEADER)
                length = int(length_bytes.decode().strip())
                data_bytes = await self._reader.readexactly(length)
                payload = Payload.model_validate_json(data_bytes.decode())
                if self._callback:
                    await self._callback(Response(payload.source, payload.destination, payload.data))
        except asyncio.CancelledError:
            logger.info("Client got terminated.")
        except asyncio.IncompleteReadError:
            logger.info("Server closed connection.")
        finally:
            await self.disconnect()

    async def _has_valid_name(self):
        await self.send("SERVER", self._name)
        response = await self._reader.readexactly(1)
        return bool(int(response.decode()))
