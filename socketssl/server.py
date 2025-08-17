from __future__ import annotations

import asyncio
import logging

from .util import HEADER, Payload

logger = logging.getLogger(__name__)


class Server:
    class _Client:
        def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, name: str):
            self.reader = reader
            self.writer = writer
            self.name = name

        async def send(self, payload: Payload):
            """Send a payload with HEADER-based framing."""
            data = payload.model_dump_json().encode()
            length_header = str(len(data)).encode()
            length_header += b' ' * (HEADER - len(length_header))
            self.writer.write(length_header + data)
            await self.writer.drain()

        async def receive(self) -> Payload | None:
            """Read a payload safely. Returns None if client disconnected."""
            try:
                length_bytes = await self.reader.readexactly(HEADER)
                length = int(length_bytes.decode().strip())
                data_bytes = await self.reader.readexactly(length)
                return Payload.model_validate_json(data_bytes)
            except asyncio.IncompleteReadError:
                return None

        def close(self):
            self.writer.close()

    def __init__(self, *, host: str, port: int):
        self.clients: list[Server._Client] = []
        self.name = "SERVER"
        self.host = host
        self.port = port

    @classmethod
    async def start(cls, host: str, port: int):
        server = cls(host=host, port=port)
        socket_server = await asyncio.start_server(server._handle_client, server.host, server.port)
        async with socket_server:
            addr = socket_server.sockets[0].getsockname()
            logger.info(f"Listening on {addr[0]}:{addr[1]}")
            try:
                await socket_server.start_serving()
                while True:
                    await asyncio.sleep(3600)
            except asyncio.CancelledError:
                pass
            finally:
                logger.info("Server shutdown requested, closing all connections...")
                for client in server.clients:
                    client.close()
                await asyncio.gather(*(client.writer.wait_closed() for client in server.clients),
                                     return_exceptions=True)
                logger.info("All connections closed.")

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info('peername')
        name, is_duplicate = await self._get_name(reader, writer)
        if is_duplicate:
            logger.info(f"Client '{addr[0]}:{addr[1]}' blocked: Name '{name}' taken")
            writer.close()
            await writer.wait_closed()
            return

        client = Server._Client(reader, writer, name)
        self.clients.append(client)
        logger.info(f"Client connected: '{name}' with {addr}")

        try:
            while payload := await client.receive():
                if payload.destination == self.name:
                    logger.info(f"Received data from '{client.name}': {payload.data}")
                    continue
                destination = next((c for c in self.clients if c.name == payload.destination), None)
                if destination:
                    await destination.send(payload)
                    logger.info(f"Forwarded data from '{client.name}' to '{destination.name}'")
        finally:
            self.clients.remove(client)
            client.close()
            await writer.wait_closed()
            logger.info(f"'{client.name}' disconnected")

    async def _get_name(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> tuple[str, bool]:
        recv_length_bytes = await reader.readexactly(HEADER)
        name_length = int(recv_length_bytes.decode().strip())
        name_bytes = await reader.readexactly(name_length)
        name = Payload.model_validate_json(name_bytes).data

        duplicate = next((c for c in self.clients if c.name == name), None)
        if not duplicate and name != self.name:
            writer.write(b"1")
            await writer.drain()
            return name, False

        writer.write(b"0")
        await writer.drain()
        writer.close()
        await writer.wait_closed()
        return name, True
