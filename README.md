# Sockets Streamlined
A wrapper around the python asyncio.networking library for easy streamlined use.

## Installation
```bash
pip install git+https://github.com/Origins-Tech/socketssl.git
```

## Usage
### Server-Side
```python
import asyncio

from socketssl import Server


async def main():
    await Server.start(host="localhost", port=9999)


if __name__ == "__main__":
    asyncio.run(main())
```

### Client-Side
```python
import asyncio

from aioconsole import ainput

from socketssl import Client, Response


async def on_message(resp: Response):
    print(f"Got: {resp.data} from {resp.source}")


async def main():
    client = await Client.connect(host="localhost", port=9999, name="<Name>", callback=on_message)
    try:
        while client.is_connected():
            await client.send("SERVER", await ainput(">>> "))
    except asyncio.CancelledError:
        print("Aborting ainput()")

if __name__ == "__main__":
    asyncio.run(main())
```
