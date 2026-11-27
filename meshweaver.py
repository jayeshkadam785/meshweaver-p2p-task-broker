"""MeshWeaver: a zero-dependency, peer-to-peer asynchronous task broker.

Every node is a MeshPeer. A peer can:
  * expose named task handlers (sync or async functions),
  * submit tasks to the mesh; they run on the least-loaded known peer,
  * fall back to local execution if no remote peer is reachable,
  * discover other peers by gossiping peer lists.

Wire protocol: one JSON object per line over TCP, one request/reply per
connection. Standard library only (Python 3.10+).
"""

import asyncio
import inspect
import json
from contextlib import suppress


class TaskError(Exception):
    """A task failed, either locally or on a remote peer."""


class MeshPeer:
    def __init__(self, host="127.0.0.1", port=0, max_concurrent=4, request_timeout=5.0):
        self.host = host
        self.port = port
        self.request_timeout = request_timeout
        self._handlers = {}
        self._peers = {}  # (host, port) -> last reported load
        self._slots = asyncio.Semaphore(max_concurrent)
        self._server = None
        self._active = 0

    # ----- registration -------------------------------------------------
    def task(self, name=None):
        """Decorator: register a function as a named task."""

        def decorator(fn):
            self._handlers[name or fn.__name__] = fn
            return fn

        return decorator

    # ----- lifecycle ----------------------------------------------------
    async def start(self):
        self._server = await asyncio.start_server(self._handle, self.host, self.port)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def stop(self):
        if self._server:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    # ----- membership ---------------------------------------------------
    async def connect(self, host, port):
        """Introduce ourselves to a peer and learn its peer list."""
        await self._hello((host, int(port)))

    async def discover(self):
        """Gossip round: ask every known peer for its peers; drop dead ones."""
        for addr in list(self._peers):
            try:
                await self._hello(addr)
            except (OSError, asyncio.TimeoutError, ValueError):
                self._peers.pop(addr, None)

    async def _hello(self, addr):
        reply = await self._request(addr, {"type": "hello", "host": self.host, "port": self.port})
        self._peers[addr] = reply.get("load", 0)
        for host, port in reply.get("peers", []):
            self._add_peer(host, port)

    def _add_peer(self, host, port, load=0):
        addr = (host, int(port))
        if addr != (self.host, self.port):
            self._peers.setdefault(addr, load)

    # ----- submitting work ----------------------------------------------
    async def submit(self, name, *args, **kwargs):
        """Run a task on the least-loaded peer, falling back to local execution."""
        for addr, _ in sorted(self._peers.items(), key=lambda kv: kv[1]):
            payload = {"type": "task", "name": name, "args": list(args), "kwargs": kwargs}
            try:
                reply = await self._request(addr, payload)
            except (OSError, asyncio.TimeoutError, ValueError):
                self._peers.pop(addr, None)  # unreachable: forget it, try the next
                continue
            self._peers[addr] = reply.get("load", 0)
            if not reply.get("ok"):
                raise TaskError(reply.get("error", "remote task failed"))
            return reply["result"]
        return await self._run(name, args, kwargs)

    async def map(self, name, items):
        """Submit one task per item concurrently; results keep input order."""
        return await asyncio.gather(*(self.submit(name, item) for item in items))

    # ----- internals ----------------------------------------------------
    async def _run(self, name, args, kwargs):
        fn = self._handlers.get(name)
        if fn is None:
            raise TaskError(f"unknown task: {name!r}")
        async with self._slots:
            self._active += 1
            try:
                result = fn(*args, **kwargs)
                if inspect.isawaitable(result):
                    result = await result
                return result
            finally:
                self._active -= 1

    async def _dispatch(self, msg):
        kind = msg.get("type")
        if kind == "hello":
            if "port" in msg:
                self._add_peer(msg.get("host", "127.0.0.1"), msg["port"])
            return {
                "ok": True,
                "load": self._active,
                "peers": [list(addr) for addr in self._peers],
            }
        if kind == "task":
            try:
                result = await self._run(msg["name"], msg.get("args", []), msg.get("kwargs", {}))
                return {"ok": True, "result": result, "load": self._active}
            except Exception as exc:
                return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "load": self._active}
        return {"ok": False, "error": f"unknown message type: {kind!r}"}

    async def _handle(self, reader, writer):
        try:
            line = await reader.readline()
            if line:
                reply = await self._dispatch(json.loads(line))
            else:
                reply = {"ok": False, "error": "empty request"}
            data = json.dumps(reply)
        except Exception as exc:  # bad JSON, non-serialisable result, etc.
            data = json.dumps({"ok": False, "error": f"{type(exc).__name__}: {exc}"})
        try:
            writer.write((data + "\n").encode())
            await writer.drain()
        finally:
            writer.close()
            with suppress(Exception):
                await writer.wait_closed()

    async def _request(self, addr, msg):
        host, port = addr
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), self.request_timeout
        )
        try:
            writer.write((json.dumps(msg) + "\n").encode())
            await writer.drain()
            line = await asyncio.wait_for(reader.readline(), self.request_timeout)
            if not line:
                raise ConnectionError(f"{addr} closed without replying")
            return json.loads(line)
        finally:
            writer.close()
            with suppress(Exception):
                await writer.wait_closed()


# ----- demo -----------------------------------------------------------------
def make_peer(**kwargs):
    peer = MeshPeer(**kwargs)

    @peer.task()
    async def square(x):
        await asyncio.sleep(0.2)  # simulate work
        return x * x

    @peer.task()
    def whoami():
        return peer.port

    return peer


async def demo():
    peers = [await make_peer().start() for _ in range(3)]
    try:
        hub = peers[0]
        await peers[1].connect("127.0.0.1", hub.port)
        await peers[2].connect("127.0.0.1", hub.port)
        await hub.discover()
        print("hub knows peers:", sorted(hub._peers))
        print("square results:", await hub.map("square", range(8)))
        print("ran on port:", await hub.submit("whoami"))
    finally:
        await asyncio.gather(*(p.stop() for p in peers))


if __name__ == "__main__":
    asyncio.run(demo())
