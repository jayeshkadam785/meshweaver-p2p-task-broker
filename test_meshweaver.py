import unittest

from meshweaver import MeshPeer, TaskError


def make_peer():
    peer = MeshPeer()

    @peer.task()
    def add(a, b):
        return a + b

    @peer.task()
    async def square(x):
        return x * x

    @peer.task()
    def whoami():
        return peer.port

    @peer.task()
    def boom():
        raise RuntimeError("kaboom")

    return peer


class MeshWeaverTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.a = await make_peer().start()
        self.b = await make_peer().start()

    async def asyncTearDown(self):
        await self.a.stop()
        await self.b.stop()

    async def test_runs_locally_when_no_peers(self):
        self.assertEqual(await self.a.submit("add", 2, 3), 5)

    async def test_runs_on_remote_peer(self):
        await self.a.connect("127.0.0.1", self.b.port)
        self.assertEqual(await self.a.submit("whoami"), self.b.port)

    async def test_map_preserves_order(self):
        await self.a.connect("127.0.0.1", self.b.port)
        self.assertEqual(await self.a.map("square", range(5)), [0, 1, 4, 9, 16])

    async def test_remote_error_is_raised_not_retried(self):
        await self.a.connect("127.0.0.1", self.b.port)
        with self.assertRaisesRegex(TaskError, "kaboom"):
            await self.a.submit("boom")

    async def test_unknown_task_raises(self):
        with self.assertRaises(TaskError):
            await self.a.submit("nope")

    async def test_fails_over_when_peer_is_down(self):
        await self.a.connect("127.0.0.1", self.b.port)
        await self.b.stop()
        self.assertEqual(await self.a.submit("whoami"), self.a.port)
        self.assertNotIn(("127.0.0.1", self.b.port), self.a._peers)

    async def test_discovery_learns_transitive_peers(self):
        c = await make_peer().start()
        try:
            await self.a.connect("127.0.0.1", self.b.port)  # a knows b
            await self.b.connect("127.0.0.1", c.port)       # b knows c
            await self.a.discover()                          # a gossips with b
            self.assertIn(("127.0.0.1", c.port), self.a._peers)
        finally:
            await c.stop()


if __name__ == "__main__":
    unittest.main()
