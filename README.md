# MeshWeaver

A zero-dependency, peer-to-peer asynchronous task broker written in Python.

## Requirements
- Python 3.10 or newer
- No third-party packages

## Files
- `meshweaver.py`: the broker (`MeshPeer`, `TaskError`) plus a runnable demo
- `test_meshweaver.py`: unit tests

## Run the demo
    python meshweaver.py

## Run the tests
    python -m unittest test_meshweaver -v

## Usage
    from meshweaver import MeshPeer

    peer = await MeshPeer().start()

    @peer.task()
    async def square(x):
        return x * x

    await peer.connect("127.0.0.1", other_port)   # join the mesh
    await peer.discover()                          # learn about more peers
    print(await peer.submit("square", 7))          # runs on least-loaded peer
    print(await peer.map("square", range(8)))      # concurrent batch, ordered

If no peer is reachable, tasks run locally.
