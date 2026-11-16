"""Node logic that can run without a network: stand-ins for the libp2p host and stream."""
from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import trio
import trio.testing

from conftest import new_pid
from gnosis_mesh import MAX_ADDRS, PEX_LIMIT, AsyncStore, Node, Store


class FakeStream:
    def __init__(self, reply: Any) -> None:
        self.reply, self.sent = json.dumps(reply).encode(), b""

    async def write(self, data: bytes) -> None:
        self.sent += data

    async def close_write(self) -> None:
        pass

    async def read(self, n: int) -> bytes:
        chunk, self.reply = self.reply[:n], self.reply[n:]
        return chunk

    async def close(self) -> None:
        pass


def test_peer_exchange_reply_is_capped(store: Store) -> None:
    """A peer can't make us store an unbounded peer list, or unbounded addresses for one peer."""
    peers = [{"id": new_pid(), "addrs": [f"/ip4/10.0.0.1/tcp/{i}" for i in range(MAX_ADDRS + 50)]}
             for _ in range(PEX_LIMIT + 20)]
    learned: dict[str, list[str]] = {}
    stream = FakeStream({"name": "bob", "peers": peers})

    async def new_stream(*_: Any) -> FakeStream:
        return stream

    fake = SimpleNamespace(store=store, astore=AsyncStore(store.home), addrs=lambda _: [],
                           host=SimpleNamespace(new_stream=new_stream),
                           learn=lambda pid, addrs: learned.__setitem__(pid.to_base58(), addrs))
    fake.remember = lambda pid, name: Node.remember(fake, pid, name)  # type: ignore[arg-type]
    remote = SimpleNamespace(to_base58=lambda: "remote")
    reply = trio.run(Node.request, fake, remote, "peers")  # type: ignore[arg-type]
    assert len(reply["peers"]) == PEX_LIMIT == len(learned)
    assert all(len(a) == MAX_ADDRS for a in learned.values())
    assert json.loads(stream.sent)["t"] == "peers"
    assert store.label("remote") == "bob"


def test_announce_loop_survives_announce_errors() -> None:
    calls = 0

    async def announce() -> bool:
        nonlocal calls
        calls += 1
        raise RuntimeError("DHT exploded")

    fake = SimpleNamespace(announce=announce)

    async def main() -> None:
        with trio.move_on_after(100):
            await Node.announce_loop(fake)  # type: ignore[arg-type]

    trio.run(main, clock=trio.testing.MockClock(autojump_threshold=0))
    assert calls >= 5  # kept retrying (every 15 s) instead of raising out of maintain()
