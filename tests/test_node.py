"""Node logic that can run without a network: stand-ins for the libp2p host and stream."""
from __future__ import annotations

import collections
import json
from types import SimpleNamespace
from typing import Any

import pytest
import trio
import trio.testing
from libp2p.peer.id import ID

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


class InboundStream(FakeStream):
    def __init__(self, request: Any, peer: ID) -> None:
        super().__init__(request)
        self.muxed_conn = SimpleNamespace(peer_id=peer)
        self.was_reset = False

    async def reset(self) -> None:
        self.was_reset = True


def node_with(store: Store) -> SimpleNamespace:
    fake = SimpleNamespace(store=store, astore=AsyncStore(store.home), addrs=lambda _: [], echo=False,
                           greeted=set(), recent_ids=collections.deque(maxlen=1000))
    fake.blocked = lambda pid: Node.blocked(fake, pid)  # type: ignore[arg-type]
    fake.remember = lambda pid, name: Node.remember(fake, pid, name)  # type: ignore[arg-type]
    return fake


def test_retried_message_is_stored_once(store: Store) -> None:
    fake, alice = node_with(store), ID.from_base58(new_pid())
    for request in ({"t": "msg", "text": "hi", "id": "m1"}, {"t": "msg", "text": "hi", "id": "m1"},
                    {"t": "msg", "text": "old peer"}, {"t": "msg", "text": "old peer"}):  # no id: never deduplicated
        stream = InboundStream(request, alice)
        trio.run(Node.handle, fake, stream)  # type: ignore[arg-type]
        assert not stream.was_reset and json.loads(stream.sent) == {"name": ""}
    assert [m["text"] for m in store.messages()] == ["hi", "old peer", "old peer"]


def test_failed_request_resets_its_stream(store: Store) -> None:
    stream = InboundStream("ignored", ID.from_base58(new_pid()))
    stream.reply = b"not json"

    async def new_stream(*_: Any) -> FakeStream:
        return stream

    fake = node_with(store)
    fake.host = SimpleNamespace(new_stream=new_stream)
    with pytest.raises(json.JSONDecodeError):
        trio.run(Node.request, fake, SimpleNamespace(to_base58=lambda: "bob"), "hello")  # type: ignore[arg-type]
    assert stream.was_reset


def test_blocked_peers_lose_their_relay_reservation(store: Store) -> None:
    a, b = ID.from_base58(new_pid()), ID.from_base58(new_pid())
    store.block(b.to_base58())
    reservations = {a: object(), b: object()}
    fake = node_with(store)
    fake.circuit = SimpleNamespace(resource_manager=SimpleNamespace(_reservations=reservations))
    fake.speaks = lambda *_: False
    stream = InboundStream({}, a)
    trio.run(Node.hop_acl, fake, stream)  # type: ignore[arg-type]
    assert list(reservations) == [a]  # nobody can reach b through us any more


def test_malformed_control_request_does_not_escape() -> None:
    class Conn:
        def __init__(self) -> None:
            self.data, self.sent = b"[]", b""

        async def receive_some(self, n: int) -> bytes:
            chunk, self.data = self.data, b""
            return chunk

        async def send_all(self, data: bytes) -> None:
            self.sent += data

        async def aclose(self) -> None:
            pass

    fake = SimpleNamespace()
    fake.command = lambda req: Node.command(fake, req)  # type: ignore[arg-type]
    conn = Conn()
    trio.run(Node.control, fake, conn, trio.CancelScope())  # type: ignore[arg-type]
    assert "error" in json.loads(conn.sent)
