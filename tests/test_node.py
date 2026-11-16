"""Node logic that can run without a network: real Node methods, a real peerstore, stand-in streams."""
from __future__ import annotations

import collections
import json
from types import SimpleNamespace
from typing import Any

import pytest
import trio
import trio.testing
from libp2p.peer.id import ID
from libp2p.peer.peerstore import PeerStore
from libp2p.relay.circuit_v2.pb.circuit_pb2 import HopMessage, PeerId

from conftest import new_pid
from gnosis_mesh import MAX_ADDRS, MESH_KEY, PEX_LIMIT, AsyncStore, Node, Store


class FakeStream:
    def __init__(self, data: Any, peer: ID | None = None) -> None:
        self.data, self.sent, self.was_reset = json.dumps(data).encode(), b"", False
        self.muxed_conn = SimpleNamespace(peer_id=peer)

    async def write(self, data: bytes) -> None:
        self.sent += data

    async def close_write(self) -> None:
        pass

    async def read(self, n: int) -> bytes:
        chunk, self.data = self.data[:n], self.data[n:]
        return chunk

    async def close(self) -> None:
        pass

    async def reset(self) -> None:
        self.was_reset = True


def pid() -> ID:
    return ID.from_base58(new_pid())


def node_with(store: Store, reply: FakeStream | None = None) -> Any:
    """A stand-in Node: real methods for the logic under test, a real libp2p peerstore, a fake network whose
    outbound streams all return `fake.reply`."""
    peerstore, me = PeerStore(), pid()

    async def new_stream(*_: Any) -> FakeStream:
        return fake.reply

    fake = SimpleNamespace(store=store, astore=AsyncStore(store.home), reply=reply, echo=False, greeted=set(),
                           recent_ids=collections.deque(maxlen=1000), protocols={},
                           host=SimpleNamespace(new_stream=new_stream, get_peerstore=lambda: peerstore,
                                                get_id=lambda: me))
    for name in ("blocked", "remember", "addrs", "learn", "relay_allowed"):
        setattr(fake, name, getattr(Node, name).__get__(fake))
    fake.speaks = lambda p, proto: fake.protocols.get(p, False)
    return fake


# --- peer exchange ---

def test_peer_exchange_is_capped_per_reply_and_in_total(store: Store) -> None:
    """A peer can't make us keep an unbounded peer list, nor unbounded addresses for one peer across many replies."""
    victims = [pid() for _ in range(PEX_LIMIT + 20)]
    fake = node_with(store)
    for round_ in range(3):  # fresh addresses for the same peers every time
        peers = [{"id": v.to_base58(), "addrs": [f"/ip4/10.{round_}.0.1/tcp/{i}" for i in range(MAX_ADDRS + 50)]}
                 for v in victims]
        fake.reply = FakeStream({"name": "bob", "peers": peers})
        reply = trio.run(Node.request, fake, pid(), "peers")
        assert len(reply["peers"]) == PEX_LIMIT
    assert all(len(fake.addrs(v)) == MAX_ADDRS for v in victims[:PEX_LIMIT])
    assert all(fake.addrs(v) == [] for v in victims[PEX_LIMIT:])


def test_failed_request_resets_its_stream(store: Store) -> None:
    stream = FakeStream(None)
    stream.data = b"not json"
    fake = node_with(store, stream)
    with pytest.raises(json.JSONDecodeError):
        trio.run(Node.request, fake, pid(), "hello")
    assert stream.was_reset


# --- incoming messages ---

def deliver(fake: Any, sender: ID, request: Any) -> FakeStream:
    stream = FakeStream(request, sender)
    trio.run(Node.handle, fake, stream)
    return stream


def test_retried_message_is_stored_once_per_sender(store: Store) -> None:
    fake, alice, bob = node_with(store), pid(), pid()
    for sender, request in ((alice, {"t": "msg", "text": "hi", "id": "m1"}),
                            (alice, {"t": "msg", "text": "hi", "id": "m1"}),  # retry: dropped
                            (bob, {"t": "msg", "text": "bob's m1", "id": "m1"}),  # same id, other sender: kept
                            (alice, {"t": "msg", "text": "no id"}), (alice, {"t": "msg", "text": "no id"}),
                            (alice, {"t": "msg", "text": "huge id", "id": "x" * 100_000}),  # not kept as a key
                            (alice, {"t": "msg", "text": "huge id", "id": "x" * 100_000})):
        stream = deliver(fake, sender, request)
        assert not stream.was_reset and json.loads(stream.sent) == {"name": ""}
    assert [m["text"] for m in store.messages()] == ["hi", "bob's m1", "no id", "no id", "huge id", "huge id"]
    assert all(len(mid) < 100 for _, mid in fake.recent_ids)


def test_retry_after_a_failed_write_is_stored(store: Store) -> None:
    fake, alice = node_with(store), pid()
    real = fake.astore.store.log_message
    fake.astore.store.log_message = lambda *a: (_ for _ in ()).throw(OSError("database is locked"))
    assert deliver(fake, alice, {"t": "msg", "text": "hi", "id": "m1"}).was_reset  # failed: no ack
    fake.astore.store.log_message = real
    assert not deliver(fake, alice, {"t": "msg", "text": "hi", "id": "m1"}).was_reset
    assert [m["text"] for m in store.messages()] == ["hi"]


def test_peer_blocked_while_its_request_arrives_is_refused(store: Store) -> None:
    fake, mallory = node_with(store), pid()

    class SlowStream(FakeStream):
        async def read(self, n: int) -> bytes:
            store.block(mallory.to_base58())  # the user blocks mallory mid-request
            return await super().read(n)

    stream = SlowStream({"t": "msg", "text": "sneaky", "id": "m1"}, mallory)
    trio.run(Node.handle, fake, stream)
    assert stream.was_reset and store.messages() == []


# --- relay (hop) requests ---

def hop(kind: int, src: ID, target: ID | None) -> tuple[FakeStream, HopMessage]:
    msg = HopMessage(type=kind)
    if target is not None:
        msg.peer.CopyFrom(PeerId(id=target.to_bytes()))
    return FakeStream(None, src), msg


def test_relay_checks_every_hop_request(store: Store) -> None:
    fake, alice, bob, mallory, stranger = node_with(store), pid(), pid(), pid(), pid()
    fake.protocols = {alice: True, bob: True, mallory: True}  # stranger doesn't speak the chat protocol
    store.block(mallory.to_base58())
    passed: list[tuple[str, ID]] = []

    async def lib_reserve(stream: FakeStream, msg: HopMessage) -> None:
        passed.append(("reserve", stream.muxed_conn.peer_id))

    async def lib_connect(stream: FakeStream, msg: HopMessage) -> None:
        passed.append(("connect", ID(msg.peer.id)))

    fake.lib_reserve, fake.lib_connect = lib_reserve, lib_connect
    cases = [  # (handler, message, should reach the library)
        (Node.relay_reserve, hop(HopMessage.RESERVE, alice, None), True),
        (Node.relay_reserve, hop(HopMessage.RESERVE, alice, alice), True),
        (Node.relay_reserve, hop(HopMessage.RESERVE, alice, mallory), False),  # reserving for someone else
        (Node.relay_reserve, hop(HopMessage.RESERVE, mallory, None), False),  # blocked source
        (Node.relay_reserve, hop(HopMessage.RESERVE, stranger, None), False),  # not a chat peer
        (Node.relay_connect, hop(HopMessage.CONNECT, alice, bob), True),
        (Node.relay_connect, hop(HopMessage.CONNECT, alice, mallory), False),  # blocked target
        (Node.relay_connect, hop(HopMessage.CONNECT, mallory, bob), False),  # blocked source
    ]
    for handler, (stream, msg), allowed in cases:
        trio.run(handler, fake, stream, msg)
        assert stream.was_reset is not allowed
    assert passed == [("reserve", alice), ("reserve", alice), ("connect", bob)]


# --- background loops ---

def test_announce_needs_both_provider_records(store: Store) -> None:
    async def closest(_: bytes) -> list[ID]:
        return []

    results: dict[str, bool] = {}

    async def provide(key: str) -> bool:
        return results["mesh" if key == MESH_KEY else "own"]

    fake = node_with(store)
    fake.me = "me"
    fake.dht = SimpleNamespace(get_routing_table_size=lambda: 1, provide=provide,
                               peer_routing=SimpleNamespace(find_closest_peers_network=closest))
    for own, mesh in ((True, True), (True, False), (False, True)):
        results.update(own=own, mesh=mesh)
        assert trio.run(Node.announce, fake) is (own and mesh)  # a failed mesh record is retried soon too


def test_announce_loop_survives_announce_errors() -> None:
    calls = 0

    async def announce() -> bool:
        nonlocal calls
        calls += 1
        raise RuntimeError("DHT exploded")

    async def main() -> None:
        with trio.move_on_after(100):
            await Node.announce_loop(SimpleNamespace(announce=announce))  # type: ignore[arg-type]

    trio.run(main, clock=trio.testing.MockClock(autojump_threshold=0))
    assert calls >= 5  # kept retrying (every 15 s) instead of raising out of maintain()


# --- local control socket ---

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
    fake.command = Node.command.__get__(fake)
    conn = Conn()
    trio.run(Node.control, fake, conn, trio.CancelScope())  # type: ignore[arg-type]
    assert "error" in json.loads(conn.sent)

