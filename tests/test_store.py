"""Round-trips through the SQLite-backed Store, checking it keeps the JSON Store's public behaviour."""
from __future__ import annotations

import stat
import time
from pathlib import Path
from types import SimpleNamespace

import click
import pytest
import trio
from libp2p.peer.id import ID
from multiaddr import Multiaddr

from conftest import new_pid
from gnosis_mesh import MAX_NAME, AsyncStore, Node, Store


# --- config ---

def test_config_defaults(store: Store) -> None:
    assert store.config() == {"username": "", "nicks": {}, "blocked": []}
    assert store.username() == ""


def test_set_config_round_trip_and_persists(store: Store, home: Path) -> None:
    store.set_config(username="alice", theme={"dark": True})
    store.set_nick("p1", "bob")
    store.block("p3")
    store.block("p2")
    store.set_config(username="alice2")  # only replaces the given key
    expected = {"username": "alice2", "nicks": {"p1": "bob"}, "blocked": ["p2", "p3"], "theme": {"dark": True}}
    assert store.config() == expected
    reopened = Store(home)
    assert reopened.config() == expected
    assert reopened.username() == "alice2"
    reopened.close()


def test_nick_and_block_helpers(store: Store) -> None:
    store.set_nick("p1", "bob")
    store.set_nick("p1", "robert")
    store.set_nick("p2", "carol")
    store.set_nick("p2", None)
    assert store.config()["nicks"] == {"p1": "robert"}
    store.block("p9")
    store.block("p9")
    store.block("p1")
    assert store.is_blocked("p9") and not store.is_blocked("p2")
    assert store.config()["blocked"] == ["p1", "p9"]
    store.unblock("p9")
    assert store.config()["blocked"] == ["p1"]


# --- peers ---

def test_remember_peer_upsert_semantics(store: Store) -> None:
    store.remember_peer("p1", ["/ip4/1.2.3.4/tcp/1"], "alice", seen=100.0)
    store.remember_peer("p1", [], None, seen=200.0)  # no addrs / name: keep the old ones, bump seen
    assert store.peers() == {"p1": {"name": "alice", "addrs": ["/ip4/1.2.3.4/tcp/1"], "seen": 200.0}}
    store.remember_peer("p1", ["/ip4/5.6.7.8/tcp/2"], "alice2", seen=300.0)
    assert store.peers()["p1"] == {"name": "alice2", "addrs": ["/ip4/5.6.7.8/tcp/2"], "seen": 300.0}
    store.remember_peer("p2", [], None)
    assert store.peers()["p2"]["addrs"] == [] and "name" not in store.peers()["p2"]
    assert abs(store.peers()["p2"]["seen"] - time.time()) < 5


def test_peers_keeps_first_met_order_and_recent_peers_is_newest_first(store: Store) -> None:
    for pid, seen in (("a", 3.0), ("b", 1.0), ("c", 2.0)):
        store.remember_peer(pid, [], None, seen=seen)
    store.remember_peer("a", [], None, seen=0.5)  # re-meeting doesn't reorder peers()
    assert list(store.peers()) == ["a", "b", "c"]
    assert list(store.recent_peers(2)) == ["c", "b"]
    store.block("c")
    assert list(store.recent_peers(2, exclude_blocked=True)) == ["b", "a"]
    assert store.has_peer("a") and not store.has_peer("zz")


def test_prune_by_age_spares_nicknamed_and_correspondents(store: Store) -> None:
    old, now = time.time() - 365 * 86400, time.time()
    for pid in ("stranger", "nicked", "friend"):
        store.remember_peer(pid, [], None, seen=old)
    store.remember_peer("fresh", [], None, seen=now)
    store.set_nick("nicked", "n")
    store.log_message("friend", "in", "hi")
    assert store.prune_peers() == 1
    assert set(store.peers()) == {"nicked", "friend", "fresh"}


def test_prune_caps_to_most_recent(store: Store) -> None:
    now = time.time()
    for i in range(10):
        store.remember_peer(f"p{i}", [], None, seen=now - i)  # p0 newest
    store.set_nick("p9", "keep me")
    assert store.prune_peers(keep=3) == 6
    assert set(store.peers()) == {"p0", "p1", "p2", "p9"}


def test_remember_peer_prunes_opportunistically(store: Store) -> None:
    store.remember_peer("ancient", [], None, seen=0.0)
    store.remember_peer("x", [], None)
    assert store.has_peer("ancient")  # not due yet
    store._last_prune = 0.0
    store.remember_peer("y", [], None)
    assert not store.has_peer("ancient")


# --- messages ---

def test_messages_round_trip_and_filters(store: Store) -> None:
    for i in range(5):
        store.log_message("a" if i % 2 == 0 else "b", "in" if i < 4 else "out", f"m{i}")
    all_ = store.messages()
    assert [m["text"] for m in all_] == ["m0", "m1", "m2", "m3", "m4"]
    assert set(all_[0]) == {"id", "ts", "peer", "dir", "text"}
    assert [m["text"] for m in store.messages(peer="a")] == ["m0", "m2", "m4"]
    assert [m["text"] for m in store.messages(last=2)] == ["m3", "m4"]
    assert [m["text"] for m in store.messages(peer="a", last=2)] == ["m2", "m4"]
    assert [m["text"] for m in store.messages(after=all_[2]["id"])] == ["m3", "m4"]
    store.block("b")
    assert [m["text"] for m in store.messages(include_blocked=False)] == ["m0", "m2", "m4"]
    assert len(store.messages()) == 5  # default still includes everything


def test_bad_direction_rejected(store: Store) -> None:
    import sqlite3
    with pytest.raises(sqlite3.IntegrityError):
        store.log_message("a", "sideways", "x")


# --- names ---

def test_label(store: Store) -> None:
    pid = new_pid()
    assert store.label(pid) == f"{pid[:6]}…{pid[-6:]}"
    store.remember_peer(pid, [], "alice")
    assert store.label(pid) == "alice"
    store.set_nick(pid, "ally")
    assert store.label(pid) == "ally"


def test_resolve(store: Store) -> None:
    a, b, c = new_pid(), new_pid(), new_pid()
    store.remember_peer(a, [], "alice")
    store.remember_peer(b, [], "dup")
    store.remember_peer(c, [], "dup")
    store.set_nick(c, "alice")  # local nicknames win over usernames
    assert store.resolve("alice") == c
    assert store.resolve(a) == a
    assert store.resolve(new_pid()) != ""  # any valid peer ID passes through
    store.remember_peer(new_pid(), [], a)  # a peer claiming a's ID as its username can't hijack it
    assert store.resolve(a) == a
    with pytest.raises(click.ClickException, match="ambiguous"):
        store.resolve("dup")
    with pytest.raises(click.ClickException, match="not a known"):
        store.resolve("nobody")


# --- files ---

def test_key_seed_stays_a_private_file(store: Store, home: Path) -> None:
    seed = store.key_seed()
    assert len(seed) == 32 and store.key_seed() == seed
    assert stat.S_IMODE((home / "key").stat().st_mode) == 0o600


def test_db_is_private_and_in_wal_mode(store: Store, home: Path) -> None:
    assert stat.S_IMODE((home / "store.db").stat().st_mode) == 0o600
    assert stat.S_IMODE(home.stat().st_mode) == 0o700
    assert store.db.execute("PRAGMA journal_mode").fetchone()[0] == "wal"


# --- Node call sites (unbound, with a stand-in for the libp2p host) ---

def test_node_remember_and_blocked_use_store(store: Store) -> None:
    pid = ID.from_base58(new_pid())
    fake = SimpleNamespace(store=store, astore=AsyncStore(store.home), addrs=lambda _: [Multiaddr("/ip4/1.2.3.4/tcp/4777")])
    trio.run(Node.remember, fake, pid, "alice")  # type: ignore[arg-type]
    fake.addrs = lambda _: []
    trio.run(Node.remember, fake, pid, None)  # type: ignore[arg-type]
    assert store.peers()[pid.to_base58()]["addrs"] == ["/ip4/1.2.3.4/tcp/4777"]
    assert store.label(pid.to_base58()) == "alice"
    trio.run(Node.remember, fake, pid, "x" * 10_000)  # type: ignore[arg-type]  # untrusted: capped
    assert store.label(pid.to_base58()) == "x" * MAX_NAME
    trio.run(Node.remember, fake, pid, {"not": "a string"})  # type: ignore[arg-type]  # ignored, name kept
    assert store.label(pid.to_base58()) == "x" * MAX_NAME
    assert not Node.blocked(fake, pid.to_base58())  # type: ignore[arg-type]
    store.block(pid.to_base58())
    assert Node.blocked(fake, pid.to_base58())  # type: ignore[arg-type]
