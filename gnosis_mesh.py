# SPDX-License-Identifier: AGPL-3.0-only
# gnosis-mesh - peer-to-peer chat over libp2p
# Copyright (C) 2026 muntedcrocodile
#
# This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero
# General Public License as published by the Free Software Foundation, version 3.
#
# This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
# implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public
# License for more details.
#
# You should have received a copy of the GNU Affero General Public License along with this program. If not, see
# <https://www.gnu.org/licenses/>.
"""Minimal libp2p peer-to-peer chat: a background daemon plus a click CLI."""

from __future__ import annotations

import collections
import contextlib
import hashlib
import ipaddress
import json
import logging
import os
import queue
import random
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
from collections.abc import Awaitable, Callable, Iterator
from pathlib import Path
from typing import Any, TypeVar

import click
import multihash
import trio
from libp2p import new_host
from libp2p.abc import IHost, INetStream
from libp2p.crypto.ed25519 import create_new_key_pair
from libp2p.crypto.x25519 import create_new_key_pair as create_x25519_key_pair
from libp2p.custom_types import TProtocol
from libp2p.discovery.events.peerDiscovery import peerDiscovery
from libp2p.host.autonat import AutoNATService, AutoNATStatus
from libp2p.host.autonat.autonat import AUTONAT_PROTOCOL_ID
from libp2p.identity.identify_push.identify_push import push_identify_to_peers
from libp2p.kad_dht.kad_dht import DHTMode, KadDHT, _encode_key
from libp2p.network.stream.exceptions import StreamEOF
from libp2p.peer.id import ID
from libp2p.peer.peerinfo import PeerInfo, info_from_p2p_addr
from libp2p.peer.peerstore import PERMANENT_ADDR_TTL
from libp2p.relay.circuit_v2.config import RelayConfig, RelayRole
from libp2p.relay.circuit_v2.dcutr import DCUtRProtocol
from libp2p.relay.circuit_v2.protocol import PROTOCOL_ID as HOP_PROTOCOL_ID
from libp2p.relay.circuit_v2.protocol import CircuitV2Protocol
from libp2p.relay.circuit_v2.resources import RelayLimits
from libp2p.relay.circuit_v2.transport import CircuitV2Transport
from libp2p.security.noise.transport import PROTOCOL_ID as NOISE_PROTOCOL_ID
from libp2p.security.noise.transport import Transport as NoiseTransport
from libp2p.tools.anyio_service import background_trio_service
from libp2p.utils.address_validation import _safe_get_network_addrs, is_ipv6_available
from multiaddr import Multiaddr
from multiaddr.protocols import P_P2P

PROTO = TProtocol("/gnosis-mesh/1.0.0")
MAX_MSG = 1 << 20
RELAY_TARGET = 3  # public relay reservations to hold while behind NAT
MESH_TARGET = 8  # chat peers to stay connected to; below this, discover() looks for more
DIAL_ON_START = 64  # most recently seen known peers to reconnect to when the node starts
MAX_NAME = 64  # longest username we store or pass on (a remote peer chooses it)
MAX_ADDRS = 32  # most addresses we keep for one peer
PEX_LIMIT = 200  # most recently seen peers to share in a "peers" reply (keeps it well under MAX_MSG)
IPFS_BOOTSTRAP = [  # public IPFS DHT entry points (Kubo defaults)
    "/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
    "/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
    "/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
    "/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
    "/dnsaddr/va1.bootstrap.libp2p.io/p2p/12D3KooWKnDdG3iXw9eTFijk3EWSunZcFi54Zka4wmtqtt6rPxc8",
    "/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
    "/ip4/104.131.131.82/udp/4001/quic-v1/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
]
Json = dict[str, Any]
log = logging.getLogger("gnosis-mesh")


# ---------- local storage: store.db (SQLite, WAL) shared by the CLI and the daemon; the identity seed stays in `key` ----------

SCHEMA = """
-- settings values are JSON, e.g. username
CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS nicks (pid TEXT PRIMARY KEY, nick TEXT NOT NULL) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS nicks_nick ON nicks (nick);
CREATE TABLE IF NOT EXISTS blocked (pid TEXT PRIMARY KEY) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS peers (pid TEXT NOT NULL UNIQUE, name TEXT, addrs TEXT NOT NULL DEFAULT '[]', seen REAL NOT NULL);
CREATE INDEX IF NOT EXISTS peers_seen ON peers (seen);
CREATE INDEX IF NOT EXISTS peers_name ON peers (name);
CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL NOT NULL, peer TEXT NOT NULL,
                                     dir TEXT NOT NULL CHECK (dir IN ('in', 'out')), text TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS messages_peer ON messages (peer, id);
"""
T = TypeVar("T")


class Store:
    PEER_TTL = 30 * 86400  # forget strangers not seen for this long...
    MAX_PEERS = 2000  # ...and keep at most this many (nicknamed peers and correspondents are never pruned)
    PRUNE_EVERY = 3600  # the daemon prunes opportunistically from remember_peer at most this often

    def __init__(self, home: Path) -> None:
        self.home = home
        home.mkdir(parents=True, exist_ok=True, mode=0o700)
        # AF_UNIX socket paths are limited to 108 bytes on Linux and 104 on macOS: use the first runtime dir whose
        # socket path fits (/tmp always does), so a long $XDG_RUNTIME_DIR or $TMPDIR can't stop the node starting.
        name = f"gnosis-mesh-{hashlib.sha1(str(home.resolve()).encode()).hexdigest()[:12]}.sock"
        for run_dir in (os.environ.get("XDG_RUNTIME_DIR"), tempfile.gettempdir(), "/tmp"):
            self.sock = os.path.join(run_dir or "/tmp", name)
            if len(self.sock.encode()) < 104:
                break
        self.db_path = home / "store.db"
        if not self.db_path.exists():  # messages are private: create the db (SQLite copies its mode to -wal/-shm) 0600
            os.close(os.open(self.db_path, os.O_WRONLY | os.O_CREAT, 0o600))
        # Autocommit; writes that must be atomic use _tx(). WAL lets readers proceed while one writer commits;
        # busy_timeout covers the brief writer-vs-writer overlap. Not tied to the creating thread so AsyncStore can
        # use its own Store from worker threads, but only one thread may use a Store at a time.
        self.db = sqlite3.connect(self.db_path, isolation_level=None, timeout=5, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")  # durable across app crashes; an OS crash may lose the last commit
        self._last_prune = time.time()
        self.db.executescript(SCHEMA)

    def close(self) -> None:
        self.db.close()

    @contextlib.contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        """A write transaction that takes the write lock up front (so read-modify-write can't lose updates)."""
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield self.db
        except BaseException:
            self.db.execute("ROLLBACK")
            raise
        self.db.execute("COMMIT")

    # --- config: username, nicknames, block list ---
    def config(self) -> Json:
        cfg: Json = {"username": "", **{r[0]: json.loads(r[1]) for r in self.db.execute("SELECT * FROM settings")}}
        cfg["nicks"] = dict(self.db.execute("SELECT pid, nick FROM nicks").fetchall())
        cfg["blocked"] = [r[0] for r in self.db.execute("SELECT pid FROM blocked ORDER BY pid")]
        return cfg

    def set_config(self, **kw: Any) -> None:
        """Set plain settings such as username (nicknames and blocks have set_nick / block / unblock)."""
        with self._tx() as db:
            db.executemany("INSERT OR REPLACE INTO settings VALUES (?, ?)", ((k, json.dumps(v)) for k, v in kw.items()))

    def username(self) -> str:
        row = self.db.execute("SELECT value FROM settings WHERE key = 'username'").fetchone()
        return json.loads(row[0]) if row else ""

    def set_nick(self, pid: str, nick: str | None) -> None:
        if nick:
            self.db.execute("INSERT OR REPLACE INTO nicks VALUES (?, ?)", (pid, nick))
        else:
            self.db.execute("DELETE FROM nicks WHERE pid = ?", (pid,))

    def is_blocked(self, pid: str) -> bool:
        return self.db.execute("SELECT 1 FROM blocked WHERE pid = ?", (pid,)).fetchone() is not None

    def block(self, pid: str) -> None:
        self.db.execute("INSERT OR IGNORE INTO blocked VALUES (?)", (pid,))

    def unblock(self, pid: str) -> None:
        self.db.execute("DELETE FROM blocked WHERE pid = ?", (pid,))

    # --- peers we've met ---
    @staticmethod
    def _peer(row: sqlite3.Row) -> Json:
        entry: Json = {"addrs": json.loads(row["addrs"]), "seen": row["seen"]}
        if row["name"] is not None:
            entry["name"] = row["name"]
        return entry

    def peers(self) -> Json:
        """Every remembered peer, {pid: {name?, addrs, seen}}, in the order first met (prefer recent_peers)."""
        return {r["pid"]: self._peer(r) for r in self.db.execute("SELECT * FROM peers ORDER BY rowid")}

    def recent_peers(self, limit: int) -> Json:
        """The `limit` most recently seen unblocked peers, newest first."""
        rows = self.db.execute("SELECT * FROM peers WHERE pid NOT IN (SELECT pid FROM blocked) ORDER BY seen DESC "
                               "LIMIT ?", (limit,))
        return {r["pid"]: self._peer(r) for r in rows}

    def has_peer(self, pid: str) -> bool:
        return self.db.execute("SELECT 1 FROM peers WHERE pid = ?", (pid,)).fetchone() is not None

    def remember_peer(self, pid: str, addrs: list[str], name: str | None, seen: float | None = None) -> None:
        """Upsert one peer atomically: keep its old name/addrs when none are given, always bump `seen`."""
        self.db.execute(
            "INSERT INTO peers (pid, name, addrs, seen) VALUES (?, ?, ?, ?) ON CONFLICT (pid) DO UPDATE SET "
            "name = coalesce(excluded.name, name), "
            "addrs = CASE excluded.addrs WHEN '[]' THEN addrs ELSE excluded.addrs END, seen = excluded.seen",
            (pid, name or None, json.dumps(addrs), time.time() if seen is None else seen))
        if time.time() - self._last_prune > self.PRUNE_EVERY:
            self.prune_peers()

    def prune_peers(self, ttl: float | None = None, keep: int | None = None) -> int:
        """Forget strangers: peers unseen for `ttl` seconds, then all but the `keep` most recently seen. Peers you
        nicknamed or exchanged messages with are never pruned. Returns how many were removed."""
        ttl, keep = self.PEER_TTL if ttl is None else ttl, self.MAX_PEERS if keep is None else keep
        kept = "pid NOT IN (SELECT pid FROM nicks) AND NOT EXISTS (SELECT 1 FROM messages m WHERE m.peer = pid)"
        self._last_prune = time.time()
        with self._tx() as db:
            n = db.execute(f"DELETE FROM peers WHERE seen < ? AND {kept}", (time.time() - ttl,)).rowcount
            n += db.execute(f"DELETE FROM peers WHERE {kept} AND rowid NOT IN "
                            "(SELECT rowid FROM peers ORDER BY seen DESC LIMIT ?)", (keep,)).rowcount
        if n:
            log.info("pruned %d stale peers", n)
        return n

    # --- identity ---
    def key_seed(self) -> bytes:
        p = self.home / "key"
        if not p.exists():
            p.write_bytes(os.urandom(32))
            p.chmod(0o600)
        return p.read_bytes()

    # --- message history ---
    def log_message(self, peer: str, direction: str, text: str) -> None:
        self.db.execute("INSERT INTO messages (ts, peer, dir, text) VALUES (?, ?, ?, ?)",
                        (time.time(), peer, direction, text))

    def last_message_id(self) -> int:
        return self.db.execute("SELECT coalesce(max(id), 0) FROM messages").fetchone()[0]

    def messages(self, peer: str | None = None, last: int | None = None, after: int = 0,
                 include_blocked: bool = True) -> list[Json]:
        """History in order, as {id, ts, peer, dir, text}: optionally one peer's, only the `last` N, only those with
        id > `after` (for following), without blocked peers. No arguments returns everything, as before."""
        where, args = ["id > ?"], [after]
        if peer is not None:
            where.append("peer = ?")
            args.append(peer)
        if not include_blocked:
            where.append("peer NOT IN (SELECT pid FROM blocked)")
        sql = f"SELECT * FROM messages WHERE {' AND '.join(where)} ORDER BY id"
        if last:
            sql = f"SELECT * FROM ({sql} DESC LIMIT ?) ORDER BY id"
            args.append(last)
        return [dict(r) for r in self.db.execute(sql, args)]

    # --- names ---
    def label(self, pid: str) -> str:
        nick = self.db.execute("SELECT nick FROM nicks WHERE pid = ?", (pid,)).fetchone()
        name = self.db.execute("SELECT name FROM peers WHERE pid = ?", (pid,)).fetchone()
        return (nick and nick[0]) or (name and name[0]) or f"{pid[:6]}…{pid[-6:]}"

    def resolve(self, who: str) -> str:
        """Map a nickname, peer ID or username to a peer ID. Usernames come last: a peer chooses its own, so one
        could otherwise claim another peer's ID as its name and receive messages meant for that peer."""
        if nicked := self.db.execute("SELECT pid FROM nicks WHERE nick = ? LIMIT 1", (who,)).fetchone():
            return nicked[0]
        try:
            multihash.decode(ID.from_base58(who).to_bytes())
            return who
        except Exception:
            pass
        named = [r[0] for r in self.db.execute("SELECT pid FROM peers WHERE name = ? ORDER BY rowid", (who,))]
        if len(named) > 1:
            raise click.ClickException(f"username '{who}' is ambiguous ({', '.join(named)}); use a peer ID or nickname")
        if named:
            return named[0]
        raise click.ClickException(f"'{who}' is not a known nickname, username or valid peer ID")


class AsyncStore:
    """Awaitable writes for trio code, on a connection of their own. A write can wait up to busy_timeout (5 s) for
    another process's write lock (the CLI running `block`, say); inline, that would freeze the event loop, so writes
    run in a worker thread, one at a time and in the order awaited (trio's limiter is FIFO). A cancelled caller
    still lets its write finish, so none is left half-done. Reads stay synchronous on the caller's Store: in WAL
    mode readers never wait for writers."""

    def __init__(self, home: Path) -> None:
        self.store, self._writer = Store(home), trio.CapacityLimiter(1)

    async def run(self, fn: Callable[..., T], *args: Any) -> T:
        return await trio.to_thread.run_sync(fn, *args, limiter=self._writer)

    async def remember_peer(self, pid: str, addrs: list[str], name: str | None) -> None:
        await self.run(self.store.remember_peer, pid, addrs, name)

    async def log_message(self, peer: str, direction: str, text: str) -> None:
        await self.run(self.store.log_message, peer, direction, text)


# ---------- libp2p daemon ----------

async def read_all(stream: INetStream) -> bytes:
    buf = b""
    try:
        while chunk := await stream.read(65536):
            buf += chunk
            if len(buf) > MAX_MSG:
                raise ValueError("message too large")
    except StreamEOF:
        pass
    return buf


def is_circuit(addr: Multiaddr) -> bool:
    return "/p2p-circuit" in str(addr)


def addr_rank(addr: Multiaddr) -> int:
    """Dial order: public/DNS first, then private LAN, link-local, loopback."""
    for proto in ("ip4", "ip6"):
        try:
            ip = ipaddress.ip_address(addr.value_for_protocol(proto))
        except Exception:
            continue
        return 3 if ip.is_loopback else 2 if ip.is_link_local else 0 if ip.is_global else 1
    return 0  # dns / dnsaddr


def expand(addr: Multiaddr) -> list[Multiaddr]:
    """Replace a wildcard listen IP with each local interface IP, keeping the rest (libp2p's helper drops /quic-v1)."""
    s = str(addr)
    for fam, wildcard in ((4, "/ip4/0.0.0.0/"), (6, "/ip6/::/")):
        if s.startswith(wildcard):
            loopback = "127.0.0.1" if fam == 4 else "::1"
            ips = dict.fromkeys(_safe_get_network_addrs(fam) + [loopback])
            return [Multiaddr(f"/ip{fam}/{ip}/{s[len(wildcard):]}") for ip in ips]
    return [addr]


def addr_order(addr: Multiaddr) -> tuple[bool, bool, int, bool]:
    """Advertise order: non-loopback IPv4 (public, then private), IPv6, loopback last; TCP before QUIC.
    DCUtR binds hole-punch dials to our *first* TCP address and other swarms dial only the first per transport."""
    rank, s = addr_rank(addr), str(addr)
    return rank == 3, s.startswith("/ip6/"), rank, "/quic" in s


def rendezvous_key(pid: str) -> str:
    """DHT provider key (hex sha256 multihash) under which a peer announces its addresses."""
    return "1220" + hashlib.sha256(f"gnosis-mesh:{pid}".encode()).hexdigest()


# Every node also announces itself under this one shared key, so a node that knows no other chat peer can find
# some (find_mesh_peers) and then learn the rest through peer exchange. ("/" can't occur in a peer ID.)
MESH_KEY = "1220" + hashlib.sha256(b"gnosis-mesh:/mesh/1").hexdigest()


class Node:
    def __init__(self, store: Store, port: int, bootstrap: list[str], echo: bool, mdns: bool, public: bool) -> None:
        self.store, self.bootstrap, self.echo, self.public = store, bootstrap, echo, public
        self.astore = AsyncStore(store.home)  # daemon writes go through here so a busy db never blocks the event loop
        fams = ["/ip4/0.0.0.0"] + (["/ip6/::"] if is_ipv6_available() else [])
        self.listen = [Multiaddr(f"{f}/{t}") for f in fams for t in (f"tcp/{port}", f"udp/{port}/quic-v1")]
        self.keys = create_new_key_pair(store.key_seed())
        self.host: IHost = new_host(
            key_pair=self.keys, listen_addrs=self.listen, enable_quic=True,
            # Noise only: py-libp2p also offers plaintext/secio by default, which would let a peer skip
            # encryption and claim any peer ID.
            sec_opt={NOISE_PROTOCOL_ID: NoiseTransport(self.keys, noise_privkey=create_x25519_key_pair().private_key)},
            enable_mDNS=mdns, enable_upnp=public, bootstrap=bootstrap + (IPFS_BOOTSTRAP if public else []) or None,
            bootstrap_allow_ipv6=True, addrs_factory=lambda addrs: [x for a in addrs for x in expand(a)])
        # BasicHost.get_addrs() truncates /p2p/<relay>/p2p-circuit addrs, so we build the advertised list ourselves
        # (identify, signed peer records and DHT provider records all read it).
        self.direct_addrs = self.host.get_addrs
        self.host.get_addrs = self.advertised  # type: ignore[method-assign]
        # The peerstore can evict our *own* entry once it holds >1000 peers (easy on the public DHT); identify
        # then raises and takes the host down. Serve our keys directly instead.
        self.host.get_public_key = lambda: self.keys.public_key  # type: ignore[method-assign]
        self.host.get_private_key = lambda: self.keys.private_key  # type: ignore[method-assign]
        # Join the public DHT as a client until AutoNAT confirms we are reachable.
        self.dht = KadDHT(self.host, DHTMode.CLIENT if public else DHTMode.SERVER, enable_random_walk=True)
        # Every node is a relay (rendezvous point) for other chat peers; see relay_reserve / relay_connect.
        self.circuit = CircuitV2Protocol(self.host, RelayLimits(duration=3600, data=16 << 20, max_circuit_conns=64,
                                                                max_reservations=64), allow_hop=True)
        self.lib_reserve, self.lib_connect = self.circuit._handle_reserve, self.circuit._handle_connect
        self.circuit._handle_reserve = self.relay_reserve  # type: ignore[method-assign]
        self.circuit._handle_connect = self.relay_connect  # type: ignore[method-assign]
        self.relay = CircuitV2Transport(self.host, self.circuit, RelayConfig(roles=RelayRole.STOP | RelayRole.CLIENT))
        self.relay.discovery.auto_reserve = False  # we choose relays ourselves (keep_relays)
        self.dcutr = DCUtRProtocol(self.host)
        self.autonat = AutoNATService(self.host)  # type: ignore[arg-type]
        self.reachability = AutoNATStatus.UNKNOWN
        self.greeted: set[ID] = set()
        self.recent_ids: collections.deque[tuple[ID, str]] = collections.deque(maxlen=1000)  # delivered (sender, id)
        self.dial_errors: list[str] = []
        self.upnp_ports: list[tuple[str, str]] = []
        self.last_addrs: set[str] = set()
        self.reannounce = trio.Event()
        self.found: queue.SimpleQueue[PeerInfo] = queue.SimpleQueue()  # mDNS callbacks arrive on another thread
        self.host.set_stream_handler(PROTO, self.handle)
        self.host.set_stream_handler(AUTONAT_PROTOCOL_ID, self.autonat.handle_stream)  # type: ignore[arg-type]
        peerDiscovery.register_peer_discovered_handler(self.found.put)

    @property
    def me(self) -> str:
        return self.host.get_id().to_base58()

    # --- addresses ---
    def addrs(self, pid: ID) -> list[Multiaddr]:
        try:
            return self.host.get_peerstore().addrs(pid)
        except Exception:  # unknown peer
            return []

    def reserved_relays(self) -> list[ID]:
        d = self.relay.discovery
        return [p for p in d.get_relays() if (i := d.get_relay_info(p)) and i.has_reservation]

    def advertised(self) -> list[Multiaddr]:
        addrs = sorted(self.direct_addrs(), key=addr_order)
        if self.reachability == AutoNATStatus.PUBLIC:
            return addrs
        relays = 0
        for relay in self.reserved_relays():  # advertise up to 3 relays that are themselves publicly dialable
            public = sorted((a for a in self.addrs(relay) if not is_circuit(a) and addr_rank(a) == 0), key=str)
            if public and relays < 3:
                relays += 1
                addrs += [Multiaddr(f"{a.decapsulate_code(P_P2P)}/p2p/{relay}/p2p-circuit/p2p/{self.me}")
                          for a in public[:2]]
        return addrs

    def learn(self, pid: ID, addrs: list[str]) -> None:
        """Add addresses for pid, at most MAX_ADDRS in total: a peer-exchange reply can name any peer, any number of
        times, and the peerstore keeps every distinct address it is given."""
        ma = []
        for a in list(addrs)[:max(0, MAX_ADDRS - len(self.addrs(pid)))]:
            try:
                ma.append(Multiaddr(a))
            except Exception:
                pass
        if ma and pid != self.host.get_id():
            self.host.get_peerstore().add_addrs(pid, ma, PERMANENT_ADDR_TTL)

    def speaks(self, pid: ID, proto: str) -> bool:
        try:
            return bool(self.host.get_peerstore().supports_protocols(pid, [proto]))
        except Exception:
            return False

    def connected(self, pid: ID) -> bool:
        return pid in self.host.get_connected_peers()

    def link(self, pid: ID) -> str:
        """'direct' if any connection to pid is direct (e.g. after DCUtR hole punching), else 'relayed'."""
        conns: Any = getattr(self.host.get_network(), "connections", {}).get(pid, [])
        for c in conns if isinstance(conns, list) else [conns]:
            try:
                if not any(is_circuit(a) for a in c.get_transport_addresses()):
                    return "direct"
            except Exception:
                return "direct"
        return "relayed"

    def blocked(self, pid: str) -> bool:
        return self.store.is_blocked(pid)

    async def remember(self, pid: ID, name: str | None) -> None:
        """Record an encountered chat peer (with its self-assigned username) in the store."""
        name = name[:MAX_NAME] if isinstance(name, str) else None  # untrusted: it's stored and re-shared via PEX
        await self.astore.remember_peer(pid.to_base58(), [str(a) for a in self.addrs(pid)][:MAX_ADDRS], name)

    # --- the /gnosis-mesh protocol: one JSON request + one JSON reply per stream ---
    async def handle(self, stream: INetStream) -> None:
        pid = stream.muxed_conn.peer_id
        if self.blocked(pid.to_base58()):
            await stream.reset()
            return
        try:
            with trio.fail_after(30):  # a peer that never finishes its request mustn't hold the handler forever
                req = json.loads(await read_all(stream))
            if self.blocked(pid.to_base58()):  # blocked while the request was arriving
                raise PermissionError("blocked")
            reply: Json = {"name": self.store.username()}
            if req["t"] == "msg":
                mid = req.get("id")  # older peers send none; a retry of a delivered message is dropped
                key = (pid, mid) if isinstance(mid, str) and len(mid) <= 64 else None
                if key is None or key not in self.recent_ids:
                    await self.astore.log_message(pid.to_base58(), "in", req["text"])
                    if key:  # only once stored, so a retry after a failed write is stored after all
                        self.recent_ids.append(key)
                    if self.echo:
                        click.echo(f"\n[{self.store.label(pid.to_base58())}] {req['text']}")
            elif req["t"] == "peers":
                reply["peers"] = [{"id": k, **v} for k, v in self.store.recent_peers(PEX_LIMIT).items()
                                  if k != pid.to_base58()]
            await self.remember(pid, req.get("name"))
            await stream.write(json.dumps(reply).encode())
            await stream.close()
            self.greeted.add(pid)
        except Exception as e:
            log.debug("bad request from %s: %s", pid, e)
            await stream.reset()

    async def request(self, pid: ID, t: str, **body: Any) -> Json:
        with trio.fail_after(30):
            stream = await self.host.new_stream(pid, [PROTO])
            try:
                await stream.write(json.dumps({"t": t, "name": self.store.username(), **body}).encode())
                await stream.close_write()  # type: ignore[attr-defined]  # NetStream has it; the ABC doesn't
                reply = json.loads(await read_all(stream))
                await stream.close()
            except BaseException:  # bad reply, too big, too slow or cancelled: don't leave the stream open
                with trio.move_on_after(1, shield=True):
                    await stream.reset()
                raise
        await self.remember(pid, reply.get("name"))
        if isinstance(reply.get("peers"), list):  # peer exchange: learn addresses of (at most PEX_LIMIT) peers they met
            reply["peers"] = reply["peers"][:PEX_LIMIT]
            for p in reply["peers"]:
                try:
                    self.learn(ID.from_base58(p["id"]), p.get("addrs", []))
                except Exception:
                    pass
        return reply

    async def greet(self, pid: ID, t: str = "hello") -> None:
        """Exchange usernames (and optionally peer lists); silently ignores failures."""
        try:
            await self.request(pid, t)
        except Exception as e:
            log.debug("%s to %s failed: %s", t, pid, e)

    # --- connecting ---
    async def race(self, addrs: list[Multiaddr], dial: Callable[[Multiaddr], Awaitable[Any]]) -> bool:
        """Happy-eyeballs: start a dial every 300ms, first success wins and cancels the rest."""
        ok = False
        async with trio.open_nursery() as n:
            async def attempt(addr: Multiaddr, delay: float) -> None:
                nonlocal ok
                await trio.sleep(delay)
                with trio.move_on_after(20):
                    try:
                        await dial(addr)
                        ok = True
                        n.cancel_scope.cancel()
                    except Exception as e:
                        log.debug("dial %s failed: %s", addr, e)
                        self.dial_errors.append(f"{addr}: {type(e).__name__}: {e}")
            for i, addr in enumerate(addrs[:16]):
                n.start_soon(attempt, addr, i * 0.3)
        return ok

    async def dial(self, pid: ID) -> bool:
        """Connect directly over every known address (best first), falling back to relayed circuits."""
        if pid == self.host.get_id():
            return False
        if self.connected(pid):
            return True
        addrs = list({str(a): a for a in self.addrs(pid)}.values())
        direct = sorted((a for a in addrs if not is_circuit(a)), key=addr_rank)
        # Stored/advertised circuit addrs usually omit the trailing /p2p/<target>, which the circuit dialer requires.
        relayed = [a if str(a).endswith(f"/p2p/{pid}") else Multiaddr(f"{a}/p2p/{pid}") for a in addrs if is_circuit(a)]
        net: Any = self.host.get_network()
        if hasattr(net, "unblock_peer"):
            net.unblock_peer(pid)  # don't let a recent failure on stale addresses block this attempt
        # Rendezvous: any chat peer we share with the target can relay for us (it holds a reservation from the
        # target), then DCUtR hole-punches a direct connection. Public relays the target advertised come last.
        # Tiers start staggered (direct, +2s shared peers, +4s public relays); the first success wins.
        shared = [Multiaddr(f"/p2p/{p}/p2p-circuit/p2p/{pid}") for p in self.chat_relays() if p != pid]
        ok, began = False, trio.current_time()
        self.dial_errors = []
        async with trio.open_nursery() as n:
            async def tier(name: str, addrs: list[Multiaddr], dial: Callable[[Multiaddr], Awaitable[Any]],
                           delay: float) -> None:
                nonlocal ok
                await trio.sleep(delay if direct else delay / 2)
                if await self.race(addrs, dial):
                    ok = True
                    log.info("connected to %s via %s in %.1fs", pid, name, trio.current_time() - began)
                    n.cancel_scope.cancel()
            for name, addrs, dial, delay in (("direct", direct, lambda a: net.dial_addr(a, pid), 0.0),
                                             ("shared chat peer", shared, lambda a: self.relay_dial(a, pid), 2.0),
                                             ("public relay", relayed, lambda a: self.relay_dial(a, pid), 4.0)):
                if addrs:
                    n.start_soon(tier, name, addrs, dial, delay)
        if not ok and (direct or shared or relayed):
            log.info("could not connect to %s (%d direct, %d shared, %d relayed candidates): %s",
                     pid, len(direct), len(shared), len(relayed), "; ".join(self.dial_errors[:4]))
        return ok

    async def relay_dial(self, circuit: Multiaddr, pid: ID) -> None:
        relay_ma, _ = self.relay.parse_circuit_ma(circuit)
        relay = ID.from_base58(str(relay_ma.get_peer_id()))
        base = relay_ma.decapsulate_code(P_P2P)
        if base.protocols():
            self.learn(relay, [str(base)])
        raw = await self.relay._dial_via_circuit_addr(circuit, PeerInfo(pid, [circuit]))
        await self.host.upgrade_outbound_connection(raw, pid)  # the receiving side drives the DCUtR hole punch

    async def try_connect(self, pid: ID) -> bool:
        if not await self.dial(pid):
            return False
        if pid not in self.greeted:
            self.greeted.add(pid)
            await self.greet(pid)
        return True

    async def find(self, pid: ID) -> bool:
        """Locate a peer by ID: known addrs, peer exchange, then DHT peer lookup + provider records in parallel."""
        if await self.try_connect(pid):
            return True
        async with trio.open_nursery() as n:
            for other in self.host.get_connected_peers():
                if self.store.has_peer(other.to_base58()):
                    n.start_soon(self.greet, other, "peers")
        if await self.try_connect(pid):
            return True
        found = False
        async with trio.open_nursery() as n:
            async def via(lookup: Callable[[], Awaitable[Any]]) -> None:
                nonlocal found
                with trio.move_on_after(60):
                    try:
                        await lookup()
                    except Exception as e:
                        log.debug("lookup failed: %s", e)
                if await self.try_connect(pid):
                    found = True
                    n.cancel_scope.cancel()
            n.start_soon(via, lambda: self.peer_lookup(pid))
            n.start_soon(via, lambda: self.provider_lookup(pid))
        return found or await self.try_connect(pid)

    async def provider_lookup(self, pid: ID) -> None:
        providers = await self.dht.find_providers(rendezvous_key(pid.to_base58()), 10)
        log.info("provider lookup for %s: %s", pid, [(str(i.peer_id), len(i.addrs)) for i in providers])
        for info in providers:
            if info.peer_id == pid:
                self.learn(pid, [str(a) for a in info.addrs])

    async def peer_lookup(self, pid: ID) -> None:
        closest = await self.dht.peer_routing.find_closest_peers_network(pid.to_bytes())
        log.info("DHT lookup for %s: target among %d closest=%s, known addrs=%d",
                 pid, len(closest), pid in closest, len(self.addrs(pid)))

    # --- staying reachable and findable ---
    async def announce(self) -> bool:
        """Self-lookup (so the DHT peers closest to our ID know our addrs) + provider records under our own key
        (to be found by ID) and the shared MESH_KEY (to be found by nodes bootstrapping into the mesh)."""
        if self.dht.get_routing_table_size() == 0:
            return False
        with trio.move_on_after(120):
            closest = await self.dht.peer_routing.find_closest_peers_network(self.host.get_id().to_bytes())
            provided = bool(await self.dht.provide(rendezvous_key(self.me))) and bool(await self.dht.provide(MESH_KEY))
            log.info("announced: %d closest peers, provider records stored=%s", len(closest), provided)
            return provided
        log.info("announce timed out")
        return False

    # --- joining the mesh: get some first chat peers, then grow through peer exchange ---
    async def find_mesh_peers(self) -> list[PeerInfo]:
        """Provider records under MESH_KEY, i.e. chat peers we didn't know about. Asks the DHT peers closest to the
        key directly: py-libp2p's find_providers() returns its local cache whenever that is non-empty (and it
        always holds at least ourselves once we've announced), so it would never ask the network."""
        key, store = _encode_key(MESH_KEY), self.dht.provider_store
        found = {i.peer_id: i for i in store.get_providers(key)}  # records we hold as a DHT server near the key
        async with trio.open_nursery() as n:
            async def ask(peer: ID) -> None:
                with trio.move_on_after(15):
                    try:
                        providers, _ = await store._get_providers_from_peer_with_closers(peer, key)
                        found.update((i.peer_id, i) for i in providers)  # fresher addrs than our cache
                    except Exception as e:
                        log.debug("mesh provider query to %s failed: %s", peer, e)
            for peer in await self.dht.peer_routing.find_closest_peers_network(key):
                n.start_soon(ask, peer)
        found.pop(self.host.get_id(), None)
        return list(found.values())

    async def discover(self) -> bool:
        """While connected to fewer than MESH_TARGET chat peers: collect candidates from the mesh's provider
        records and from peer exchange with the chat peers we have, then dial a random batch of them."""
        have = self.chat_peers()
        if len(have) >= MESH_TARGET:
            return True
        if self.dht.get_routing_table_size() == 0 and not have:
            return False  # nothing to ask yet; retry soon
        candidates: set[ID] = set()
        async with trio.open_nursery() as n:
            async def from_dht() -> None:
                with trio.move_on_after(60), contextlib.suppress(Exception):
                    for info in await self.find_mesh_peers():
                        self.learn(info.peer_id, [str(a) for a in info.addrs])
                        candidates.add(info.peer_id)

            async def from_peer(pid: ID) -> None:  # peer exchange (request() also learns their addresses)
                with contextlib.suppress(Exception):
                    candidates.update(ID.from_base58(p["id"]) for p in (await self.request(pid, "peers"))["peers"])
            n.start_soon(from_dht)
            for pid in have:
                n.start_soon(from_peer, pid)
        fresh = [p for p in candidates if p != self.host.get_id() and p not in have and not self.connected(p)
                 and not self.blocked(p.to_base58())]
        random.shuffle(fresh)  # spread connections out instead of everyone piling onto the same few peers
        batch = fresh[:2 * (MESH_TARGET - len(have))]  # some will be offline or unreachable
        async with trio.open_nursery() as n:
            for pid in batch:
                n.start_soon(self.try_connect, pid)
        log.info("discovery: %d chat peers connected (was %d), %d candidates, dialled %d",
                 len(self.chat_peers()), len(have), len(fresh), len(batch))
        return True

    def chat_peers(self) -> list[ID]:
        """Connected, unblocked peers that speak the chat protocol."""
        return [p for p in self.host.get_connected_peers() if self.speaks(p, PROTO) and not self.blocked(p.to_base58())]

    def chat_relays(self) -> list[ID]:
        """Connected chat peers offering the relay (hop) service, i.e. usable rendezvous points."""
        return [p for p in self.chat_peers() if self.speaks(p, HOP_PROTOCOL_ID)]

    # Relay only for fellow chat peers (a rendezvous point for them, not a public relay), never to or from a blocked
    # peer. Checked on every hop request, not once per stream: one hop stream can carry any number of them.
    def relay_allowed(self, pid: ID) -> bool:
        return self.speaks(pid, PROTO) and not self.blocked(pid.to_base58())

    async def relay_reserve(self, stream: INetStream, msg: Any) -> None:
        src = stream.muxed_conn.peer_id  # the library would reserve for whatever peer ID the message names
        ok = self.relay_allowed(src) and (not msg.peer.id or msg.peer.id == src.to_bytes())
        await (self.lib_reserve(stream, msg) if ok else stream.reset())

    async def relay_connect(self, stream: INetStream, msg: Any) -> None:
        ok = self.relay_allowed(stream.muxed_conn.peer_id) and not self.blocked(ID(msg.peer.id).to_base58())
        await (self.lib_connect(stream, msg) if ok else stream.reset())

    async def reserve(self, relay: ID) -> None:
        d = self.relay.discovery
        with trio.move_on_after(20):
            if d.get_relay_info(relay) is None:
                await d._add_relay(relay)
            await d.make_reservation(relay)

    async def keep_relays(self) -> bool:
        """Keep a reservation on every connected chat peer (so others sharing it can reach us) and, while not
        publicly reachable, on RELAY_TARGET public relays; drop dead reservations and renew before expiry."""
        d, now = self.relay.discovery, time.time()
        for p in d.get_relays():
            info = d.get_relay_info(p)
            if info and info.has_reservation and (not self.connected(p) or (info.reservation_expires_at or now + 1e9) - now < 300):
                info.has_reservation = False
        reserved = set(self.reserved_relays())
        async with trio.open_nursery() as n:
            for p in self.chat_relays():
                if p not in reserved:
                    n.start_soon(self.reserve, p)
        public = [p for p in self.reserved_relays() if not self.speaks(p, PROTO)]
        hop = [p for p in self.host.get_connected_peers() if self.speaks(p, HOP_PROTOCOL_ID)]
        log.info("relays: %d connected peers offer relaying, %d known to discovery, reserved on %d public + %d chat",
                 len(hop), len(d.get_relays()), len(public), len(self.reserved_relays()) - len(public))
        if self.reachability != AutoNATStatus.PUBLIC and len(public) < RELAY_TARGET:
            # Prefer relays with a public address (only those can be advertised), try a few at a time.
            candidates = [p for p in hop if p not in reserved and not self.speaks(p, PROTO)
                          and any(addr_rank(a) == 0 and not is_circuit(a) for a in self.addrs(p))]
            random.shuffle(candidates)
            async with trio.open_nursery() as n:
                for p in candidates[:RELAY_TARGET - len(public)]:
                    n.start_soon(self.reserve, p)
        return True

    async def check_nat(self) -> bool:
        """AutoNAT: become a DHT server when reachable, a DHT client when not."""
        servers = [p for p in self.host.get_connected_peers() if self.speaks(p, AUTONAT_PROTOCOL_ID)]
        if len(servers) < 4:
            return False
        with trio.move_on_after(120):
            self.reachability = await self.autonat.check_reachability(random.sample(servers, min(8, len(servers))))
        public = self.reachability == AutoNATStatus.PUBLIC
        if self.reachability != AutoNATStatus.UNKNOWN:
            await self.dht.switch_mode(DHTMode.SERVER if public else DHTMode.CLIENT)
        log.info("reachability: %s (asked %d AutoNAT servers)", self.reachability, len(servers))
        return self.reachability != AutoNATStatus.UNKNOWN

    async def every(self, seconds: float, job: Callable[[], Awaitable[bool | None]], retry: float = 15) -> None:
        """Run job forever: every `seconds` after it reports success (True/None), sooner (`retry`) after False."""
        while True:
            ok: bool | None = False
            try:
                ok = await job()
            except Exception as e:
                log.debug("%s failed: %s", job.__name__, e)
            await trio.sleep(seconds if ok is not False else retry)

    async def watch_addrs(self) -> bool:
        """When our advertised addrs change (e.g. new relay reservation): push identify and re-announce."""
        addrs = {str(a) for a in self.host.get_addrs()}
        if addrs != self.last_addrs:
            self.last_addrs = addrs
            with trio.move_on_after(30):
                await push_identify_to_peers(self.host)
            self.reannounce.set()
        return True

    async def announce_loop(self) -> None:
        while True:
            try:
                announced = await self.announce()
            except Exception as e:  # a DHT error must not escape maintain() and take the node down
                log.info("announce failed: %s", e)
                announced = False
            if not announced:
                await trio.sleep(15)
                continue
            self.reannounce = trio.Event()
            with trio.move_on_after(600):  # refresh every 10 min, or right away when our addrs change
                await self.reannounce.wait()

    async def maintain(self) -> None:
        async with trio.open_nursery() as n:
            n.start_soon(self.every, 300, self.check_nat)
            n.start_soon(self.every, 15, self.keep_relays)
            n.start_soon(self.every, 5, self.watch_addrs)
            n.start_soon(self.announce_loop)
            n.start_soon(self.every, 60, self.discover)

    async def background(self) -> None:
        """Reconnect to known/bootstrap peers on start; then greet every chat peer we connect to."""
        async with trio.open_nursery() as n:
            for addr in self.bootstrap:
                try:
                    info = info_from_p2p_addr(Multiaddr(addr))
                except Exception:
                    continue  # e.g. /dnsaddr: resolved by libp2p's bootstrap discovery instead
                self.learn(info.peer_id, [str(a) for a in info.addrs])
                n.start_soon(self.try_connect, info.peer_id)
            for known, p in self.store.recent_peers(DIAL_ON_START).items():
                self.learn(ID.from_base58(known), p.get("addrs", []))
                n.start_soon(self.try_connect, ID.from_base58(known))
            n.start_soon(self.maintain)
            seen: set[ID] = set()
            while True:
                while not self.found.empty():
                    info = self.found.get()
                    if not self.blocked(info.peer_id.to_base58()):
                        self.learn(info.peer_id, [str(a) for a in info.addrs])
                        n.start_soon(self.try_connect, info.peer_id)
                for pid in self.host.get_connected_peers():
                    if pid not in seen:
                        seen.add(pid)
                        n.start_soon(self.dht.add_peer, pid)
                    if pid not in self.greeted and self.speaks(pid, PROTO) and not self.blocked(pid.to_base58()):
                        self.greeted.add(pid)
                        n.start_soon(self.greet, pid)
                await trio.sleep(2)

    # --- local control socket used by the CLI ---
    async def control(self, conn: trio.SocketStream, stop: trio.CancelScope) -> None:
        try:  # a misbehaving or impatient client must never take the daemon down
            buf = b""
            while chunk := await conn.receive_some(65536):
                buf += chunk
            req = json.loads(buf)
            try:
                reply = await self.command(req)
            except Exception as e:
                reply = {"error": str(e) or type(e).__name__}
            await conn.send_all(json.dumps(reply).encode() + b"\n")
            await conn.aclose()
            if req.get("cmd") == "stop":
                stop.cancel()
        except Exception as e:
            log.debug("control connection failed: %s", e)

    async def command(self, req: Json) -> Json:
        if req["cmd"] == "status":
            conns = self.host.get_connected_peers()
            return {"id": self.me, "addrs": [str(a) for a in self.host.get_addrs()],
                    "reachability": {AutoNATStatus.PUBLIC: "public", AutoNATStatus.PRIVATE: "behind NAT"}.get(
                        self.reachability, "unknown"),
                    "dht": f"{self.dht.mode.value}, {self.dht.get_routing_table_size()} peers in routing table",
                    "relays": [p.to_base58() for p in self.reserved_relays()], "connections": len(conns),
                    "connected": [[p.to_base58(), self.link(p)] for p in conns if self.store.has_peer(p.to_base58())]}
        if req["cmd"] == "stop":
            return {}
        pid = ID.from_base58(req["peer"])
        error = "not found"
        mid = os.urandom(8).hex()  # the same for every attempt, so the recipient can drop duplicates
        for attempt in range(3):  # a connection can vanish right after connecting (relay limits, DCUtR handover)
            if not await self.find(pid):
                return {"error": f"could not reach {req['peer']}"}
            try:
                if req["cmd"] == "send":
                    await self.request(pid, "msg", text=req["text"], id=mid)
                    await self.astore.log_message(req["peer"], "out", req["text"])
                    return {}
                if req["cmd"] == "peers":
                    return await self.request(pid, "peers")
                return {}  # "find"
            except Exception as e:
                error = f"{type(e).__name__}: {e}"
                log.info("request to %s failed (attempt %d): %s", pid, attempt + 1, error)
                await trio.sleep(1)
        return {"error": f"could not deliver to {req['peer']}: {error}"}

    async def slow_start(self) -> None:
        """UPnP + public bootstrap take seconds; libp2p would block host.run() on them, so do them in the background."""
        if self.upnp is not None:
            with trio.move_on_after(30):
                if await self.upnp.discover():
                    self.upnp_ports = [(p, proto) for p in {a.value_for_protocol("tcp") for a in self.listen
                                                            if "/tcp/" in str(a)} for proto in ("TCP", "UDP")]
                    for port, proto in self.upnp_ports:
                        await self.upnp.add_port_mapping(int(port), proto)
        if self.boot is not None:
            await self.boot.start()

    async def shutdown(self) -> None:
        if self.boot is not None:
            self.boot.stop()
        with trio.move_on_after(5):
            for port, proto in self.upnp_ports:
                await self.upnp.remove_port_mapping(int(port), proto)

    async def run(self) -> None:
        self.boot, self.host.bootstrap = self.host.bootstrap, None  # type: ignore[attr-defined]
        self.upnp, self.host.upnp = self.host.upnp, None  # type: ignore[attr-defined]
        self.upnp_ports = []
        try:
            await self.serve()
        finally:
            self.astore.store.close()
            with trio.CancelScope(shield=True):
                await self.shutdown()

    async def serve(self) -> None:
        async with self.host.run(self.listen), background_trio_service(self.dht), \
                background_trio_service(self.circuit), background_trio_service(self.relay.discovery), \
                background_trio_service(self.dcutr), trio.open_nursery() as n:
            click.echo(f"peer id: {self.me}")
            for a in self.host.get_addrs():
                click.echo(f"listening: {a}")
            n.start_soon(self.slow_start)
            n.start_soon(self.background)
            if os.path.exists(self.store.sock):  # only bind once the host is up, so the CLI never waits on a half-started node
                os.unlink(self.store.sock)
            sock = trio.socket.socket(trio.socket.AF_UNIX)
            await sock.bind(self.store.sock)
            os.chmod(self.store.sock, 0o600)  # before listen(): until then nobody can connect
            sock.listen()
            n.start_soon(trio.serve_listeners, lambda c: self.control(c, n.cancel_scope), [trio.SocketListener(sock)])
        os.unlink(self.store.sock)


# ---------- CLI ----------

def daemon_call(store: Store, timeout: float = 90, **req: Any) -> Json:
    try:
        with socket.socket(socket.AF_UNIX) as s:
            s.settimeout(timeout)
            s.connect(store.sock)
            s.sendall(json.dumps(req).encode())
            s.shutdown(socket.SHUT_WR)
            buf = b""
            while not buf.endswith(b"\n") and (chunk := s.recv(65536)):
                buf += chunk
    except (FileNotFoundError, ConnectionRefusedError):
        raise click.ClickException("daemon is not running; start it with `gnosis_mesh.py start -d`")
    except TimeoutError:
        raise click.ClickException("daemon did not respond in time")
    reply = json.loads(buf)
    if "error" in reply:
        raise click.ClickException(reply["error"])
    return reply


def running(store: Store) -> bool:
    try:
        daemon_call(store, timeout=5, cmd="status")
        return True
    except click.ClickException:
        return False


def fmt_msg(store: Store, m: Json) -> str:
    who = "me → " + store.label(m["peer"]) if m["dir"] == "out" else store.label(m["peer"])
    return f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(m['ts']))} [{who}] {m['text']}"


@click.group()
@click.option("--home", type=click.Path(path_type=Path), envvar="GNOSIS_MESH_HOME",
              default=Path.home() / ".gnosis-mesh", show_default=True, help="Data dir (one per identity).")
@click.pass_context
def cli(ctx: click.Context, home: Path) -> None:
    """Minimal libp2p chat. Wherever PEER is expected you may pass a peer ID, a username or a nickname."""
    ctx.obj = Store(home)


@cli.command()
@click.option("-p", "--port", default=4777, show_default=True)
@click.option("-b", "--bootstrap", multiple=True, help="Multiaddr (with /p2p/ID) of a peer to join through.")
@click.option("-d", "--detach", is_flag=True, help="Run in the background and return immediately.")
@click.option("--no-mdns", is_flag=True, help="Disable automatic LAN discovery.")
@click.option("--private", is_flag=True, help="Don't join the public IPFS DHT (only LAN/--bootstrap peers).")
@click.option("-v", "--verbose", count=True, help="-v: info logging, -vv: full libp2p debug logging.")
@click.pass_obj
def start(store: Store, port: int, bootstrap: tuple[str, ...], detach: bool, no_mdns: bool, private: bool,
          verbose: int) -> None:
    """Start the chat node (foreground prints incoming messages; -d detaches)."""
    if running(store):
        raise click.ClickException("a node is already running for this --home")
    if detach:
        args = [sys.executable, __file__, "--home", str(store.home), "start", "-p", str(port)]
        args += [x for b in bootstrap for x in ("-b", b)] + ["-v"] * verbose
        args += (["--no-mdns"] if no_mdns else []) + (["--private"] if private else [])
        with open(os.open(store.home / "daemon.log", os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600), "a") as logfile:
            subprocess.Popen(args, stdout=logfile, stderr=logfile, stdin=subprocess.DEVNULL, start_new_session=True)
        for _ in range(300):
            time.sleep(0.2)
            if running(store):
                click.echo(f"started in background, peer id: {daemon_call(store, cmd='status')['id']}")
                return
        raise click.ClickException(f"daemon failed to start, see {store.home / 'daemon.log'}")
    level = {0: logging.CRITICAL, 1: logging.INFO}.get(verbose, logging.DEBUG)
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("libp2p").setLevel(level if verbose > 1 else logging.CRITICAL)
    logging.getLogger("libp2p").propagate = verbose > 1  # libp2p silences itself unless LIBP2P_DEBUG is set
    store.prune_peers()  # the daemon also prunes hourly as it meets peers (Store.remember_peer)
    while True:  # supervise: a crash inside libp2p restarts the node instead of killing the daemon
        node = Node(store, port, list(bootstrap), echo=True, mdns=not no_mdns, public=not private)
        try:
            trio.run(node.run)
            return  # clean `stop`
        except KeyboardInterrupt:
            return
        except BaseException as e:
            log.error("node crashed (%s: %s); restarting in 5s", type(e).__name__, e)
            time.sleep(5)
        finally:
            peerDiscovery.unregister_peer_discovered_handler(node.found.put)


@cli.command()
@click.pass_obj
def stop(store: Store) -> None:
    """Stop the background node."""
    daemon_call(store, cmd="stop")
    click.echo("stopped")


@cli.command()
@click.pass_obj
def status(store: Store) -> None:
    """Show own peer ID, addresses and connected peers."""
    r = daemon_call(store, cmd="status")
    click.echo(f"peer id:      {r['id']}\nusername:     {store.username() or '-'}\n"
               f"reachability: {r['reachability']}\ndht:          {r['dht']}\nconnections:  {r['connections']}")
    for p in r["relays"]:
        click.echo(f"relay:        {store.label(p) if store.has_peer(p) else p}")
    for a in r["addrs"]:
        click.echo(f"addr:         {a}")
    for p, link in r["connected"]:
        click.echo(f"online:       {store.label(p)} ({p}) [{link}]")


@cli.command()
@click.argument("name", required=False)
@click.pass_obj
def username(store: Store, name: str | None) -> None:
    """Show or set the username you present to peers you meet."""
    if name is not None:
        store.set_config(username=name)
    click.echo(store.username() or "(no username set)")


@cli.command()
@click.argument("peer")
@click.argument("name", required=False)
@click.pass_obj
def nick(store: Store, peer: str, name: str | None) -> None:
    """Locally nickname PEER (overrides their own username). Omit NAME to remove the nickname."""
    pid = store.resolve(peer)
    store.set_nick(pid, name)
    click.echo(f"{pid} -> {name or '(nickname removed)'}")


@cli.command()
@click.argument("peer")
@click.argument("message", nargs=-1, required=True)
@click.pass_obj
def send(store: Store, peer: str, message: tuple[str, ...]) -> None:
    """Send MESSAGE to PEER (found by ID via known peers, peer exchange and the DHT)."""
    pid = store.resolve(peer)
    if store.is_blocked(pid):
        raise click.ClickException("that peer is blocked; unblock it first")
    daemon_call(store, cmd="send", peer=pid, text=" ".join(message))
    click.echo(f"sent to {store.label(pid)}")


@cli.command()
@click.argument("peer", required=False)
@click.option("-n", "--last", default=20, show_default=True, help="Number of messages to show.")
@click.option("-f", "--follow", is_flag=True, help="Keep printing new messages.")
@click.pass_obj
def read(store: Store, peer: str | None, last: int, follow: bool) -> None:
    """Read messages, optionally only the conversation with PEER."""
    pid = store.resolve(peer) if peer else None
    seen, limit = 0, last or None
    while True:
        # Every message up to `top` is now either shown or hidden (blocked or another peer's); skip past both, so a
        # poll only scans new rows and unblocking someone later doesn't replay their old messages.
        top = store.last_message_id()
        for m in store.messages(peer=pid, after=seen, last=limit, include_blocked=False):
            click.echo(fmt_msg(store, m))
            top = max(top, m["id"])
        if not follow:
            return
        seen, limit = top, None
        time.sleep(1)


@cli.command()
@click.option("--from", "source", metavar="PEER", help="Ask PEER for the peers it has encountered instead.")
@click.option("--named", is_flag=True, help="Only peers with a username or nickname.")
@click.pass_obj
def peers(store: Store, source: str | None, named: bool) -> None:
    """List encountered peers with their usernames and your nicknames."""
    cfg = store.config()
    if source:
        rows = {p["id"]: p for p in daemon_call(store, cmd="peers", peer=store.resolve(source))["peers"]}
    else:
        rows = store.peers()
    for pid, p in rows.items():
        nickname, name = cfg["nicks"].get(pid, ""), p.get("name", "")
        if named and not (nickname or name):
            continue
        flags = " [blocked]" if pid in cfg["blocked"] else ""
        click.echo(f"{pid}  username={name or '-'}  nick={nickname or '-'}{flags}")


@cli.command()
@click.argument("peer")
@click.pass_obj
def find(store: Store, peer: str) -> None:
    """Locate and connect to PEER by its ID."""
    pid = store.resolve(peer)
    daemon_call(store, cmd="find", peer=pid)
    click.echo(f"connected to {store.label(pid)}")


@cli.command()
@click.argument("peer")
@click.pass_obj
def block(store: Store, peer: str) -> None:
    """Block PEER: drop its messages and refuse its requests."""
    pid = store.resolve(peer)
    store.block(pid)
    click.echo(f"blocked {pid}")


@cli.command()
@click.argument("peer")
@click.pass_obj
def unblock(store: Store, peer: str) -> None:
    """Unblock PEER."""
    pid = store.resolve(peer)
    store.unblock(pid)
    click.echo(f"unblocked {pid}")


@cli.command()
@click.pass_obj
def blocked(store: Store) -> None:
    """List blocked peers."""
    for pid in store.config()["blocked"]:
        click.echo(f"{pid}  {store.label(pid)}")


if __name__ == "__main__":
    cli()
