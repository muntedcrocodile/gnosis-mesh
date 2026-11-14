# SPDX-License-Identifier: AGPL-3.0-only

import hashlib, ipaddress, json, logging, os, queue, random, socket, subprocess, sys, tempfile, time
from collections.abc import Awaitable, Callable
from contextlib import suppress
from pathlib import Path
from typing import Any, cast

import click, multihash, trio
from click import echo
from trio import move_on_after as deadline, open_nursery as nursery
from libp2p import ID, new_host
from libp2p.abc import INetStream as Stream
from libp2p.crypto import ed25519, x25519
from libp2p.discovery.events.peerDiscovery import peerDiscovery
from libp2p.host.autonat import AutoNATService, AutoNATStatus as Nat
from libp2p.host.autonat.autonat import AUTONAT_PROTOCOL_ID
from libp2p.identity.identify_push.identify_push import push_identify_to_peers
from libp2p.kad_dht import DHTMode as Mode, KadDHT
from libp2p.network.stream.exceptions import StreamEOF
from libp2p.peer.peerinfo import PeerInfo, info_from_p2p_addr
from libp2p.relay.circuit_v2 import PROTOCOL_ID as HOP, CircuitV2Protocol, CircuitV2Transport, DCUtRProtocol
from libp2p.relay.circuit_v2.config import RelayConfig, RelayLimits, RelayRole
from libp2p.security.noise.transport import PROTOCOL_ID as NOISE, Transport as Noise
from libp2p.tools.anyio_service import background_trio_service as service
from libp2p.utils.address_validation import _safe_get_network_addrs, is_ipv6_available
from multiaddr import Multiaddr as Addr
from multiaddr.protocols import P_P2P

PROTO: Any = "/p2pchat/1.0.0"
RELAY_TARGET = 3
MARS = "/ip4/104.131.131.82/{}/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ"
IPFS_BOOTSTRAP = [
    *(f"/dnsaddr/bootstrap.libp2p.io/p2p/Qm{p}" for p in ("NnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
      "QCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa", "bLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
      "cZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt")),
    "/dnsaddr/va1.bootstrap.libp2p.io/p2p/12D3KooWKnDdG3iXw9eTFijk3EWSunZcFi54Zka4wmtqtt6rPxc8",
    MARS.format("tcp/4001"), MARS.format("udp/4001/quic-v1"),
]
Json = dict[str, Any]
log = logging.getLogger("p2pchat")
Error = click.ClickException


class Store:
    def __init__(self, home: Path) -> None:
        self.home = home
        home.mkdir(parents=True, exist_ok=True)
        run = os.environ.get("XDG_RUNTIME_DIR") or tempfile.gettempdir()
        self.sock = f"{run}/p2pchat-{hashlib.sha1(bytes(home.resolve())).hexdigest()[:12]}.sock"

    def load(self, name: str) -> Json:
        p = self.home / name
        return json.loads(p.read_text()) if p.exists() else {}

    def save(self, name: str, data: Json) -> None:
        tmp = self.home / f".{name}.tmp"
        tmp.write_text(json.dumps(data, indent=2))
        tmp.replace(self.home / name)

    def config(self) -> Json:
        return {"username": "", "nicks": {}, "blocked": [], **self.load("config.json")}

    def configure(self, **kw: Any) -> None:
        self.save("config.json", self.config() | kw)

    def peers(self) -> Json:
        return self.load("peers.json")

    def blocked(self, pid: str) -> bool:
        return pid in self.config()["blocked"]

    def key_seed(self) -> bytes:
        p = self.home / "key"
        if not p.exists():
            p.write_bytes(os.urandom(32))
            p.chmod(0o600)
        return p.read_bytes()

    def add_message(self, peer: str, direction: str, text: str) -> None:
        with open(self.home / "messages.jsonl", "a") as f:
            print(json.dumps({"ts": time.time(), "peer": peer, "dir": direction, "text": text}), file=f)

    def messages(self) -> list[Json]:
        p = self.home / "messages.jsonl"
        return [json.loads(line) for line in p.read_text().splitlines()] if p.exists() else []

    def label(self, pid: str) -> str:
        return self.config()["nicks"].get(pid) or self.peers().get(pid, {}).get("name") or f"{pid[:6]}…{pid[-6:]}"

    def resolve(self, who: str) -> str:
        named = [p for p, n in self.config()["nicks"].items() if n == who]
        named = named or [p for p, v in self.peers().items() if v.get("name") == who]
        if len(named) > 1:
            raise Error(f"'{who}' is ambiguous ({', '.join(named)}); use a peer ID")
        if named:
            return named[0]
        with suppress(Exception):
            multihash.decode(ID.from_base58(who).to_bytes())
            return who
        raise Error(f"'{who}' is not a known nickname, username or valid peer ID")


async def read_all(stream: Stream) -> bytes:
    buf = b""
    with suppress(StreamEOF):
        while chunk := await stream.read(65536):
            buf += chunk
            if len(buf) > 1 << 20:
                raise ValueError("message too large")
    return buf


def is_circuit(addr: Addr) -> bool:
    return "/p2p-circuit" in str(addr)


def addr_rank(addr: Addr) -> int:
    for proto in ("ip4", "ip6"):
        with suppress(Exception):
            ip = ipaddress.ip_address(addr.value_for_protocol(proto))
            return 3 if ip.is_loopback else 2 if ip.is_link_local else 0 if ip.is_global else 1
    return 0


def expand(addr: Addr) -> list[Addr]:
    s = str(addr)
    for fam, wildcard, loopback in ((4, "/ip4/0.0.0.0/", "127.0.0.1"), (6, "/ip6/::/", "::1")):
        if s.startswith(wildcard):
            ips = dict.fromkeys(_safe_get_network_addrs(fam) + [loopback])
            return [Addr(f"/ip{fam}/{ip}/{s[len(wildcard):]}") for ip in ips]
    return [addr]


def addr_order(addr: Addr) -> tuple[int, ...]:
    rank, s = addr_rank(addr), str(addr)
    return rank == 3, s.startswith("/ip6/"), rank, "/quic" in s


def rendezvous_key(pid: ID) -> str:
    return "1220" + hashlib.sha256(f"p2pchat:{pid}".encode()).hexdigest()


class Node:
    def __init__(self, store: Store, port: int, bootstrap: list[str], mdns: bool, public: bool) -> None:
        self.store, self.port, self.bootstrap = store, port, bootstrap
        fams = ["/ip4/0.0.0.0"] + (["/ip6/::"] if is_ipv6_available() else [])
        self.listen = [Addr(f"{f}/{t}") for f in fams for t in (f"tcp/{port}", f"udp/{port}/quic-v1")]
        keys = ed25519.create_new_key_pair(store.key_seed())
        self.host: Any = new_host(
            key_pair=keys, listen_addrs=self.listen, enable_quic=True,
            sec_opt={NOISE: Noise(keys, noise_privkey=x25519.create_new_key_pair().private_key)},
            enable_mDNS=mdns, enable_upnp=public, bootstrap=bootstrap + (IPFS_BOOTSTRAP if public else []) or None,
            bootstrap_allow_ipv6=True, addrs_factory=lambda addrs: [x for a in addrs for x in expand(a)])
        self.ps = self.host.get_peerstore()
        self.id: ID = self.host.get_id()
        self.direct_addrs = self.host.get_addrs
        vars(self.host).update(get_addrs=self.advertised, get_public_key=lambda: keys.public_key,
                               get_private_key=lambda: keys.private_key)
        self.dht = KadDHT(self.host, Mode.CLIENT if public else Mode.SERVER, enable_random_walk=True)
        self.circuit = CircuitV2Protocol(self.host, RelayLimits(3600, 16 << 20, 64, 64), allow_hop=True)
        self.relay_hop = self.circuit._handle_hop_stream
        vars(self.circuit).update(_handle_hop_stream=self.hop_acl)
        self.relay = CircuitV2Transport(self.host, self.circuit, RelayConfig(roles=RelayRole.STOP | RelayRole.CLIENT))
        self.discovery = self.relay.discovery
        self.discovery.auto_reserve = False
        self.autonat = AutoNATService(cast(Any, self.host))
        self.nat = Nat.UNKNOWN
        self.greeted: set[ID] = set()
        self.last_addrs: set[str] = set()
        self.found: queue.SimpleQueue[PeerInfo] = queue.SimpleQueue()
        self.host.set_stream_handler(PROTO, self.handle)
        self.host.set_stream_handler(AUTONAT_PROTOCOL_ID, self.autonat.handle_stream)
        peerDiscovery.register_peer_discovered_handler(self.found.put)

    def online(self) -> list[ID]:
        return self.host.get_connected_peers()

    def addrs(self, pid: ID) -> list[Addr]:
        with suppress(Exception):
            return self.ps.addrs(pid)
        return []

    def reserved(self) -> list[ID]:
        return [p for p, i in self.discovery._discovered_relays.items() if i.has_reservation]

    def advertised(self) -> list[Addr]:
        addrs = sorted(self.direct_addrs(), key=addr_order)
        if self.nat != Nat.PUBLIC:
            for relay in list(filter(self.public_addrs, self.reserved()))[:3]:
                addrs += [Addr(f"{a.decapsulate_code(P_P2P)}/p2p/{relay}/p2p-circuit/p2p/{self.id}")
                          for a in self.public_addrs(relay)[:2]]
        return addrs

    def public_addrs(self, pid: ID) -> list[Addr]:
        return sorted((a for a in self.addrs(pid) if not is_circuit(a) and addr_rank(a) == 0), key=str)

    def learn(self, pid: ID, addrs: list[Any]) -> None:
        ma = []
        for a in addrs:
            with suppress(Exception):
                ma.append(Addr(str(a)))
        if ma and pid != self.id:
            self.ps.add_addrs(pid, ma, 0)

    def speaks(self, pid: ID, proto: str) -> bool:
        return bool(self.ps.supports_protocols(pid, [proto]))

    def chat_peer(self, pid: ID) -> bool:
        return self.speaks(pid, PROTO) and not self.store.blocked(str(pid))

    def link(self, pid: ID) -> str:
        for c in self.host.get_network().connections.get(pid, []):
            if not any(map(is_circuit, c.get_transport_addresses())):
                return "direct"
        return "relayed"

    def remember(self, pid: ID, name: str | None) -> None:
        peers = self.store.peers()
        entry = peers.setdefault(str(pid), {})
        entry.update(addrs=list(map(str, self.addrs(pid))) or entry.get("addrs", []), seen=time.time())
        if name:
            entry["name"] = name
        self.store.save("peers.json", peers)

    async def handle(self, stream: Stream) -> None:
        pid = stream.muxed_conn.peer_id
        sender = str(pid)
        try:
            if self.store.blocked(sender):
                raise PermissionError
            req = json.loads(await read_all(stream))
            self.remember(pid, req.get("name"))
            reply: Json = {"name": self.store.config()["username"]}
            if req["t"] == "msg":
                self.store.add_message(sender, "in", req["text"])
                echo(f"\n[{self.store.label(sender)}] {req['text']}")
            elif req["t"] == "peers":
                reply["peers"] = [{"id": k, **v} for k, v in self.store.peers().items()
                                  if k != sender and not self.store.blocked(k)]
            await stream.write(json.dumps(reply).encode())
            await stream.close()
            self.greeted.add(pid)
        except Exception:
            await stream.reset()

    async def request(self, pid: ID, t: str, **body: Any) -> Json:
        with trio.fail_after(30):
            stream = await self.host.new_stream(pid, [PROTO])
            await stream.write(json.dumps({"t": t, "name": self.store.config()["username"], **body}).encode())
            await stream.close_write()
            reply = json.loads(await read_all(stream))
            await stream.close()
        self.remember(pid, reply.get("name"))
        for p in reply.get("peers", []):
            with suppress(Exception):
                self.learn(ID.from_base58(p["id"]), p.get("addrs", []))
        return reply

    async def greet(self, pid: ID, t: str = "hello") -> None:
        with suppress(Exception):
            await self.request(pid, t)

    async def relay_dial(self, circuit: Addr, pid: ID) -> None:
        relay_ma, _ = self.relay.parse_circuit_ma(circuit)
        if (base := relay_ma.decapsulate_code(P_P2P)).protocols():
            self.learn(ID.from_base58(str(relay_ma.get_peer_id())), [base])
        raw = await self.relay._dial_via_circuit_addr(circuit, PeerInfo(pid, [circuit]))
        await self.host.upgrade_outbound_connection(raw, pid)

    async def connect(self, pid: ID) -> bool:
        if pid == self.id:
            return False
        if pid not in self.online():
            addrs = dict.fromkeys(self.addrs(pid))
            direct = sorted((a for a in addrs if not is_circuit(a)), key=addr_rank)
            target = f"/p2p/{pid}"
            relayed = [Addr(str(a).removesuffix(target) + target) for a in addrs if is_circuit(a)]
            shared = [Addr(f"/p2p/{p}/p2p-circuit{target}") for p in self.chat_relays() if p != pid]
            net = self.host.get_network()
            net.unblock_peer(pid)
            won, began = "", trio.current_time()
            errors: list[str] = []
            async with nursery() as n:
                async def attempt(delay: float, addr: Addr) -> None:
                    nonlocal won
                    await trio.sleep(delay)
                    with deadline(20):
                        try:
                            await (self.relay_dial if is_circuit(addr) else net.dial_addr)(addr, pid)
                            won = str(addr)
                            n.cancel_scope.cancel()
                        except Exception as e:
                            errors.append(f"{addr}: {type(e).__name__}: {e}")
                for base, tier in ((0, direct[:16]), (2, shared[:8]), (4, relayed[:8])):
                    for i, addr in enumerate(tier):
                        n.start_soon(attempt, (base + i * 0.3) * (1 if direct else 0.5), addr)
            if not won:
                log.info("could not connect to %s: %s", pid, "; ".join(errors[:4]) or "no addresses")
                return False
            log.info("connected to %s via %s in %.1fs", pid, won, trio.current_time() - began)
        if pid not in self.greeted:
            self.greeted.add(pid)
            await self.greet(pid)
        return True

    async def find(self, pid: ID) -> bool:
        if await self.connect(pid):
            return True
        async with nursery() as n:
            for other in self.online():
                if str(other) in self.store.peers():
                    n.start_soon(self.greet, other, "peers")
        if await self.connect(pid):
            return True
        async with nursery() as n:
            async def via(lookup: Awaitable[Any]) -> None:
                with deadline(60), suppress(Exception):
                    await lookup
                if await self.connect(pid):
                    n.cancel_scope.cancel()
            n.start_soon(via, self.dht.peer_routing.find_closest_peers_network(pid.to_bytes()))
            n.start_soon(via, self.provider_lookup(pid))
        return await self.connect(pid)

    async def provider_lookup(self, pid: ID) -> None:
        for info in await self.dht.find_providers(rendezvous_key(pid), 10):
            if info.peer_id == pid:
                self.learn(pid, info.addrs)

    async def announce(self) -> bool:
        if not self.dht.get_routing_table_size():
            return False
        with deadline(120):
            await self.dht.peer_routing.find_closest_peers_network(self.id.to_bytes())
            provided = bool(await self.dht.provide(rendezvous_key(self.id)))
            log.info("announced, provider record stored=%s", provided)
            return provided
        return False

    async def watch_addrs(self) -> bool:
        if (addrs := {str(a) for a in self.advertised()}) != self.last_addrs:
            self.last_addrs = addrs
            with deadline(30):
                await push_identify_to_peers(self.host)
            await self.announce()
        return True

    def chat_relays(self) -> list[ID]:
        return [p for p in self.online() if self.chat_peer(p) and self.speaks(p, HOP)]

    async def hop_acl(self, stream: Stream) -> None:
        if self.chat_peer(stream.muxed_conn.peer_id):
            await self.relay_hop(stream)
        else:
            await stream.reset()

    async def reserve(self, relay: ID) -> None:
        with deadline(20):
            if relay not in self.discovery.get_relays():
                await self.discovery._add_relay(relay)
            await self.discovery.make_reservation(relay)

    async def keep_relays(self) -> None:
        for p, i in self.discovery._discovered_relays.items():
            if p not in self.online() or (i.reservation_expires_at or 1e11) < time.time() + 300:
                i.has_reservation = False
        reserved = self.reserved()
        wanted = [p for p in self.chat_relays() if p not in reserved]
        if self.nat != Nat.PUBLIC:
            candidates = [p for p in self.online() if self.speaks(p, HOP) and p not in reserved
                          and not self.speaks(p, PROTO) and self.public_addrs(p)]
            random.shuffle(candidates)
            wanted += candidates[:RELAY_TARGET - len([p for p in reserved if not self.speaks(p, PROTO)])]
        async with nursery() as n:
            for p in wanted:
                n.start_soon(self.reserve, p)

    async def check_nat(self) -> bool:
        servers = [p for p in self.online() if self.speaks(p, AUTONAT_PROTOCOL_ID)]
        if len(servers) < 4:
            return False
        with deadline(120):
            self.nat = await self.autonat.check_reachability(random.sample(servers, min(8, len(servers))))
        log.info("reachability: %s", self.nat)
        if self.nat == Nat.UNKNOWN:
            return False
        await self.dht.switch_mode(Mode.SERVER if self.nat == Nat.PUBLIC else Mode.CLIENT)
        return True

    async def every(self, seconds: float, job: Callable[[], Awaitable[Any]]) -> None:
        while True:
            ok = False
            with suppress(Exception):
                ok = await job()
            await trio.sleep(seconds if ok else 15)

    async def background(self) -> None:
        async with nursery() as n:
            for addr in self.bootstrap:
                with suppress(Exception):
                    self.found.put(info_from_p2p_addr(Addr(addr)))
            for known, p in self.store.peers().items():
                self.found.put(PeerInfo(ID.from_base58(known), p.get("addrs", [])))
            jobs = (300, self.check_nat), (15, self.keep_relays), (5, self.watch_addrs), (600, self.announce)
            for seconds, job in jobs:
                n.start_soon(self.every, seconds, job)
            seen: set[ID] = set()
            while True:
                while not self.found.empty():
                    info = self.found.get()
                    if not self.store.blocked(str(info.peer_id)):
                        self.learn(info.peer_id, info.addrs)
                        n.start_soon(self.connect, info.peer_id)
                for pid in self.online():
                    if pid not in seen:
                        seen.add(pid)
                        n.start_soon(self.dht.add_peer, pid)
                    if pid not in self.greeted and self.chat_peer(pid):
                        self.greeted.add(pid)
                        n.start_soon(self.greet, pid)
                await trio.sleep(2)

    async def control(self, conn: trio.SocketStream, stop: trio.CancelScope) -> None:
        req = None
        with suppress(Exception):
            buf = b""
            while chunk := await conn.receive_some(65536):
                buf += chunk
            req = json.loads(buf)
            try:
                reply = await self.command(req)
            except Exception as e:
                reply = {"error": str(e) or type(e).__name__}
            await conn.send_all(json.dumps(reply).encode())
            await conn.aclose()
        if req == {"cmd": "stop"}:
            stop.cancel()

    async def command(self, req: Json) -> Json:
        cmd = req["cmd"]
        if cmd == "status":
            label, known = self.store.label, self.store.peers()
            rows = [
                ("peer id", self.id), ("username", self.store.config()["username"] or "-"),
                ("reachability", {Nat.PUBLIC: "public", Nat.PRIVATE: "behind NAT"}.get(self.nat, "unknown")),
                ("dht", f"{self.dht.mode.value}, {self.dht.get_routing_table_size()} peers in routing table"),
                ("connections", len(self.online())), *[("relay", label(str(p))) for p in self.reserved()],
                *[("addr", a) for a in self.advertised()],
                *[("online", f"{label(str(p))} ({p}) [{self.link(p)}]") for p in self.online() if str(p) in known]]
            return {"id": str(self.id), "text": "\n".join(f"{key + ':':14}{value}" for key, value in rows)}
        if cmd == "stop":
            return {}
        pid, error = ID.from_base58(req["peer"]), ""
        for _ in range(3):
            if not await self.find(pid):
                return {"error": f"could not reach {pid}"}
            try:
                if cmd == "send":
                    await self.request(pid, "msg", text=req["text"])
                    self.store.add_message(str(pid), "out", req["text"])
                return await self.request(pid, "peers") if cmd == "peers" else {}
            except Exception as e:
                error = f"{type(e).__name__}: {e}"
                await trio.sleep(1)
        return {"error": f"could not deliver to {pid}: {error}"}

    async def run(self) -> None:
        boot, upnp, self.host.bootstrap, self.host.upnp = self.host.bootstrap, self.host.upnp, None, None
        mapped = False
        try:
            async with self.host.run(self.listen), service(self.dht), service(self.circuit), \
                    service(self.discovery), service(DCUtRProtocol(self.host)), nursery() as n:
                echo(f"peer id: {self.id}")
                n.start_soon(self.background)
                with suppress(FileNotFoundError):
                    os.unlink(self.store.sock)
                sock = trio.socket.socket(trio.socket.AF_UNIX)
                await sock.bind(self.store.sock)
                sock.listen()
                n.start_soon(trio.serve_listeners, lambda c: self.control(c, n.cancel_scope),
                             [trio.SocketListener(sock)])
                with deadline(30):
                    if upnp and await upnp.discover():
                        for p in ("TCP", "UDP"):
                            mapped = await upnp.add_port_mapping(self.port, p)
                if boot:
                    await boot.start()
            os.unlink(self.store.sock)
        finally:
            with trio.CancelScope(shield=True), deadline(5):
                if boot:
                    boot.stop()
                for p in ("TCP", "UDP") if mapped else ():
                    await upnp.remove_port_mapping(self.port, p)


store: Store
peer_arg = click.argument("peer")


def call(timeout: float = 90, **req: Any) -> Json:
    try:
        with socket.socket(socket.AF_UNIX) as s:
            s.settimeout(timeout)
            s.connect(store.sock)
            s.sendall(json.dumps(req).encode())
            s.shutdown(socket.SHUT_WR)
            reply = json.load(s.makefile())
    except (FileNotFoundError, ConnectionRefusedError):
        raise Error("daemon is not running; start it with `p2pchat.py start -d`")
    except TimeoutError:
        raise Error("daemon did not respond in time")
    if "error" in reply:
        raise Error(reply["error"])
    return reply


def running() -> bool:
    with suppress(Error):
        return bool(call(timeout=5, cmd="status"))
    return False


@click.group()
@click.option("--home", type=click.Path(path_type=Path), envvar="P2PCHAT_HOME", default=Path.home() / ".p2pchat",
              show_default=True, help="Data dir (one per identity).")
def cli(home: Path) -> None:
    global store
    store = Store(home)


@cli.command()
@click.option("-p", "--port", default=4777, show_default=True)
@click.option("-b", "--bootstrap", multiple=True, help="Multiaddr (with /p2p/ID) of a peer to join through.")
@click.option("-d", "--detach", is_flag=True, help="Run in the background.")
@click.option("--no-mdns", is_flag=True, help="Disable LAN discovery.")
@click.option("--private", is_flag=True, help="Don't join the public IPFS DHT.")
@click.option("-v", "--verbose", count=True, help="-v info, -vv libp2p debug.")
def start(port: int, bootstrap: tuple[str, ...], detach: bool, no_mdns: bool, private: bool, verbose: int) -> None:
    if running():
        raise Error("a node is already running for this --home")
    if detach and not os.environ.get("P2PCHAT_DAEMON"):
        with open(logfile := store.home / "daemon.log", "a") as out:
            subprocess.Popen([sys.executable, *sys.argv], env=os.environ | {"P2PCHAT_DAEMON": "1"},
                             stdout=out, stderr=out, stdin=subprocess.DEVNULL, start_new_session=True)
        for _ in range(300):
            time.sleep(0.2)
            if running():
                return echo(f"started in background, peer id: {call(cmd='status')['id']}")
        raise Error(f"daemon failed to start, see {logfile}")
    logging.basicConfig(level=[50, 20, 10][min(verbose, 2)], format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    (lib := logging.getLogger("libp2p")).setLevel(10 if verbose > 1 else 50)
    lib.propagate = verbose > 1
    while True:
        node = Node(store, port, list(bootstrap), not no_mdns, not private)
        try:
            return trio.run(node.run)
        except KeyboardInterrupt:
            return
        except BaseException as e:
            log.error("node crashed (%s: %s); restarting in 5s", type(e).__name__, e)
            time.sleep(5)
        finally:
            peerDiscovery.unregister_peer_discovered_handler(node.found.put)


@cli.command()
def stop() -> None:
    call(cmd="stop")
    echo("stopped")


@cli.command()
def status() -> None:
    echo(call(cmd="status")["text"])


@cli.command()
@click.argument("name", required=False)
def username(name: str | None) -> None:
    if name is not None:
        store.configure(username=name)
    echo(store.config()["username"] or "(no username set)")


@cli.command()
@peer_arg
@click.argument("name", required=False)
def nick(peer: str, name: str | None) -> None:
    pid, nicks = store.resolve(peer), store.config()["nicks"]
    if name:
        nicks[pid] = name
    else:
        nicks.pop(pid, None)
    store.configure(nicks=nicks)
    echo(f"{pid} -> {name or '(nickname removed)'}")


@cli.command()
@peer_arg
@click.argument("message", nargs=-1, required=True)
def send(peer: str, message: tuple[str, ...]) -> None:
    if store.blocked(pid := store.resolve(peer)):
        raise Error("that peer is blocked; unblock it first")
    call(cmd="send", peer=pid, text=" ".join(message))
    echo(f"sent to {store.label(pid)}")


@cli.command()
@click.argument("peer", required=False)
@click.option("-n", "--last", default=20, show_default=True, help="Messages to show.")
@click.option("-f", "--follow", is_flag=True, help="Keep printing new messages.")
def read(peer: str | None, last: int, follow: bool) -> None:
    pid = store.resolve(peer) if peer else None
    count = 0
    while True:
        msgs = store.messages()
        for m in [m for m in msgs[count:] if pid in (None, m["peer"]) and not store.blocked(m["peer"])][-last:]:
            who = ("me → " if m["dir"] == "out" else "") + store.label(m["peer"])
            echo(f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(m['ts']))} [{who}] {m['text']}")
        if not follow:
            break
        count, last = len(msgs), 0
        time.sleep(1)


@cli.command()
@click.option("--from", "source", metavar="PEER", help="Ask PEER for the peers it has met.")
@click.option("--named", is_flag=True, help="Only peers with a username or nickname.")
def peers(source: str | None, named: bool) -> None:
    nicks = store.config()["nicks"]
    rows = {p["id"]: p for p in call(cmd="peers", peer=store.resolve(source))["peers"]} if source else store.peers()
    for pid, p in rows.items():
        nickname, name = nicks.get(pid, ""), p.get("name", "")
        if nickname or name or not named:
            echo(f"{pid}  username={name or '-'}  nick={nickname or '-'}" + " [blocked]" * store.blocked(pid))


@cli.command()
@peer_arg
def find(peer: str) -> None:
    call(cmd="find", peer=(pid := store.resolve(peer)))
    echo(f"connected to {store.label(pid)}")


@cli.command()
@peer_arg
def block(peer: str) -> None:
    pid = store.resolve(peer)
    store.configure(blocked=sorted({*store.config()["blocked"], pid}))
    echo(f"blocked {pid}")


@cli.command()
@peer_arg
def unblock(peer: str) -> None:
    pid = store.resolve(peer)
    store.configure(blocked=sorted({*store.config()["blocked"]} - {pid}))
    echo(f"unblocked {pid}")


@cli.command()
def blocked() -> None:
    for pid in store.config()["blocked"]:
        echo(f"{pid}  {store.label(pid)}")


if __name__ == "__main__":
    cli()
