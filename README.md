# gnosis-mesh

A small peer-to-peer chat for the terminal, built on [py-libp2p](https://github.com/libp2p/py-libp2p).
There are no servers and no accounts: you message people by their **libp2p peer ID**, and the app finds them
through the public IPFS DHT, the local network, or peers you both know. It also gets through NATs and firewalls
where it can.

Everything lives in one file, `gnosis_mesh.py`. It contains a background node (daemon) and a command-line interface
built with [click](https://click.palletsprojects.com/).

```text
$ gnosis_mesh.py username alice
$ gnosis_mesh.py start -d
started in background, peer id: 12D3KooWQbUFXhUhcSBiPWuyXNjLaSvDt6wFEiD1TDF34JFecsjB
$ gnosis_mesh.py send 12D3KooWEptJT6b8abshyeWdxC52WvkHUcf6Z7xsAQV2QCbKhZsk hi bob
sent to bob
$ gnosis_mesh.py read
2026-11-15 00:29:23 [me → bob] hi bob
2026-11-15 00:29:31 [bob] hey alice!
```

---

## Features

- **Find peers by peer ID alone.** Lookups use the public IPFS DHT (Kademlia), LAN discovery (mDNS), and peer
  exchange with chat peers you already know.
- **Joins the mesh on its own.** A new node that knows nobody finds other gnosis-mesh nodes through a shared DHT
  key, then meets more of them through peer exchange.
- **Peer lists on request.** Any peer will tell you which other chat peers it has met (`peers --from`).
- **Usernames and nicknames.** Each peer chooses a username that others see when they meet it. You can override
  anyone's username with your own local nickname. Every command accepts a peer ID, a username or a nickname.
- **Blocking.** Blocked peers can't message you or use you as a relay, and their messages are hidden from `read`.
- **Runs in the background.** `start -d` detaches a daemon, and the other commands talk to it.
- **Gets through NATs:**
  - TCP and QUIC, over IPv4 and IPv6;
  - UPnP port mapping;
  - AutoNAT reachability detection;
  - Circuit Relay v2, using public relays and any chat peer you share with the other person;
  - DCUtR hole punching, which upgrades relayed connections to direct ones.
- **Encrypted connections.** libp2p encrypts every connection end to end, even when it goes through a relay
  (see [Security and privacy](#security-and-privacy)).

---

## Install

You need Python 3.11+ (tested on 3.13) on Linux or macOS.

py-libp2p depends on `fastecdsa`, which is compiled from source and needs the GMP and Python headers:

```bash
# Debian / Ubuntu
sudo apt install libgmp-dev python3-dev
# Fedora
sudo dnf install gmp-devel python3-devel
# macOS
brew install gmp
```

Then install the Python dependencies in a virtual environment:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

The examples below assume an alias:

```bash
alias gnosis-mesh="$PWD/.venv/bin/python $PWD/gnosis_mesh.py"
```

---

## Quick start

```bash
gnosis-mesh username alice      # choose the name other peers will see (optional)
gnosis-mesh start -d            # start the node in the background; prints your peer ID
gnosis-mesh status              # your peer ID, addresses, relays and who's online
```

Give your peer ID to a friend. Once they're running a node too:

```bash
gnosis-mesh send 12D3KooW...theirID... hello!   # the first message finds them by ID
gnosis-mesh nick 12D3KooW...theirID... bob      # optional: your own name for them
gnosis-mesh send bob how are you?               # nicknames and usernames work anywhere a peer is expected
gnosis-mesh read bob                            # your conversation with bob
gnosis-mesh read -f                             # follow all incoming messages live
gnosis-mesh stop
```

Run `gnosis-mesh start` without `-d` to keep the node in the foreground. It then prints incoming messages as they
arrive.

---

## Commands

Wherever `PEER` appears, you can give a **peer ID**, a **username** or a **nickname**
(see [Names](#names-usernames-and-nicknames)).

| Command | What it does |
|---|---|
| `start [-d] [-p PORT] [-b ADDR]... [--private] [--no-mdns] [-v\|-vv]` | Start the node. `-d` runs it in the background. `-p` sets the port, used for TCP and for UDP/QUIC (default 4777). `-b` adds a bootstrap peer (a multiaddr ending in `/p2p/<id>`; repeatable). `--private` stays off the public IPFS network. `--no-mdns` turns off LAN discovery. `-v` / `-vv` turn on info / full libp2p debug logging. |
| `stop` | Stop the background node. |
| `status` | Show your peer ID and username; reachability (public, behind NAT or unknown); DHT mode; relay reservations; the addresses you advertise; and online chat peers marked `[direct]` or `[relayed]`. |
| `username [NAME]` | Show or set the username you present to peers you meet. |
| `nick PEER [NAME]` | Give PEER a local nickname that overrides their own username. Leave out NAME to remove it. |
| `send PEER MESSAGE...` | Send a message. Finds PEER first if you're not already connected. |
| `read [PEER] [-n N] [-f]` | Show the last N messages (default 20), optionally just your conversation with PEER. `-f` keeps printing new messages. `-n 0` shows everything. |
| `peers [--named] [--from PEER]` | List chat peers you've met, with their usernames and your nicknames. `--named` hides peers that have neither. `--from PEER` asks PEER for the chat peers *it* has met. |
| `find PEER` | Locate and connect to PEER without sending anything. |
| `block PEER` / `unblock PEER` / `blocked` | Manage your block list. |

All commands accept `--home DIR` (or the `GNOSIS_MESH_HOME` environment variable) to choose the data directory.
See [Running several identities](#running-several-identities).

`username`, `nick`, `block`, `read` and `peers` (without `--from`) work even when the node isn't running.

---

## Names: usernames and nicknames

- A **username** is what a peer calls itself (`gnosis-mesh username alice`). Every request includes it, so peers
  learn it as soon as they meet you, and they see changes the next time you talk.
- A **nickname** is what *you* call a peer (`gnosis-mesh nick <peer> bob`). It's stored only on your machine and
  always takes priority over their username in output and when resolving names.

When you pass a name to any command, the CLI resolves it in this order:

1. your nicknames;
2. a valid peer ID, taken as is (so a peer can't claim someone else's ID as its username);
3. usernames of peers you've met (if two peers claim the same username, you're asked to use the peer ID or a
   nickname instead).

Usernames only resolve for peers you've already met. To reach someone for the first time, use their peer ID.

---

## How peers find each other

When you message a peer ID you aren't connected to, the node tries these in order, stopping at the first that
works:

1. **Addresses it already knows**, from earlier conversations, LAN discovery or bootstrap peers.
2. **Peer exchange.** It asks the chat peers it's connected to for the peers *they* have met, along with their
   addresses.
3. **The public DHT**, two lookups in parallel:
   - a Kademlia *find-peer* query for the ID itself;
   - a lookup of the target's *provider record*: every node publishes one, with its current addresses, under a
     key derived from its peer ID.

To stay findable, each node re-announces itself every 10 minutes, and straight away whenever its addresses
change (for example after getting a new relay reservation). Announcing means two things:

- it looks up its own ID, so the DHT servers closest to it know its addresses;
- it republishes its provider record.

**Speed.** In testing:
- a peer three DHT hops away on the same network was found in under a second;
- a peer behind NAT on the public IPFS network took about 30–60 s on first contact.

After that, peers reconnect straight away using the addresses they've stored.

### Joining the mesh

A brand-new node knows no other chat peers. To get its first ones:

- **Every node announces itself under one shared DHT key** (`MESH_KEY`) as well as its own, so any node can look
  up "some gnosis-mesh nodes" without knowing a single peer ID.
- **Every 60 s, while connected to fewer than 8 chat peers**, the node:
  1. asks the DHT servers closest to that key for its provider records;
  2. asks each chat peer it's connected to for the peers *it* has met (peer exchange);
  3. dials a random selection of the new candidates. The selection is random so that new nodes don't all pile
     onto the same few peers.

Every peer it reaches is saved in `store.db`, and the 64 most recently seen are redialled on the next start. So
the DHT is only needed the first time, or when every known peer has gone away.

**Measured on the public IPFS DHT:** a brand-new node, with mDNS off and no bootstrap peers or saved peers, found
and connected to the only other gnosis-mesh node about 60 s after starting. Both nodes were behind NAT. That
was its first discovery round once its DHT routing table had filled.

Use `--private` for a closed group (a LAN, or your own bootstrap node with `-b`). Discovery then uses only
mDNS, peer exchange and a DHT made up of the chat peers themselves.

---

## How peers connect (NAT traversal)

Most machines sit behind a NAT or firewall that blocks incoming connections. Once the node knows where a peer
might be, it tries several ways in parallel, staggered so the cheap options go first:

| Starts at | Path | Notes |
|---|---|---|
| 0 s | **Direct** | Every known address, public ones first, attempts 300 ms apart (happy eyeballs). |
| +2 s | **Through a shared chat peer** | If you and the target are both connected to some chat peer C, you connect *through* C. |
| +4 s | **Through a public relay** | Circuit Relay v2 addresses the target advertises, through public IPFS relays. |

As soon as a relayed connection exists, **DCUtR hole punching** runs. Both sides learn each other's public
address through the relay and dial each other at the same moment (TCP simultaneous open). That usually opens
a **direct** connection through both NATs, after which the relay is no longer needed. `status` shows whether
each online peer is `[direct]` or `[relayed]`.

What keeps this working:

- **Every node is a rendezvous point.** Each node runs a Circuit Relay v2 relay, but only for other chat peers.
  Each node also holds a reservation on every chat peer it's connected to. So if A and B both know C, A can
  reach B through C even when A and B are both behind NAT.
- **Public relays.** While a node isn't confirmed publicly reachable, it holds reservations on 3 public IPFS
  relays. It advertises `/p2p-circuit` addresses through them, so strangers on the internet can reach it from
  its peer ID alone. Reservations are renewed before they expire and replaced if a relay goes away.
- **AutoNAT** asks other peers to dial back, to learn whether this node is publicly reachable:
  - **public:** it becomes a DHT server and stops advertising relay addresses;
  - **behind NAT:** it stays a DHT client.
- **UPnP** asks your router to forward the port, where the router allows it.
- **QUIC and IPv6** are used alongside TCP and IPv4. On many home connections, IPv6 is directly reachable.

**Measured results:**
- **Simulated NAT:** two hosts behind separate masquerading NATs that drop unsolicited traffic, sharing one
  public chat peer. Messages arrived in about 3 s through the shared peer, and every trial upgraded to a
  direct, hole-punched connection within 5–7 s.
- **Public IPFS network:** two NATed nodes with nothing in common found each other through the DHT, connected
  through a public relay, and hole-punched to a direct connection.

---

## Running several identities

Each identity is a data directory. Give each one its own `--home` and port:

```bash
gnosis-mesh --home ~/.gnosis-mesh-work start -d -p 4778
GNOSIS_MESH_HOME=~/.gnosis-mesh-work gnosis-mesh status
```

The default is `~/.gnosis-mesh` on port 4777. The port is used for TCP and for UDP (QUIC).

### Files in the data directory

| File | Contents |
|---|---|
| `key` | Your Ed25519 private key seed (mode 600). **This is your identity**: copy it to keep your peer ID, and keep it secret. |
| `store.db` | SQLite database (mode 600, WAL mode, so `store.db-wal` and `store.db-shm` sit next to it) shared safely by the CLI and the daemon. It holds your username, nicknames and block list, the chat peers you've met (their usernames, last known addresses and when you last saw them), and your message history. Peers you haven't seen for 30 days are forgotten, and at most 2000 are kept. Peers you've nicknamed or exchanged messages with are always kept. |
| `daemon.log` | Output from the background node. |

To inspect the data by hand, use `sqlite3 ~/.gnosis-mesh/store.db`.

The CLI talks to the daemon through a Unix socket named after the data directory. It lives in `$XDG_RUNTIME_DIR`,
or the system temp directory if that isn't set. If the resulting path would be too long for a Unix socket (about
104 bytes), it falls back to the temp directory and then `/tmp`.

---

## Security and privacy

- **Connections are encrypted and authenticated end to end** with Noise (QUIC uses TLS 1.3). Your peer ID *is*
  your public key, so nobody can impersonate a peer ID without its `key` file. Relayed connections are encrypted
  end to end as well, so relays can't read your messages. py-libp2p also offers unencrypted (plaintext) and
  deprecated (SECIO) security by default; gnosis-mesh turns both off.
- **Message history and settings are stored unencrypted** in the data directory.
- **Joining the public DHT makes your peer ID and addresses public.** Anyone can look you up and see your IP
  addresses or your relay addresses. Use `--private` if that's a problem.
- **Anyone can list gnosis-mesh nodes.** The shared mesh key tells anyone who looks it up which peer IDs and
  addresses run gnosis-mesh, and anyone can publish fake records under it. Candidates are only used once they
  connect and answer on the chat protocol.
- **Your node relays traffic for other chat peers.** That's what makes the shared-peer rendezvous work. It's
  limited to peers that speak this chat protocol and aren't blocked, never reaches a blocked peer, and has
  per-connection limits of 1 hour and 16 MiB.
- **Blocking is local.** A blocked peer can still see that you exist on the DHT. Blocking only stops your node
  from talking to it or relaying for it.

---

## Protocol

Chat traffic uses the libp2p protocol `/gnosis-mesh/1.0.0`. Each exchange opens one stream, sends one JSON request
and closes its write side, then reads one JSON reply:

```jsonc
// request                                         // reply
{"t": "hello", "name": "alice"}                    {"name": "bob"}
{"t": "msg",   "name": "alice", "text": "hi"}      {"name": "bob"}
{"t": "peers", "name": "alice"}                    {"name": "bob", "peers": [{"id": "12D3…", "name": "carol", "addrs": ["/ip4/…"], "seen": 1794660000.0}]}
```

`name` is the sender's current username (an empty string if it hasn't set one). A blocked peer's streams are
reset without a reply.

The provider record key for a peer is the sha256 multihash of `gnosis-mesh:<peer ID>`. Every node also provides
the shared mesh key, the sha256 multihash of `gnosis-mesh:/mesh/1`.

---

## Troubleshooting

- **"daemon is not running".** Start one with `gnosis-mesh start -d`, and check you're using the same `--home`.
- **"could not reach …".** The peer may be offline, or (for a peer you've never met) not yet announced: give a
  freshly started node about a minute. Run the node with `-v` and check `daemon.log`; the
  `could not connect to …` lines show what was tried and why each attempt failed.
- **Stays `[relayed]`.** Hole punching can't get through *symmetric* NATs (common on mobile networks and
  carrier-grade NAT). The relayed connection keeps working, it just goes through a third party.
- **Logs.** `-v` logs discovery, relay and connection decisions. `-vv` adds libp2p's full debug output, which
  runs to gigabytes per hour on the public network.
- **The node crashes.** The daemon restarts it automatically after 5 s and logs `node crashed (…)` to
  `daemon.log`.

---

## Known limitations

- **AutoNAT often never reaches a verdict** on the public network, so `status` shows `reachability: unknown`.
  That's safe, because the node then behaves as if it's behind NAT (relay reservations, DHT client mode). The
  catch is that a publicly reachable node may never become a DHT server.
- **Public IPFS relays limit each relayed connection** to about 2 minutes and 128 KiB (Kubo's defaults). That's
  plenty for chat and for setting up a hole punch. If hole punching fails, the next message just reconnects.
- **Hole punching only uses TCP addresses**, because py-libp2p's DCUtR predicts TCP addresses only. QUIC is
  still used for direct dials.
- **First contact over the public DHT is slow** (around 30–60 s), mostly because py-libp2p's Kademlia lookups
  are slow.
- **The mesh key only finds a sample of nodes.** Each DHT server keeps at most 20 provider records per key, so
  a lookup returns a few dozen nodes at most. Peer exchange finds the rest.
- **Messages to offline peers aren't queued.** `send` fails if the peer can't be reached.

### py-libp2p workarounds

`gnosis_mesh.py` works around several py-libp2p 0.8.1 issues. Each one is commented in the code:

- `BasicHost.get_addrs()` mangles `/p2p-circuit` addresses, so the node builds its advertised address list
  itself.
- The wildcard-address helper drops `/quic-v1`, so the node expands `0.0.0.0` / `::` itself.
- The peerstore evicts its oldest entries (the longest-connected chat peers, or the node's *own* entry, which
  crashes identify) once full, so the node raises its limit from 1000 to 20,000 and serves its own keys directly.
- DCUtR binds hole-punch dials to the *first* advertised TCP address, so addresses are advertised in order:
  IPv4 before IPv6, loopback last.
- The swarm only dials one address per transport, so the node uses its own dialer, which tries every address
  and falls back to relays.
- `host.run()` blocks on UPnP and bootstrap, so the node starts those in the background (startup takes about
  2 s instead of about 17 s).
- Relayed connections aren't dialled automatically, and stored circuit addresses are missing the
  `/p2p/<target>` suffix. The node handles both itself.
- The default security list includes plaintext and SECIO, so the node passes a Noise-only list.
- A brand-new relayed connection can vanish while DCUtR hands over to a direct one, so `send` retries
  (re-finding the peer) up to 3 times.

---

## License

Copyright (C) 2026 muntedcrocodile.

gnosis-mesh is free software: you can redistribute it and/or modify it under the terms of the
[GNU Affero General Public License, version 3](LICENSE) (`AGPL-3.0-only`), as published by the Free Software
Foundation.

In short: you may use, study, share and modify it. If you distribute it, or **let people use a modified version
over a network**, you must make the complete corresponding source available under the same licence. It comes
with no warranty. See [LICENSE](LICENSE) for the full terms.
