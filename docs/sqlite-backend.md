# SQLite storage backend: assessment and prototype

Branch `experiment/sqlite-backend`. Status: a working prototype. Tests pass, and two real daemons exchanged
messages using it.

**Recommendation: adopt with changes.** Replace `config.json`, `peers.json` and `messages.jsonl` with one
`store.db` that uses the standard-library `sqlite3` module in WAL mode. Reads run inline; the daemon's writes go through `AsyncStore`.
Keep `key` as its own file. The changes still needed before merging to `main` are listed at the end.

## 1. What is wrong today, measured

The measurements use the legacy `Store` from `main` and the prototype on the same seeded data: 2000 peers with 8
addresses each, and 100k messages. All times are medians on this machine. The script is in the session scratchpad;
the numbers are reproduced here.

| Operation | JSON files | SQLite |
|---|---:|---:|
| Daemon bookkeeping per inbound request (`blocked` + `remember` + username + `label`) | 7.2 ms | 0.06 ms |
| `label(pid)` (used for every message printed) | 2.1 ms | 0.006 ms |
| `blocked(pid)` | 0.017 ms | 0.003 ms |
| `log_message` | 0.011 ms | 0.011 ms |
| `read -n 20` | 187 ms | 0.03 ms |
| One `read -f` poll with nothing new (runs every second) | 187 ms | 0.004 ms |
| Peer list for startup dialing (all of them vs the 64 most recent) | 2.3 ms | 0.15 ms |
| One-time migration of the whole data set | – | 370 ms |
| Size on disk | 15.6 MB | 13.4 MB |

The JSON costs grow linearly: `remember` rewrites all of `peers.json` on every protocol request, and `read`
re-parses the whole history. The SQLite costs stay flat as the data grows, because every hot path is an indexed
point lookup or range scan.

### Concurrency: a correction and a worse finding

* **There is no lost-update race between trio tasks today.** `Node.remember` loads, modifies and saves
  `peers.json` with no `await` in between. Trio is cooperative, so no other task can run in the middle. The race
  would appear as soon as someone moved storage onto a thread or added an `await` there.
* **Concurrent processes can corrupt the files, not just lose updates.** `Store._save` writes every save to the
  same temporary file, `.<name>.tmp`, and then calls `replace()`. When two writers overlap, their bytes interleave
  in that temporary file before it is renamed into place. Two CLI processes each running `nick` 200 times
  concurrently left `config.json` unparseable (`JSONDecodeError: Extra data`) in 2 of 2 runs. Doing the same to
  `peers.json` gave the same result. After that, every `config()` call raises. That breaks every CLI command, and in
  the daemon `handle()` resets every incoming stream. Two concurrent CLI writers are unusual for a human but easy for
  a script. With the SQLite store, the same test kept 400 of 400 nicknames, and a daemon-plus-CLI write test
  (`tests/test_concurrency.py`) loses nothing.
* **Peer exchange stops working at about 2700 peers.** One `peers` reply entry is about 384 bytes, and the reply
  contains every peer, so it exceeds `MAX_MSG` (1 MiB) at about 2700 peers. The requester's `read_all` then raises
  `message too large`. Mesh discovery makes it easy to reach that many peers. The prototype limits the reply to the
  200 most recently seen peers (`PEX_LIMIT`), with usernames capped at `MAX_NAME` (64) characters and addresses at
  `MAX_ADDRS` (32) per peer, so a reply stays well under `MAX_MSG`.

## 2. stdlib `sqlite3` vs async drivers under trio

**Use stdlib `sqlite3`: reads inline on the trio thread, daemon writes in a worker thread (`AsyncStore`).**

* **There is no trio-native driver worth using.** `aiosqlite` is asyncio-only and doesn't run under trio. Async
  SQLAlchemy needs an asyncio driver too. Every "async SQLite" library is really a worker thread plus a queue, so
  `trio.to_thread.run_sync` is the honest way to do it.
* **Writes can block for seconds, so they go to a thread.** A write waits up to `busy_timeout` (5 s) whenever
  another process holds the write lock, for example the CLI running `block`, `nick` or a startup prune. Run inline,
  that wait freezes the daemon's whole event loop, networking included. `AsyncStore` runs `remember_peer` and
  `log_message` in a worker thread instead. The hop costs about 90 µs per write (about 41 µs inline
  versus about 129 µs threaded), which is negligible next to a network round trip.
  `tests/test_async.py` holds the lock from another process for 0.5 s and checks that the event loop never pauses
  for more than 0.25 s. It also checks that the same write done inline does stall for the whole lock.
* **Writes are serialised and ordered.** `AsyncStore` uses a `trio.CapacityLimiter(1)`, which is FIFO, so writes
  run one at a time in the order they were awaited. Messages are stored in the order they were logged, and there's
  never more than one thread waiting on the lock.
* **Cancellation is safe.** `trio.to_thread.run_sync` waits for the thread to finish before delivering a
  cancellation, so a write whose task is cancelled still commits exactly once and leaves no transaction open
  (tested).
* **Reads stay inline.** In WAL mode readers never wait for writers, and a point read (`is_blocked`, `label`,
  `has_peer`) is far cheaper than a thread hop.
* **Two connections in the daemon.** The trio thread reads through the daemon's `Store`. `AsyncStore` opens its own
  `Store`, which only one worker thread uses at a time (enforced by the limiter), so no connection is ever shared
  between threads at once. SQLite's file locking serialises the two connections exactly as it does the CLI and the
  daemon.
* **One rule still matters: never hold a transaction across an `await`.** Every Store method is self-contained:
  each `_tx()` opens and commits within one synchronous call on one thread.
* **When to reconsider.** If the daemon starts running heavy queries, such as full-text search over a large
  history or bulk exports, run them through `AsyncStore.run(...)` too.

## 3. WAL mode and CLI + daemon concurrency

Each process opens one connection with these settings:

```python
sqlite3.connect(path, isolation_level=None, timeout=5)  # autocommit; timeout = busy_timeout
PRAGMA journal_mode=WAL      # readers never block the writer and vice versa; persists in the file
PRAGMA synchronous=NORMAL    # no fsync per commit; safe against app crashes; an OS crash may lose the last commit
```

* **One writer at a time, and readers never wait.** `read -f` and `status` never block the daemon, and the daemon
  never blocks them. When writers overlap (the daemon's `remember_peer` and the CLI's `block`), one of them waits up
  to `busy_timeout`.
* **Read-modify-write uses `BEGIN IMMEDIATE`** (`Store._tx`). A deferred transaction that starts as a reader and
  then tries to write fails with `SQLITE_BUSY` straight away, without honouring `busy_timeout`, when another
  writer got in first. Taking the write lock up front avoids that. Most writes are now single atomic statements
  anyway: an `UPSERT` for peers, `INSERT OR IGNORE` for blocks, `INSERT OR REPLACE` for nicknames. The CLI no
  longer reads the whole config, edits it and writes it back.
* **Caveats.**
  * WAL needs shared memory (the `-shm` file), so it doesn't work when `--home` is on a network filesystem such as
    NFS or SMB. The old JSON files didn't fully work there either. If this matters, fall back to
    `journal_mode=DELETE`, which is slower but correct.
  * The `-wal` and `-shm` files sit next to the database, and SQLite gives them the same file mode (`0600`).
  * Copying `store.db` alone while the daemon is running can miss recent commits that are still in the `-wal` file.
    For a backup, use `sqlite3 store.db ".backup copy.db"`.

## 4. Schema

```sql
settings (key TEXT PRIMARY KEY, value TEXT)           -- JSON values: username and any future keys
nicks    (pid TEXT PRIMARY KEY, nick TEXT)            -- index on nick, for resolve()
blocked  (pid TEXT PRIMARY KEY)
peers    (pid TEXT UNIQUE, name TEXT, addrs TEXT, seen REAL)   -- indexes on seen and name
messages (id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, peer TEXT,
          dir TEXT CHECK (dir IN ('in','out')), text TEXT)     -- index on (peer, id)
PRAGMA user_version = 1                                -- schema version, for future migrations
```

* `nicks` and `blocked` are tables instead of JSON lists. That makes `is_blocked()` an indexed lookup, makes
  `nick`/`block`/`unblock` single-row writes, and lets SQL filter out blocked peers (`NOT IN (SELECT pid FROM
  blocked)`).
* `peers.addrs` is a JSON text column. Addresses are always read and written as a group and never queried
  individually, so a child table would add joins and gain nothing.
* `peers` keeps an implicit `rowid`, so `peers()` still returns peers in the order they were first met, as the old
  dict did. `seen` is indexed, which makes the two things this work needs cheap: pruning by age and "the N most
  recently seen".
* `messages.id` uses `AUTOINCREMENT`, so ids only ever grow, even if history pruning is added later. That makes it
  a safe cursor for `read -f`: each poll asks for `id > last_seen`.

### Public interface

Every existing method keeps its signature and return shape: `home`, `sock`, `config()`, `set_config(**kw)`,
`peers()`, `save_peers()`, `key_seed()`, `log_message()`, `messages()`, `label()`, `resolve()`. One small
difference: `messages()` rows now also include an `id`.

New methods were added for the hot paths:

| Method | Replaces |
|---|---|
| `is_blocked`, `block`, `unblock`, `set_nick`, `username` | Read-modify-write of the whole config |
| `remember_peer(pid, addrs, name)` | `peers()` → edit → `save_peers()` (now one atomic `UPSERT`) |
| `has_peer`, `recent_peers(limit, exclude_blocked)` | Loading the whole peer dict to test membership or pick peers |
| `prune_peers(ttl, keep)` | – (new) |
| `messages(peer, last, after, include_blocked)` | Python-side filtering of the whole history |

### Call-site changes outside `Store`

A small refactor moved the socket-path calculation out of `Store.__init__` into `socket_path(home)`, so the
migration guard and the tests can use it. `store.sock` is unchanged.

The changes are deliberately small. None of them touch the mesh-discovery code (`discover`, `find_mesh_peers`,
`exchange`).

* `Node.blocked` calls `store.is_blocked`.
* `Node.remember` and message logging are awaited through `AsyncStore` (see section 2).
* `Node.handle`: the `peers` reply uses `store.recent_peers(PEX_LIMIT, exclude_blocked=True)`.
* `Node.find` and the `status` command use `store.has_peer` instead of loading every peer.
* `Node.background` dials `store.recent_peers(DIAL_ON_START=64, exclude_blocked=True)` instead of every peer.
* CLI: `start` prunes once at startup; `nick`/`block`/`unblock` use the single-row methods; `read`/`read -f`
  filter in SQL and follow by id; `send`/`username`/`status` use the new lookups.

## 5. Pruning

`prune_peers()` removes peers not seen for `PEER_TTL` (30 days), then everything beyond the `MAX_PEERS` (2000) most
recently seen. **Peers you nicknamed or exchanged messages with are never pruned**, so `read` keeps showing their
usernames. Blocking lives in its own table, so pruning never unblocks anyone.

Pruning runs when `start` launches the daemon, and again from `remember_peer` at most once an hour (`PRUNE_EVERY`),
so a long-running daemon stays bounded without changes to `Node`'s maintenance loops. On startup the daemon dials
only the 64 most recently seen peers; the rest are still known and can be found through `find`. All four limits are
class or module constants and should be tuned against real peer counts from mesh discovery.

## 6. Migrating the JSON files on first run

`Store.__init__` checks `PRAGMA user_version`. If the schema is older than the code expects, it does the following
in **one `BEGIN IMMEDIATE` transaction**:

1. Create the schema.
2. Import `config.json`. `nicks` and `blocked` go into their tables; every other key goes into `settings`, so
   unknown keys survive.
3. Import `peers.json`.
4. Import `messages.jsonl` in file order. A truncated or garbled line, such as a partial last line from a crash
   mid-append, is skipped with a warning.
5. Set `user_version`.

Only **after the commit** are the files renamed to `*.migrated`. They are kept as a backup and never deleted.

Properties covered by the tests in `tests/test_migration.py`:

* **Atomic.** A corrupt `peers.json` makes the import raise. Nothing is half-imported, the original files stay
  untouched, and the next run retries after the file is fixed.
* **Exactly once, even when racing.** The CLI and the daemon starting at the same moment are serialised by the
  write lock, and the loser sees the new `user_version` and skips the import. The test starts four processes at
  once.
* **Never re-imported.** If something recreates `messages.jsonl` after the migration, the file is ignored and not
  imported a second time.
* **Refuses while an old daemon is running.** An old daemon would keep writing the JSON files after the import,
  and those writes would be lost. So if legacy files exist and the control socket accepts a connection, `Store()`
  raises "stop it, then retry" and leaves everything untouched. A stale socket file that nothing listens on doesn't
  block the migration.
* **No downgrade path.** An older version would start with empty JSON files. Renaming the `*.migrated` files back
  restores the pre-upgrade state, but not anything added since.

## 7. What stays a file

* **`key` stays a file.** It is the identity. The README tells users to copy it to keep their peer ID, it needs
  its own `0600` mode, and it should never end up inside a database file that someone attaches to a bug report.
  Its code is unchanged.
* **`daemon.log` stays a file.** It is an append-only stream that people read with `tail -f`, and it's written by
  the detached daemon's redirected stdout and stderr, not by `Store`.
* **The control socket stays where it is**, in the runtime directory and unrelated to storage.
* `store.db` itself is created `0600`, because message history is private. The old JSON files used the umask
  default.

## 8. Recommendation: adopt with changes

**Why adopt:**

1. It removes a real corruption bug that leaves the node unusable, instead of patching around it. Fixing
   `_save`'s fixed temporary filename on `main` would stop the corruption but not the lost updates.
2. It makes the pain points that grow with mesh discovery cheap. Peer upserts, pruning, "most recently seen N",
   and bounded peer-exchange replies are O(log n) instead of rewriting or re-parsing whole files. `read` and
   `read -f` stop re-parsing the whole history (from 187 ms to under 0.1 ms at 100k messages).
3. The cost is low. It uses only the standard library, with no new dependency (`requirements.txt` is unchanged).
   Async use needs only a small `AsyncStore` wrapper around `trio.to_thread`. The `Store` interface is mostly
   preserved, and the `Node` changes are a handful of small call-site edits.
4. You can still inspect the data with `sqlite3 store.db`, and the schema is versioned for future changes.

**Changes to make before merging to `main`:**

1. **Tune `DIAL_ON_START`, `PEX_LIMIT`, `MAX_PEERS` and `PEER_TTL`** against real peer counts from mesh
   discovery, which decides how many peers a node actually meets. (The branch is already rebased onto it.)
2. **Regenerate `scripture.py`** with `make_scripture.py` when merging, if it is meant to track `gnosis_mesh.py`.
   The experiment branch leaves it alone.
3. Optional: add a `gnosis_mesh.py export` command that writes the old JSON or JSONL files, for anyone who wants
   plain-text history or a downgrade path.

**Why not "don't adopt":** the main alternative is to patch the JSON store. That means unique temporary names plus
`fcntl` locks, a separate pruned peers file, and an offset index for `messages.jsonl`. It would rebuild a weaker
database by hand and still rewrite the whole peers file on every request.

## Tests

`.venv/bin/python -m pytest` runs 37 tests in about 5 seconds:

* `tests/test_store.py`: round-trips for config, nicknames, blocks, peers (`UPSERT` semantics and ordering),
  pruning (age, cap, exemptions, hourly trigger), messages (filters, `last`, `after`, blocked), `label`/`resolve`,
  file modes, WAL mode, and `Node.remember`/`Node.blocked` called with a stand-in host.
* `tests/test_migration.py`: a full import that matches the source data exactly, running only once, partial and
  empty files, a truncated last line, corrupt JSON leaving the files untouched, four processes migrating at once,
  and refusing while a legacy daemon's socket is live (but not for a stale socket file).
* `tests/test_concurrency.py`: two processes writing peers (including the same rows), messages, blocks and
  nicknames at once with no lost updates, and a `read -f` style cursor seeing every message from another process
  exactly once.
* `tests/test_async.py`: `AsyncStore` with 20 concurrent trio tasks (nothing lost, per-task order kept), a write
  lock held by another process without stalling the event loop (with an inline control), and cancellation while a
  write is blocked.
* `tests/test_node.py`: `Node` logic with stand-ins for the host and streams. It covers peer-exchange intake caps,
  `announce_loop` surviving errors, retried messages stored once, failed requests resetting their stream, blocked
  peers losing relay reservations, and malformed control requests.
* `tests/test_cli.py`: `username`/`nick`/`block`/`unblock`/`blocked`/`read`/`peers` against a temporary `--home`,
  plus the CLI migrating a legacy home.

A manual end-to-end check passed too: two `--private --no-mdns` daemons on temporary homes, one of them migrated
from legacy files, ran `send`, `read`, `read -f`, `peers` and `status` successfully.
