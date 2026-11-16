"""The Store under asynchronous use: many trio tasks, worker threads, a write lock held by another process,
and cancellation."""
from __future__ import annotations

import sqlite3
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest
import trio

from gnosis_mesh import AsyncStore, Store

ROOT = Path(__file__).parent.parent

# Another process (think: the CLI) takes the write lock, says so, and holds it until told to stop.
HOLDER = """
import sqlite3, sys, time
from pathlib import Path
db = sqlite3.connect(sys.argv[1], isolation_level=None)
db.execute("BEGIN IMMEDIATE")
print("locked", flush=True)
while not Path(sys.argv[2]).exists():
    time.sleep(0.005)
db.execute("COMMIT")
"""


class LockHolder:
    """Hold store.db's write lock in another process until release() is called."""

    def __init__(self, home: Path) -> None:
        self.release_file = home / "release"
        self.proc = subprocess.Popen([sys.executable, "-c", HOLDER, str(home / "store.db"), str(self.release_file)],
                                     stdout=subprocess.PIPE, text=True, cwd=ROOT)
        assert self.proc.stdout is not None and self.proc.stdout.readline().strip() == "locked"

    def release(self) -> None:
        self.release_file.touch()
        assert self.proc.wait(timeout=10) == 0


def test_store_is_usable_from_many_threads(store: Store) -> None:
    def work(t: int) -> None:
        for i in range(100):
            store.remember_peer(f"t{t}-{i}", [f"/ip4/10.0.{t}.{i}/tcp/1"], f"name{t}")
            store.remember_peer("shared", [], f"t{t}")  # same row from every thread
            store.log_message(f"t{t}-{i}", "in", f"{t}:{i}")
            assert store.has_peer(f"t{t}-{i}")
            if i % 25 == 0:
                store.set_config(username=f"u{t}")  # a _tx() per thread, on that thread's own connection

    with ThreadPoolExecutor(8) as pool:
        list(pool.map(work, range(8)))
    assert len(store.peers()) == 8 * 100 + 1
    assert len(store.messages()) == 8 * 100
    assert len(store._conns) >= 2  # the worker threads really did get their own connections


def test_concurrent_trio_tasks_lose_nothing_and_keep_order(store: Store) -> None:
    astore = AsyncStore(store)

    async def main() -> None:
        async with trio.open_nursery() as n:
            for t in range(20):
                async def task(t: int = t) -> None:
                    for i in range(25):
                        await astore.remember_peer(f"t{t}", [], f"name{t}-{i}")
                        await astore.log_message(f"t{t}", "in", str(i))
                        assert store.has_peer(f"t{t}")  # sync reads interleave with threaded writes
                        await trio.sleep(0)
                n.start_soon(task)

    trio.run(main)
    assert len(store.peers()) == 20
    for t in range(20):  # each task's messages were stored in the order it logged them
        assert [m["text"] for m in store.messages(peer=f"t{t}")] == [str(i) for i in range(25)]
        assert store.label(f"t{t}") == f"name{t}-24"


def test_waiting_on_another_process_does_not_block_the_event_loop(store: Store, home: Path) -> None:
    astore = AsyncStore(store)
    holder = LockHolder(home)
    gaps: list[float] = []

    async def main() -> None:
        async with trio.open_nursery() as n:
            async def ticker() -> None:  # stands in for the daemon's networking
                last = trio.current_time()
                while True:
                    await trio.sleep(0.01)
                    now = trio.current_time()
                    gaps.append(now - last)
                    last = now
            n.start_soon(ticker)
            async with trio.open_nursery() as writes:
                writes.start_soon(astore.log_message, "p", "in", "while locked")
                writes.start_soon(astore.remember_peer, "p", [], "alice")
                await trio.sleep(0.5)  # the writes are stuck behind the other process's lock...
                assert store.messages() == []
                assert store.peers() == {}  # ...and reads still work (WAL)
                await trio.to_thread.run_sync(holder.release)
            n.cancel_scope.cancel()

    trio.run(main)
    assert [m["text"] for m in store.messages()] == ["while locked"]
    assert store.label("p") == "alice"
    assert max(gaps) < 0.25, f"event loop stalled for {max(gaps):.2f}s"


def test_sync_write_would_have_stalled(home: Path) -> None:
    """Control for the test above: the same wait done inline blocks the calling thread for the whole lock."""
    store = Store(home)
    holder = LockHolder(home)
    timer = threading.Timer(0.3, holder.release)
    timer.start()
    t0 = time.monotonic()
    store.log_message("p", "in", "x")
    assert time.monotonic() - t0 >= 0.25
    timer.join()
    store.close()


def test_cancelled_write_still_completes_exactly_once(store: Store, home: Path) -> None:
    astore = AsyncStore(store)
    holder = LockHolder(home)

    async def main() -> None:
        with trio.move_on_after(0.2) as scope:
            async with trio.open_nursery() as n:
                n.start_soon(astore.log_message, "p", "in", "cancelled mid-wait")
                await trio.sleep(0.1)
                n.start_soon(trio.to_thread.run_sync, holder.release)
                await trio.sleep_forever()
        assert scope.cancelled_caught

    trio.run(main)
    assert [m["text"] for m in store.messages()] == ["cancelled mid-wait"]
    assert store.db.in_transaction is False  # nothing left open on this thread's connection
    store.log_message("p", "in", "after")  # and the store still works
    assert len(store.messages()) == 2


def test_close_closes_every_threads_connection(home: Path) -> None:
    store = Store(home)
    conns: list[sqlite3.Connection] = []
    t = threading.Thread(target=lambda: conns.append(store.db))
    t.start()
    t.join()
    main_conn = store.db
    store.close()
    for conn in [*conns, main_conn]:
        with pytest.raises(sqlite3.ProgrammingError):
            conn.execute("SELECT 1")
    assert store.has_peer("x") is False  # reopening after close works


def test_connections_of_exited_threads_are_closed(home: Path) -> None:
    store = Store(home)
    for _ in range(5):
        t = threading.Thread(target=lambda: store.db)
        t.start()
        t.join()
    store.db  # opening another connection sweeps the dead threads' ones
    assert len(store._conns) <= 2
    store.close()
