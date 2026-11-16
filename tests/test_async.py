"""AsyncStore under trio: many concurrent tasks, a write lock held by another process, and cancellation."""
from __future__ import annotations

import subprocess
import sys
import threading
import time
from collections.abc import Iterator
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


@pytest.fixture
def lock(store: Store, home: Path) -> Iterator[LockHolder]:
    holder = LockHolder(home)
    yield holder
    if holder.proc.poll() is None:  # a failed test must not leave the holder running
        holder.proc.kill()
        holder.proc.wait()


def test_concurrent_trio_tasks_lose_nothing_and_keep_order(store: Store) -> None:
    astore = AsyncStore(store.home)

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


def test_waiting_on_another_process_does_not_block_the_event_loop(store: Store, lock: LockHolder) -> None:
    astore = AsyncStore(store.home)
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
                await trio.to_thread.run_sync(lock.release)
            n.cancel_scope.cancel()

    trio.run(main)
    assert [m["text"] for m in store.messages()] == ["while locked"]
    assert store.label("p") == "alice"
    assert max(gaps) < 0.25, f"event loop stalled for {max(gaps):.2f}s"


def test_sync_write_would_have_stalled(store: Store, lock: LockHolder) -> None:
    """Control for the test above: the same wait done inline blocks the calling thread for the whole lock."""
    timer = threading.Timer(0.3, lock.release)
    timer.start()
    t0 = time.monotonic()
    store.log_message("p", "in", "x")
    assert time.monotonic() - t0 >= 0.25
    timer.join()


def test_cancelled_write_still_completes_exactly_once(store: Store, lock: LockHolder) -> None:
    astore = AsyncStore(store.home)
    timer = threading.Timer(0.4, lock.release)  # released from outside trio, well after the cancellation
    timer.start()

    async def main() -> float:
        t0 = trio.current_time()
        with trio.move_on_after(0.2) as scope:  # cancelled while the write is still blocked on the lock
            await astore.log_message("p", "in", "cancelled mid-wait")
        assert scope.cancel_called  # the deadline passed mid-write...
        return trio.current_time() - t0

    elapsed = trio.run(main)
    timer.join()
    assert elapsed >= 0.35  # ...yet the call waited for the write rather than abandoning it mid-transaction
    assert [m["text"] for m in store.messages()] == ["cancelled mid-wait"]
    assert astore.store.db.in_transaction is False
    store.log_message("p", "in", "after")  # and the store still works
    assert len(store.messages()) == 2
