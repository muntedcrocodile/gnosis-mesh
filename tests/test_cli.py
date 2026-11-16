"""CLI commands that only touch the store (no daemon), run against a temporary --home."""
from __future__ import annotations

import os
import subprocess
import sys
import threading
import time
from pathlib import Path

from click.testing import CliRunner

from conftest import new_pid
from gnosis_mesh import Store, cli


def run(home: Path, *args: str) -> str:
    result = CliRunner().invoke(cli, ["--home", str(home), *args])
    assert result.exit_code == 0, result.output
    return result.output


def test_username_nick_block(home: Path) -> None:
    pid = new_pid()
    assert run(home, "username", "alice").strip() == "alice"
    assert run(home, "username").strip() == "alice"
    run(home, "nick", pid, "bob")
    run(home, "block", "bob")
    assert pid in run(home, "blocked") and "bob" in run(home, "blocked")
    run(home, "unblock", pid)
    assert run(home, "blocked") == ""
    run(home, "nick", pid)
    assert Store(home).config()["nicks"] == {}


def test_read_and_peers(home: Path) -> None:
    a, b = new_pid(), new_pid()
    s = Store(home)
    s.remember_peer(a, [], "alice")
    s.remember_peer(b, [], None)
    for i in range(5):
        s.log_message(a, "in", f"a{i}")
    s.log_message(b, "out", "to b")
    s.block(b)
    s.close()
    out = run(home, "read", "-n", "2")
    assert "a2" not in out and "a3" in out and "a4" in out and "to b" not in out  # last 2 non-blocked
    assert out.count("\n") == 2
    assert run(home, "read", "alice", "-n", "0").count("\n") == 5
    out = run(home, "peers")
    assert f"{a}  username=alice" in out and "[blocked]" in out
    assert b not in run(home, "peers", "--named")


def test_follow_never_replays_a_blocked_peers_messages(home: Path) -> None:
    a, b = new_pid(), new_pid()
    s = Store(home)
    s.block(b)
    env = {**os.environ, "PYTHONUNBUFFERED": "1"}
    proc = subprocess.Popen([sys.executable, "gnosis_mesh.py", "--home", str(home), "read", "-f"],
                            cwd=Path(__file__).parent.parent, stdout=subprocess.PIPE, text=True, env=env)
    killer = threading.Timer(60, proc.kill)  # never hang the suite
    killer.start()
    seen: list[str] = []

    def say(peer: str, text: str) -> None:  # log a message and wait until the follower has printed it
        s.log_message(peer, "in", text)
        while proc.stdout and (line := proc.stdout.readline()):
            seen.append(line)
            if text in line:
                return
        raise AssertionError(f"follower never printed {text!r}")

    try:
        say(a, "first")  # the follower is polling
        s.log_message(b, "in", "hidden")
        time.sleep(2.5)  # at least one 1 s poll passes with "hidden" as the newest message, while b is blocked
        s.unblock(b)
        say(a, "second")
    finally:
        proc.kill()
        proc.wait()
        killer.cancel()
        s.close()
    assert not any("hidden" in line for line in seen)
