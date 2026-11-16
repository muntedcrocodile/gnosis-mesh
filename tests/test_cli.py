"""CLI commands that only touch the store (no daemon), run against a temporary --home."""
from __future__ import annotations

import json
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


def test_cli_migrates_legacy_home(home: Path) -> None:
    pid = new_pid()
    home.mkdir()
    (home / "config.json").write_text(json.dumps({"username": "old", "nicks": {pid: "pal"}, "blocked": []}))
    (home / "messages.jsonl").write_text(json.dumps({"ts": 1.0, "peer": pid, "dir": "in", "text": "legacy"}) + "\n")
    assert run(home, "username").strip() == "old"
    assert "[pal] legacy" in run(home, "read")
