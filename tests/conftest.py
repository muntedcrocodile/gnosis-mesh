from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from libp2p.crypto.ed25519 import create_new_key_pair
from libp2p.peer.id import ID

from gnosis_mesh import Store


@pytest.fixture
def home(tmp_path: Path) -> Path:
    return tmp_path / "home"


@pytest.fixture
def store(home: Path) -> Iterator[Store]:
    s = Store(home)
    yield s
    s.close()


def new_pid() -> str:
    """A real, valid base58 peer ID (resolve() checks validity)."""
    return ID.from_pubkey(create_new_key_pair().public_key).to_base58()
