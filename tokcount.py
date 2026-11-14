"""Count LLM tokens in files (o200k_base = GPT-4o/o-series; cl100k_base = GPT-4/3.5 for comparison)."""

from pathlib import Path

import click
import tiktoken


@click.command()
@click.argument("files", nargs=-1, required=True, type=click.Path(exists=True, path_type=Path))
def main(files: tuple[Path, ...]) -> None:
    encs = [tiktoken.get_encoding(name) for name in ("o200k_base", "cl100k_base")]
    for f in files:
        text = f.read_text()
        counts = "  ".join(f"{e.name}={len(e.encode(text))}" for e in encs)
        click.echo(f"{f}: {counts}  lines={text.count(chr(10))}  chars={len(text)}")


if __name__ == "__main__":
    main()
