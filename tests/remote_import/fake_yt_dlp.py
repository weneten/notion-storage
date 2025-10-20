#!/usr/bin/env python3
"""Test double simulating the ``yt-dlp`` executable for integration tests."""

from __future__ import annotations

import sys
import time
from pathlib import Path


def _parse_arguments(argv: list[str]) -> tuple[Path, str]:
    """Return the resolved output directory and requested URL."""

    output_template: Path | None = None
    url: str | None = None
    idx = 0
    while idx < len(argv):
        token = argv[idx]
        if token in {"--output", "-o"}:
            if idx + 1 >= len(argv):
                raise SystemExit("missing value for --output")
            output_template = Path(argv[idx + 1])
            idx += 2
            continue
        if token.startswith('-'):
            idx += 1
            continue
        url = token
        idx += 1

    if output_template is None:
        raise SystemExit("--output argument is required")
    if url is None:
        raise SystemExit("url argument is required")

    directory = output_template.parent
    directory.mkdir(parents=True, exist_ok=True)
    return directory, url


def main() -> int:
    directory, url = _parse_arguments(sys.argv[1:])

    # Emit deterministic progress output for the downloader stage.
    progress_lines = [
        "[download]   0.0% of 10.00MiB at 1.00MiB/s ETA 00:10",
        "[download]  50.0% of 10.00MiB at 1.00MiB/s ETA 00:05",
        "[download] 100.0% of 10.00MiB at 1.00MiB/s ETA 00:00",
    ]
    for line in progress_lines:
        print(line, flush=True)

    print(f"[upload] preparing files for {url}", flush=True)

    files = [
        ("001_fake_video.mp4", b"fake video payload #1"),
        ("002_fake_video.mp4", b"fake video payload #2"),
    ]

    for name, payload in files:
        target = directory / name
        with target.open('wb') as handle:
            handle.write(payload)
        # Touch a companion .part file and remove it to simulate yt-dlp completing a file.
        part_path = target.with_suffix(target.suffix + '.part')
        part_path.write_text('partial')
        part_path.unlink()
        print(f"[download] Destination: {target}", flush=True)

    print("[upload] completed staging", flush=True)
    time.sleep(1.2)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
