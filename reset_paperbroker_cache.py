"""Safely reset local PaperBroker cache/state artifacts.

Usage examples:
  python reset_paperbroker_cache.py
  python reset_paperbroker_cache.py --apply
  python reset_paperbroker_cache.py --apply --include-temp

Default behavior is dry-run (no files are deleted).
"""

from __future__ import annotations

import argparse
import os
import shutil
import tempfile
from pathlib import Path
from typing import Iterable, List


def _collect_repo_targets(repo_root: Path) -> List[Path]:
    targets: List[Path] = []

    exact_paths = [
        repo_root / "orders.db",
        repo_root / "logs",
        repo_root / "paperbroker_auth.json",
        repo_root / "session.json",
        repo_root / ".token",
    ]

    for path in exact_paths:
        if path.exists():
            targets.append(path)

    # Optional common cache dir names at repo root.
    for cache_name in (".cache", "cache"):
        cache_dir = repo_root / cache_name
        if not cache_dir.exists() or not cache_dir.is_dir():
            continue

        for child in cache_dir.iterdir():
            lowered = child.name.lower()
            if any(key in lowered for key in ("paperbroker", "token", "session", "auth")):
                targets.append(child)

    return list(dict.fromkeys(targets))


def _collect_temp_targets() -> List[Path]:
    temp_dir = Path(tempfile.gettempdir())
    if not temp_dir.exists():
        return []

    candidates: List[Path] = []
    # SDK-generated transient config files observed as paperbroker_XXXX.cfg.
    for path in temp_dir.glob("paperbroker_*.cfg"):
        if path.exists():
            candidates.append(path)

    return candidates


def _fmt_size(path: Path) -> str:
    if not path.exists():
        return "missing"
    if path.is_file():
        return f"{path.stat().st_size} bytes"

    total = 0
    for file_path in path.rglob("*"):
        if file_path.is_file():
            try:
                total += file_path.stat().st_size
            except OSError:
                continue
    return f"{total} bytes"


def _remove_path(path: Path) -> str:
    if not path.exists():
        return "already missing"

    try:
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
        return "deleted"
    except Exception as exc:  # pragma: no cover - runtime FS edge cases
        return f"failed: {exc}"


def _print_targets(title: str, paths: Iterable[Path]) -> None:
    print(title)
    count = 0
    for path in paths:
        count += 1
        kind = "DIR " if path.is_dir() else "FILE"
        print(f"  - [{kind}] {path} ({_fmt_size(path)})")
    if count == 0:
        print("  - (none)")


def main() -> int:
    parser = argparse.ArgumentParser(description="Reset local PaperBroker cache/log state.")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually delete discovered files/directories. Without this flag, dry-run only.",
    )
    parser.add_argument(
        "--include-temp",
        action="store_true",
        help="Also delete temporary SDK cfg files from the system temp directory.",
    )
    parser.add_argument(
        "--repo-root",
        default=str(Path(__file__).resolve().parent),
        help="Repository root path. Defaults to this script's directory.",
    )
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    if not repo_root.exists() or not repo_root.is_dir():
        print(f"Invalid repo root: {repo_root}")
        return 2

    repo_targets = _collect_repo_targets(repo_root)
    temp_targets = _collect_temp_targets() if args.include_temp else []

    print("PaperBroker local-state cleanup")
    print(f"Repository root: {repo_root}")
    print(f"Mode: {'APPLY (delete)' if args.apply else 'DRY-RUN (no changes)'}")
    print("")

    _print_targets("Repository targets:", repo_targets)
    if args.include_temp:
        print("")
        _print_targets(f"Temp targets ({Path(tempfile.gettempdir())}):", temp_targets)

    all_targets = repo_targets + temp_targets
    if not all_targets:
        print("\nNo matching cache/log targets found.")
        return 0

    if not args.apply:
        print("\nDry-run complete. Re-run with --apply to delete listed paths.")
        return 0

    print("\nDeleting targets...")
    failures = 0
    for path in all_targets:
        result = _remove_path(path)
        print(f"  - {path}: {result}")
        if result.startswith("failed:"):
            failures += 1

    print("")
    if failures:
        print(f"Completed with {failures} failure(s).")
        return 1

    print("Cleanup completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
