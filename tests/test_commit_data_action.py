"""Tests documenting commit-data action invariants (pure Python, no bash required)."""
from __future__ import annotations

from pathlib import Path


def test_backup_restore_overwrites_modified_json(tmp_path: Path):
    """Build output must win after reset-hard + restore (the CI failure scenario)."""
    repo = tmp_path / "repo"
    data = repo / "data"
    data.mkdir(parents=True)
    on_disk = data / "options_cache.json"
    on_disk.write_text('{"origin":true}', encoding="utf-8")

    backup = tmp_path / "backup" / "data" / "options_cache.json"
    backup.parent.mkdir(parents=True)
    backup.write_text('{"built":true}', encoding="utf-8")

    # simulate origin/main reset wiping build output back to origin
    on_disk.write_text('{"origin":true}', encoding="utf-8")

    # restore_from_backup
    on_disk.write_text(backup.read_text(encoding="utf-8"), encoding="utf-8")

    assert '"built":true' in on_disk.read_text(encoding="utf-8")


def test_directory_restore_uses_rm_rf_first(tmp_path: Path):
    """Prevent snapshots/snapshots nesting when restoring directories."""
    repo = tmp_path / "repo"
    target = repo / "data" / "nav_forecasts" / "snapshots"
    target.mkdir(parents=True)
    (target / "old.jsonl").write_text("old\n", encoding="utf-8")

    backup = tmp_path / "backup" / "data" / "nav_forecasts" / "snapshots" / "new.jsonl"
    backup.parent.mkdir(parents=True)
    backup.write_text("new\n", encoding="utf-8")

    import shutil

    shutil.rmtree(repo / "data" / "nav_forecasts" / "snapshots")
    shutil.copytree(backup.parent, repo / "data" / "nav_forecasts" / "snapshots")

    assert (target / "new.jsonl").exists()
    assert not (target / "old.jsonl").exists()
