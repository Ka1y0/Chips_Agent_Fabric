import asyncio
import subprocess

import pytest

from project_supervisor.worktrees import GitWorktreeManager, WorktreeError


def git(repository, *arguments: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repository), *arguments],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


@pytest.mark.asyncio
async def test_concurrent_workers_receive_distinct_worktrees(tmp_path) -> None:
    repository = tmp_path / "repo"
    repository.mkdir()
    git(repository, "init", "-b", "main")
    (repository / "README.md").write_text("fixture\n", encoding="utf-8")
    git(repository, "add", "README.md")
    git(
        repository,
        "-c",
        "user.name=Supervisor Test",
        "-c",
        "user.email=supervisor@example.invalid",
        "commit",
        "-m",
        "fixture",
    )
    base = git(repository, "rev-parse", "HEAD")
    manager = GitWorktreeManager(repository, tmp_path / "worktrees")

    first, second = await asyncio.gather(
        manager.create(task_id="T001", worker_id="claude", base_commit=base),
        manager.create(task_id="T002", worker_id="grok", base_commit=base),
    )

    assert first.path != second.path
    assert first.branch != second.branch
    assert first.base_commit == second.base_commit == base
    assert (first.path / "README.md").is_file()
    assert (second.path / "README.md").is_file()
    assert len(await manager.list()) == 3


@pytest.mark.asyncio
async def test_existing_worktree_path_is_never_reused(tmp_path) -> None:
    repository = tmp_path / "not-a-repo"
    repository.mkdir()
    manager = GitWorktreeManager(repository, tmp_path / "worktrees")
    with pytest.raises(WorktreeError):
        await manager.create(task_id="T001", worker_id="worker")
