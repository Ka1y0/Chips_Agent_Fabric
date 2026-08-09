from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass
from pathlib import Path


class WorktreeError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class WorktreeInfo:
    path: Path
    branch: str
    base_commit: str


class GitWorktreeManager:
    def __init__(self, repository_root: str | Path, worktrees_root: str | Path) -> None:
        self.repository_root = Path(repository_root).resolve()
        self.worktrees_root = Path(worktrees_root).resolve()
        self.worktrees_root.mkdir(parents=True, exist_ok=True)
        self._mutation_lock = asyncio.Lock()

    async def validate_repository(self) -> str:
        inside = await self._git("rev-parse", "--is-inside-work-tree")
        if inside.strip() != "true":
            raise WorktreeError(f"not a Git working tree: {self.repository_root}")
        return (await self._git("rev-parse", "HEAD")).strip()

    async def create(
        self,
        *,
        task_id: str,
        worker_id: str,
        base_commit: str | None = None,
    ) -> WorktreeInfo:
        async with self._mutation_lock:
            head = await self.validate_repository()
            base = base_commit or head
            safe_task = self._safe(task_id)
            safe_worker = self._safe(worker_id)
            path = (self.worktrees_root / f"{safe_worker}-{safe_task}").resolve()
            if self.worktrees_root not in path.parents:
                raise WorktreeError("resolved worktree path escapes configured root")
            if path.exists():
                raise WorktreeError(f"worktree path already exists: {path}")
            branch = f"supervisor/{safe_task}/{safe_worker}"
            await self._git("worktree", "add", "-b", branch, str(path), base)
            return WorktreeInfo(path=path, branch=branch, base_commit=base)

    async def list(self) -> list[dict[str, str]]:
        output = await self._git("worktree", "list", "--porcelain")
        entries: list[dict[str, str]] = []
        current: dict[str, str] = {}
        for line in output.splitlines():
            if not line:
                if current:
                    entries.append(current)
                    current = {}
                continue
            key, _, value = line.partition(" ")
            current[key] = value
        if current:
            entries.append(current)
        return entries

    async def _git(self, *arguments: str) -> str:
        process = await asyncio.create_subprocess_exec(
            "git",
            "-C",
            str(self.repository_root),
            *arguments,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            raise WorktreeError(stderr.decode(errors="replace").strip())
        return stdout.decode(errors="replace")

    @staticmethod
    def _safe(value: str) -> str:
        safe = re.sub(r"[^A-Za-z0-9_.-]", "-", value).strip(".-")
        if not safe:
            raise WorktreeError("empty worktree identity")
        return safe
