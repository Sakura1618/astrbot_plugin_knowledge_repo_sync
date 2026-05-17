from __future__ import annotations

import asyncio
import shutil
import tempfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlparse

from sqlmodel import delete

from astrbot.api import star
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.message_components import Plain
from astrbot.core import logger
from astrbot.core.knowledge_base.kb_helper import KBHelper
from astrbot.core.knowledge_base.models import KBDocument, KBMedia, KnowledgeBase
from astrbot.core.message.message_event_result import MessageChain
from astrbot.core.provider.provider import EmbeddingProvider, RerankProvider
from astrbot.core.star.star_tools import StarTools
from astrbot.core.utils.astrbot_path import get_astrbot_temp_path

DEFAULT_IGNORE_PATHS = {
    Path("README.md"),
}
DEFAULT_AUTO_SYNC_INTERVAL_HOURS = 24
DEFAULT_CHUNK_SIZE = 512
DEFAULT_CHUNK_OVERLAP = 50
DEFAULT_EMBEDDING_BATCH_SIZE = 64
DEFAULT_EMBEDDING_TASKS_LIMIT = 1
DEFAULT_EMBEDDING_MAX_RETRIES = 7
MARKDOWN_SUFFIXES = {".md", ".markdown"}
NEXT_CHECK_AT_KEY = "next_check_at"
LAST_SYNC_HEAD_KEY = "last_sync_head"
LAST_SYNC_BRANCH_KEY = "last_sync_branch"
LAST_SYNC_REMOTE_URL_KEY = "last_sync_remote_url"


@dataclass(frozen=True)
class KBConfigSnapshot:
    description: str | None
    emoji: str | None
    embedding_provider_id: str
    rerank_provider_id: str | None
    chunk_size: int | None
    chunk_overlap: int | None
    top_k_dense: int | None
    top_k_sparse: int | None
    top_m_final: int | None


@dataclass(frozen=True)
class SyncRunResult:
    branch: str
    file_count: int
    imported_count: int
    deleted_count: int
    kb_name: str
    warning: str | None
    remote_head: str
    sync_mode: str


@dataclass(frozen=True)
class RemoteRepoConfig:
    remote_url: str
    branch: str | None


class Main(star.Star):
    def __init__(self, context: star.Context, config: dict | None = None) -> None:
        super().__init__(context)
        self.context = context
        self.config = config or {}
        self._sync_lock = asyncio.Lock()
        self._manual_sync_pending = False
        self._auto_sync_task: asyncio.Task | None = None
        self._stopping = False

    async def initialize(self) -> None:
        self._stopping = False
        await self._sync_branch_from_remote_url()

    async def terminate(self) -> None:
        self._stopping = True
        await self._cancel_auto_sync_task()

    @filter.on_astrbot_loaded()
    async def on_astrbot_loaded(self) -> None:
        if self._stopping:
            return
        self._restart_auto_sync_task()

    @filter.permission_type(filter.PermissionType.ADMIN)
    @filter.command("knowledge_sync")
    async def sync_knowledge_repo(self, event: AstrMessageEvent):
        if self._sync_lock.locked() or self._manual_sync_pending:
            yield event.plain_result("知识库同步任务正在进行中，请稍后再试。")
            return

        self._manual_sync_pending = True
        try:
            if self._sync_lock.locked():
                yield event.plain_result("知识库同步任务正在进行中，请稍后再试。")
                return

            async with self._sync_lock:
                yield event.plain_result("开始进行知识库同步操作，请稍候。")

                try:
                    result = await self._sync_target_kb()
                except Exception as exc:
                    logger.error("knowledge_repo_sync failed: %s", exc, exc_info=True)
                    yield event.plain_result(f"知识库同步失败：{exc}")
                    return

            await self._record_sync_state(result)
            await self._schedule_next_auto_check_after_run()
            yield event.plain_result(self._format_sync_success_message(result))
        finally:
            self._manual_sync_pending = False

    def _restart_auto_sync_task(self) -> None:
        if self._auto_sync_task and not self._auto_sync_task.done():
            self._auto_sync_task.cancel()
        self._auto_sync_task = asyncio.create_task(
            self._auto_sync_loop(),
            name="knowledge_repo_sync:auto_sync",
        )

    async def _cancel_auto_sync_task(self) -> None:
        if self._auto_sync_task is None:
            return
        task = self._auto_sync_task
        self._auto_sync_task = None
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    async def _auto_sync_loop(self) -> None:
        while not self._stopping:
            if not self._is_auto_sync_enabled():
                await self._clear_auto_sync_state()
                await asyncio.sleep(60)
                continue

            if not self._has_required_sync_config():
                await self._clear_auto_sync_state()
                await asyncio.sleep(60)
                continue

            next_check_at = await self._ensure_next_check_at()
            delay_seconds = max(
                0.0,
                (next_check_at - datetime.now(timezone.utc)).total_seconds(),
            )
            if delay_seconds > 0:
                await asyncio.sleep(delay_seconds)
                continue

            if self._sync_lock.locked() or self._manual_sync_pending:
                logger.info(
                    "knowledge_repo_sync auto-check skipped because another sync is running."
                )
                await self._schedule_next_auto_check_after_run()
                continue

            try:
                should_rebuild = await self._has_remote_changes()
            except Exception as exc:
                logger.error(
                    "knowledge_repo_sync auto-check failed: %s", exc, exc_info=True
                )
                await self._schedule_next_auto_check_after_run()
                continue

            if not should_rebuild:
                logger.info("knowledge_repo_sync auto-check found no remote changes.")
                await self._schedule_next_auto_check_after_run()
                continue

            repo_config = self._parse_remote_repo_config()
            branch = await self._get_remote_branch(repo_config)
            kb_name = self._get_target_kb_name()

            await self._send_notifications(
                self._format_rebuild_start_notification(repo_config, branch, kb_name)
            )

            async with self._sync_lock:
                try:
                    result = await self._sync_target_kb()
                    await self._record_sync_state(result)
                    logger.info(self._format_sync_success_message(result))
                    await self._send_notifications(
                        self._format_sync_success_message(result)
                    )
                except Exception as exc:
                    logger.error(
                        "knowledge_repo_sync auto-sync failed: %s", exc, exc_info=True
                    )
                finally:
                    await self._schedule_next_auto_check_after_run()

    async def _ensure_next_check_at(self) -> datetime:
        next_check_at = await self._get_stored_next_check_at()
        if next_check_at is None:
            next_check_at = datetime.now(timezone.utc) + timedelta(
                hours=self._get_auto_sync_interval_hours()
            )
            await self._store_next_check_at(next_check_at)
        return next_check_at

    async def _get_stored_next_check_at(self) -> datetime | None:
        raw_next_check = await self.get_kv_data(NEXT_CHECK_AT_KEY, None)
        if not isinstance(raw_next_check, str) or not raw_next_check.strip():
            return None
        try:
            parsed = datetime.fromisoformat(raw_next_check)
        except ValueError:
            logger.warning(
                "Invalid stored next_check_at for knowledge_repo_sync: %s",
                raw_next_check,
            )
            await self.delete_kv_data(NEXT_CHECK_AT_KEY)
            return None

        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    async def _store_next_check_at(self, next_check_at: datetime) -> None:
        normalized = next_check_at.astimezone(timezone.utc)
        await self.put_kv_data(NEXT_CHECK_AT_KEY, normalized.isoformat())

    async def _schedule_next_auto_check_after_run(self) -> None:
        if not self._is_auto_sync_enabled():
            await self._clear_auto_sync_state()
            return

        next_check_at = datetime.now(timezone.utc) + timedelta(
            hours=self._get_auto_sync_interval_hours()
        )
        await self._store_next_check_at(next_check_at)

    async def _clear_auto_sync_state(self) -> None:
        await self.delete_kv_data(NEXT_CHECK_AT_KEY)

    def _is_auto_sync_enabled(self) -> bool:
        return bool(self.config.get("auto_sync_enabled", True))

    def _is_notify_owner_enabled(self) -> bool:
        return bool(self.config.get("notify_owner_enabled", False))

    def _is_notify_group_enabled(self) -> bool:
        return bool(self.config.get("notify_group_enabled", False))

    def _get_notify_group_id(self) -> str | None:
        group_id = self.config.get("notify_group_id", "")
        if isinstance(group_id, str) and group_id.strip():
            return group_id.strip()
        return None

    async def _send_notifications(self, text: str) -> None:
        tasks = []

        if self._is_notify_owner_enabled():
            admin_ids = self.context.get_config().get("admins_id", [])
            if isinstance(admin_ids, list):
                for admin_id in admin_ids:
                    if isinstance(admin_id, str) and admin_id.strip():
                        tasks.append(
                            self._send_private_notification(admin_id.strip(), text)
                        )

        if self._is_notify_group_enabled():
            notify_group_id = self._get_notify_group_id()
            if notify_group_id:
                tasks.append(self._send_group_notification(notify_group_id, text))

        if not tasks:
            return

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception):
                logger.warning("sync notification failed: %s", result)

    async def _send_private_notification(self, user_id: str, text: str) -> None:
        await StarTools.send_message_by_id(
            "PrivateMessage",
            user_id,
            MessageChain([Plain(text)]),
            platform="aiocqhttp",
        )

    async def _send_group_notification(self, group_id: str, text: str) -> None:
        await StarTools.send_message_by_id(
            "GroupMessage",
            group_id,
            MessageChain([Plain(text)]),
            platform="aiocqhttp",
        )

    def _get_auto_sync_interval_hours(self) -> int:
        raw_value = self.config.get(
            "auto_sync_interval_hours",
            DEFAULT_AUTO_SYNC_INTERVAL_HOURS,
        )
        if not isinstance(raw_value, int) or raw_value <= 0:
            return DEFAULT_AUTO_SYNC_INTERVAL_HOURS
        return raw_value

    async def _sync_branch_from_remote_url(self) -> None:
        try:
            parsed = self._parse_remote_repo_url()
        except ValueError:
            return

        normalized_remote_url = self._strip_branch_from_remote_url(parsed)
        branch_hint = await self._infer_branch_from_remote_url(
            parsed, normalized_remote_url
        )
        updates = {}
        raw_remote_url = self.config.get("remote_repo_url", "")
        if (
            isinstance(raw_remote_url, str)
            and raw_remote_url.strip() != normalized_remote_url
        ):
            updates["remote_repo_url"] = normalized_remote_url
        if branch_hint and self._get_config_branch() != branch_hint:
            updates["remote_branch"] = branch_hint
        if updates:
            self._save_plugin_config_updates(updates)

    def _save_plugin_config_updates(self, updates: dict) -> None:
        changed = False
        for key, value in updates.items():
            if self.config.get(key) != value:
                self.config[key] = value
                changed = True
        if changed and hasattr(self.config, "save_config"):
            self.config.save_config()

    def _parse_remote_repo_url(self):
        raw_url = self.config.get("remote_repo_url", "")
        if not isinstance(raw_url, str) or not raw_url.strip():
            raise ValueError("请在插件配置中填写 remote_repo_url。")

        cleaned_url = raw_url.strip()
        parsed = urlparse(cleaned_url)
        if parsed.scheme not in {"http", "https", "ssh", "git"} or not parsed.netloc:
            raise ValueError("remote_repo_url 必须是合法的完整远程仓库链接。")
        return parsed

    def _parse_remote_repo_config(self) -> RemoteRepoConfig:
        parsed = self._parse_remote_repo_url()
        config_branch = self._get_config_branch()
        return RemoteRepoConfig(
            remote_url=self._strip_branch_from_remote_url(parsed),
            branch=config_branch,
        )

    def _get_config_branch(self) -> str | None:
        raw_branch = self.config.get("remote_branch", "")
        if not isinstance(raw_branch, str) or not raw_branch.strip():
            return None
        return raw_branch.strip()

    def _extract_branch_tail_from_remote_url(self, parsed) -> str | None:
        parts = [part for part in parsed.path.split("/") if part]
        for idx, part in enumerate(parts):
            if part in {"tree", "blob"} and idx + 1 < len(parts):
                return "/".join(parts[idx + 1 :])
        return None

    def _strip_branch_from_remote_url(self, parsed) -> str:
        parts = [part for part in parsed.path.split("/") if part]
        for idx, part in enumerate(parts):
            if part in {"tree", "blob"}:
                cut_idx = idx - 1 if idx > 0 and parts[idx - 1] == "-" else idx
                parts = parts[:cut_idx]
                break
        cleaned_path = "/" + "/".join(parts)
        return parsed._replace(
            path=cleaned_path, params="", query="", fragment=""
        ).geturl()

    async def _infer_branch_from_remote_url(
        self,
        parsed,
        normalized_remote_url: str,
    ) -> str | None:
        branch_tail = self._extract_branch_tail_from_remote_url(parsed)
        if not branch_tail:
            return None

        try:
            output = await self._run_git_command(
                ["ls-remote", "--heads", normalized_remote_url]
            )
        except Exception:
            return branch_tail.split("/", 1)[0]

        remote_branches = []
        for line in output.splitlines():
            parts = line.split()
            if len(parts) >= 2 and parts[1].startswith("refs/heads/"):
                remote_branches.append(parts[1].removeprefix("refs/heads/"))

        matched_branches = [
            branch
            for branch in remote_branches
            if branch_tail == branch or branch_tail.startswith(f"{branch}/")
        ]
        if matched_branches:
            return max(matched_branches, key=len)
        return branch_tail.split("/", 1)[0]

    async def _run_git_command(
        self,
        args: list[str],
        workdir: str | None = None,
    ) -> str:
        process = await asyncio.create_subprocess_exec(
            "git",
            *args,
            cwd=workdir,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        if process.returncode != 0:
            stderr_text = stderr.decode("utf-8", errors="replace").strip()
            raise RuntimeError(f"git {' '.join(args)} 执行失败：{stderr_text}")
        return stdout.decode("utf-8", errors="replace").strip()

    async def _get_remote_branch(self, repo_config: RemoteRepoConfig) -> str:
        if repo_config.branch:
            return repo_config.branch

        head_ref = await self._run_git_command(
            [
                "ls-remote",
                "--symref",
                repo_config.remote_url,
                "HEAD",
            ]
        )
        for line in head_ref.splitlines():
            if line.startswith("ref:") and "\tHEAD" in line:
                ref = line.split()[1]
                if ref.startswith("refs/heads/"):
                    return ref.removeprefix("refs/heads/")
        raise RuntimeError("无法解析远程仓库默认分支。")

    async def _get_remote_head(self, repo_config: RemoteRepoConfig, branch: str) -> str:
        output = await self._run_git_command(
            ["ls-remote", repo_config.remote_url, f"refs/heads/{branch}"]
        )
        for line in output.splitlines():
            parts = line.split()
            if len(parts) >= 1 and parts[0]:
                return parts[0]
        raise RuntimeError("无法获取远程仓库分支的最新提交。")

    async def _has_remote_changes(self) -> bool:
        repo_config = self._parse_remote_repo_config()
        branch = await self._get_remote_branch(repo_config)
        remote_head = await self._get_remote_head(repo_config, branch)
        last_remote_url = await self.get_kv_data(LAST_SYNC_REMOTE_URL_KEY, None)
        last_branch = await self.get_kv_data(LAST_SYNC_BRANCH_KEY, None)
        last_head = await self.get_kv_data(LAST_SYNC_HEAD_KEY, None)

        return not (
            last_remote_url == repo_config.remote_url
            and last_branch == branch
            and last_head == remote_head
        )

    async def _record_sync_state(self, result: SyncRunResult) -> None:
        repo_config = self._parse_remote_repo_config()
        await self.put_kv_data(LAST_SYNC_REMOTE_URL_KEY, repo_config.remote_url)
        await self.put_kv_data(LAST_SYNC_BRANCH_KEY, result.branch)
        await self.put_kv_data(LAST_SYNC_HEAD_KEY, result.remote_head)

    def _format_rebuild_start_notification(
        self,
        repo_config: RemoteRepoConfig,
        branch: str,
        kb_name: str,
    ) -> str:
        return (
            "检测到远端仓库与本地已同步状态存在差异，开始执行知识库同步。"
            f"\n仓库：{repo_config.remote_url}"
            f"\n分支：{branch}"
            f"\n目标知识库：{kb_name}"
        )

    def _get_ignore_paths(self) -> set[Path]:
        raw_paths = self.config.get("ignore_paths", list(DEFAULT_IGNORE_PATHS))
        if not isinstance(raw_paths, list):
            return set(DEFAULT_IGNORE_PATHS)

        ignore_paths = set()
        for entry in raw_paths:
            if not isinstance(entry, str):
                continue
            normalized = self._normalize_repo_relative_path(entry)
            if normalized is not None:
                ignore_paths.add(normalized)

        return ignore_paths

    def _normalize_repo_relative_path(self, raw_path: str) -> Path | None:
        cleaned = raw_path.strip().replace("\\", "/").strip("/")
        if not cleaned:
            return None
        normalized = Path(cleaned)
        if normalized.is_absolute() or ".." in normalized.parts:
            raise ValueError(f"忽略路径非法：{raw_path}")
        return normalized

    def _get_chunk_override(self, key: str, default: int) -> int:
        raw_value = self.config.get(key, default)
        if not isinstance(raw_value, int) or raw_value <= 0:
            return default
        return raw_value

    def _get_embedding_batch_size(self) -> int:
        return self._get_chunk_override(
            "embedding_batch_size",
            DEFAULT_EMBEDDING_BATCH_SIZE,
        )

    def _get_embedding_tasks_limit(self) -> int:
        return self._get_chunk_override(
            "embedding_tasks_limit",
            DEFAULT_EMBEDDING_TASKS_LIMIT,
        )

    def _get_embedding_max_retries(self) -> int:
        return self._get_chunk_override(
            "embedding_max_retries",
            DEFAULT_EMBEDDING_MAX_RETRIES,
        )

    def _resolve_chunk_size(self, kb: KnowledgeBase | None) -> int:
        configured = self._get_chunk_override("chunk_size", DEFAULT_CHUNK_SIZE)
        if kb is None:
            return configured
        return configured or kb.chunk_size or DEFAULT_CHUNK_SIZE

    def _resolve_chunk_overlap(self, kb: KnowledgeBase | None) -> int:
        configured = self._get_chunk_override("chunk_overlap", DEFAULT_CHUNK_OVERLAP)
        if kb is None:
            return configured
        return configured or kb.chunk_overlap or DEFAULT_CHUNK_OVERLAP

    def _get_configured_target_kb_name(self) -> str | None:
        value = self.config.get("target_kb_name", "")
        if isinstance(value, str) and value.strip():
            return value.strip()
        return None

    def _get_configured_new_kb_name(self) -> str | None:
        value = self.config.get("new_kb_name", "")
        if isinstance(value, str) and value.strip():
            return value.strip()
        return None

    def _has_required_sync_config(self) -> bool:
        remote_repo_url = self.config.get("remote_repo_url", "")
        if not isinstance(remote_repo_url, str) or not remote_repo_url.strip():
            return False
        return bool(
            self._get_configured_target_kb_name() or self._get_configured_new_kb_name()
        )

    def _get_target_kb_name(self) -> str:
        target_kb_name = self._get_configured_target_kb_name()
        if target_kb_name:
            return target_kb_name

        new_kb_name = self._get_configured_new_kb_name()
        if new_kb_name:
            return new_kb_name

        raise ValueError("请至少在插件配置中填写 target_kb_name 或 new_kb_name。")

    async def _select_embedding_provider_id(self) -> str:
        provider_manager = self.context.kb_manager.provider_manager
        if not provider_manager.embedding_provider_insts:
            raise ValueError("当前没有可用的嵌入模型，无法自动创建知识库。")

        provider = provider_manager.embedding_provider_insts[0]
        provider_id = provider.meta().id
        resolved = await provider_manager.get_provider_by_id(provider_id)
        if not isinstance(resolved, EmbeddingProvider):
            raise ValueError("自动选择的嵌入模型不可用，无法创建知识库。")
        return provider_id

    async def _select_rerank_provider_id(self) -> str | None:
        provider_manager = self.context.kb_manager.provider_manager
        if not provider_manager.rerank_provider_insts:
            return None

        provider = provider_manager.rerank_provider_insts[0]
        provider_id = provider.meta().id
        resolved = await provider_manager.get_provider_by_id(provider_id)
        if not isinstance(resolved, RerankProvider):
            return None
        return provider_id

    async def _get_or_create_target_kb(self):
        kb_manager = self.context.kb_manager

        target_kb_name = self._get_configured_target_kb_name()
        new_kb_name = self._get_configured_new_kb_name()
        lookup_names = []
        if target_kb_name:
            lookup_names.append(target_kb_name)
        if new_kb_name and new_kb_name not in lookup_names:
            lookup_names.append(new_kb_name)

        for kb_name in lookup_names:
            kb_helper = await kb_manager.get_kb_by_name(kb_name)
            if kb_helper is not None:
                return kb_helper, False

        create_kb_name = new_kb_name or target_kb_name
        if not create_kb_name:
            raise ValueError("请至少在插件配置中填写 target_kb_name 或 new_kb_name。")

        embedding_provider_id = await self._select_embedding_provider_id()
        rerank_provider_id = await self._select_rerank_provider_id()
        chunk_size = self._resolve_chunk_size(None)
        chunk_overlap = self._resolve_chunk_overlap(None)

        kb_helper = await kb_manager.create_kb(
            kb_name=create_kb_name,
            description="由 knowledge_repo_sync 自动创建",
            emoji="📚",
            embedding_provider_id=embedding_provider_id,
            rerank_provider_id=rerank_provider_id,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
        )
        return kb_helper, True

    async def _sync_target_kb(self) -> SyncRunResult:
        await self._sync_branch_from_remote_url()
        repo_config = self._parse_remote_repo_config()
        kb_helper, created_new = await self._get_or_create_target_kb()
        kb_helper, kb_settings_changed = await self._ensure_target_kb_sync_settings(
            kb_helper
        )

        active_kb_name = kb_helper.kb.kb_name
        chunk_size = self._resolve_chunk_size(kb_helper.kb)
        chunk_overlap = self._resolve_chunk_overlap(kb_helper.kb)
        branch = await self._get_remote_branch(repo_config)
        remote_head = await self._get_remote_head(repo_config, branch)
        last_remote_url = await self.get_kv_data(LAST_SYNC_REMOTE_URL_KEY, None)
        last_branch = await self.get_kv_data(LAST_SYNC_BRANCH_KEY, None)
        last_head = await self.get_kv_data(LAST_SYNC_HEAD_KEY, None)

        full_sync = (
            created_new
            or kb_settings_changed
            or last_remote_url != repo_config.remote_url
            or last_branch != branch
            or not isinstance(last_head, str)
            or not last_head.strip()
        )
        changed_paths: set[str] = set()

        temp_root = Path(get_astrbot_temp_path())
        temp_root.mkdir(parents=True, exist_ok=True)

        with tempfile.TemporaryDirectory(
            dir=temp_root,
            prefix="knowledge_repo_sync_",
        ) as temporary_directory:
            temp_dir = Path(temporary_directory)
            repo_dir = temp_dir / "repo"

            await self._run_git_command(
                [
                    "clone",
                    "--branch",
                    branch,
                    "--single-branch",
                    "--no-tags",
                    repo_config.remote_url,
                    str(repo_dir),
                ]
            )

            markdown_files = self._collect_markdown_files(repo_dir)
            current_files = self._build_current_markdown_index(repo_dir, markdown_files)

            if not full_sync:
                changed_paths = await self._get_changed_markdown_paths(
                    repo_dir=repo_dir,
                    last_head=last_head,
                    remote_head=remote_head,
                )
                if changed_paths is None:
                    full_sync = True

            if not hasattr(kb_helper, "vec_db"):
                await kb_helper.initialize()

            imported_count, deleted_count = await self._sync_markdown_documents(
                kb_helper=kb_helper,
                current_files=current_files,
                changed_paths=changed_paths,
                full_sync=full_sync,
                chunk_size=chunk_size,
                chunk_overlap=chunk_overlap,
            )

        return SyncRunResult(
            branch=branch,
            file_count=len(markdown_files),
            imported_count=imported_count,
            deleted_count=deleted_count,
            kb_name=active_kb_name,
            warning=None,
            remote_head=remote_head,
            sync_mode="full" if full_sync else "incremental",
        )

    def _format_sync_success_message(self, result: SyncRunResult) -> str:
        sync_mode = "全量同步" if result.sync_mode == "full" else "差异同步"
        message = (
            "知识库同步完成："
            f"已对知识库 {result.kb_name} 执行{sync_mode}。"
            f"\n分支：{result.branch}"
            f"\n当前 Markdown 文档：{result.file_count} 个"
            f"\n新增/更新：{result.imported_count} 个"
            f"\n删除：{result.deleted_count} 个"
        )
        if result.warning:
            message = f"{message}\n警告：{result.warning}"
        return message

    def _build_kb_config(self, kb: KnowledgeBase) -> KBConfigSnapshot:
        if not kb.embedding_provider_id:
            raise ValueError(f"目标知识库 {kb.kb_name} 缺少 embedding_provider_id。")

        return KBConfigSnapshot(
            description=kb.description,
            emoji=kb.emoji,
            embedding_provider_id=kb.embedding_provider_id,
            rerank_provider_id=kb.rerank_provider_id,
            chunk_size=kb.chunk_size,
            chunk_overlap=kb.chunk_overlap,
            top_k_dense=kb.top_k_dense,
            top_k_sparse=kb.top_k_sparse,
            top_m_final=kb.top_m_final,
        )

    async def _ensure_target_kb_sync_settings(
        self,
        kb_helper: KBHelper,
    ) -> tuple[KBHelper, bool]:
        desired_chunk_size = self._resolve_chunk_size(kb_helper.kb)
        desired_chunk_overlap = self._resolve_chunk_overlap(kb_helper.kb)
        if (
            kb_helper.kb.chunk_size == desired_chunk_size
            and kb_helper.kb.chunk_overlap == desired_chunk_overlap
        ):
            return kb_helper, False

        kb_config = self._build_kb_config(kb_helper.kb)
        updated_kb = await self.context.kb_manager.update_kb(
            kb_id=kb_helper.kb.kb_id,
            kb_name=kb_helper.kb.kb_name,
            description=kb_config.description,
            emoji=kb_config.emoji,
            embedding_provider_id=kb_config.embedding_provider_id,
            rerank_provider_id=kb_config.rerank_provider_id,
            chunk_size=desired_chunk_size,
            chunk_overlap=desired_chunk_overlap,
            top_k_dense=kb_config.top_k_dense,
            top_k_sparse=kb_config.top_k_sparse,
            top_m_final=kb_config.top_m_final,
        )
        if updated_kb is None:
            raise RuntimeError("更新目标知识库配置失败。")
        if (
            updated_kb.kb.chunk_size != desired_chunk_size
            or updated_kb.kb.chunk_overlap != desired_chunk_overlap
        ):
            raise RuntimeError("更新目标知识库分块配置失败。")
        return updated_kb, True

    def _collect_markdown_files(self, snapshot_root: Path) -> list[Path]:
        markdown_files = [
            path
            for path in snapshot_root.rglob("*")
            if path.is_file()
            and path.suffix.lower() in MARKDOWN_SUFFIXES
            and self._is_included_markdown_path(path.relative_to(snapshot_root))
            and ".git" not in path.parts
        ]
        return sorted(
            markdown_files,
            key=lambda path: path.relative_to(snapshot_root).as_posix(),
        )

    def _is_included_markdown_path(self, relative_path: Path) -> bool:
        ignore_paths = self._get_ignore_paths()

        if any(
            path == relative_path or path in relative_path.parents
            for path in ignore_paths
        ):
            return False
        return True

    def _build_current_markdown_index(
        self,
        snapshot_root: Path,
        markdown_files: list[Path],
    ) -> dict[str, Path]:
        return {
            markdown_file.relative_to(snapshot_root).as_posix(): markdown_file
            for markdown_file in markdown_files
        }

    async def _get_changed_markdown_paths(
        self,
        repo_dir: Path,
        last_head: str,
        remote_head: str,
    ) -> set[str] | None:
        if last_head == remote_head:
            return set()

        try:
            output = await self._run_git_command(
                [
                    "diff",
                    "--name-status",
                    "--find-renames",
                    last_head,
                    remote_head,
                ],
                workdir=str(repo_dir),
            )
        except Exception as exc:
            logger.warning(
                "knowledge_repo_sync could not diff %s..%s, falling back to full sync: %s",
                last_head,
                remote_head,
                exc,
            )
            return None

        changed_paths = set()
        for raw_line in output.splitlines():
            line = raw_line.strip()
            if not line:
                continue

            parts = line.split("\t")
            if len(parts) < 2:
                continue

            status = parts[0]
            path_candidates = (
                [parts[2]]
                if status.startswith(("R", "C")) and len(parts) >= 3
                else [parts[1]]
            )
            for raw_path in path_candidates:
                try:
                    normalized = self._normalize_repo_relative_path(raw_path)
                except ValueError as exc:
                    logger.warning(
                        "knowledge_repo_sync skipped invalid diff path %s: %s",
                        raw_path,
                        exc,
                    )
                    continue

                if normalized is None:
                    continue
                if normalized.suffix.lower() not in MARKDOWN_SUFFIXES:
                    continue
                if not self._is_included_markdown_path(normalized):
                    continue
                changed_paths.add(normalized.as_posix())

        return changed_paths

    async def _list_all_documents(self, kb_helper: KBHelper) -> list[KBDocument]:
        documents: list[KBDocument] = []
        offset = 0
        limit = 200

        while True:
            batch = await kb_helper.list_documents(offset=offset, limit=limit)
            documents.extend(batch)
            if len(batch) < limit:
                return documents
            offset += len(batch)

    async def _sync_markdown_documents(
        self,
        kb_helper: KBHelper,
        current_files: dict[str, Path],
        changed_paths: set[str],
        full_sync: bool,
        chunk_size: int,
        chunk_overlap: int,
    ) -> tuple[int, int]:
        existing_documents = await self._list_all_documents(kb_helper)
        documents_by_name: dict[str, list[KBDocument]] = {}
        for document in existing_documents:
            documents_by_name.setdefault(document.doc_name, []).append(document)

        current_names = set(current_files)
        existing_names = set(documents_by_name)
        missing_names = current_names - existing_names
        upsert_names = (
            set(current_names) if full_sync else (changed_paths & current_names)
        )
        upsert_names.update(missing_names)
        delete_names = (existing_names - current_names) | (
            upsert_names & existing_names
        )

        deleted_count = 0
        for name in sorted(delete_names):
            deleted_count += await self._delete_documents(
                kb_helper, documents_by_name[name]
            )

        imported_count = await self._import_markdown_documents(
            kb_helper=kb_helper,
            markdown_files=[
                (name, current_files[name]) for name in sorted(upsert_names)
            ],
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
        )
        return imported_count, deleted_count

    async def _delete_documents(
        self,
        kb_helper: KBHelper,
        documents: list[KBDocument],
    ) -> int:
        deleted_count = 0
        for document in documents:
            await self._delete_document_with_media(kb_helper, document)
            logger.info(
                "knowledge_repo_sync deleted document: file=%s doc_id=%s",
                document.doc_name,
                document.doc_id,
            )
            deleted_count += 1
        return deleted_count

    async def _delete_document_with_media(
        self,
        kb_helper: KBHelper,
        document: KBDocument,
    ) -> None:
        media_items = await kb_helper.kb_db.list_media_by_doc(document.doc_id)
        media_paths = [
            Path(media.file_path) for media in media_items if media.file_path
        ]

        await self._delete_kb_media_records(kb_helper, document.doc_id)
        await kb_helper.delete_document(document.doc_id)

        for media_path in media_paths:
            try:
                if media_path.exists():
                    media_path.unlink()
            except Exception as exc:
                logger.warning(
                    "knowledge_repo_sync failed to remove media file %s: %s",
                    media_path,
                    exc,
                )

        media_dir = kb_helper.kb_medias_dir / document.doc_id
        if media_dir.exists():
            try:
                shutil.rmtree(media_dir)
            except Exception as exc:
                logger.warning(
                    "knowledge_repo_sync failed to remove media directory %s: %s",
                    media_dir,
                    exc,
                )

    async def _delete_kb_media_records(self, kb_helper: KBHelper, doc_id: str) -> None:
        async with kb_helper.kb_db.get_db() as session:
            async with session.begin():
                await session.execute(delete(KBMedia).where(KBMedia.doc_id == doc_id))
                await session.commit()

    async def _import_markdown_documents(
        self,
        kb_helper: KBHelper,
        markdown_files: list[tuple[str, Path]],
        chunk_size: int,
        chunk_overlap: int,
    ) -> int:
        imported_count = 0

        for relative_name, markdown_file in markdown_files:
            file_bytes = markdown_file.read_bytes()

            async def progress_callback(stage: str, current: int, total: int) -> None:
                logger.info(
                    "knowledge_repo_sync progress: file=%s stage=%s progress=%s/%s",
                    relative_name,
                    stage,
                    current,
                    total,
                )

            doc = await kb_helper.upload_document(
                file_name=relative_name,
                file_content=file_bytes,
                file_type="md",
                chunk_size=chunk_size,
                chunk_overlap=chunk_overlap,
                batch_size=self._get_embedding_batch_size(),
                tasks_limit=self._get_embedding_tasks_limit(),
                max_retries=self._get_embedding_max_retries(),
                progress_callback=progress_callback,
            )
            logger.info(
                "knowledge_repo_sync imported document: file=%s chunks=%s",
                relative_name,
                doc.chunk_count,
            )
            imported_count += 1

        return imported_count
