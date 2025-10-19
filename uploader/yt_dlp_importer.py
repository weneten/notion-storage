"""Utilities for importing downloads via yt-dlp and streaming them to Notion."""

from __future__ import annotations

import logging
import os
import queue
import shlex
import shutil
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Set


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class _JobContext:
    """Track state shared between the downloader and uploader threads."""

    job_id: str
    output_dir: Path
    files_queue: "queue.Queue[Optional[Path]]"
    stop_event: threading.Event
    process_done_event: threading.Event
    counters: Dict[str, int] = field(default_factory=lambda: {'files_discovered': 0, 'files_uploaded': 0})
    upload_error: Optional[Exception] = None
    discovery_error: Optional[Exception] = None

    def record_discovery(self) -> None:
        self.counters['files_discovered'] = self.counters.get('files_discovered', 0) + 1

    def record_upload(self, index: int) -> None:
        self.counters['files_uploaded'] = index


class YtDlpImporter:
    """Manage yt-dlp based import jobs with background processing."""

    def __init__(
        self,
        upload_manager,
        job_registry,
        ensure_folder_structure: Optional[Callable[[str, str], None]] = None,
    ) -> None:
        self.upload_manager = upload_manager
        self.job_registry = job_registry
        self.ensure_folder_structure = ensure_folder_structure

    # Public API -----------------------------------------------------------------
    def execute(self, job_id: str, parse_progress: Callable[[str], Optional[Dict[str, Any]]]) -> None:
        """Execute a yt-dlp job and stream resulting files to Notion."""
        job_snapshot = self.job_registry.get_internal(job_id)
        if not job_snapshot:
            logger.warning('Job %s missing from registry; aborting execution', job_id)
            return

        logger.info(
            'Preparing yt-dlp job %s (url=%s)',
            job_id,
            job_snapshot.get('url'),
        )

        if self.upload_manager is None:
            logger.error('Upload manager not configured; aborting job %s', job_id)
            self.job_registry.update(job_id, status='failed', error='Streaming upload manager is not configured')
            self.job_registry.update_progress(job_id, {'stage': 'failed'})
            return

        user_database_id = job_snapshot.get('user_database_id')
        folder_path = (job_snapshot.get('folder_path') or '/').strip() or '/'
        logger.info(
            'Starting yt-dlp job %s for destination %s (initial database=%s)',
            job_id,
            folder_path,
            user_database_id,
        )

        if not user_database_id:
            resolved_database_id = self._resolve_user_database_id(job_snapshot.get('requested_by'))
            if resolved_database_id:
                user_database_id = resolved_database_id
                job_snapshot['user_database_id'] = resolved_database_id
                self.job_registry.update(job_id, user_database_id=resolved_database_id)
                logger.info('Resolved database %s for job %s', resolved_database_id, job_id)
            else:
                logger.error('Unable to resolve database for job %s', job_id)
                self.job_registry.update(job_id, status='failed', error='User database ID is required to upload files')
                self.job_registry.update_progress(job_id, {'stage': 'failed'})
                return

        if self.ensure_folder_structure:
            try:
                self.ensure_folder_structure(user_database_id, folder_path)
                logger.debug('Ensured folder structure %s for job %s', folder_path, job_id)
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.exception('Folder structure preparation failed for job %s', job_id)
                self.job_registry.update(job_id, status='failed', error=str(exc))
                self.job_registry.update_progress(job_id, {'stage': 'failed'})
                return

        normalized_command = job_snapshot.get('normalized_command')
        if not normalized_command:
            logger.error('No normalized command found for job %s', job_id)
            self.job_registry.update(job_id, status='failed', error='No command arguments were generated for yt-dlp')
            self.job_registry.update_progress(job_id, {'stage': 'failed'})
            return

        with tempfile.TemporaryDirectory(prefix=f"yt-dlp-{job_id}-") as output_dir:
            command_args = self._build_command(normalized_command, output_dir)
            human_command = ' '.join(shlex.quote(part) for part in command_args)
            logger.info('Executing yt-dlp for job %s: %s', job_id, human_command)
            self.job_registry.append_log(job_id, f'Executing: {human_command}')

            executable = command_args[0]
            if shutil.which(executable) is None:
                logger.error("Executable '%s' missing for job %s", executable, job_id)
                self.job_registry.update(job_id, status='failed', error=f"Executable '{executable}' is not available on the server")
                self.job_registry.update_progress(job_id, {'stage': 'failed'})
                return

            try:
                process = subprocess.Popen(
                    command_args,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    universal_newlines=True,
                )
            except Exception as exc:
                logger.exception('Failed to start yt-dlp process for job %s', job_id)
                self.job_registry.update(job_id, status='failed', error=str(exc))
                self.job_registry.update_progress(job_id, {'stage': 'failed'})
                return

            self.job_registry.set_process(job_id, process)
            self.job_registry.update(job_id, status='running', started_at=self._utcnow())
            self.job_registry.update_progress(job_id, {'stage': 'downloading'})
            logger.info('yt-dlp process started for job %s (pid=%s)', job_id, getattr(process, 'pid', None))

            context = _JobContext(
                job_id=job_id,
                output_dir=Path(output_dir),
                files_queue=queue.Queue(),
                stop_event=threading.Event(),
                process_done_event=threading.Event(),
            )

            monitor_thread = threading.Thread(
                target=self._monitor_downloads,
                args=(context,),
                daemon=True,
            )
            upload_thread = threading.Thread(
                target=self._process_files,
                args=(context, user_database_id, folder_path),
                daemon=True,
            )

            monitor_thread.start()
            upload_thread.start()

            try:
                exit_code = self._consume_process_output(process, context.job_id, parse_progress)
            finally:
                context.process_done_event.set()
                context.stop_event.set()
                self.job_registry.clear_process(job_id)

            monitor_thread.join()
            upload_thread.join()

            logger.info('yt-dlp process finished for job %s with exit code %s', job_id, exit_code)

            if context.discovery_error:
                failure_message = f'Failed to prepare downloaded files: {context.discovery_error}'
                logger.error('Job %s discovery error: %s', job_id, context.discovery_error)
                self.job_registry.update(job_id, status='failed', error=failure_message, completed_at=self._utcnow())
                self.job_registry.update_progress(job_id, {'stage': 'failed'})
                return

            if context.upload_error:
                logger.error('Job %s upload error: %s', job_id, context.upload_error)
                self.job_registry.update(job_id, status='failed', error=str(context.upload_error), completed_at=self._utcnow())
                self.job_registry.update_progress(job_id, {'stage': 'failed'})
                return

            final_snapshot = self.job_registry.get_internal(job_id)
            if not final_snapshot or final_snapshot.get('status') == 'cancelled':
                logger.info('Job %s cancelled during execution', job_id)
                return

            files_discovered = context.counters.get('files_discovered', 0)

            if exit_code == 0:
                if files_discovered == 0:
                    failure_message = 'yt-dlp completed without downloading any files.'
                    logger.error('Job %s completed without downloads', job_id)
                    self.job_registry.update(
                        job_id,
                        status='failed',
                        error=failure_message,
                        completed_at=self._utcnow(),
                    )
                    self.job_registry.update_progress(
                        job_id,
                        {
                            'stage': 'failed',
                            'status_message': failure_message,
                            'files_completed': 0,
                            'total_files': 0,
                        },
                    )
                    return
                logger.info('Job %s completed successfully (%s files)', job_id, files_discovered)
                self.job_registry.append_log(job_id, f'Completed successfully with {files_discovered} files')
                self.job_registry.update(job_id, status='completed', completed_at=self._utcnow(), error=None)
                self.job_registry.update_progress(
                    job_id,
                    {
                        'percentage': 100.0,
                        'upload_percentage': 100.0,
                        'stage': 'done',
                    },
                )
            else:
                logger.error('yt-dlp exited with code %s for job %s', exit_code, job_id)
                self.job_registry.append_log(job_id, f'yt-dlp exited with code {exit_code}')
                self.job_registry.update(job_id, status='failed', error=f'yt-dlp exited with code {exit_code}', completed_at=self._utcnow())
                self.job_registry.update_progress(job_id, {'stage': 'failed'})

    # Internal helpers -----------------------------------------------------------
    def _build_command(self, normalized_command: str, output_dir: str) -> Iterable[str]:
        parts = shlex.split(normalized_command)
        if not parts:
            return []

        # ``_normalize_yt_dlp_inputs`` always appends the target URL as the final
        # argument.  Preserve that behaviour explicitly so we can safely add our
        # own ``--output`` argument *before* the URL.  Putting the URL before the
        # option caused yt-dlp to treat the destination path as a second URL in
        # some environments, which meant downloads never started.
        url_token = parts[-1]
        option_tokens = parts[:-1]

        cleaned_parts = []
        skip_next = False
        for token in option_tokens:
            if skip_next:
                skip_next = False
                continue
            if token in {'-o', '--output'}:
                skip_next = True
                continue
            cleaned_parts.append(token)

        cleaned_parts.extend([
            '--output',
            os.path.join(output_dir, '%(autonumber+000)3d_%(title)s.%(ext)s'),
            url_token,
        ])
        return cleaned_parts

    def _consume_process_output(
        self,
        process: subprocess.Popen,
        job_id: str,
        parse_progress: Callable[[str], Optional[Dict[str, Any]]],
    ) -> int:
        exit_code = 1
        try:
            if process.stdout is not None:
                for raw_line in process.stdout:
                    line = raw_line.rstrip('\n')
                    self.job_registry.append_log(job_id, line)
                    progress_update = parse_progress(line)
                    if progress_update:
                        progress_update['stage'] = 'downloading'
                        self.job_registry.update_progress(job_id, progress_update)
            exit_code = process.wait()
        finally:
            if process.stdout is not None:
                try:
                    process.stdout.close()
                except Exception:
                    pass
        return exit_code

    def _monitor_downloads(self, context: _JobContext) -> None:
        observed_sizes: Dict[Path, int] = {}
        processed: Set[Path] = set()

        try:
            while not context.stop_event.is_set() or not context.process_done_event.is_set():
                try:
                    entries = list(context.output_dir.iterdir())
                except FileNotFoundError:
                    break

                saw_ready_file = False
                for entry in entries:
                    if entry in processed:
                        continue
                    if not entry.is_file():
                        continue
                    if entry.name.endswith('.part'):
                        continue
                    part_marker = entry.with_suffix(entry.suffix + '.part')
                    if part_marker.exists():
                        continue

                    size = entry.stat().st_size
                    previous = observed_sizes.get(entry)
                    if previous is None or previous != size:
                        observed_sizes[entry] = size
                        continue

                    processed.add(entry)
                    context.files_queue.put(entry)
                    context.record_discovery()
                    logger.info('Job %s discovered file %s (%s bytes)', context.job_id, entry.name, size)
                    try:
                        self.job_registry.append_log(context.job_id, f'Discovered {entry.name} ({size} bytes)')
                    except Exception:
                        pass
                    saw_ready_file = True

                if context.process_done_event.is_set():
                    pending = [
                        entry
                        for entry in entries
                        if entry.is_file()
                        and not entry.name.endswith('.part')
                        and entry not in processed
                    ]
                    if not pending:
                        break

                if not saw_ready_file:
                    time.sleep(0.5)
        except Exception as exc:  # pragma: no cover - defensive logging
            context.discovery_error = exc
            try:
                self.job_registry.append_log(context.job_id, f'File discovery error: {exc}')
            except Exception:
                pass
        finally:
            context.files_queue.put(None)

    def _process_files(
        self,
        context: _JobContext,
        user_database_id: str,
        folder_path: str,
    ) -> None:
        index = 0
        try:
            while True:
                try:
                    entry = context.files_queue.get(timeout=0.5)
                except queue.Empty:
                    if context.stop_event.is_set() and context.process_done_event.is_set():
                        break
                    continue

                if entry is None:
                    context.files_queue.task_done()
                    break

                index += 1
                try:
                    self._upload_file(context.job_id, entry, user_database_id, folder_path, index)
                    context.record_upload(index)
                except Exception as exc:  # pragma: no cover - surfaced to caller
                    context.upload_error = exc
                    context.stop_event.set()
                    try:
                        self.job_registry.append_log(context.job_id, f'Upload error: {exc}')
                    except Exception:
                        pass
                finally:
                    context.files_queue.task_done()
        finally:
            while not context.files_queue.empty():
                try:
                    context.files_queue.get_nowait()
                    context.files_queue.task_done()
                except queue.Empty:
                    break

    def _upload_file(self, job_id: str, file_path: Path, user_database_id: str, folder_path: str, index: int) -> None:
        file_size = file_path.stat().st_size
        file_name = file_path.name

        progress_update = {
            'stage': 'uploading',
            'current_file': file_name,
            'current_file_index': index,
            'current_file_size': file_size,
            'upload_percentage': 0.0,
        }
        self.job_registry.update_progress(job_id, progress_update)
        logger.info(
            'Job %s uploading file #%s %s (%s bytes) to %s',
            job_id,
            index,
            file_name,
            file_size,
            folder_path,
        )
        try:
            self.job_registry.append_log(job_id, f'Uploading {file_name} ({file_size} bytes)')
        except Exception:
            pass

        def stream() -> Iterable[bytes]:
            with file_path.open('rb') as handle:
                while True:
                    chunk = handle.read(1024 * 1024)
                    if not chunk:
                        break
                    yield chunk

        def upload_progress(percentage: float, bytes_uploaded: int) -> None:
            self.job_registry.update_progress(
                job_id,
                {
                    'stage': 'uploading',
                    'upload_percentage': percentage,
                    'current_file_uploaded_bytes': bytes_uploaded,
                    'current_file_size': file_size,
                    'current_file': file_name,
                    'current_file_index': index,
                },
            )

        upload_id = self.upload_manager.create_upload_session(
            filename=file_name,
            file_size=file_size,
            user_database_id=user_database_id,
            progress_callback=upload_progress,
            folder_path=folder_path,
        )

        try:
            self.upload_manager.process_upload_stream(upload_id, stream())
            logger.info('Job %s uploaded %s', job_id, file_name)
            try:
                self.job_registry.append_log(job_id, f'Uploaded {file_name}')
            except Exception:
                pass
            self.job_registry.update_progress(
                job_id,
                {
                    'stage': 'uploading',
                    'upload_percentage': 100.0,
                    'current_file_uploaded_bytes': file_size,
                    'current_file_size': file_size,
                    'current_file': file_name,
                    'current_file_index': index,
                    'files_completed': index,
                    'total_files': index,
                },
            )
        finally:
            try:
                self.job_registry.update_progress(
                    job_id,
                    {
                        'stage': 'deleting',
                        'current_file': file_name,
                        'current_file_index': index,
                    },
                )
                file_path.unlink()
                logger.debug('Job %s removed temporary file %s', job_id, file_name)
            except FileNotFoundError:
                pass

    @staticmethod
    def _utcnow() -> str:
        from datetime import datetime, timezone

        return datetime.now(timezone.utc).isoformat()

    def _resolve_user_database_id(self, user_id: Optional[str]) -> Optional[str]:
        """Best-effort resolution of a user's database ID at execution time."""

        if not user_id:
            return None

        manager = self.upload_manager
        if manager is None:
            return None

        candidates: List[Any] = []

        def _append_candidate(candidate: Any) -> None:
            if candidate is None:
                return
            if any(existing is candidate for existing in candidates):
                return
            candidates.append(candidate)

        _append_candidate(getattr(manager, 'notion_uploader', None))

        nested = getattr(manager, 'uploader', None)
        _append_candidate(nested)
        if nested is not None:
            _append_candidate(getattr(nested, 'notion_uploader', None))

        for uploader in candidates:
            resolver = getattr(uploader, 'get_user_database_id', None)
            if not callable(resolver):
                continue
            try:
                resolved = resolver(user_id)
            except Exception:
                continue
            if resolved:
                return resolved

        return None
