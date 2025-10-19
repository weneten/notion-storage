"""Orchestrate yt-dlp based remote imports into Notion."""

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
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class _JobMetrics:
    """Light-weight counter tracking for a running job."""

    files_discovered: int = 0
    files_uploaded: int = 0

    def record_discovery(self) -> None:
        self.files_discovered += 1

    def record_upload(self, index: int) -> None:
        self.files_uploaded = index


@dataclass(slots=True)
class _PipelineContext:
    """Shared state between directory observers and upload workers."""

    job_id: str
    output_dir: Path
    files_queue: "queue.Queue[Optional[Path]]" = field(default_factory=queue.Queue)
    stop_event: threading.Event = field(default_factory=threading.Event)
    process_completed: threading.Event = field(default_factory=threading.Event)
    metrics: _JobMetrics = field(default_factory=_JobMetrics)
    discovery_error: Optional[BaseException] = None
    upload_error: Optional[BaseException] = None


class _DirectoryObserver(threading.Thread):
    """Poll a download directory for finished files."""

    def __init__(self, context: _PipelineContext, job_registry) -> None:
        super().__init__(daemon=True)
        self._context = context
        self._job_registry = job_registry
        self._observed_sizes: Dict[Path, int] = {}
        self._processed: set[Path] = set()
        self._sentinel_emitted = False

    def run(self) -> None:  # pragma: no cover - thin wrapper around _poll
        try:
            self._poll()
        finally:
            if not self._sentinel_emitted:
                self._context.files_queue.put(None)
                self._sentinel_emitted = True

    def _poll(self) -> None:
        context = self._context
        try:
            while not context.stop_event.is_set():
                ready_files = self._scan_directory()
                if ready_files:
                    for path in ready_files:
                        context.files_queue.put(path)
                        context.metrics.record_discovery()
                        self._processed.add(path)
                        try:
                            size = path.stat().st_size
                        except FileNotFoundError:
                            size = 0
                        logger.info('Job %s discovered file %s (%s bytes)', context.job_id, path.name, size)
                        try:
                            self._job_registry.append_log(
                                context.job_id,
                                f'Discovered {path.name} ({size} bytes)',
                            )
                        except Exception:  # pragma: no cover - logging should never break pipeline
                            pass
                    continue

                if context.process_completed.is_set():
                    if not self._pending_entries_exist():
                        break

                time.sleep(0.5)
        except Exception as exc:  # pragma: no cover - surfaced to caller
            context.discovery_error = exc
            try:
                self._job_registry.append_log(context.job_id, f'File discovery error: {exc}')
            except Exception:
                pass
        finally:
            context.files_queue.put(None)
            self._sentinel_emitted = True

    def _scan_directory(self) -> List[Path]:
        context = self._context
        directory = context.output_dir
        ready: List[Path] = []
        try:
            entries = list(directory.iterdir())
        except FileNotFoundError:
            context.stop_event.set()
            return ready

        for entry in entries:
            if entry in self._processed:
                continue
            if not entry.is_file():
                continue
            if entry.name.endswith('.part'):
                continue
            part_marker = entry.with_suffix(entry.suffix + '.part')
            if part_marker.exists():
                continue

            size = entry.stat().st_size
            previous_size = self._observed_sizes.get(entry)
            if previous_size is None or previous_size != size:
                # Wait for the size to stabilise before queuing the file.
                self._observed_sizes[entry] = size
                continue

            ready.append(entry)

        return ready

    def _pending_entries_exist(self) -> bool:
        context = self._context
        try:
            for entry in context.output_dir.iterdir():
                if entry in self._processed:
                    continue
                if not entry.is_file():
                    continue
                if entry.name.endswith('.part'):
                    continue
                part_marker = entry.with_suffix(entry.suffix + '.part')
                if part_marker.exists():
                    continue
                return True
        except FileNotFoundError:
            context.stop_event.set()
        return False


class _UploadWorker(threading.Thread):
    """Consume discovered files and stream them to Notion."""

    def __init__(
        self,
        context: _PipelineContext,
        upload_manager,
        job_registry,
        user_database_id: str,
        folder_path: str,
    ) -> None:
        super().__init__(daemon=True)
        self._context = context
        self._upload_manager = upload_manager
        self._job_registry = job_registry
        self._user_database_id = user_database_id
        self._folder_path = folder_path

    def run(self) -> None:
        context = self._context
        index = 0

        try:
            while True:
                try:
                    item = context.files_queue.get(timeout=0.5)
                except queue.Empty:
                    if context.stop_event.is_set() and context.process_completed.is_set():
                        break
                    continue

                if item is None:
                    context.files_queue.task_done()
                    break

                index += 1
                try:
                    self._upload_file(item, index)
                    context.metrics.record_upload(index)
                except Exception as exc:  # pragma: no cover - propagated to main thread
                    context.upload_error = exc
                    context.stop_event.set()
                    try:
                        self._job_registry.append_log(context.job_id, f'Upload error: {exc}')
                    except Exception:
                        pass
                finally:
                    context.files_queue.task_done()
        finally:
            self._drain_queue()

    def _upload_file(self, file_path: Path, index: int) -> None:
        file_size = file_path.stat().st_size
        file_name = file_path.name
        job_id = self._context.job_id

        self._job_registry.update_progress(
            job_id,
            {
                'stage': 'uploading',
                'current_file': file_name,
                'current_file_index': index,
                'current_file_size': file_size,
                'upload_percentage': 0.0,
            },
        )

        logger.info(
            'Job %s uploading file #%s %s (%s bytes) to %s',
            job_id,
            index,
            file_name,
            file_size,
            self._folder_path,
        )
        try:
            self._job_registry.append_log(job_id, f'Uploading {file_name} ({file_size} bytes)')
        except Exception:
            pass

        def stream() -> Iterable[bytes]:
            with file_path.open('rb') as handle:
                while True:
                    chunk = handle.read(1024 * 1024)
                    if not chunk:
                        break
                    yield chunk

        def progress_callback(percentage: float, bytes_uploaded: int) -> None:
            self._job_registry.update_progress(
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

        upload_id = self._upload_manager.create_upload_session(
            filename=file_name,
            file_size=file_size,
            user_database_id=self._user_database_id,
            progress_callback=progress_callback,
            folder_path=self._folder_path,
        )

        try:
            self._upload_manager.process_upload_stream(upload_id, stream())
            logger.info('Job %s uploaded %s', job_id, file_name)
            try:
                self._job_registry.append_log(job_id, f'Uploaded {file_name}')
            except Exception:
                pass
            self._job_registry.update_progress(
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
                self._job_registry.update_progress(
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

    def _drain_queue(self) -> None:
        context = self._context
        while not context.files_queue.empty():
            try:
                item = context.files_queue.get_nowait()
            except queue.Empty:
                break
            try:
                if isinstance(item, Path):
                    try:
                        item.unlink()
                    except FileNotFoundError:
                        pass
            finally:
                context.files_queue.task_done()


class YtDlpImporter:
    """Execute yt-dlp jobs and stream downloads to Notion."""

    def __init__(
        self,
        upload_manager,
        job_registry,
        ensure_folder_structure: Optional[Callable[[str, str], None]] = None,
    ) -> None:
        self.upload_manager = upload_manager
        self.job_registry = job_registry
        self.ensure_folder_structure = ensure_folder_structure

    # ------------------------------------------------------------------ Public
    def execute(self, job_id: str, parse_progress: Callable[[str], Optional[Dict[str, Any]]]) -> None:
        job_snapshot = self.job_registry.get_internal(job_id)
        if not job_snapshot:
            logger.warning('Job %s missing from registry; aborting execution', job_id)
            return

        logger.info('Preparing yt-dlp job %s (url=%s)', job_id, job_snapshot.get('url'))

        if self.upload_manager is None:
            self.job_registry.update(
                job_id,
                status='failed',
                error='Streaming upload manager is not configured',
            )
            self.job_registry.update_progress(job_id, {'stage': 'failed'})
            logger.error('Upload manager not configured; aborting job %s', job_id)
            return

        user_database_id = job_snapshot.get('user_database_id')
        folder_path = (job_snapshot.get('folder_path') or '/').strip() or '/'
        if not user_database_id:
            resolved = self._resolve_user_database_id(job_snapshot.get('requested_by'))
            if resolved:
                user_database_id = resolved
                job_snapshot['user_database_id'] = resolved
                self.job_registry.update(job_id, user_database_id=resolved)
                logger.info('Resolved database %s for job %s', resolved, job_id)
            else:
                self.job_registry.update(
                    job_id,
                    status='failed',
                    error='User database ID is required to upload files',
                )
                self.job_registry.update_progress(job_id, {'stage': 'failed'})
                logger.error('Unable to resolve database for job %s', job_id)
                return

        if self.ensure_folder_structure:
            try:
                self.ensure_folder_structure(user_database_id, folder_path)
            except Exception as exc:  # pragma: no cover - defensive
                logger.exception('Folder structure preparation failed for job %s', job_id)
                self.job_registry.update(job_id, status='failed', error=str(exc))
                self.job_registry.update_progress(job_id, {'stage': 'failed'})
                return

        normalized_command = job_snapshot.get('normalized_command')
        if not normalized_command:
            self.job_registry.update(job_id, status='failed', error='No command arguments were generated for yt-dlp')
            self.job_registry.update_progress(job_id, {'stage': 'failed'})
            logger.error('No normalized command found for job %s', job_id)
            return

        with tempfile.TemporaryDirectory(prefix=f"yt-dlp-{job_id}-") as tmpdir:
            command = list(self._build_command(normalized_command, tmpdir))
            if not command:
                self.job_registry.update(job_id, status='failed', error='Unable to construct yt-dlp command')
                self.job_registry.update_progress(job_id, {'stage': 'failed'})
                logger.error('Failed to build command for job %s', job_id)
                return

            executable = command[0]
            if shutil.which(executable) is None:
                message = f"Executable '{executable}' is not available on the server"
                self.job_registry.update(job_id, status='failed', error=message)
                self.job_registry.update_progress(job_id, {'stage': 'failed'})
                logger.error('Executable %s missing for job %s', executable, job_id)
                return

            pretty_command = ' '.join(shlex.quote(token) for token in command)
            logger.info('Executing yt-dlp for job %s: %s', job_id, pretty_command)
            self.job_registry.append_log(job_id, f'Executing: {pretty_command}')

            try:
                process = subprocess.Popen(
                    command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                    universal_newlines=True,
                )
            except Exception as exc:  # pragma: no cover - process launch failure is rare
                logger.exception('Failed to start yt-dlp process for job %s', job_id)
                self.job_registry.update(job_id, status='failed', error=str(exc))
                self.job_registry.update_progress(job_id, {'stage': 'failed'})
                return

            self.job_registry.set_process(job_id, process)
            self.job_registry.update(
                job_id,
                status='running',
                started_at=self._utcnow(),
            )
            self.job_registry.update_progress(job_id, {'stage': 'downloading'})

            context = _PipelineContext(job_id=job_id, output_dir=Path(tmpdir))
            observer = self._create_directory_observer(context)
            uploader = self._create_upload_worker(context, user_database_id, folder_path)
            observer.start()
            uploader.start()

            try:
                exit_code = self._consume_process_output(process, job_id, parse_progress)
            finally:
                context.process_completed.set()
                context.stop_event.set()
                self.job_registry.clear_process(job_id)

            observer.join()
            uploader.join()

            logger.info('yt-dlp process finished for job %s with exit code %s', job_id, exit_code)

            if context.discovery_error:
                error_message = f'Failed to prepare downloaded files: {context.discovery_error}'
                logger.error('Job %s discovery error: %s', job_id, context.discovery_error)
                self.job_registry.update(job_id, status='failed', error=error_message, completed_at=self._utcnow())
                self.job_registry.update_progress(job_id, {'stage': 'failed'})
                return

            if context.upload_error:
                logger.error('Job %s upload error: %s', job_id, context.upload_error)
                self.job_registry.update(
                    job_id,
                    status='failed',
                    error=str(context.upload_error),
                    completed_at=self._utcnow(),
                )
                self.job_registry.update_progress(job_id, {'stage': 'failed'})
                return

            final_snapshot = self.job_registry.get_internal(job_id)
            if not final_snapshot or final_snapshot.get('status') == 'cancelled':
                logger.info('Job %s cancelled during execution', job_id)
                return

            if exit_code != 0:
                message = f'yt-dlp exited with code {exit_code}'
                logger.error('yt-dlp exited with code %s for job %s', exit_code, job_id)
                self.job_registry.append_log(job_id, message)
                self.job_registry.update(job_id, status='failed', error=message, completed_at=self._utcnow())
                self.job_registry.update_progress(job_id, {'stage': 'failed'})
                return

            if context.metrics.files_discovered == 0:
                message = 'yt-dlp completed without downloading any files.'
                logger.error('Job %s completed without downloads', job_id)
                self.job_registry.update(job_id, status='failed', error=message, completed_at=self._utcnow())
                self.job_registry.update_progress(
                    job_id,
                    {
                        'stage': 'failed',
                        'status_message': message,
                        'files_completed': 0,
                        'total_files': 0,
                    },
                )
                return

            logger.info('Job %s completed successfully (%s files)', job_id, context.metrics.files_discovered)
            self.job_registry.append_log(
                job_id,
                f'Completed successfully with {context.metrics.files_discovered} files',
            )
            self.job_registry.update(
                job_id,
                status='completed',
                completed_at=self._utcnow(),
                error=None,
            )
            self.job_registry.update_progress(
                job_id,
                {
                    'percentage': 100.0,
                    'upload_percentage': 100.0,
                    'stage': 'done',
                },
            )

    # --------------------------------------------------------------- Internals
    def _create_directory_observer(self, context: _PipelineContext) -> _DirectoryObserver:
        return _DirectoryObserver(context, self.job_registry)

    def _create_upload_worker(
        self,
        context: _PipelineContext,
        user_database_id: str,
        folder_path: str,
    ) -> _UploadWorker:
        return _UploadWorker(context, self.upload_manager, self.job_registry, user_database_id, folder_path)

    def _build_command(self, normalized_command: str, output_dir: str) -> Sequence[str]:
        tokens = shlex.split(normalized_command)
        if not tokens:
            return []

        url_token = tokens[-1]
        option_tokens = tokens[:-1]
        cleaned: List[str] = []
        skip_next = False
        for token in option_tokens:
            if skip_next:
                skip_next = False
                continue
            if token in {'-o', '--output'}:
                skip_next = True
                continue
            cleaned.append(token)

        cleaned.extend([
            '--output',
            os.path.join(output_dir, '%(autonumber+000)3d_%(title)s.%(ext)s'),
            url_token,
        ])
        return cleaned

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

    @staticmethod
    def _utcnow() -> str:
        from datetime import datetime, timezone

        return datetime.now(timezone.utc).isoformat()

    def _resolve_user_database_id(self, user_id: Optional[str]) -> Optional[str]:
        if not user_id:
            return None

        manager = self.upload_manager
        if manager is None:
            return None

        candidates: List[Any] = []

        def _append(candidate: Any) -> None:
            if candidate and candidate not in candidates:
                candidates.append(candidate)

        _append(getattr(manager, 'notion_uploader', None))
        nested = getattr(manager, 'uploader', None)
        _append(nested)
        if nested is not None:
            _append(getattr(nested, 'notion_uploader', None))

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
