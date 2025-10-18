"""Utilities for importing downloads via yt-dlp and streaming them to Notion."""

from __future__ import annotations

import os
import queue
import shlex
import shutil
import subprocess
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Optional, Set


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
        self._job_counters: Dict[str, Dict[str, int]] = {}

    # Public API -----------------------------------------------------------------
    def execute(self, job_id: str, parse_progress: Callable[[str], Optional[Dict[str, Any]]]) -> None:
        """Execute a yt-dlp job and stream resulting files to Notion."""
        job_snapshot = self.job_registry.get_internal(job_id)
        if not job_snapshot:
            return

        if self.upload_manager is None:
            self.job_registry.update(job_id, status='failed', error='Streaming upload manager is not configured')
            return

        user_database_id = job_snapshot.get('user_database_id')
        folder_path = (job_snapshot.get('folder_path') or '/').strip() or '/'

        if not user_database_id:
            self.job_registry.update(job_id, status='failed', error='User database ID is required to upload files')
            return

        if self.ensure_folder_structure:
            try:
                self.ensure_folder_structure(user_database_id, folder_path)
            except Exception as exc:  # pragma: no cover - defensive logging
                self.job_registry.update(job_id, status='failed', error=str(exc))
                return

        normalized_command = job_snapshot.get('normalized_command')
        if not normalized_command:
            self.job_registry.update(job_id, status='failed', error='No command arguments were generated for yt-dlp')
            return

        with tempfile.TemporaryDirectory(prefix=f"yt-dlp-{job_id}-") as output_dir:
            command_args = self._build_command(normalized_command, output_dir)

            executable = command_args[0]
            if shutil.which(executable) is None:
                self.job_registry.update(job_id, status='failed', error=f"Executable '{executable}' is not available on the server")
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
                self.job_registry.update(job_id, status='failed', error=str(exc))
                return

            self.job_registry.set_process(job_id, process)
            self.job_registry.update(job_id, status='running', started_at=self._utcnow())

            stop_event = threading.Event()
            process_done_event = threading.Event()
            files_queue: "queue.Queue[Path]" = queue.Queue()
            upload_error: Dict[str, Exception] = {}

            monitor_thread = threading.Thread(
                target=self._monitor_downloads,
                args=(job_id, Path(output_dir), files_queue, stop_event, process_done_event),
                daemon=True,
            )
            upload_thread = threading.Thread(
                target=self._process_files,
                args=(job_id, user_database_id, folder_path, files_queue, stop_event, upload_error),
                daemon=True,
            )

            monitor_thread.start()
            upload_thread.start()

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
                process_done_event.set()
                stop_event.set()
                self.job_registry.clear_process(job_id)

            # Ensure monitoring stopped discovering files
            monitor_thread.join()
            # Signal uploader that discovery is complete
            files_queue.put(None)
            upload_thread.join()
            self._job_counters.pop(job_id, None)

            if upload_error.get('error'):
                self.job_registry.update(job_id, status='failed', error=str(upload_error['error']), completed_at=self._utcnow())
                return

            final_snapshot = self.job_registry.get_internal(job_id)
            if not final_snapshot or final_snapshot.get('status') == 'cancelled':
                return

            if exit_code == 0:
                self.job_registry.update(job_id, status='completed', completed_at=self._utcnow(), error=None)
                self.job_registry.update_progress(
                    job_id,
                    {
                        'percentage': 100.0,
                        'upload_percentage': 100.0,
                        'stage': 'completed',
                    },
                )
            else:
                self.job_registry.update(job_id, status='failed', error=f'yt-dlp exited with code {exit_code}', completed_at=self._utcnow())

    # Internal helpers -----------------------------------------------------------
    def _build_command(self, normalized_command: str, output_dir: str) -> Iterable[str]:
        parts = shlex.split(normalized_command)
        cleaned_parts = []
        skip_next = False
        for idx, token in enumerate(parts):
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
        ])
        return cleaned_parts

    def _monitor_downloads(
        self,
        job_id: str,
        output_dir: Path,
        files_queue: 'queue.Queue[Optional[Path]]',
        stop_event: threading.Event,
        process_done_event: threading.Event,
    ) -> None:
        observed_sizes: Dict[Path, int] = {}
        processed: Set[Path] = set()

        while True:
            try:
                entries = list(output_dir.iterdir())
            except FileNotFoundError:
                break

            pending_rescan = False
            for entry in entries:
                if entry in processed:
                    continue
                if not entry.is_file():
                    continue
                if entry.suffix == '.part':
                    continue
                part_marker = entry.with_suffix(entry.suffix + '.part')
                if part_marker.exists():
                    continue

                size = entry.stat().st_size
                previous = observed_sizes.get(entry)
                if previous is None or previous != size:
                    observed_sizes[entry] = size
                    pending_rescan = True
                    continue

                processed.add(entry)
                files_queue.put(entry)

            if process_done_event.is_set() and not entries:
                break

            if stop_event.is_set() and process_done_event.is_set() and not pending_rescan:
                break

            time.sleep(0.5)

    def _process_files(
        self,
        job_id: str,
        user_database_id: str,
        folder_path: str,
        files_queue: 'queue.Queue[Optional[Path]]',
        stop_event: threading.Event,
        upload_error: Dict[str, Exception],
    ) -> None:
        counters = self._job_counters.setdefault(job_id, {'total_files': 0})
        index = 0
        while True:
            try:
                entry = files_queue.get(timeout=0.5)
            except queue.Empty:
                time.sleep(0.1)
                continue

            if entry is None:
                files_queue.task_done()
                break

            index += 1
            try:
                self._upload_file(job_id, entry, user_database_id, folder_path, index)
                counters['total_files'] = max(counters.get('total_files', 0), index)
            except Exception as exc:  # pragma: no cover - surfaced to caller
                upload_error['error'] = exc
                stop_event.set()
            finally:
                files_queue.task_done()

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
                    'total_files': max(index, self._job_counters.get(job_id, {}).get('total_files', index)),
                },
            )
        finally:
            try:
                file_path.unlink()
            except FileNotFoundError:
                pass

    @staticmethod
    def _utcnow() -> str:
        from datetime import datetime, timezone

        return datetime.now(timezone.utc).isoformat()
