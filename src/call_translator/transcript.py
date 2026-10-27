"""Opt-in append-only receipt fragments; disk never runs on the audio event loop."""
import asyncio
from datetime import datetime, timezone
import os
from pathlib import Path
import queue
import threading
import time
import uuid

# Git-ignored folder inside the checkout; transcripts never leave the machine.
PRIVATE_DIRECTORY = Path(__file__).resolve().parents[2] / '.local' / 'transcripts'
MAX_TEXT = 16384


class Transcript:
    def __init__(self, languages, notify, *, directory=PRIVATE_DIRECTORY,
                 clock=time.monotonic, capacity=256):
        self.clock, self.started, self.notify = clock, clock(), notify
        self.languages = languages
        self.directory = Path(directory)
        self.path = self.directory / (datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ-')
                                      + uuid.uuid4().hex + '.md')
        self.queue = queue.Queue(maxsize=capacity)
        self.stop = threading.Event()
        self.disabled = False
        self.partial = False
        self.outcome = 'unfinished'
        self.thread = threading.Thread(target=self._write, daemon=True, name='private-transcript')
        self.thread.start()

    def fail(self):
        if not self.disabled:
            self.disabled = self.partial = True
            self.notify('transcript_warning', reason='saving_unavailable', voice_continues=True)

    def submit(self, direction, generation, kind, text='', finished=False):
        if self.disabled or self.stop.is_set():
            return
        if len(text) > MAX_TEXT:
            self.fail()
            return
        try:
            self.queue.put_nowait((self.clock() - self.started, direction, generation,
                                   kind, text, finished))
        except queue.Full:
            self.fail()

    def gap(self, direction):
        self.partial = True
        self.submit(direction, 0, 'gap')

    def _open(self):
        # Refuse symlinked private directories; never redirect speech to an arbitrary path.
        for path in (self.directory.parent, self.directory):
            path.mkdir(mode=0o700, exist_ok=True)
            if path.is_symlink() or not path.is_dir():
                raise OSError('private_directory_unavailable')
        os.chmod(self.directory, 0o700)
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        return os.fdopen(fd, 'w', encoding='utf-8')

    def _write(self):
        tails = set()
        try:
            with self._open() as output:
                output.write('# Machine call transcript\n\n'
                             f'Start (UTC): {datetime.now(timezone.utc).isoformat()}\n\n'
                             'Receipt times are relative, not word timestamps. Independent fragments; '
                             'no phrase pairing or speaker identification. Missing text is unknown. '
                             'Treat this file as partial unless an End footer says otherwise.\n\n')
                for direction, (source, target) in self.languages.items():
                    output.write(f'{direction}: {source} → {target}\n\n')
                output.flush()
                while not self.stop.is_set() or not self.queue.empty():
                    try:
                        elapsed, direction, generation, kind, text, finished = self.queue.get(timeout=.05)
                    except queue.Empty:
                        continue
                    if kind == 'gap':
                        output.write(f'\n> +{elapsed:.3f}s {direction}: recovery gap; text/audio may be missing.\n\n')
                    else:
                        key = (direction, generation, kind)
                        if text:
                            tails.add(key)
                        if finished:
                            tails.discard(key)
                        who = 'me' if direction == 'outgoing' else 'remote participants'
                        output.write(f'\n### +{elapsed:.3f}s {direction} / {who} / generation {generation} / {kind}'
                                     f' / {"finished" if finished else "fragment"}\n\n')
                        # Indented literal text prevents speech from impersonating headings/HTML.
                        output.write(''.join('    ' + line + '\n' for line in text.splitlines()) + '\n')
                    output.flush()
                if tails:
                    self.partial = True
                    for direction, generation, kind in sorted(tails):
                        output.write(f'\n> Partial tail: {direction} generation {generation} {kind}; completion unknown.\n')
                output.write(f'\nEnd: {self.outcome}; partial={str(self.partial).lower()}. '
                             'Machine text may omit or mistranslate speech.\n')
                output.flush()
        except Exception:
            # Never expose filesystem exceptions which could contain private content.
            self.fail()
        finally:
            while True:
                try:
                    self.queue.get_nowait()
                except queue.Empty:
                    break

    async def close(self, outcome, *, timeout=1.):
        self.outcome = outcome
        if outcome not in ('selected_meet_left', 'selected_tab_closed'):
            self.partial = True
        self.stop.set()
        deadline = time.monotonic() + timeout
        while self.thread.is_alive() and time.monotonic() < deadline:
            await asyncio.sleep(.01)
        if self.thread.is_alive():
            self.fail()
        self.notify('transcript_saved', path=str(self.path), partial=self.partial,
                    writer_finished=not self.thread.is_alive(), saving_disabled=self.disabled)
