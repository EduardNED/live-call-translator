"""Explicit local helper build with the installed Apple toolchain; no installation or UI."""
from pathlib import Path
import os
import subprocess
import tempfile


def prepare(target):
    target = Path(target)
    if target.parent != Path('.local') or target.is_symlink() or target.parent.is_symlink():
        raise ValueError('Build the helper directly under .local/')
    target.parent.mkdir(exist_ok=True)
    source = Path(__file__).parent / 'native' / 'meet_observer.swift'
    with tempfile.TemporaryDirectory(prefix='.native-build-', dir=target.parent) as directory:
        binary = Path(directory) / 'meet-observer'
        subprocess.run(['/usr/bin/xcrun', 'swiftc', str(source), '-o', str(binary)],
                       check=True, capture_output=True, timeout=60)
        binary.chmod(0o700)
        os.replace(binary, target)
