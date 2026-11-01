#!/usr/bin/env python3
"""Tie local benchmark images to their build inputs and hash actual packaged bytes."""
import argparse
import hashlib
import os
import subprocess
import tarfile
from pathlib import Path


def source_hash(root):
    paths = subprocess.check_output(['git', 'ls-files', '-cz', '--others', '--exclude-standard'], cwd=root).split(b'\0')
    digest = hashlib.sha256()
    for raw in sorted(set(paths)):
        if not raw:
            continue
        path = root / os.fsdecode(raw)
        if path.is_file():
            digest.update(raw + b'\0' + path.read_bytes())
    return digest.hexdigest()


def validate_image(metadata, commit, source, variant, expected_id=None):
    labels = metadata['Config'].get('Labels') or {}
    expected_runtime = 'jvm' if variant == 'jvm-tuned' else variant
    if (labels.get('org.opencontainers.image.revision') != commit or
            labels.get('gg.grounds.source-tree-sha256') != source or
            labels.get('gg.grounds.runtime') != expected_runtime):
        raise ValueError('Image build provenance does not match the measured source/runtime')
    if expected_id is not None and metadata['Id'] != expected_id:
        raise ValueError('Image tag changed after the matrix was frozen')


def copy_file(container, path):
    process = subprocess.Popen(['docker', 'cp', container + ':' + path, '-'], stdout=subprocess.PIPE)
    try:
        with tarfile.open(fileobj=process.stdout, mode='r|') as archive:
            member = next(iter(archive))
            source = archive.extractfile(member)
            if source is None:
                raise ValueError('Expected a regular binary/checksum file')
            digest = hashlib.sha256()
            text = bytearray()
            while block := source.read(1024 * 1024):
                digest.update(block)
                if path.endswith('.sha256'):
                    text.extend(block)
            for _ in archive:
                pass
        if process.wait() != 0:
            raise RuntimeError('docker cp failed')
        return text.decode().split()[0] if text else digest.hexdigest()
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-hash', action='store_true', required=True)
    args = parser.parse_args()
    print(source_hash(Path(__file__).resolve().parents[2]))
