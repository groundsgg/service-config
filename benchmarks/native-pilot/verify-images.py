#!/usr/bin/env python3
"""Check the actual native bytes, claimed checksum and nonroot architecture of each image."""
import json
import subprocess
import sys
from provenance import copy_file


def verify(images):
    hashes = set()
    for image in images:
        metadata = json.loads(subprocess.check_output(['docker', 'image', 'inspect', image]))[0]
        if metadata['Architecture'] != 'amd64' or metadata['Os'] != 'linux':
            raise ValueError(f'{image}: expected linux/amd64')
        user = metadata['Config'].get('User', '').split(':')[0]
        if user in ('', '0', 'root'):
            raise ValueError(f'{image}: must run as nonroot')
        if metadata['Config']['Entrypoint'][0] != '/deployments/application':
            raise ValueError(f'{image}: expected direct native entrypoint')
        container = subprocess.check_output(['docker', 'create', image], text=True).strip()
        try:
            actual = copy_file(container, '/deployments/application')
            claimed = copy_file(container, '/deployments/application.sha256')
            if actual != claimed:
                raise ValueError(f'{image}: binary checksum mismatch')
            hashes.add(actual)
            print(json.dumps({'image': image, 'binarySha256': actual, 'user': user, 'architecture': 'amd64'}))
        finally:
            subprocess.run(['docker', 'rm', container], check=True, stdout=subprocess.DEVNULL)
    if len(hashes) != 1:
        raise ValueError('Runtime images were built from different native binaries')


if __name__ == '__main__':
    if len(sys.argv) < 3:
        raise SystemExit('Usage: verify-images.py IMAGE IMAGE [IMAGE...]')
    verify(sys.argv[1:])
