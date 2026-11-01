#!/usr/bin/env python3
"""Run the full comparison serially, rotating order; never deploy or change live pins."""
import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from provenance import source_hash, validate_image


def main(args):
    here = Path(__file__).resolve().parent
    root = here.parents[1]
    scenario = json.loads(args.scenario.read_text())
    args.output.mkdir(parents=True, exist_ok=True)
    status_path = args.output / 'matrix-status.json'
    variants = ['jvm', 'jvm-tuned', 'native-micro', 'native-distroless']
    commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip()
    tree = source_hash(root)
    frozen = {}
    for variant in variants:
        reference = 'grounds/service-config:' + ('jvm' if variant == 'jvm-tuned' else variant) + '-pilot'
        metadata = json.loads(subprocess.check_output(['docker', 'image', 'inspect', reference]))[0]
        validate_image(metadata, commit, tree, variant)
        frozen[variant] = {'reference': reference, 'imageId': metadata['Id']}
    (args.output / 'build-lock.json').write_text(json.dumps({'commit': commit, 'sourceTreeSha256': tree, 'images': frozen}, indent=2) + '\n')
    status = {'state': 'running', 'startedUnix': time.time(), 'completedRuns': [], 'totalRuns': len(variants) * scenario['repetitions']}
    def save():
        temp = status_path.with_suffix('.tmp')
        temp.write_text(json.dumps(status, indent=2) + '\n')
        temp.replace(status_path)
    try:
        for repetition in range(scenario['repetitions']):
            order = variants[repetition:] + variants[:repetition]
            for variant in order:
                status.update(currentVariant=variant, currentRepetition=repetition)
                save()
                image = frozen[variant]['reference']
                with (args.output / f'{variant}-r{repetition}.log').open('w') as log:
                    subprocess.run([sys.executable, str(here / 'run.py'), '--variant', variant, '--image', image,
                                    '--repetition', str(repetition), '--scenario', str(args.scenario.resolve()),
                                    '--expected-image-id', frozen[variant]['imageId'],
                                    '--output', str(args.output.resolve())], cwd=root, stdout=log, stderr=subprocess.STDOUT, check=True)
                status['completedRuns'].append(f'{variant}-r{repetition}')
                save()
        summary = args.output / 'summary.json'
        result = subprocess.run([sys.executable, str(here / 'summarize.py'), '--input', str(args.output.resolve()),
                                 '--scenario', str(args.scenario.resolve()), '--output', str(summary.resolve())], cwd=root)
        status.update(state='passed' if result.returncode == 0 else 'gates_failed', completedUnix=time.time(), summary=str(summary.resolve()))
        save()
        return result.returncode
    except BaseException as error:
        status.update(state='failed', error=type(error).__name__ + ': ' + str(error), completedUnix=time.time())
        save()
        raise


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--scenario', type=Path, default=Path(__file__).with_name('scenarios.json'))
    parser.add_argument('--output', type=Path, required=True)
    raise SystemExit(main(parser.parse_args()))
