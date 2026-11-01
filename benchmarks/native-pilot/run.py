#!/usr/bin/env python3
"""Run one packaged-image repetition against disposable dependencies on the local Docker host."""
import argparse
import concurrent.futures
import hashlib
import http.client
import json
import math
import os
import platform
import random
import signal
import socket
import subprocess
import threading
import time
from pathlib import Path
from urllib.parse import urlsplit
from provenance import source_hash, validate_image, copy_file


def make_content(revision, size):
    body = {'revision': revision, 'padding': ''}
    padding = size - len(json.dumps(body, separators=(',', ':')).encode())
    if padding < 0:
        raise ValueError('Document contentBytes is too small')
    body['padding'] = 'x' * padding
    return json.dumps(body, separators=(',', ':'))


def validate_scenario(scenario):
    data = scenario['dataset']
    if scenario['schemaVersion'] != 1 or data['documents'] < 25 or data['namespaces'] <= 0 or data['documents'] % data['namespaces']:
        raise ValueError('Dataset must have at least 25 documents evenly divided between namespaces')
    make_content(0, data['contentBytes'])
    if not scenario['phases'] or len({p['name'] for p in scenario['phases']}) != len(scenario['phases']):
        raise ValueError('Nonempty unique phases required')
    for phase in scenario['phases']:
        if phase['seconds'] <= 0 or min(phase['readsPerSecond'], phase['writesPerSecond']) < 0:
            raise ValueError('Duration must be positive and rates nonnegative')
    if sum(scenario['readMix'].values()) != 1:
        raise ValueError('Read mixture must sum to one')


class Client:
    def __init__(self, base_url, token):
        url = urlsplit(base_url)
        if url.scheme != 'http':
            raise ValueError('Fixture endpoint must be plain HTTP within the isolated test network')
        self.host, self.port, self.token = url.hostname, url.port, token
        self.local = threading.local()

    def request(self, method, path, body=None, headers=None):
        connection = getattr(self.local, 'connection', None)
        if connection is None:
            connection = self.local.connection = http.client.HTTPConnection(self.host, self.port, timeout=8)
        request_headers = {'Authorization': 'Bearer ' + self.token, **(headers or {})}
        if body is not None:
            request_headers['Content-Type'] = 'application/json'
        try:
            connection.request(method, path, None if body is None else json.dumps(body), request_headers)
            response = connection.getresponse()
            data = response.read()
            return response.status, dict((k.lower(), v) for k, v in response.getheaders()), data
        except Exception:
            connection.close()
            self.local.connection = None
            raise


class DockerConnection(http.client.HTTPConnection):
    def __init__(self):
        super().__init__('localhost', timeout=10)
    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        address = os.environ.get('DOCKER_HOST', 'unix:///var/run/docker.sock')
        if not address.startswith('unix://'):
            raise ValueError('Pilot metrics require a local unix Docker endpoint')
        self.sock.connect(address.removeprefix('unix://'))


def docker_stats(container):
    connection = DockerConnection()
    try:
        connection.request('GET', f'/containers/{container}/stats?stream=false&one-shot=true')
        response = connection.getresponse()
        if response.status != 200:
            raise RuntimeError(f'Docker metrics HTTP {response.status}')
        result = json.loads(response.read())
    finally:
        connection.close()
    memory = result['memory_stats']
    detail = memory['stats']
    return {'workingSetBytes': memory['usage'] - detail.get('inactive_file', detail.get('total_inactive_file', 0)),
            'rssBytes': detail.get('rss', detail.get('anon', 0)),
            'cpuNs': result['cpu_stats']['cpu_usage']['total_usage']}


def request_timing(scheduled, dispatched, completed):
    return {'scheduledTime': scheduled, 'dispatchedTime': dispatched, 'completedTime': completed,
            'dispatchDelayMs': (dispatched - scheduled) * 1000, 'latencyMs': (completed - scheduled) * 1000}


def seed(client, scenario):
    data = scenario['dataset']
    documents = [{'namespace': f"ns-{i % data['namespaces']}", 'configKey': f'doc-{i}',
                  'defaultContentJson': make_content(0, data['contentBytes'])} for i in range(data['documents'])]
    status, _, body = client.request('POST', '/v1/config/apps/benchmark/envs/pilot/defaults', {'defaults': documents})
    if status != 200 or len(json.loads(body)['createdKeys']) != data['documents']:
        raise RuntimeError(f'Fresh fixture seed failed: HTTP {status}')
    status, _, body = client.request('GET', '/v1/config/apps/benchmark/envs/pilot/snapshot')
    if status != 200:
        raise RuntimeError('Cannot load seeded document versions')
    return {d['configKey']: d['version'] for d in json.loads(body)['documents']}


def drive_phase(client, scenario, phase, container, versions, write):
    count = scenario['dataset']['documents']
    locks = [threading.Lock() for _ in range(count)]
    etags = threading.local()
    pending = threading.BoundedSemaphore(128)
    clock = time.monotonic()
    stop_metrics = threading.Event()

    def metric():
        sample = docker_stats(container)
        write({'kind': 'metrics', 'phase': phase['name'], 'time': time.monotonic() - clock, **sample})

    metric()
    metric_errors = []
    def metrics_loop():
        while not stop_metrics.wait(scenario['metricsIntervalSeconds']):
            try:
                metric()
            except Exception as error:
                metric_errors.append(str(error))
    sampler = threading.Thread(target=metrics_loop, daemon=True)
    sampler.start()

    def request(operation, index, scheduled):
        started = time.monotonic()
        row = {'kind': 'request', 'phase': phase['name'], 'operation': operation, 'time': started - clock, 'ok': False}
        try:
            namespace = index % scenario['dataset']['namespaces']
            key = f'doc-{index}'
            if operation == 'put':
                with locks[index]:
                    version = versions[key]
                    status, headers, _ = client.request('PUT', f'/v1/config/admin/apps/benchmark/envs/pilot/namespaces/ns-{namespace}/documents/{key}',
                                                        {'contentJson': make_content(version, scenario['dataset']['contentBytes']), 'expectedVersion': version})
                    if status == 200:
                        versions[key] = int(headers['etag'].strip('"'))
            elif operation == 'snapshot':
                headers = {}
                tag = getattr(etags, 'snapshot', None)
                if tag:
                    headers['If-None-Match'] = tag
                status, response_headers, _ = client.request('GET', '/v1/config/apps/benchmark/envs/pilot/snapshot', headers=headers)
                if status in (200, 304):
                    etags.snapshot = response_headers.get('etag', tag)
            elif operation == 'namespaceSnapshot':
                status, _, _ = client.request('GET', f'/v1/config/apps/benchmark/envs/pilot/namespaces/ns-{namespace}/snapshot')
            else:
                status, _, _ = client.request('GET', f'/v1/config/apps/benchmark/envs/pilot/namespaces/ns-{namespace}/documents/{key}')
            row.update(status=status, ok=status == 200 or (operation == 'snapshot' and status == 304))
        except Exception as error:
            row['error'] = type(error).__name__
        finally:
            row.update(request_timing(scheduled, started - clock, time.monotonic() - clock))
            write(row)
            pending.release()

    rng = random.Random(101)
    reads = writes = 0
    mix = scenario['readMix']
    with concurrent.futures.ThreadPoolExecutor(max_workers=128) as pool:
        while True:
            now = time.monotonic() - clock
            if now >= phase['seconds']:
                break
            due_read = reads / phase['readsPerSecond'] if phase['readsPerSecond'] else math.inf
            due_write = writes / phase['writesPerSecond'] if phase['writesPerSecond'] else math.inf
            due = min(due_read, due_write)
            if due >= phase['seconds']:
                time.sleep(min(.05, phase['seconds'] - now))
                continue
            if due > now:
                time.sleep(min(.01, due - now))
                continue
            if due_write <= due_read:
                operation, index = 'put', writes % 25
                writes += 1
            else:
                selector = rng.random()
                operation = 'snapshot' if selector < mix['snapshot'] else 'document' if selector < mix['snapshot'] + mix['document'] else 'namespaceSnapshot'
                index = rng.randrange(count)
                reads += 1
            if pending.acquire(blocking=False):
                pool.submit(request, operation, index, due)
            else:
                write({'kind': 'request', 'phase': phase['name'], 'operation': operation, 'time': now,
                       **request_timing(due, now, now), 'ok': False, 'error': 'driver_saturated'})
    stop_metrics.set()
    sampler.join()
    metric()
    if metric_errors:
        raise RuntimeError('Missing resource samples: ' + '; '.join(metric_errors))


def run(args):
    root = Path(__file__).resolve().parents[2]
    scenario_bytes = args.scenario.read_bytes()
    scenario = json.loads(scenario_bytes)
    validate_scenario(scenario)
    directory = args.output.resolve() / f'{args.variant}-r{args.repetition}'
    directory.mkdir(parents=True, exist_ok=False)
    candidate_commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip()
    candidate_source = source_hash(root)
    image = json.loads(subprocess.check_output(['docker', 'image', 'inspect', args.image]))[0]
    validate_image(image, candidate_commit, candidate_source, args.variant, args.expected_image_id)
    fixture_path = directory / '.fixture.json'
    stop_path = directory / '.fixture.json.stop'
    args_gradle = ['./gradlew', '--no-daemon', 'runtimeFixture', '-Pkotlin.compiler.execution.strategy=in-process',
                   '-Dgrounds.runtime.image=' + args.image, '-Dgrounds.runtime.manifest=' + str(fixture_path)]
    if args.variant == 'jvm-tuned':
        args_gradle += ['-Dgrounds.runtime.java-options=-Xms16m -Xmx128m']
    with (directory / 'fixture.log').open('w') as log:
        process = subprocess.Popen(args_gradle, cwd=root, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            end = time.monotonic() + 240
            while not fixture_path.exists():
                if process.poll() is not None or time.monotonic() > end:
                    raise RuntimeError(f'Fixture startup failed; see {directory / "fixture.log"}')
                time.sleep(.25)
            fixture = json.loads(fixture_path.read_text())
            running = json.loads(subprocess.check_output(['docker', 'inspect', fixture['containerIds'][0]]))[0]
            actual_resources = {'memoryBytes': running['HostConfig']['Memory'], 'nanoCpus': running['HostConfig']['NanoCpus']}
            if actual_resources != scenario['resources'] or running['Image'] != image['Id']:
                raise RuntimeError('Actual container resources or image differ from the scenario')
            context = {'resources': scenario['resources'], 'host': platform.node(), 'architecture': platform.machine(),
                       'sidecar': 'none-local-docker', 'telemetry': 'OTEL_SDK_DISABLED=true', 'dependencies': fixture['dependencyImages']}
            manifest = {'schemaVersion': 1, 'variant': args.variant, 'repetition': args.repetition,
                        'commit': candidate_commit,
                        'sourceTreeSha256': candidate_source, 'scenarioSha256': hashlib.sha256(scenario_bytes).hexdigest(),
                        'comparisonContextSha256': hashlib.sha256(json.dumps(context, sort_keys=True).encode()).hexdigest(),
                        'context': context, 'image': args.image, 'imageId': image['Id'], 'imageUncompressedBytes': image['Size'],
                        'readySeconds': fixture['readySeconds'], 'scope': 'local-docker-not-stage', 'completed': False}
            if args.variant.startswith('native-'):
                container_id = fixture['containerIds'][0]
                actual = copy_file(container_id, '/deployments/application')
                if actual != copy_file(container_id, '/deployments/application.sha256'):
                    raise RuntimeError('Native binary differs from its packaged checksum')
                manifest['binarySha256'] = actual
            (directory / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
            client = Client(fixture['baseUrls'][0], fixture['adminToken'])
            versions = seed(client, scenario)
            lock = threading.Lock()
            with (directory / 'raw.jsonl').open('w', buffering=1) as output:
                def write(row):
                    with lock:
                        output.write(json.dumps(row) + '\n')
                for phase in scenario['phases']:
                    print(f"{args.variant} r{args.repetition}: {phase['name']}", flush=True)
                    drive_phase(client, scenario, phase, fixture['containerIds'][0], versions, write)
            state = json.loads(subprocess.check_output(['docker', 'inspect', fixture['containerIds'][0]]))[0]
            if source_hash(root) != manifest['sourceTreeSha256']:
                raise RuntimeError('Source changed during measurement; discard this run')
            manifest.update(completed=True, restartCount=state['RestartCount'], oomKilled=state['State']['OOMKilled'])
            (directory / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
        finally:
            stop_path.touch()
            try:
                process.wait(timeout=60)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGTERM)
                process.wait(timeout=15)
            fixture_path.unlink(missing_ok=True)
            stop_path.unlink(missing_ok=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--variant', required=True, choices=('jvm', 'jvm-tuned', 'native-micro', 'native-distroless'))
    parser.add_argument('--image', required=True)
    parser.add_argument('--scenario', type=Path, default=Path(__file__).with_name('scenarios.json'))
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--repetition', type=int, default=0)
    parser.add_argument('--expected-image-id')
    run(parser.parse_args())
