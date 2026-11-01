#!/usr/bin/env python3
"""Summarize raw pilot measurements. Missing evidence always fails the decision gate."""
import argparse
import json
import math
import statistics
from pathlib import Path


def percentile(values, quantile):
    if not values:
        return None
    return sorted(values)[max(0, math.ceil(quantile * len(values)) - 1)]


def weighted_mean(samples, field, end=None):
    if len(samples) < 2:
        raise ValueError('At least two resource samples are required')
    stop = samples[-1]['time'] if end is None else min(end, samples[-1]['time'])
    elapsed = stop - samples[0]['time']
    if elapsed <= 0 or any(b['time'] <= a['time'] for a, b in zip(samples, samples[1:])):
        raise ValueError('Resource timestamps must increase')
    return sum(a[field] * max(0, min(b['time'], stop) - a['time']) for a, b in zip(samples, samples[1:])) / elapsed


def summarize_phase(phase, requests, samples, interval_seconds=15, minimum_rate_ratio=.99):
    seconds = phase['seconds']
    offered = seconds * (phase['readsPerSecond'] + phase['writesPerSecond'])
    completed_successes = sum(r['ok'] for r in requests)
    successes = sum(r['ok'] and r.get('completedTime', r.get('latencyMs', 0) / 1000) <= seconds for r in requests)
    errors = len(requests) - completed_successes
    reasons = []
    memory = rss = cpu = None
    try:
        memory = weighted_mean(samples, 'workingSetBytes', seconds) / 1048576
        rss = weighted_mean(samples, 'rssBytes', seconds) / 1048576
        if samples[0]['time'] > .5 or samples[-1]['time'] < seconds - .5:
            reasons.append('incomplete_metrics_coverage')
        if any(b['time'] - a['time'] > 2 * interval_seconds + 1 for a, b in zip(samples, samples[1:])):
            reasons.append('metrics_gap')
        # Clip the last counter interval to the nominal phase, excluding drain CPU.
        cpu_end = samples[-1]['cpuNs']
        for a, b in zip(samples, samples[1:]):
            if a['time'] <= seconds <= b['time']:
                fraction = (seconds - a['time']) / (b['time'] - a['time'])
                cpu_end = a['cpuNs'] + fraction * (b['cpuNs'] - a['cpuNs'])
                break
        delta = cpu_end - samples[0]['cpuNs']
        if delta < 0:
            reasons.append('cpu_counter_reset')
        elif successes:
            cpu = delta / 1e9 / successes
    except (ValueError, KeyError, TypeError):
        reasons.append('insufficient_metrics')
    if errors:
        reasons.append('unexpected_request_errors')
    if any(not all(field in r for field in ('scheduledTime', 'dispatchedTime', 'completedTime')) for r in requests):
        reasons.append('missing_request_timestamps')
    if offered and successes < offered * minimum_rate_ratio:
        reasons.append('offered_rate_not_achieved')
    operations = {}
    for op in sorted({r['operation'] for r in requests}):
        latencies = [r['latencyMs'] for r in requests if r['operation'] == op and r['ok']]
        operations[op] = {'count': len(latencies), 'p95Ms': percentile(latencies, .95), 'p99Ms': percentile(latencies, .99)}
    return {'phase': phase['name'], 'seconds': seconds, 'offeredRequests': offered,
            'successes': successes, 'completedSuccesses': completed_successes,
            'drainedSuccesses': completed_successes - successes,
            'errors': errors, 'achievedPerSecond': successes / seconds,
            'meanWorkingSetMiB': memory, 'meanRssMiB': rss,
            'peakWorkingSetMiB': max((s.get('workingSetBytes', 0) / 1048576 for s in samples), default=None),
            'cpuSecondsPerSuccess': cpu, 'operations': operations,
            'valid': not reasons, 'invalidReasons': reasons}


def compare(runs, scenario):
    required = ('jvm', 'jvm-tuned', 'native-micro')
    groups = {v: [r for r in runs if r['manifest']['variant'] == v] for v in (*required, 'native-distroless')}
    failures = []
    for variant in (*required, *(['native-distroless'] if groups['native-distroless'] else [])):
        group = groups[variant]
        if len(group) != scenario['repetitions'] or len({r['manifest'].get('repetition') for r in group}) != scenario['repetitions']:
            failures.append(f'{variant}: missing or duplicate repetitions')
    for field in ('commit', 'sourceTreeSha256', 'scenarioSha256', 'comparisonContextSha256'):
        values = {r['manifest'].get(field) for r in runs}
        if not runs or None in values or len(values) != 1:
            failures.append(f'{field}: missing or different inputs')
    for run in runs:
        variant = run['manifest']['variant']
        if not run['manifest'].get('completed', False):
            failures.append(f'{variant}: driver did not finish')
        if [p['phase'] for p in run['phases']] != [p['name'] for p in scenario.get('phases', [])]:
            failures.append(f'{variant}: missing or reordered phases')
        for phase in run['phases']:
            if not phase['valid']:
                failures.append(f"{variant}/{phase['phase']}: {','.join(phase['invalidReasons'])}")
        if run['manifest'].get('restartCount', 0) or run['manifest'].get('oomKilled', False):
            failures.append(f'{variant}: restart or OOM')
    if failures:
        return {'passed': False, 'failures': failures, 'variants': {}}
    gates = scenario['gates']
    medians = {}
    for variant, group in groups.items():
        if not group:
            continue
        regular = [next(p for p in r['phases'] if p['phase'] == 'regular') for r in group]
        medians[variant] = {
            'workingSetMiB': statistics.median(p['meanWorkingSetMiB'] for p in regular),
            'cpuSecondsPerSuccess': statistics.median(p['cpuSecondsPerSuccess'] for p in regular),
            'operations': {op: {q: statistics.median(p['operations'][op][q] for p in regular)
                                for q in ('p95Ms', 'p99Ms')}
                           for op in regular[0]['operations']}}
        medians[variant]['phaseOperations'] = {}
        for phase_name in ('regular', 'burst'):
            phase_rows = [p for r in group for p in r['phases'] if p['phase'] == phase_name]
            if not phase_rows:
                continue
            medians[variant]['phaseOperations'][phase_name] = {
                op: {q: statistics.median(p['operations'][op][q] for p in phase_rows)
                     for q in ('p95Ms', 'p99Ms')}
                for op in phase_rows[0]['operations']}
    base, tuned = medians['jvm'], medians['jvm-tuned']
    native_binaries = {r['manifest'].get('binarySha256') for r in runs if r['manifest']['variant'].startswith('native-')}
    if None in native_binaries or len(native_binaries) != 1:
        failures.append('Native runtime images do not contain the same verified binary')
    for variant in ('native-micro', 'native-distroless'):
        if variant not in medians:
            continue
        native = medians[variant]
        if native['workingSetMiB'] > base['workingSetMiB'] * (1 - gates['nativeSavingVsJvm']):
            failures.append(f'{variant}: RAM saving versus JVM below gate')
        if native['workingSetMiB'] > tuned['workingSetMiB'] * (1 - gates['nativeSavingVsTunedJvm']):
            failures.append(f'{variant}: RAM saving versus tuned JVM below gate')
        if native['cpuSecondsPerSuccess'] > base['cpuSecondsPerSuccess'] * (1 + gates['cpuRelativeRegression']):
            failures.append(f'{variant}: CPU regression above gate')
        for phase_name, baseline_operations in base['phaseOperations'].items():
            native_operations = native['phaseOperations'].get(phase_name, {})
            for op in baseline_operations:
                if op not in native_operations:
                    failures.append(f'{variant}/{phase_name}/{op}: missing latency measurement')
                    continue
                for q, relative, absolute in [('p95Ms', 'p95RelativeRegression', 'p95AbsoluteAllowanceMs'), ('p99Ms', 'p99RelativeRegression', 'p99AbsoluteAllowanceMs')]:
                    baseline = baseline_operations[op][q]
                    if native_operations[op][q] > baseline + max(baseline * gates[relative], gates[absolute]):
                        failures.append(f'{variant}/{phase_name}/{op}: {q} regression above gate')
    return {'passed': not failures, 'failures': failures, 'variants': medians}


def load_run(directory, scenario):
    manifest = json.loads((directory / 'manifest.json').read_text())
    rows = [json.loads(line) for line in (directory / 'raw.jsonl').read_text().splitlines() if line]
    phases = [summarize_phase(p,
                             [r for r in rows if r['kind'] == 'request' and r['phase'] == p['name']],
                             [r for r in rows if r['kind'] == 'metrics' and r['phase'] == p['name']],
                             scenario['metricsIntervalSeconds'], scenario['gates']['minimumAchievedRateRatio'])
              for p in scenario['phases']]
    return {'manifest': manifest, 'phases': phases}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--scenario', type=Path, default=Path(__file__).with_name('scenarios.json'))
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    scenario = json.loads(args.scenario.read_text())
    runs = [load_run(p.parent, scenario) for p in sorted(args.input.glob('*/manifest.json'))]
    result = {'scenario': scenario, 'runs': runs, 'decision': compare(runs, scenario)}
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result['decision'], indent=2))
    raise SystemExit(0 if result['decision']['passed'] else 1)
