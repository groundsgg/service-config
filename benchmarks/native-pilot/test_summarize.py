"""These tests catch false positive benchmark decisions and incorrect resource accounting."""
import unittest
import copy
from summarize import percentile, weighted_mean, summarize_phase, compare

class SummarizeTests(unittest.TestCase):
    def test_nearest_rank_quantiles(self):
        self.assertEqual(percentile([1, 2, 3, 4, 100], .95), 100)
        self.assertEqual(percentile([1, 2, 3, 4, 100], .5), 3)
        self.assertIsNone(percentile([], .99))

    def test_memory_is_weighted_by_elapsed_time(self):
        samples = [{'time': 0, 'workingSetBytes': 10}, {'time': 1, 'workingSetBytes': 30}, {'time': 4, 'workingSetBytes': 99}]
        self.assertEqual(weighted_mean(samples, 'workingSetBytes'), 25)
        with self.assertRaises(ValueError): weighted_mean(samples[:1], 'workingSetBytes')

    def test_incomplete_measurement_and_missing_requests_cannot_pass(self):
        result = summarize_phase({'name': 'regular', 'seconds': 10, 'readsPerSecond': 2, 'writesPerSecond': 0}, [], [])
        self.assertFalse(result['valid'])
        self.assertIn('insufficient_metrics', result['invalidReasons'])
        self.assertIn('offered_rate_not_achieved', result['invalidReasons'])
        self.assertIsNone(result['cpuSecondsPerSuccess'])

    def test_errors_and_dropped_work_are_not_successes(self):
        phase = {'name': 'regular', 'seconds': 2, 'readsPerSecond': 1, 'writesPerSecond': 0}
        samples = [{'time': 0, 'workingSetBytes': 1048576, 'rssBytes': 524288, 'cpuNs': 0}, {'time': 2, 'workingSetBytes': 1048576, 'rssBytes': 524288, 'cpuNs': 2000000000}]
        requests = [{'operation': 'document', 'ok': True, 'latencyMs': 4}, {'operation': 'document', 'ok': False, 'latencyMs': 100, 'error': 'timeout'}]
        result = summarize_phase(phase, requests, samples)
        self.assertEqual(result['successes'], 1)
        self.assertEqual(result['errors'], 1)
        self.assertEqual(result['meanWorkingSetMiB'], 1)
        self.assertEqual(result['cpuSecondsPerSuccess'], 2)
        self.assertFalse(result['valid'])

    def test_idle_phase_has_no_cpu_per_request(self):
        phase = {'name': 'idle', 'seconds': 2, 'readsPerSecond': 0, 'writesPerSecond': 0}
        samples = [{'time': 0, 'workingSetBytes': 100, 'rssBytes': 100, 'cpuNs': 0}, {'time': 2, 'workingSetBytes': 100, 'rssBytes': 100, 'cpuNs': 100}]
        result = summarize_phase(phase, [], samples)
        self.assertTrue(result['valid'])
        self.assertIsNone(result['cpuSecondsPerSuccess'])

    def test_missing_repetition_or_different_source_fails_comparison(self):
        self.assertFalse(compare([], {'repetitions': 3})['passed'])
        run = {'manifest': {'variant': 'native-micro', 'commit': 'different', 'scenarioSha256': 'a'}, 'phases': []}
        self.assertFalse(compare([run], {'repetitions': 3})['passed'])

    def comparison_fixture(self):
        scenario = {'repetitions': 3, 'phases': [{'name': 'regular'}], 'gates': {
            'nativeSavingVsJvm': .3, 'nativeSavingVsTunedJvm': .15,
            'cpuRelativeRegression': .2, 'p95RelativeRegression': .1, 'p95AbsoluteAllowanceMs': 2,
            'p99RelativeRegression': .2, 'p99AbsoluteAllowanceMs': 5}}
        runs = []
        for variant, memory in [('jvm', 200), ('jvm-tuned', 160), ('native-micro', 100)]:
            for repetition in range(3):
                runs.append({'manifest': {'variant': variant, 'repetition': repetition, 'completed': True,
                                         'commit': 'a', 'sourceTreeSha256': 'b', 'scenarioSha256': 'c',
                                         'comparisonContextSha256': 'd', 'binarySha256': 'native-binary'},
                             'phases': [{'phase': 'regular', 'valid': True, 'invalidReasons': [],
                                         'meanWorkingSetMiB': memory, 'cpuSecondsPerSuccess': .001,
                                         'operations': {'document': {'p95Ms': 4, 'p99Ms': 5}}}]})
        return scenario, runs

    def test_complete_comparable_repetitions_can_pass(self):
        scenario, runs = self.comparison_fixture()
        self.assertTrue(compare(runs, scenario)['passed'])

    def test_crashed_driver_cannot_pass_even_if_raw_measurements_exist(self):
        scenario, runs = self.comparison_fixture()
        runs[-1]['manifest']['completed'] = False
        self.assertFalse(compare(runs, scenario)['passed'])

    def test_cpu_and_latency_regressions_fail_otherwise_good_memory_result(self):
        scenario, runs = self.comparison_fixture()
        for run in runs[-3:]:
            run['phases'][0]['cpuSecondsPerSuccess'] = .003
            run['phases'][0]['operations']['document']['p95Ms'] = 20
        decision = compare(runs, scenario)
        self.assertFalse(decision['passed'])
        self.assertTrue(any('CPU regression' in f for f in decision['failures']))
        self.assertTrue(any('p95Ms regression' in f for f in decision['failures']))

    def test_optional_distroless_is_not_reported_with_incomplete_repetitions(self):
        scenario, runs = self.comparison_fixture()
        optional = copy.deepcopy(runs[-2:])
        for run in optional: run['manifest']['variant'] = 'native-distroless'
        self.assertFalse(compare(runs + optional, scenario)['passed'])

    def test_requests_finishing_after_deadline_do_not_meet_offered_rate(self):
        phase = {'name': 'burst', 'seconds': 2, 'readsPerSecond': 1, 'writesPerSecond': 0}
        samples = [{'time': 0, 'workingSetBytes': 100, 'rssBytes': 100, 'cpuNs': 0},
                   {'time': 6, 'workingSetBytes': 100, 'rssBytes': 100, 'cpuNs': 6000000000}]
        requests = [{'operation': 'document', 'ok': True, 'latencyMs': 3000,
                     'scheduledTime': 0, 'dispatchedTime': 0, 'completedTime': 3},
                    {'operation': 'document', 'ok': True, 'latencyMs': 5000,
                     'scheduledTime': 1, 'dispatchedTime': 1, 'completedTime': 6}]
        result = summarize_phase(phase, requests, samples)
        self.assertFalse(result['valid'])
        self.assertEqual(result['achievedPerSecond'], 0)
        self.assertEqual(result['completedSuccesses'], 2)

    def test_burst_latency_regression_cannot_hide_behind_good_regular_results(self):
        scenario, runs = self.comparison_fixture()
        scenario['phases'].append({'name': 'burst'})
        for run in runs:
            burst = copy.deepcopy(run['phases'][0])
            burst['phase'] = 'burst'
            if run['manifest']['variant'].startswith('native-'):
                burst['operations']['document'] = {'p95Ms': 100000, 'p99Ms': 100000}
            run['phases'].append(burst)
        self.assertFalse(compare(runs, scenario)['passed'])

if __name__ == '__main__': unittest.main()
