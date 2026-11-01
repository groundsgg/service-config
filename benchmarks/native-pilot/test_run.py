import json
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from run import Client, make_content, validate_scenario, request_timing
from provenance import validate_image


class DriverTests(unittest.TestCase):
    def test_latency_includes_arrival_to_dispatch_delay(self):
        timing = request_timing(1, 3, 4)
        self.assertEqual(timing['latencyMs'], 3000)
        self.assertEqual(timing['dispatchDelayMs'], 2000)
        self.assertEqual(timing['completedTime'], 4)

    def test_image_must_match_current_source_and_frozen_image_id(self):
        image = {'Id': 'sha256:one', 'Config': {'Labels': {
            'org.opencontainers.image.revision': 'a' * 40,
            'gg.grounds.source-tree-sha256': 'b' * 64,
            'gg.grounds.runtime': 'native-micro'}}}
        validate_image(image, 'a' * 40, 'b' * 64, 'native-micro', 'sha256:one')
        for commit, tree, identity in [('c' * 40, 'b' * 64, 'sha256:one'),
                                       ('a' * 40, 'c' * 64, 'sha256:one'),
                                       ('a' * 40, 'b' * 64, 'sha256:other')]:
            with self.assertRaises(ValueError):
                validate_image(image, commit, tree, 'native-micro', identity)
        with self.assertRaises(ValueError):
            validate_image({'Id': 'sha256:one', 'Config': {}}, 'a' * 40, 'b' * 64, 'native-micro')

    def test_dataset_bytes_are_exact_for_different_revisions(self):
        for revision in (0, 99999):
            content = make_content(revision, 4096)
            self.assertEqual(len(content.encode()), 4096)
            self.assertEqual(json.loads(content)['revision'], revision)

    def test_inconsistent_data_or_rates_are_rejected(self):
        with self.assertRaises(ValueError):
            validate_scenario({'schemaVersion': 1, 'dataset': {'documents': 100, 'namespaces': 3}, 'phases': []})

    def test_real_http_client_preserves_failure_and_etag_instead_of_hiding_them(self):
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(401 if self.headers.get('Authorization') != 'Bearer test' else 304)
                self.send_header('ETag', '"7"')
                self.send_header('Content-Length', '0')
                self.end_headers()
            def log_message(self, *args): pass
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            client = Client(f'http://127.0.0.1:{server.server_port}', 'bad')
            self.assertEqual(client.request('GET', '/')[0], 401)
            status, headers, body = Client(f'http://127.0.0.1:{server.server_port}', 'test').request('GET', '/')
            self.assertEqual(status, 304)
            self.assertEqual(headers['etag'], '"7"')
            self.assertEqual(body, b'')
        finally:
            server.shutdown(); server.server_close(); thread.join()

if __name__ == '__main__': unittest.main()
