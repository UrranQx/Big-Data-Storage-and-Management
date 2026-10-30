"""Keep-alive, wire gzip и возврат 429 через loopback HTTP fixture.

Transport не должен скрывать throttling от общего клиента и его retry-политики."""

import gzip
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading
import unittest
from urllib.error import HTTPError
from urllib.request import Request

from speedrun_etl.http_transport import PooledTransport


class TransportTests(unittest.TestCase):
    def test_reuses_connection_preserves_wire_gzip_and_exposes_429(self):
        ports = []
        raw = gzip.compress(b'{"exact":0.1234567890123456789}', mtime=0)

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self):
                ports.append(self.client_address[1])
                self.send_response(429 if self.path == "/throttle" else 200)
                self.send_header("Content-Length", str(len(raw)))
                self.send_header("Content-Encoding", "gzip")
                self.send_header("Retry-After", "120")
                self.end_headers()
                self.wfile.write(raw)

            def log_message(self, *args):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        transport = PooledTransport()
        try:
            url = f"http://127.0.0.1:{server.server_port}"
            for _ in range(2):
                with transport(Request(url)) as response:
                    self.assertEqual("gzip", response.headers["Content-Encoding"])
                    self.assertEqual(raw, response.read())
            self.assertEqual(ports[0], ports[1])
            with self.assertRaises(HTTPError) as caught:
                transport(Request(url + "/throttle"))
            self.assertEqual(429, caught.exception.code)
            self.assertEqual("120", caught.exception.headers["Retry-After"])
            self.assertEqual(raw, caught.exception.read())
        finally:
            transport.close()
            server.shutdown()
            server.server_close()
            thread.join(3)
