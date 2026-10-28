"""Keep-alive transport with all retries and source pacing owned by Client."""
import io
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import getproxies, proxy_bypass

import urllib3


class PooledTransport:
    def __init__(self):
        self.pool = None

    def __call__(self, request, timeout=60):
        if self.pool is None:
            host = urlsplit(request.full_url).hostname
            proxy = getproxies().get("https") if not proxy_bypass(host) else None
            self.pool = (urllib3.ProxyManager(proxy, maxsize=1, block=True) if proxy else
                         urllib3.PoolManager(maxsize=1, block=True))
        try:
            response = self.pool.request(request.get_method(), request.full_url,
                body=request.data, headers=dict(request.header_items()),
                timeout=urllib3.Timeout(total=timeout), retries=False, redirect=False,
                preload_content=False, decode_content=False)
            if response.status >= 300:
                try:
                    raw = response.read(decode_content=False)
                finally:
                    response.release_conn()
                raise HTTPError(request.full_url, response.status, "Source HTTP error",
                                response.headers, io.BytesIO(raw))
            return PooledResponse(response)
        except urllib3.exceptions.HTTPError as error:
            raise URLError(type(error).__name__) from error

    def close(self):
        if self.pool is not None:
            self.pool.clear()


class PooledResponse:
    def __init__(self, response):
        self.response = response
        self.status = response.status
        self.headers = response.headers
        self.consumed = False

    def __enter__(self):
        return self

    def read(self):
        try:
            data = self.response.read(decode_content=False)
            self.consumed = True
            return data
        except urllib3.exceptions.HTTPError as error:
            raise URLError(type(error).__name__) from error

    def __exit__(self, *args):
        if not self.consumed:
            self.response.close()
        self.response.release_conn()
