import os
from http.server import BaseHTTPRequestHandler, HTTPServer

LOG_PATH = os.environ.get("PLEX_STUB_LOG_PATH", "")


def _record(path: str) -> None:
    if not LOG_PATH:
        return

    with open(LOG_PATH, "a", encoding="utf-8") as handle:
        handle.write(path + "\n")


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        _record(self.path)
        if self.path == "/library/sections":
            body = b'<MediaContainer><Directory key="1" /></MediaContainer>'
            self.send_response(200)
            self.send_header("Content-Type", "application/xml")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if self.path == "/library/sections/1/refresh":
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        self.send_response(404)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:
        return


if __name__ == "__main__":
    HTTPServer(("0.0.0.0", 32400), Handler).serve_forever()
