from io import BytesIO
import logging
import os
from pathlib import Path
import threading
from typing import Callable, Dict, Any

from flask import Flask, jsonify, request, send_file

from werkzeug.serving import make_server, run_simple, WSGIRequestHandler

from lansync.models import NodeChunk
from lansync.market import Market
from lansync.session import instance as session


class WSGIRequestHandlerHTTP11(WSGIRequestHandler):
    protocol_version = "HTTP/1.0"


certs_dir = Path.cwd() / "certs"


app = Flask(__name__)


@app.route("/market/<namespace_name>/<key>", methods=["POST"])
def exchange(namespace_name, key):
    market = Market.load_from_file(request.stream)
    own_market_exists = Market.load_from_db(namespace_name, key) is not None
    market.exchange_with_db()
    if not own_market_exists:
        session.sync_worker.schedule_event("scheduled_sync")
    fd = BytesIO()
    market.dump_to_file(fd)
    fd.seek(os.SEEK_SET)
    return send_file(fd, mimetype="application/octet-stream")


@app.route("/chunk/<namespace_name>/<content_hash>", methods=["GET", "HEAD"])
def chunk(namespace_name, content_hash):
    node_chunk_pair = NodeChunk.find(namespace_name, content_hash)
    if node_chunk_pair is None:
        return jsonify({"ok": False, "error": "Not found"}), 404

    chunk, read_chunk = node_chunk_pair

    if request.method == "HEAD":
        return "", 200

    fd = BytesIO(read_chunk())
    return send_file(fd, mimetype="application/octet-stream")


def run(app, debug=False, on_start: Callable[[int], None] = None):
    options: Dict[str, Any] = {}
    options.setdefault("threaded", True)
    options.setdefault(
        "ssl_context", (os.fspath(certs_dir / "alpha.crt"), os.fspath(certs_dir / "alpha.key"),)
    )
    options.setdefault("request_handler", WSGIRequestHandlerHTTP11)

    host = "0.0.0.0"
    port = 0

    if debug:
        options.setdefault("use_reloader", debug)
        options.setdefault("use_debugger", debug)
        run_simple(host, port, app, **options)  # ???
    else:
        server = make_server(host, port, app, **options)
        _, port = server.server_address

        logging.info("Serving on port: %d", port)
        if on_start is not None:
            on_start(port)

        server.serve_forever()


def run_in_thread(debug=False, on_start: Callable[[int], None] = None):
    threading.Thread(target=run, args=(app, debug, on_start), daemon=True).start()
