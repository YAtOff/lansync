from io import BytesIO
import logging
import os
from pathlib import Path
import threading
from typing import Callable, Dict, Any

from flask import Flask, jsonify, request, send_file

from werkzeug.serving import make_server, run_simple

from lansync.models import NodeChunk
from lansync.market import Market
from lansync.node import LocalNode
from lansync.session import instance as session


certs_dir = Path.cwd() / "certs"


app = Flask(__name__)


@app.route("/market/<namespace_name>/<key>", methods=["POST"])
def exchange(namespace_name, key):
    other_market = Market.load_from_file(request.stream)

    market = session.market_repo.load(namespace_name, key)
    if market is not None:
        market.merge(other_market)
        market = session.market_repo.save(market)
    else:
        market = session.market_repo.save(other_market)

    fd = BytesIO()
    market.dump_to_file(fd)
    fd.seek(os.SEEK_SET)
    return send_file(fd, mimetype="application/octet-stream")


@app.route("/chunk/<namespace_name>/<content_hash>", methods=["GET", "HEAD"])
def chunk(namespace_name, content_hash):
    node_chunk_pair = NodeChunk.find(namespace_name, content_hash)
    if node_chunk_pair is None:
        return jsonify({"ok": False, "error": "Not found"}), 404

    node, chunk = node_chunk_pair

    if request.method == "HEAD":
        return "", 200

    local_node = LocalNode.create(node.local_path, session)
    print("Reading chunk", chunk, "from", node.local_path)
    fd = BytesIO(local_node.read_chunk(chunk))
    return send_file(fd, mimetype="application/octet-stream")


def run(app, debug=False, on_start: Callable[[int], None] = None):
    options: Dict[str, Any] = {}
    options.setdefault("threaded", True)
    options.setdefault("threaded", True)
    options.setdefault(
        "ssl_context", (os.fspath(certs_dir / "alpha.crt"), os.fspath(certs_dir / "alpha.key"),)
    )

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
