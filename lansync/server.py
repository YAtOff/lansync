import logging
import os
from pathlib import Path
import threading
from typing import Callable, Dict, Any

import peewee  # type: ignore
from flask import Flask, send_from_directory, jsonify, request

from werkzeug.serving import make_server, run_simple

from lansync.models import Namespace, StoredNode


certs_dir = Path.cwd() / "certs"


app = Flask(__name__)


@app.route("/content/<namespace_name>/<key>", methods=["GET", "HEAD"])
def content(namespace_name, key):
    try:
        namespace = Namespace.get(Namespace.name == namespace_name)
        node = StoredNode.get(
            StoredNode.namespace == namespace,
            StoredNode.key == key
        )
    except peewee.DoesNotExist:
        return jsonify({"ok": False, "error": "Not found"}), 404

    if request.method == "HEAD":
        return "", 200

    path = node.local_path
    return send_from_directory(os.fspath(path.parent), path.name)


def run(app, debug=False, on_start: Callable[[int], None] = None):
    options: Dict[str, Any] = {}
    options.setdefault("threaded", True)
    options.setdefault("threaded", True)
    options.setdefault(
        "ssl_context",
        (
            os.fspath(certs_dir / "alpha.crt"),
            os.fspath(certs_dir / "alpha.key"),
        )
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
