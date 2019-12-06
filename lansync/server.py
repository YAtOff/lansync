import logging
import threading

from flask import Flask

from werkzeug.serving import make_server, run_simple


app = Flask(__name__)


@app.route("/")
def index():
    return "Hello!"


def run(app, server_name=None, debug=False):
    options = {}
    options.setdefault("use_reloader", debug)
    options.setdefault("use_debugger", debug)
    options.setdefault("threaded", True)

    sn_host, sn_port = None, None
    if server_name and server_name != "random":
        sn_host, _, sn_port = server_name.partition(":")

    host = sn_host or "127.0.0.1"
    port = int(sn_port or 0)

    if debug:
        run_simple(host, port, app, **options)  # ???
    else:
        server = make_server(host, port, app, threaded=True)
        _, port = server.server_address

        logging.info("Serving on port: %d", port)
        server.serve_forever()


def run_in_thread(server_name=None, debug=False):
    threading.Thread(target=run, args=(app, server_name, debug), daemon=True).start()
