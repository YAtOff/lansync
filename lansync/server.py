import asyncio
import logging
import os
from pathlib import Path
import socket
from threading import Thread
import time

# from flask import Flask, jsonify, request, send_file
from sanic import Sanic, response  # type: ignore

from lansync.models import NodeChunk
from lansync.market import Market
from lansync.session import instance as session
from lansync.node import RemoteNode
from lansync.util.http import error_response


certs_dir = Path.cwd() / "certs"

app = Sanic(__name__)
app.config.KEEP_ALIVE = True
app.config.KEEP_ALIVE_TIMEOUT = 300


@app.route("/node/<namespace_name>", methods=["POST"])
def exchange_node(request, namespace_name):
    session.receive_queue.put(RemoteNode.load(request.json))
    return response.json({"ok": True})


@app.route("/market/<namespace_name>/<key>", methods=["POST"])
def exchange_market(request, namespace_name, key):
    market = Market.load(request.body)
    market.exchange_with_db()
    return response.raw(market.dump(), content_type="application/octet-stream")


@app.route("/chunk/<namespace_name>/<content_hash>", methods=["GET", "HEAD"])
def chunk(request, namespace_name, content_hash):
    node_chunk_pair = NodeChunk.find(namespace_name, content_hash)
    if node_chunk_pair is None:
        return error_response(404)

    chunk, read_chunk = node_chunk_pair

    if request.method == "HEAD":
        return response.empty()

    return response.raw(read_chunk(), content_type="application/octet-stream")


def create_socket(host=None, port=None):
    address = (host or "0.0.0.0", port or 0)
    completed = False
    try:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        except socket.error:
            # Assume it's a bad family/type/protocol combination.
            logging.warning(
                "[SERVER] create_server() failed to create socket.socket(%r, %r)",
                socket.AF_INET,
                socket.SOCK_STREAM,
                exc_info=True,
            )
            raise

        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, True)
        if not hasattr(socket, "SO_REUSEPORT"):
            logging.warning("[SERVER] reuse_port not supported by socket module")

        sock.bind(address)
        completed = True
    finally:
        if not completed and sock:
            sock.close()

    return sock


def asyncio_exception_handler(loop, context):
    logging.error("Asyncio error: %r", context)


class Server:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.host = None
        self.port = None

    def run(self):
        sock = create_socket(
            host=self.kwargs.get("host"),
            port=self.kwargs.get("port")
        )
        host, port = sock.getsockname()
        self.host = host
        self.port = port
        logging.info("[SERVER] running server on port: %d", port)

        ssl = {
            "cert": os.fspath(certs_dir / "alpha.crt"),
            "key": os.fspath(certs_dir / "alpha.key")
        }
        server_core = app.create_server(
            sock=sock, access_log=True, ssl=ssl,
            debug=self.kwargs.get("debug", False),
            return_asyncio_server=True,
            asyncio_server_kwargs={
                "start_serving": False
            }
        )
        loop = asyncio.get_event_loop()
        async_server = loop.run_until_complete(server_core)
        try:
            loop.run_until_complete(async_server.server.serve_forever())
        finally:
            async_server.close()

    def run_in_thread(self):
        def run():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.set_exception_handler(asyncio_exception_handler)
            self.run()
        Thread(target=run, daemon=True).start()
        while self.port is None:
            time.sleep(0.1)
