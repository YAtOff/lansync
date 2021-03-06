import json
from threading import Condition

from dynaconf import settings  # type: ignore
from flask import Flask, jsonify, request, Response

from lansync.database import open_database
from lansync_server.models import all_models
from lansync_server.service import load_events, store_events
from lansync_server.util import error_response


app = Flask(__name__)


@app.route("/namespace/<namespace>/events", methods=["GET", "POST"])
def node(namespace):
    if request.method == "POST":
        if not request.is_json:
            return error_response(406)

        events = []
        last_sequence_number = store_events(namespace, request.get_json())
    else:
        events = load_events(namespace, int(request.args.get("min_sequence_number", 0)))
        last_sequence_number = events[-1]["sequence_number"] if events else 0

    return jsonify({"last_sequence_number": last_sequence_number, "events": events})


@app.route("/namespace/<namespace>/feed")
def feed():
    def get_message():
        message = json.dumps({})
        return f"data: {message}\n\n"

    def event_stream():
        yield get_message()
        condition = Condition()
        while True:
            with condition:
                condition.wait()
                yield get_message()

    return Response(event_stream(), mimetype="text/event-stream")


if __name__ == "__main__":
    with open_database(settings.SERVER_DB, models=all_models):
        app.run(port=5555, debug=True)
