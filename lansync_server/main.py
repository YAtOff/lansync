from flask import Flask, jsonify, request

from dynaconf import settings  # type: ignore
import peewee  # tpye: ignore

from lansync_server.models import Namespace, Node, all_models
from lansync_server.serializers import NodeSerializer
from lansync.database import open_database


app = Flask(__name__)


def response_404():
    return jsonify({"ok": False, "error": "Not found"}), 404


def response_406():
    return jsonify({"ok": False, "error": "Not acceptable"}), 406


@app.route("/namespace/<name>",  methods=["GET", "PUT"])
def namespace(name):
    if request.method == "PUT":
        namespace, created = Namespace.get_or_create(name=name)
    else:
        try:
            created = False
            namespace = Namespace.get(Namespace.name == name)
        except peewee.DoesNotExist:
            return response_404()

    nodes = Node.select().where(Node.namespace == namespace) if not created \
        else []
    serializer = NodeSerializer()
    data = [serializer.dump(node) for node in nodes]
    return jsonify(data)


@app.route("/namespace/<namespace_name>/node/<key>",  methods=["GET", "PUT"])
def node(namespace_name, key):
    try:
        namespace = Namespace.get(Namespace.name == namespace_name)
    except peewee.DoesNotExist:
        return response_404()

    serializer = NodeSerializer()
    if request.method == "PUT":
        if not request.is_json:
            return response_406()

        data = serializer.load(request.get_json())
        node, created = Node.get_or_create(
            namespace=namespace, key=key, defaults=data
        )
        if not created:
            for k, v in data.items():
                setattr(node, k, v)
            node.save()
    else:
        try:
            node = (
                Node.select()
                .where(
                    Node.namespace == namespace,
                    Node.key == key
                )
                .get()
            )
        except peewee.DoesNotExist:
            return response_404()

    result = serializer.dump(node)
    return jsonify(result)


if __name__ == "__main__":
    with open_database(settings.SERVER_DB, models=all_models):
        app.run(port=5555, debug=True)
