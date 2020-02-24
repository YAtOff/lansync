from sanic import response  # type: ignore


messages = {
    404: "Not found",
    406: "Not acceptable",
}


def error_response(code: int, message: str = None):
    return response.json({"ok": False, "error": message or messages.get(code)}), code
