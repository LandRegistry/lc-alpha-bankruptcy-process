from application import app
from flask import Response
from application.health import healthcheck


@app.route('/', methods=["GET"])
def index():
    return Response(status=200)


@app.route('/health', methods=['GET'])
def health():
    return healthcheck()
