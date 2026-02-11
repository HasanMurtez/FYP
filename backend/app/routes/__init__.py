
from flask import Blueprint
from .sync import sync_bp
from .predictions import predictions_bp


def register_routes(app):
    app.register_blueprint(sync_bp)
    app.register_blueprint(predictions_bp)
