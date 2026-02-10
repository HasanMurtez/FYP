from flask import Blueprint
from .sync import sync_bp


def register_routes(app):
    app.register_blueprint(sync_bp)
