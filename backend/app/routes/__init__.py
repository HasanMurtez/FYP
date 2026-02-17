
from .sync import sync_bp
from .predictions import predictions_bp
from .players import players_bp
from .scouting import scouting_bp


def register_routes(app):
    app.register_blueprint(sync_bp)
    app.register_blueprint(predictions_bp)
    app.register_blueprint(players_bp)
    app.register_blueprint(scouting_bp)
