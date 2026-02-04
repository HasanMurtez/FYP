from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
from config import config
import os

# Initialize extensions
db = SQLAlchemy()

def create_app(config_name=None):
    """
    Application factory pattern
    Creates and configures the Flask application
    """
    if config_name is None:
        config_name = os.environ.get('FLASK_ENV', 'development')
    
    app = Flask(__name__)
    app.config.from_object(config[config_name])
    
    # Initialize extensions
    db.init_app(app)
    CORS(app)  # Enable CORS for React frontend
    
    # Import models (so SQLAlchemy knows about them)
    # This must be done AFTER db.init_app(app)
    with app.app_context():
        from app import models
    
    # Register blueprints (will add later)
    # from app.routes import teams, players, predictions
    # app.register_blueprint(teams.bp)
    # app.register_blueprint(players.bp)
    # app.register_blueprint(predictions.bp)
    
    # Basic routes
    @app.route('/')
    def index():
        return {
            'message': 'FPL Analytics API',
            'version': '1.0.0',
            'status': 'running'
        }
    
    @app.route('/health')
    def health():
        return {
            'status': 'healthy',
            'message': 'Flask API is running',
            'database': 'connected' if db.engine else 'disconnected'
        }
    
    return app