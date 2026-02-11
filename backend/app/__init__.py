from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS
from config import config
import os

# Initialize extensions
db = SQLAlchemy()

def create_app(config_name=None):
    if config_name is None:
        config_name = os.environ.get('FLASK_ENV', 'development')
    
    app = Flask(__name__)
    app.config.from_object(config[config_name])
    
    # Initialize extensions
    db.init_app(app)
    CORS(app)
    
    with app.app_context():
        from app import models
        
        # Register routes
        from app.routes import register_routes
        register_routes(app)
    
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
