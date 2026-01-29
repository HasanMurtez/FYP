import os
from app import create_app, db

# Get environment (default to development)
config_name = os.environ.get('FLASK_ENV', 'development')
app = create_app(config_name)

@app.cli.command()
def init_db():
    """Initialize the database"""
    with app.app_context():
        db.create_all()
        print(" Database initialized successfully!")

@app.cli.command()
def drop_db():
    """Drop all database tables"""
    with app.app_context():
        db.drop_all()
        print(" Database tables dropped!")

if __name__ == '__main__':
    print(f"Starting Flask app in {config_name} mode...")
    print(f" API running at: http://localhost:5000")
    print(f" Health check: http://localhost:5000/health")
    app.run(debug=True, port=5000, host='0.0.0.0')