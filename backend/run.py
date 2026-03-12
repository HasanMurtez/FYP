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
        print("✅ Database initialized successfully!")

@app.cli.command()
def drop_db():
    """Drop all database tables"""
    with app.app_context():
        db.drop_all()
        print("✅ Database tables dropped!")

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = config_name == 'development'
    
    print(f"Starting Flask app in {config_name} mode...")
    print(f"API running at: http://0.0.0.0:{port}")
    print(f"Health check: http://0.0.0.0:{port}/health")
    
    app.run(host='0.0.0.0', port=port, debug=debug)
