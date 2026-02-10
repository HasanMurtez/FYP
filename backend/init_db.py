import sys
import os

# Add current directory (backend) to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app import create_app, db
from app.models import Team, Player


def init_database():
    """Initialize the database"""
    app = create_app()
    
    with app.app_context():
        print("Creating database tables")
        
        db.drop_all()
        print("Dropped existing tables")
        
        # Create all tables
        db.create_all()
        print("Created all tables")
        
        # Verify tables were created
        inspector = db.inspect(db.engine)
        tables = inspector.get_table_names()
        
        print(f"\nDatabase tables created:")
        for table in tables:
            columns = inspector.get_columns(table)
            print(f"   • {table} ({len(columns)} columns)")
        
        print("\nDatabase initialization complete!")
        print(f"   Database: {app.config['SQLALCHEMY_DATABASE_URI']}")


def test_models():
    """Test creating sample team and player"""
    app = create_app()
    
    with app.app_context():
        print("\nTesting models...")
        
        # Create a test team
        test_team = Team(
            fpl_id=1,
            name="Arsenal",
            short_name="ARS",
            strength=5,
            total_upcoming_fixtures=4,
            congestion_level="High"
        )
        
        db.session.add(test_team)
        db.session.commit()
        print("Created test team: Arsenal")
        
        # Create a test player
        test_player = Player(
            fpl_id=123,
            web_name="Saka",
            full_name="Bukayo Saka",
            position="MID",
            team_id=test_team.id,
            status='a',
            status_description='Available',
            chance_of_playing_next=100,
            minutes=2500,
            starts=28,
            goals_scored=10,
            assists=8,
            injury_prone=0,
            is_overworked=1
        )
        
        db.session.add(test_player)
        db.session.commit()
        print("Created test player: Bukayo Saka")
        
        # Test querying
        teams = Team.query.all()
        players = Player.query.all()
        
        print(f"\nDatabase contents:")
        print(f"   Teams: {len(teams)}")
        print(f"   Players: {len(players)}")
        
        # Test to_dict() methods
        if teams:
            print(f"\n   Team data sample:")
            print(f"   {teams[0].to_dict()}")
        
        if players:
            print(f"\n   Player data sample:")
            print(f"   {players[0].to_dict()}")
        
        # Clean up test data
        db.session.delete(test_player)
        db.session.delete(test_team)
        db.session.commit()
        print("\nCleaned up test data")
        
        print("\nModel tests passed!")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Database initialization')
    parser.add_argument('--test', action='store_true', help='Run model tests after initialization')
    args = parser.parse_args()
    
    init_database()
    
    if args.test:
        test_models()
