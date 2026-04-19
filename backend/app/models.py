from app import db
from datetime import datetime


class Team(db.Model):
    """
    Team model 
    """
    __tablename__ = 'teams'
    
    id = db.Column(db.Integer, primary_key=True)
    fpl_id = db.Column(db.Integer, unique=True, nullable=False)
    name = db.Column(db.String(100), nullable=False)
    short_name = db.Column(db.String(3), nullable=True)
    strength = db.Column(db.Integer, default=0)
    
    # Fixture congestion data
    total_upcoming_fixtures = db.Column(db.Integer, default=0)
    congestion_level = db.Column(db.String(20), default='Medium')
    
    # Timestamps
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationship to players
    players = db.relationship('Player', backref='team', lazy=True, cascade='all, delete-orphan')
    
    def __repr__(self):
        return f'<Team {self.name}>'
    
    def to_dict(self):
        """Convert team to dictionary for JSON responses"""
        return {
            'id': self.id,
            'fpl_id': self.fpl_id,
            'name': self.name,
            'short_name': self.short_name,
            'strength': self.strength,
            'total_upcoming_fixtures': self.total_upcoming_fixtures,
            'congestion_level': self.congestion_level,
            'player_count': len(self.players),
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }


class Player(db.Model):
    """
    Player model - represents a football player with all stats
    """
    __tablename__ = 'players'
    
    id = db.Column(db.Integer, primary_key=True)
    fpl_id = db.Column(db.Integer, unique=True, nullable=False)
    code = db.Column(db.Integer, nullable=True)  # FPL player code for photo URL
    
    # Basic Info
    web_name = db.Column(db.String(50), nullable=False)
    full_name = db.Column(db.String(100), nullable=False)
    position = db.Column(db.String(10), nullable=False)
    
    # Team relationship
    team_id = db.Column(db.Integer, db.ForeignKey('teams.id'), nullable=False)
    
    # Availability Status
    status = db.Column(db.String(1), default='a')
    status_description = db.Column(db.String(50), default='Available')
    chance_of_playing_next = db.Column(db.Integer, default=100)
    news = db.Column(db.Text, default='')
    
    # Playing Time Stats
    minutes = db.Column(db.Integer, default=0)
    starts = db.Column(db.Integer, default=0)
    recent_minutes_last_5 = db.Column(db.Integer, default=0)
    total_minutes_last_10 = db.Column(db.Integer, default=0)
    
    # Performance Stats
    goals_scored = db.Column(db.Integer, default=0)
    assists = db.Column(db.Integer, default=0)
    yellow_cards = db.Column(db.Integer, default=0)
    red_cards = db.Column(db.Integer, default=0)
    
    # Advanced Metrics (ICT Index)
    influence = db.Column(db.Float, default=0.0)
    creativity = db.Column(db.Float, default=0.0)
    threat = db.Column(db.Float, default=0.0)
    ict_index = db.Column(db.Float, default=0.0)
    
    # Injury History Features
    times_unavailable_last_10 = db.Column(db.Integer, default=0)
    max_consecutive_unavailable = db.Column(db.Integer, default=0)
    availability_score = db.Column(db.Integer, default=10)
    injury_prone = db.Column(db.Integer, default=0)
    
    # Workload Features
    workload_intensity = db.Column(db.Float, default=0.0)
    is_overworked = db.Column(db.Integer, default=0)
    
    # Physical Demand
    physical_demand = db.Column(db.Integer, default=0)
    position_risk = db.Column(db.Integer, default=0)
    
    # Congestion
    congestion_numeric = db.Column(db.Integer, default=1)
    
    # ML Predictions
    injury_risk_level = db.Column(db.String(20), nullable=True)
    injury_risk_score = db.Column(db.Float, nullable=True)
    predicted_at = db.Column(db.DateTime, nullable=True)
    
    # Timestamps
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __repr__(self):
        return f'<Player {self.full_name} ({self.position})>'
    
    def to_dict(self, include_team=False):
        """Convert player to dictionary for JSON responses"""
        data = {
            'id': self.id,
            'fpl_id': self.fpl_id,
            'code': self.code,
            'web_name': self.web_name,
            'full_name': self.full_name,
            'position': self.position,
            'status': self.status,
            'status_description': self.status_description,
            'chance_of_playing_next': self.chance_of_playing_next,
            'news': self.news,
            'minutes': self.minutes,
            'starts': self.starts,
            'recent_minutes_last_5': self.recent_minutes_last_5,
            'total_minutes_last_10': self.total_minutes_last_10,
            'goals_scored': self.goals_scored,
            'assists': self.assists,
            'yellow_cards': self.yellow_cards,
            'red_cards': self.red_cards,
            'influence': self.influence,
            'creativity': self.creativity,
            'threat': self.threat,
            'ict_index': self.ict_index,
            'times_unavailable_last_10': self.times_unavailable_last_10,
            'max_consecutive_unavailable': self.max_consecutive_unavailable,
            'availability_score': self.availability_score,
            'injury_prone': bool(self.injury_prone),
            'workload_intensity': self.workload_intensity,
            'is_overworked': bool(self.is_overworked),
            'physical_demand': self.physical_demand,
            'position_risk': self.position_risk,
            'congestion_numeric': self.congestion_numeric,
            'injury_risk_level': self.injury_risk_level,
            'injury_risk_score': self.injury_risk_score,
            'predicted_at': self.predicted_at.isoformat() if self.predicted_at else None,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }
        
        if include_team and self.team:
            data['team'] = self.team.to_dict()
        else:
            data['team_id'] = self.team_id
        
        return data
    
    def get_features_for_prediction(self):
        """Extract features for ML model in correct order"""
        return [
            self.minutes,
            self.starts,
            self.recent_minutes_last_5,
            self.total_minutes_last_10,
            self.workload_intensity,
            self.is_overworked,
            self.times_unavailable_last_10,
            self.max_consecutive_unavailable,
            self.availability_score,
            self.injury_prone,
            self.yellow_cards,
            self.red_cards,
            self.physical_demand,
            self.position_risk,
            self.team.total_upcoming_fixtures if self.team else 3,
            self.congestion_numeric,
            self.influence,
            self.creativity,
            self.threat,
            self.ict_index
        ]
