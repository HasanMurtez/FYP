
from flask import Blueprint, jsonify, request
from app import db
from app.models import Team, Player

# Create blueprint
players_bp = Blueprint('players', __name__)


@players_bp.route('/api/teams', methods=['GET'])
def get_all_teams():
    """
    Get all teams with optional filters
    """
    try:
        teams = Team.query.all()
        
        teams_data = [team.to_dict() for team in teams]
        
        return jsonify({
            'success': True,
            'data': {
                'count': len(teams),
                'teams': teams_data
            }
        }), 200
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@players_bp.route('/api/teams/<int:team_id>', methods=['GET'])
def get_team(team_id):
    """
    Get single team details
    """
    try:
        team = Team.query.get(team_id)
        
        if not team:
            return jsonify({
                'success': False,
                'error': 'Team not found'
            }), 404
        
        return jsonify({
            'success': True,
            'data': team.to_dict()
        }), 200
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@players_bp.route('/api/teams/<int:team_id>/players', methods=['GET'])
def get_team_players(team_id):
    """
    Get all players for a specific team with optional filters
    Query params:
    - position: Filter by position (GK, DEF, MID, FWD)
    - risk: Filter by risk level (Low, Medium, High)
    - status: Filter by status (a, d, i, u, s, n)
    """
    try:
        team = Team.query.get(team_id)
        
        if not team:
            return jsonify({
                'success': False,
                'error': 'Team not found'
            }), 404
        
        # Start with all players for this team
        query = Player.query.filter_by(team_id=team_id)
        
        # Apply filters from query params
        position = request.args.get('position')
        if position:
            query = query.filter_by(position=position.upper())
        
        risk = request.args.get('risk')
        if risk:
            query = query.filter_by(injury_risk_level=risk.capitalize())
        
        status = request.args.get('status')
        if status:
            query = query.filter_by(status=status.lower())
        
        players = query.all()
        
        players_data = [player.to_dict() for player in players]
        
        return jsonify({
            'success': True,
            'data': {
                'team': team.to_dict(),
                'player_count': len(players),
                'players': players_data
            }
        }), 200
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@players_bp.route('/api/players', methods=['GET'])
def get_all_players():
    """
    Get all players with optional filters
    Query params:
    - position: Filter by position (GK, DEF, MID, FWD)
    - risk: Filter by risk level (Low, Medium, High)
    - status: Filter by status (a, d, i, u, s, n)
    - team_id: Filter by team
    - limit: Limit results (default: 100, max: 817)
    - offset: Offset for pagination (default: 0)
    """
    try:
        # Start with all players
        query = Player.query
        
        # Apply filters
        position = request.args.get('position')
        if position:
            query = query.filter_by(position=position.upper())
        
        risk = request.args.get('risk')
        if risk:
            query = query.filter_by(injury_risk_level=risk.capitalize())
        
        status = request.args.get('status')
        if status:
            query = query.filter_by(status=status.lower())
        
        team_id = request.args.get('team_id')
        if team_id:
            query = query.filter_by(team_id=int(team_id))
        
        # Get total count before pagination
        total_count = query.count()
        
        # Pagination
        limit = int(request.args.get('limit', 100))
        limit = min(limit, 817)  # Max 817
        
        offset = int(request.args.get('offset', 0))
        
        # Apply pagination
        players = query.offset(offset).limit(limit).all()
        
        players_data = [player.to_dict(include_team=True) for player in players]
        
        return jsonify({
            'success': True,
            'data': {
                'total_count': total_count,
                'returned_count': len(players),
                'limit': limit,
                'offset': offset,
                'players': players_data
            }
        }), 200
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@players_bp.route('/api/players/<int:player_id>', methods=['GET'])
def get_player(player_id):
    """
    Get single player details with full data
    """
    try:
        player = Player.query.get(player_id)
        
        if not player:
            return jsonify({
                'success': False,
                'error': 'Player not found'
            }), 404
        
        return jsonify({
            'success': True,
            'data': player.to_dict(include_team=True)
        }), 200
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@players_bp.route('/api/players/search', methods=['GET'])
def search_players():
    """
    Search players by name
    Query params:
    - q: Search query (searches web_name and full_name)
    - limit: Limit results (default: 20)
    """
    try:
        search_query = request.args.get('q', '').strip()
        
        if not search_query:
            return jsonify({
                'success': False,
                'error': 'Search query required (use ?q=player_name)'
            }), 400
        
        limit = int(request.args.get('limit', 20))
        
        # Search in both web_name and full_name (case-insensitive)
        players = Player.query.filter(
            db.or_(
                Player.web_name.ilike(f'%{search_query}%'),
                Player.full_name.ilike(f'%{search_query}%')
            )
        ).limit(limit).all()
        
        players_data = [player.to_dict(include_team=True) for player in players]
        
        return jsonify({
            'success': True,
            'data': {
                'query': search_query,
                'count': len(players),
                'players': players_data
            }
        }), 200
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@players_bp.route('/api/stats/summary', methods=['GET'])
def get_stats_summary():
    """
    Get overall statistics summary
    """
    try:
        total_teams = Team.query.count()
        total_players = Player.query.count()
        
        # By position
        goalkeepers = Player.query.filter_by(position='GK').count()
        defenders = Player.query.filter_by(position='DEF').count()
        midfielders = Player.query.filter_by(position='MID').count()
        forwards = Player.query.filter_by(position='FWD').count()
        
        # By status
        available = Player.query.filter_by(status='a').count()
        injured = Player.query.filter_by(status='i').count()
        doubtful = Player.query.filter_by(status='d').count()
        
        # By risk
        low_risk = Player.query.filter_by(injury_risk_level='Low').count()
        medium_risk = Player.query.filter_by(injury_risk_level='Medium').count()
        high_risk = Player.query.filter_by(injury_risk_level='High').count()
        
        # Overworked players
        overworked = Player.query.filter_by(is_overworked=1).count()
        
        # Injury prone players
        injury_prone = Player.query.filter_by(injury_prone=1).count()
        
        return jsonify({
            'success': True,
            'data': {
                'totals': {
                    'teams': total_teams,
                    'players': total_players
                },
                'by_position': {
                    'goalkeepers': goalkeepers,
                    'defenders': defenders,
                    'midfielders': midfielders,
                    'forwards': forwards
                },
                'by_status': {
                    'available': available,
                    'injured': injured,
                    'doubtful': doubtful
                },
                'by_risk': {
                    'low': low_risk,
                    'medium': medium_risk,
                    'high': high_risk
                },
                'flags': {
                    'overworked': overworked,
                    'injury_prone': injury_prone
                }
            }
        }), 200
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
