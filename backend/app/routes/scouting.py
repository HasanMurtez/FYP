
from flask import Blueprint, jsonify, request
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import StandardScaler
import numpy as np
from app import db
from app.models import Player

# Create blueprint
scouting_bp = Blueprint('scouting', __name__)


def prepare_player_features(player):
    """
     Extract features for similarity comparison
    """
    return [
        # Performance stats
        player.goals_scored,
        player.assists,
        player.minutes,
        player.starts,
        
        # Playing style (ICT Index)
        player.influence,
        player.creativity,
        player.threat,
        player.ict_index,
        
        # Physical involvement
        player.yellow_cards,
        player.red_cards,
        
        # Form metrics
        player.recent_minutes_last_5,
        player.total_minutes_last_10,
        
        # Availability
        player.availability_score,
    ]


@scouting_bp.route('/api/scouting/similar', methods=['GET'])
def find_similar_players():
    """
    Find similar players to a given player
    Query params:
    player_id: ID of the player to find replacements for (required)
    limit: Number of similar players to return (default: 10, max: 20)
    same_position: Only return players in same position (default: true)
    exclude_team: Exclude players from same team (default: false)
    """
    try:
        # Get query parameters
        player_id = request.args.get('player_id')
        if not player_id:
            return jsonify({
                'success': False,
                'error': 'player_id is required'
            }), 400
        
        player_id = int(player_id)
        limit = min(int(request.args.get('limit', 10)), 20)
        same_position = request.args.get('same_position', 'true').lower() == 'true'
        exclude_team = request.args.get('exclude_team', 'false').lower() == 'true'
        
        # Get the target player
        target_player = Player.query.get(player_id)
        if not target_player:
            return jsonify({
                'success': False,
                'error': 'Player not found'
            }), 404
        
        # Build query for candidate players
        query = Player.query
        
        # Filter by position if requested
        if same_position:
            query = query.filter_by(position=target_player.position)
        
        # Exclude same team if requested
        if exclude_team:
            query = query.filter(Player.team_id != target_player.team_id)
        
        # Exclude the target player itself
        query = query.filter(Player.id != target_player.id)
        
        # Get all candidate players
        candidate_players = query.all()
        
        if not candidate_players:
            return jsonify({
                'success': False,
                'error': 'No similar players found with current filters'
            }), 404
        
        print(f"\n=== Finding similar players to {target_player.web_name} ===")
        print(f"Comparing against {len(candidate_players)} candidates...")
        
        # Prepare features for all players
        target_features = prepare_player_features(target_player)
        candidate_features = [prepare_player_features(p) for p in candidate_players]
        
        # Convert to numpy arrays
        target_array = np.array(target_features).reshape(1, -1)
        candidates_array = np.array(candidate_features)
        
        # Scale features
        scaler = StandardScaler()
        candidates_scaled = scaler.fit_transform(candidates_array)
        target_scaled = scaler.transform(target_array)
        
        # Use KNN to find similar players
        # n_neighbors = limit + 1 to account for excluding the target player
        n_neighbors = min(limit + 1, len(candidate_players))
        knn = NearestNeighbors(n_neighbors=n_neighbors, metric='euclidean')
        knn.fit(candidates_scaled)
        
        # Find nearest neighbors
        distances, indices = knn.kneighbors(target_scaled)
        
        # Build response with similar players
        similar_players = []
        for idx, distance in zip(indices[0], distances[0]):
            similar_player = candidate_players[idx]
            
            # Calculate similarity score
            # Lower distance = higher similarity
            # Convert euclidean distance to percentage similarity
            max_distance = 10.0  # Reasonable max distance
            similarity_score = max(0, min(100, 100 * (1 - distance / max_distance)))
            
            similar_players.append({
                'player': similar_player.to_dict(include_team=True),
                'similarity_score': round(similarity_score, 2),
                'distance': round(float(distance), 4)
            })
        
        # Limit to requested number
        similar_players = similar_players[:limit]
        
        print(f"Found {len(similar_players)} similar players")
        
        return jsonify({
            'success': True,
            'data': {
                'target_player': target_player.to_dict(include_team=True),
                'filters': {
                    'same_position': same_position,
                    'exclude_team': exclude_team,
                    'position': target_player.position if same_position else 'Any'
                },
                'similar_players_count': len(similar_players),
                'similar_players': similar_players
            }
        }), 200
        
    except Exception as e:
        print(f"\nScouting error: {str(e)}\n")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@scouting_bp.route('/api/scouting/replacements', methods=['GET'])
def find_replacements():
    """
    Smart replacement finder for injured/high-risk players
    Query params:
     player_id: ID of player to replace (required)
     max_risk: Maximum injury risk level for replacements (Low, Medium, High)
     limit: Number of replacements to return (default: 5)
    """
    try:
        player_id = request.args.get('player_id')
        if not player_id:
            return jsonify({
                'success': False,
                'error': 'player_id is required'
            }), 400
        
        player_id = int(player_id)
        max_risk = request.args.get('max_risk', 'Medium')
        limit = int(request.args.get('limit', 5))
        
        # Get target player
        target_player = Player.query.get(player_id)
        if not target_player:
            return jsonify({
                'success': False,
                'error': 'Player not found'
            }), 404
        
        # Find similar players (same position, exclude same team)
        query = Player.query.filter_by(position=target_player.position)
        query = query.filter(Player.team_id != target_player.team_id)
        query = query.filter(Player.id != target_player.id)
        
        # Filter by risk level
        risk_order = {'Low': 0, 'Medium': 1, 'High': 2}
        max_risk_level = risk_order.get(max_risk, 1)
        
        if max_risk == 'Low':
            query = query.filter_by(injury_risk_level='Low')
        elif max_risk == 'Medium':
            query = query.filter(Player.injury_risk_level.in_(['Low', 'Medium']))
        # If 'High', include all risk levels
        
        # Only available players
        query = query.filter_by(status='a')
        
        candidate_players = query.all()
        
        if not candidate_players:
            return jsonify({
                'success': False,
                'error': f'No available {target_player.position} replacements found with max risk: {max_risk}'
            }), 404
        
        print(f"\n=== Finding replacements for {target_player.web_name} ===")
        print(f"Position: {target_player.position}, Max Risk: {max_risk}")
        print(f"Candidates: {len(candidate_players)}")
        
        # Prepare features
        target_features = prepare_player_features(target_player)
        candidate_features = [prepare_player_features(p) for p in candidate_players]
        
        target_array = np.array(target_features).reshape(1, -1)
        candidates_array = np.array(candidate_features)
        
        # Scale
        scaler = StandardScaler()
        candidates_scaled = scaler.fit_transform(candidates_array)
        target_scaled = scaler.transform(target_array)
        
        # KNN
        n_neighbors = min(limit, len(candidate_players))
        knn = NearestNeighbors(n_neighbors=n_neighbors, metric='euclidean')
        knn.fit(candidates_scaled)
        
        distances, indices = knn.kneighbors(target_scaled)
        
        # Build replacements list
        replacements = []
        for idx, distance in zip(indices[0], distances[0]):
            replacement = candidate_players[idx]
            
            max_distance = 10.0
            similarity_score = max(0, min(100, 100 * (1 - distance / max_distance)))
            
            replacements.append({
                'player': replacement.to_dict(include_team=True),
                'similarity_score': round(similarity_score, 2),
                'match_quality': 'Excellent' if similarity_score > 85 else 'Good' if similarity_score > 70 else 'Fair',
                'distance': round(float(distance), 4)
            })
        
        print(f"Found {len(replacements)} suitable replacements")
        
        return jsonify({
            'success': True,
            'data': {
                'target_player': target_player.to_dict(include_team=True),
                'search_criteria': {
                    'position': target_player.position,
                    'max_risk_level': max_risk,
                    'status': 'Available only',
                    'exclude_same_team': True
                },
                'replacements_count': len(replacements),
                'replacements': replacements
            }
        }), 200
        
    except Exception as e:
        print(f"\nReplacement search error: {str(e)}\n")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
