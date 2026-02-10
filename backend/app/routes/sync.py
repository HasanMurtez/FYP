from flask import Blueprint, jsonify
import requests
import time
from app import db
from app.models import Team, Player

# Create blueprint
sync_bp = Blueprint('sync', __name__)

FPL_BASE_URL = "https://fantasy.premierleague.com/api"


def calculate_features(player_data, history_data):
    """
    Calculate engineered features from raw FPL data
    Same logic as your collect_fpl_data.py
    """
    history = history_data.get('history', [])

    # Last 5 games minutes
    last_5 = history[-5:] if len(history) >= 5 else history
    recent_minutes_last_5 = sum([g['minutes'] for g in last_5])

    # Last 10 games
    last_10 = history[-10:] if len(history) >= 10 else history
    times_unavailable_last_10 = len([g for g in last_10 if g['minutes'] == 0])
    total_minutes_last_10 = sum([g['minutes'] for g in last_10])

    # Max consecutive games missed
    consecutive_count = 0
    max_consecutive = 0
    for game in history:
        if game['minutes'] == 0:
            consecutive_count += 1
            max_consecutive = max(max_consecutive, consecutive_count)
        else:
            consecutive_count = 0

    # Engineered features
    minutes = player_data.get('minutes', 0)
    starts = player_data.get('starts', 0)
    yellow_cards = player_data.get('yellow_cards', 0)
    red_cards = player_data.get('red_cards', 0)

    workload_intensity = minutes / (starts + 1)
    is_overworked = 1 if recent_minutes_last_5 > 400 else 0
    availability_score = 10 - times_unavailable_last_10
    injury_prone = 1 if times_unavailable_last_10 >= 3 else 0
    physical_demand = yellow_cards + (red_cards * 3)

    position_map = {'GK': 0, 'DEF': 1, 'MID': 2, 'FWD': 2}
    position = ['GK', 'DEF', 'MID', 'FWD'][player_data.get('element_type', 1) - 1]
    position_risk = position_map.get(position, 1)

    return {
        'recent_minutes_last_5': recent_minutes_last_5,
        'times_unavailable_last_10': times_unavailable_last_10,
        'total_minutes_last_10': total_minutes_last_10,
        'max_consecutive_unavailable': max_consecutive,
        'workload_intensity': round(workload_intensity, 2),
        'is_overworked': is_overworked,
        'availability_score': availability_score,
        'injury_prone': injury_prone,
        'physical_demand': physical_demand,
        'position_risk': position_risk,
        'position': position
    }


@sync_bp.route('/api/sync', methods=['POST'])
def sync_fpl_data():
    """
    Fetch latest FPL data and save to database
    """
    try:
        print("Starting FPL data sync...")

        # Step 1: Fetch bootstrap data (teams + players)
        print("Fetching FPL bootstrap data...")
        response = requests.get(f"{FPL_BASE_URL}/bootstrap-static/", timeout=30)
        response.raise_for_status()
        data = response.json()

        teams_data = data['elements']  # This is actually players
        raw_teams = data['teams']
        events = data['events']

        # Get current gameweek
        current_gw = None
        for event in events:
            if event['is_current']:
                current_gw = event['id']
                break
        if not current_gw:
            current_gw = max([e['id'] for e in events if e['finished']], default=1)

        print(f"Current gameweek: {current_gw}")

        # Step 2: Fetch fixture congestion
        print("Fetching fixtures...")
        fixtures_response = requests.get(f"{FPL_BASE_URL}/fixtures/", timeout=30)
        fixtures = fixtures_response.json()
        upcoming_fixtures = [f for f in fixtures if not f['finished']]

        # Calculate congestion per team
        team_congestion = {}
        for raw_team in raw_teams:
            team_id = raw_team['id']
            home = len([f for f in upcoming_fixtures if f['team_h'] == team_id])
            away = len([f for f in upcoming_fixtures if f['team_a'] == team_id])
            total = home + away
            level = 'High' if total >= 5 else 'Medium' if total >= 3 else 'Low'
            congestion_map = {'High': 2, 'Medium': 1, 'Low': 0}
            team_congestion[team_id] = {
                'total': total,
                'level': level,
                'numeric': congestion_map[level]
            }

        # Step 3: Save teams to database
        print(f"Saving {len(raw_teams)} teams...")
        teams_saved = 0
        fpl_id_to_db_id = {}  # Map FPL team ID to database ID

        for raw_team in raw_teams:
            congestion = team_congestion.get(raw_team['id'], {'total': 3, 'level': 'Medium', 'numeric': 1})

            # Check if team already exists
            team = Team.query.filter_by(fpl_id=raw_team['id']).first()

            if team:
                # Update existing team
                team.name = raw_team['name']
                team.short_name = raw_team['short_name']
                team.strength = raw_team.get('strength', 0)
                team.total_upcoming_fixtures = congestion['total']
                team.congestion_level = congestion['level']
            else:
                # Create new team
                team = Team(
                    fpl_id=raw_team['id'],
                    name=raw_team['name'],
                    short_name=raw_team['short_name'],
                    strength=raw_team.get('strength', 0),
                    total_upcoming_fixtures=congestion['total'],
                    congestion_level=congestion['level']
                )
                db.session.add(team)

            db.session.flush()  # Get the ID without committing
            fpl_id_to_db_id[raw_team['id']] = team.id
            teams_saved += 1

        db.session.commit()
        print(f"Saved {teams_saved} teams")

        # Step 4: Save players to database
        print(f"Saving {len(teams_data)} players...")
        position_map = {1: 'GK', 2: 'DEF', 3: 'MID', 4: 'FWD'}
        status_map = {
            'a': 'Available', 'd': 'Doubtful', 'i': 'Injured',
            'u': 'Unavailable', 's': 'Suspended', 'n': 'Not in squad'
        }

        players_saved = 0
        players_failed = 0

        for idx, player_data in enumerate(teams_data):

            # Progress update every 100 players
            if (idx + 1) % 100 == 0:
                print(f"   Processing player {idx + 1}/{len(teams_data)}...")

            try:
                # Fetch player history for feature engineering
                history_response = requests.get(
                    f"{FPL_BASE_URL}/element-summary/{player_data['id']}/",
                    timeout=15
                )
                history_data = history_response.json()
                time.sleep(0.05)  # Rate limiting

                # Calculate engineered features
                features = calculate_features(player_data, history_data)

                # Get team database ID
                fpl_team_id = player_data['team']
                db_team_id = fpl_id_to_db_id.get(fpl_team_id)

                if not db_team_id:
                    players_failed += 1
                    continue

                # Get congestion for this player's team
                congestion = team_congestion.get(fpl_team_id, {'numeric': 1})

                # Check if player already exists
                player = Player.query.filter_by(fpl_id=player_data['id']).first()

                player_fields = {
                    'web_name': player_data['web_name'],
                    'full_name': f"{player_data['first_name']} {player_data['second_name']}",
                    'position': features['position'],
                    'team_id': db_team_id,
                    'status': player_data.get('status', 'a'),
                    'status_description': status_map.get(player_data.get('status', 'a'), 'Available'),
                    'chance_of_playing_next': player_data.get('chance_of_playing_next_round') or 100,
                    'news': player_data.get('news', '') or '',
                    'minutes': player_data.get('minutes', 0),
                    'starts': player_data.get('starts', 0),
                    'goals_scored': player_data.get('goals_scored', 0),
                    'assists': player_data.get('assists', 0),
                    'yellow_cards': player_data.get('yellow_cards', 0),
                    'red_cards': player_data.get('red_cards', 0),
                    'influence': float(player_data.get('influence', 0) or 0),
                    'creativity': float(player_data.get('creativity', 0) or 0),
                    'threat': float(player_data.get('threat', 0) or 0),
                    'ict_index': float(player_data.get('ict_index', 0) or 0),
                    'recent_minutes_last_5': features['recent_minutes_last_5'],
                    'total_minutes_last_10': features['total_minutes_last_10'],
                    'times_unavailable_last_10': features['times_unavailable_last_10'],
                    'max_consecutive_unavailable': features['max_consecutive_unavailable'],
                    'availability_score': features['availability_score'],
                    'injury_prone': features['injury_prone'],
                    'workload_intensity': features['workload_intensity'],
                    'is_overworked': features['is_overworked'],
                    'physical_demand': features['physical_demand'],
                    'position_risk': features['position_risk'],
                    'congestion_numeric': congestion['numeric'],
                }

                if player:
                    # Update existing player
                    for key, value in player_fields.items():
                        setattr(player, key, value)
                else:
                    # Create new player
                    player = Player(**player_fields)
                    db.session.add(player)

                players_saved += 1

            except Exception as e:
                players_failed += 1
                continue

        db.session.commit()
        print(f"Sync complete! Saved {players_saved} players, {players_failed} failed")

        return jsonify({
            'success': True,
            'message': 'FPL data synced successfully',
            'data': {
                'teams_synced': teams_saved,
                'players_synced': players_saved,
                'players_failed': players_failed,
                'gameweek': current_gw
            }
        }), 200

    except requests.exceptions.Timeout:
        return jsonify({
            'success': False,
            'error': 'FPL API request timed out'
        }), 504

    except requests.exceptions.ConnectionError:
        return jsonify({
            'success': False,
            'error': 'Could not connect to FPL API'
        }), 503

    except Exception as e:
        db.session.rollback()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@sync_bp.route('/api/sync/status', methods=['GET'])
def sync_status():
    """
    Check how many teams and players are currently in the database
    """
    try:
        team_count = Team.query.count()
        player_count = Player.query.count()
        injured_count = Player.query.filter_by(status='i').count()
        doubtful_count = Player.query.filter_by(status='d').count()

        return jsonify({
            'success': True,
            'data': {
                'teams_in_database': team_count,
                'players_in_database': player_count,
                'injured_players': injured_count,
                'doubtful_players': doubtful_count,
                'database_populated': player_count > 0
            }
        }), 200

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
