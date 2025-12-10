import requests
import pandas as pd
import json
import os
from datetime import datetime

def ensure_directories():
    """Create necessary directories if they don't exist"""
    os.makedirs('data/raw', exist_ok=True)
    os.makedirs('data/processed', exist_ok=True)
    print("✅ Directories created/verified")

def fetch_basic_player_data():
    """
    Fetch basic player data from FPL bootstrap-static endpoint
    This includes current season stats, team info, and basic metrics
    """
    print("FETCHING BASIC PLAYER DATA")
    
    # Fetch from FPL API
    url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    print(f"\n📡 Fetching data from: {url}")
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        print(" Successfully fetched data from FPL API")
    except Exception as e:
        print(f" Error fetching data: {e}")
        return None
    
    # Extract relevant data
    players = data['elements']
    teams = data['teams']
    events = data['events']  # Gameweeks
    
    print(f"\nData Summary:")
    print(f"   Players: {len(players)}")
    print(f"   Teams: {len(teams)}")
    print(f"   Gameweeks: {len(events)}")
    
    # Create team lookup dictionary
    team_lookup = {team['id']: {
        'name': team['name'],
        'short_name': team['short_name'],
        'strength': team['strength']
    } for team in teams}
    
    # Get current gameweek
    current_gw = None
    for event in events:
        if event['is_current']:
            current_gw = event['id']
            break
    
    if not current_gw:
        # If no current GW, get the last finished one
        current_gw = max([e['id'] for e in events if e['finished']])
    
    print(f"   Current Gameweek: {current_gw}")
    
    # Process player data
    print("\n Processing player data...")
    processed_players = []
    
    position_map = {1: 'GK', 2: 'DEF', 3: 'MID', 4: 'FWD'}
    status_map = {
        'a': 'Available',
        'd': 'Doubtful', 
        'i': 'Injured',
        'u': 'Unavailable',
        's': 'Suspended',
        'n': 'Not in squad'
    }
    
    for player in players:
        team_info = team_lookup[player['team']]
        
        processed_players.append({
            # Identifiers
            'id': player['id'],
            'code': player['code'],
            'web_name': player['web_name'],
            'first_name': player['first_name'],
            'second_name': player['second_name'],
            'full_name': f"{player['first_name']} {player['second_name']}",
            
            # Team information
            'team_id': player['team'],
            'team_name': team_info['name'],
            'team_short_name': team_info['short_name'],
            'team_strength': team_info['strength'],
            
            # Position
            'position_id': player['element_type'],
            'position': position_map[player['element_type']],
            
            # Availability & Status
            'status': player['status'],
            'status_description': status_map.get(player['status'], 'Unknown'),
            'chance_of_playing_next': player['chance_of_playing_next_round'],
            'chance_of_playing_this': player['chance_of_playing_this_round'],
            'news': player['news'],
            'news_added': player['news_added'],
            
            # Performance Stats (injury-relevant only)
            'minutes': player['minutes'],
            'starts': player['starts'],
            
            # Playing Time & Workload
            'expected_starts': float(player['expected_goal_involvements']) if player.get('expected_goal_involvements') else 0,
            
            # Attacking Stats
            'goals_scored': player['goals_scored'],
            'assists': player['assists'],
            'expected_goals': float(player['expected_goals']) if player['expected_goals'] else 0,
            'expected_assists': float(player['expected_assists']) if player['expected_assists'] else 0,
            'expected_goal_involvements': float(player['expected_goal_involvements']) if player['expected_goal_involvements'] else 0,
            
            # Defensive Stats (for defenders)
            'clean_sheets': player['clean_sheets'],
            'goals_conceded': player['goals_conceded'],
            'own_goals': player['own_goals'],
            'penalties_saved': player['penalties_saved'],
            'penalties_missed': player['penalties_missed'],
            'saves': player['saves'],
            
            # Discipline
            'yellow_cards': player['yellow_cards'],
            'red_cards': player['red_cards'],
            'bonus': player['bonus'],
            'bps': player['bps'],
            
            # Advanced Metrics
            'influence': float(player['influence']),
            'creativity': float(player['creativity']),
            'threat': float(player['threat']),
            'ict_index': float(player['ict_index']),
            
            # Cost & Ownership
            'now_cost': player['now_cost'] / 10,  # Convert to actual price (e.g., 100 = £10.0m)
            'cost_change_start': player['cost_change_start'] / 10,
            'cost_change_event': player['cost_change_event'] / 10,
            'selected_by_percent': float(player['selected_by_percent']),
            'transfers_in': player['transfers_in'],
            'transfers_out': player['transfers_out'],
            'transfers_in_event': player['transfers_in_event'],
            'transfers_out_event': player['transfers_out_event'],
            
            # Meta
            'in_dreamteam': player['in_dreamteam'],
            'dreamteam_count': player['dreamteam_count'],
            'value_form': float(player['value_form']) if player['value_form'] else 0,
            'value_season': float(player['value_season']) if player['value_season'] else 0,
            
            # Timestamp
            'fetch_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'current_gameweek': current_gw
        })
    
    # Create DataFrame
    df = pd.DataFrame(processed_players)
    
    # Save to CSV
    output_path = 'data/raw/fpl_players_basic.csv'
    df.to_csv(output_path, index=False)
    print(f"\n Saved to: {output_path}")
    print(f"   Shape: {df.shape[0]} players × {df.shape[1]} columns")
    
    # Display sample
    print("\n Sample Data (first 3 players):")
    print(df[['full_name', 'team_name', 'position', 'status_description', 
              'total_points', 'minutes']].head(3).to_string(index=False))
    
    return df

def main():
    ensure_directories()
    df = fetch_basic_player_data()
    
    if df is not None:
        print(" BASIC DATA COLLECTION COMPLETE")

if __name__ == "__main__":
    main()