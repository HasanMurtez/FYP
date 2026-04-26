import requests
import pandas as pd
import time
import os
from datetime import datetime



def fetch_basic_player_data():
    """Fetch basic player stats and team info"""
    print(" FETCHING BASIC PLAYER DATA")
    
    url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    print(f"\n Fetching from FPL API...")
    
    response = requests.get(url)
    data = response.json()
    
    players = data['elements']
    teams = data['teams']
    events = data['events']
    
    print(f" Fetched {len(players)} players from {len(teams)} teams")
    
    # Team lookup
    team_lookup = {team['id']: team['name'] for team in teams}
    
    # Current gameweek
    current_gw = None
    for event in events:
        if event['is_current']:
            current_gw = event['id']
            break
    if not current_gw:
        current_gw = max([e['id'] for e in events if e['finished']])
    
    print(f"   Current Gameweek: {current_gw}")
    
    # Process players
    position_map = {1: 'GK', 2: 'DEF', 3: 'MID', 4: 'FWD'}
    status_map = {'a': 'Available', 'd': 'Doubtful', 'i': 'Injured', 
                  'u': 'Unavailable', 's': 'Suspended', 'n': 'Not in squad'}
    
    processed_players = []
    
    for player in players:
        team_info = team_lookup[player['team']]
        
        processed_players.append({
            # Identity
            'id': player['id'],
            'web_name': player['web_name'],
            'full_name': f"{player['first_name']} {player['second_name']}",
            'team_id': player['team'],
            'team_name': team_info,
            'position': position_map[player['element_type']],
            
            # Availability
            'status': player['status'],
            'status_description': status_map.get(player['status'], 'Unknown'),
            'chance_of_playing_next': player['chance_of_playing_next_round'],
            'news': player['news'] if player['news'] else '',
            
            # Playing Time
            'minutes': player['minutes'],
            'starts': player['starts'],
            
            # Physical Stats
            'goals_scored': player['goals_scored'],
            'assists': player['assists'],
            'yellow_cards': player['yellow_cards'],
            'red_cards': player['red_cards'],
            
            # Advanced Metrics (physical involvement)
            'influence': float(player['influence']),
            'creativity': float(player['creativity']),
            'threat': float(player['threat']),
            'ict_index': float(player['ict_index']),
            
            # Meta
            'current_gameweek': current_gw
        })
    
    df_basic = pd.DataFrame(processed_players)
    df_basic.to_csv('data/raw/fpl_players_basic.csv', index=False)
    print(f" Saved: data/raw/fpl_players_basic.csv ({df_basic.shape})\n")
    
    return df_basic

def fetch_player_history(df_basic):
    """Fetch gameweek by gameweek history for workload analysis"""
    print(" FETCHING PLAYER HISTORY (Workload and Injury Patterns)")
    
    print(f"\n Fetching history for {len(df_basic)} players...")
    
    player_histories = []
    
    for idx, row in df_basic.iterrows():
        player_id = row['id']
        player_name = row['full_name']
        
        if (idx + 1) % 50 == 0:
            print(f"   Progress: {idx + 1}/{len(df_basic)} ({(idx+1)/len(df_basic)*100:.1f}%)")
        
        url = f"https://fantasy.premierleague.com/api/element-summary/{player_id}/"
        
        try:
            response = requests.get(url)
            data = response.json()
            history = data.get('history', [])
            
            if not history:
                player_histories.append({
                    'player_id': player_id,
                    'recent_minutes_last_5': 0,
                    'times_unavailable_last_10': 0,
                    'max_consecutive_unavailable': 0,
                    'total_minutes_last_10': 0,
                })
                continue
            
            # Last 5 games
            last_5 = history[-5:] if len(history) >= 5 else history
            recent_minutes_5 = sum([g['minutes'] for g in last_5])
            
            # Last 10 games
            last_10 = history[-10:] if len(history) >= 10 else history
            times_unavailable_10 = len([g for g in last_10 if g['minutes'] == 0])
            total_minutes_10 = sum([g['minutes'] for g in last_10])
            
            # Max consecutive games missed
            consecutive_count = 0
            max_consecutive = 0
            for game in history:
                if game['minutes'] == 0:
                    consecutive_count += 1
                    max_consecutive = max(max_consecutive, consecutive_count)
                else:
                    consecutive_count = 0
            
            player_histories.append({
                'player_id': player_id,
                'recent_minutes_last_5': recent_minutes_5,
                'times_unavailable_last_10': times_unavailable_10,
                'max_consecutive_unavailable': max_consecutive,
                'total_minutes_last_10': total_minutes_10,
            })
            
            time.sleep(0.05)  # API rate limiting
            
        except Exception as e:
            player_histories.append({
                'player_id': player_id,
                'recent_minutes_last_5': 0,
                'times_unavailable_last_10': 0,
                'max_consecutive_unavailable': 0,
                'total_minutes_last_10': 0,
            })
    
    df_history = pd.DataFrame(player_histories)
    df_history.to_csv('data/raw/fpl_player_history.csv', index=False)
    print(f"\n Saved: data/raw/fpl_player_history.csv ({df_history.shape})\n")
    
    return df_history

def fetch_fixtures():
    """Get fixture congestion data"""
    print(" FETCHING FIXTURE CONGESTION")
    
    url = "https://fantasy.premierleague.com/api/fixtures/"
    print(f"\n Fetching fixtures...")
    
    response = requests.get(url)
    fixtures = response.json()
    
    # Get team names
    base_url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    base_data = requests.get(base_url).json()
    teams = base_data['teams']
    team_lookup = {team['id']: team['name'] for team in teams}
    
    # Count upcoming fixtures
    upcoming_fixtures = [f for f in fixtures if not f['finished']]
    print(f" Found {len(upcoming_fixtures)} upcoming fixtures")
    
    team_congestion = {}
    for team_id in team_lookup.keys():
        home = len([f for f in upcoming_fixtures if f['team_h'] == team_id])
        away = len([f for f in upcoming_fixtures if f['team_a'] == team_id])
        total = home + away
        
        # Congestion level
        if total >= 5:
            level = 'High'
        elif total >= 3:
            level = 'Medium'
        else:
            level = 'Low'
        
        team_congestion[team_id] = {
            'team_id': team_id,
            'team_name': team_lookup[team_id],
            'total_upcoming_fixtures': total,
            'congestion_level': level
        }
    
    df_congestion = pd.DataFrame(list(team_congestion.values()))
    df_congestion.to_csv('data/raw/fixture_congestion.csv', index=False)
    print(f" Saved: data/raw/fixture_congestion.csv ({df_congestion.shape})\n")
    
    return df_congestion

def merge_and_prepare_data():
    """Merge all data sources and prepare final dataset"""
    print("MERGING DATA & FEATURE ENGINEERING")
    
    print("\n Loading all datasets...")
    df_basic = pd.read_csv('data/raw/fpl_players_basic.csv')
    df_history = pd.read_csv('data/raw/fpl_player_history.csv')
    df_congestion = pd.read_csv('data/raw/fixture_congestion.csv')
    
    # Merge
    print(" Merging datasets...")
    df = df_basic.merge(df_history, left_on='id', right_on='player_id', how='left')
    df = df.merge(df_congestion, on='team_id', how='left', suffixes=('', '_team'))
    df = df.drop(columns=['player_id', 'team_name_team'], errors='ignore')
    
    # Fill missing values
    df = df.fillna({
        'recent_minutes_last_5': 0,
        'times_unavailable_last_10': 0,
        'max_consecutive_unavailable': 0,
        'total_minutes_last_10': 0,
        'chance_of_playing_next': 100,
        'news': '',
        'total_upcoming_fixtures': 3,
        'congestion_level': 'Medium'
    })
    
    print(" Engineering features")
    
    # Workload features
    df['workload_intensity'] = df['minutes'] / (df['starts'] + 1)
    df['is_overworked'] = (df['recent_minutes_last_5'] > 400).astype(int)
    
    # Injury history
    df['availability_score'] = 10 - df['times_unavailable_last_10']
    df['injury_prone'] = (df['times_unavailable_last_10'] >= 3).astype(int)
    
    # Physical demand
    df['physical_demand'] = df['yellow_cards'] + (df['red_cards'] * 3)
    
    # Position risk
    position_risk = {'GK': 0, 'DEF': 1, 'MID': 2, 'FWD': 2}
    df['position_risk'] = df['position'].map(position_risk)
    
    # Congestion numeric
    congestion_map = {'High': 2, 'Medium': 1, 'Low': 0}
    df['congestion_numeric'] = df['congestion_level'].map(congestion_map)
    
    # Save
    output_path = 'data/processed/fpl_complete_dataset.csv'
    df.to_csv(output_path, index=False)
    print(f" Saved: {output_path} ({df.shape})\n")
    
    # Summary
    print(" Dataset Summary:")
    print(f"   Total players: {len(df)}")
    print(f"   Currently injured: {len(df[df['status'] == 'i'])}")
    print(f"   Doubtful: {len(df[df['status'] == 'd'])}")
    print(f"   Overworked: {df['is_overworked'].sum()}")
    print(f"   Injury-prone: {df['injury_prone'].sum()}")
    
    return df

def ensure_directories():
    """Create necessary directories for data storage"""
    os.makedirs('data/raw', exist_ok=True)
    os.makedirs('data/processed', exist_ok=True)

def main():
    """Run complete data collection pipeline"""
    print("FPL injury risk data collection started")
    
    start_time = datetime.now()
    
    # Create directories
    ensure_directories()
    
    # Step 1: Basic data
    df_basic = fetch_basic_player_data()
    
    # Step 2: Player history
    df_history = fetch_player_history(df_basic)
    
    # Step 3: Fixtures
    df_congestion = fetch_fixtures()
    
    # Step 4: Merge and prepare
    df_final = merge_and_prepare_data()
    
    elapsed = (datetime.now() - start_time).total_seconds()
    
    print(" Data Collection Complete")
    print(f"\n Output files:")
    print(f"   • data/raw/fpl_players_basic.csv")
    print(f"   • data/raw/fpl_player_history.csv")
    print(f"   • data/raw/fixture_congestion.csv")
    print(f"   • data/processed/fpl_complete_dataset.csv")
    print(f"\n Dataset: {df_final.shape[0]} players × {df_final.shape[1]} features")

if __name__ == "__main__":
    main()
