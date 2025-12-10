import requests
import pandas as pd
import time
from datetime import datetime

def fetch_player_history():
    """
    Fetch detailed history for each player
    This includes gameweek-by-gameweek performance data
    """
    print(" FETCHING PLAYER HISTORY DATA")
    
    # Load basic player data
    print("\n Loading basic player data...")
    try:
        df_basic = pd.read_csv('data/raw/fpl_players_basic.csv')
        print(f" Loaded {len(df_basic)} players")
    except FileNotFoundError:
        print(" Error: fpl_players_basic.csv not found!")
        print("   Run fetch_basic_data.py first")
        return None
    
    # Prepare to collect history
    player_histories = []
    total_players = len(df_basic)
    
    print(f"\nFetching detailed history for {total_players} players...")
    print()
    
    for idx, row in df_basic.iterrows():
        player_id = row['id']
        player_name = row['full_name']
        
        # Progress indicator
        if (idx + 1) % 50 == 0:
            print(f"   Progress: {idx + 1}/{total_players} players ({(idx+1)/total_players*100:.1f}%)")
        
        # Fetch player's detailed data
        url = f"https://fantasy.premierleague.com/api/element-summary/{player_id}/"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            # Extract gameweek history
            history = data.get('history', [])
            
            if not history:
                # Player hasn't played yet this season
                player_histories.append({
                    'player_id': player_id,
                    'player_name': player_name,
                    'games_played': 0,
                    'recent_minutes_last_5': 0,
                    'recent_games_last_5': 0,
                    'avg_minutes_per_game_last_5': 0,
                    'times_unavailable_last_10': 0,
                    'max_consecutive_unavailable': 0,
                    'total_minutes_last_10': 0,
                    'games_started_last_10': 0,
                    'workload_trend': 'none'
                })
                continue
            
            # Calculate metrics from history
            
            # Last 5 games
            last_5 = history[-5:] if len(history) >= 5 else history
            recent_minutes_5 = sum([g['minutes'] for g in last_5])
            recent_games_5 = len([g for g in last_5 if g['minutes'] > 0])
            avg_minutes_5 = recent_minutes_5 / len(last_5) if last_5 else 0
            
            # Last 10 games  
            last_10 = history[-10:] if len(history) >= 10 else history
            times_unavailable_10 = len([g for g in last_10 if g['minutes'] == 0])
            total_minutes_10 = sum([g['minutes'] for g in last_10])
            games_started_10 = len([g for g in last_10 if g['minutes'] >= 60])
            
            # Calculate max consecutive games missed (injury indicator)
            consecutive_count = 0
            max_consecutive = 0
            for game in history:
                if game['minutes'] == 0:
                    consecutive_count += 1
                    max_consecutive = max(max_consecutive, consecutive_count)
                else:
                    consecutive_count = 0
            
            # Calculate workload intensity - high minutes = higher fatigue/injury risk
            if len(history) >= 10:
                first_half_minutes = sum([g['minutes'] for g in history[-10:-5]]) / 5
                second_half_minutes = sum([g['minutes'] for g in history[-5:]]) / 5
                
                if second_half_minutes > first_half_minutes * 1.3:
                    workload_trend = 'increasing'
                elif second_half_minutes < first_half_minutes * 0.7:
                    workload_trend = 'decreasing'
                else:
                    workload_trend = 'stable'
            else:
                workload_trend = 'insufficient_data'
            
            player_histories.append({
                'player_id': player_id,
                'player_name': player_name,
                'games_played': len(history),
                'recent_minutes_last_5': recent_minutes_5,
                'recent_games_last_5': recent_games_5,
                'avg_minutes_per_game_last_5': round(avg_minutes_5, 1),
                'times_unavailable_last_10': times_unavailable_10,
                'max_consecutive_unavailable': max_consecutive,
                'total_minutes_last_10': total_minutes_10,
                'games_started_last_10': games_started_10,
                'workload_trend': workload_trend
            })
            
            # Small delay to respect API rate limits
            time.sleep(0.05)  # 50ms between requests
            
        except Exception as e:
            print(f"    Error fetching data for {player_name}: {e}")
            # Add placeholder data
            player_histories.append({
                'player_id': player_id,
                'player_name': player_name,
                'games_played': 0,
                'recent_minutes_last_5': 0,
                'recent_games_last_5': 0,
                'avg_minutes_per_game_last_5': 0,
                'times_unavailable_last_10': 0,
                'max_consecutive_unavailable': 0,
                'total_minutes_last_10': 0,
                'games_started_last_10': 0,
                'workload_trend': 'error'
            })
            continue
    
    # Create DataFrame
    df_history = pd.DataFrame(player_histories)
    
    # Save
    output_path = 'data/raw/fpl_player_history.csv'
    df_history.to_csv(output_path, index=False)
    
    print(f"\nSaved to: {output_path}")
    print(f"   Shape: {df_history.shape[0]} players × {df_history.shape[1]} columns")
    
    # Display summary statistics
    print("\n Summary Statistics:")
    print(f"   Players with game time: {len(df_history[df_history['games_played'] > 0])}")
    print(f"   Avg minutes (last 5 games): {df_history['recent_minutes_last_5'].mean():.1f}")
    print(f"   Players unavailable 5+ times (last 10): {len(df_history[df_history['times_unavailable_last_10'] >= 5])}")
    
    # Sample data
    print("\n📋 Sample Data (players with most unavailability):")
    sample = df_history.nlargest(5, 'times_unavailable_last_10')[
        ['player_name', 'recent_minutes_last_5', 'times_unavailable_last_10', 
         'max_consecutive_unavailable', 'workload_trend']
    ]
    print(sample.to_string(index=False))
    
    return df_history

def main():
    df = fetch_player_history()
    
    if df is not None:
        print("PLAYER HISTORY DATA COLLECTION COMPLETE")

if __name__ == "__main__":
    main()