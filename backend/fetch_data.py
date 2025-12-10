import requests
import pandas as pd
import json

def fetch_all_players():
   
    print("FETCHING PL DATA - 2025/2026 SEASON")
    
    # FPL API
    url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    
    print("\nFetching data from Fantasy Premier League API...")
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        
        # Save raw response
        with open('data/raw/fpl_full_data.json', 'w') as f:
            json.dump(data, f, indent=2)
        
        print("Y Data fetched successfully!")
        
        # Extract players
        players = data['elements']
        teams = data['teams']
        
        # create team lookup dictionary
        team_lookup = {team['id']: team['name'] for team in teams}
        
        # process player data
        players_list = []
        for player in players:
            players_list.append({
                'id': player['id'],
                'name': player['web_name'],
                'full_name': player['first_name'] + ' ' + player['second_name'],
                'team': team_lookup[player['team']],
                'position': player['element_type'],  # 1=GK, 2=DEF, 3=MID, 4=FWD
                'cost': player['now_cost'] / 10,  # Convert to millions
                
                # Performance stats
                'total_points': player['total_points'],
                'minutes': player['minutes'],
                'goals': player['goals_scored'],
                'assists': player['assists'],
                'clean_sheets': player['clean_sheets'],
                'yellow_cards': player['yellow_cards'],
                'red_cards': player['red_cards'],
                
                # injury/availability data
                'chance_of_playing_next_round': player['chance_of_playing_next_round'],
                'news': player['news'],  # Injury news
                'status': player['status'],  # a=available, d=doubtful, i=injured, u=unavailable
                
                # Form metrics
                'form': float(player['form']) if player['form'] else 0,
                'selected_by_percent': float(player['selected_by_percent']),
                'transfers_in': player['transfers_in'],
                'transfers_out': player['transfers_out'],
                
                # Additional useful fields
                'games_played': player['starts'],
                'bonus_points': player['bonus'],
                'influence': float(player['influence']),
                'creativity': float(player['creativity']),
                'threat': float(player['threat']),
                'ict_index': float(player['ict_index'])
            })
        
        # convert to DataFrame
        df = pd.DataFrame(players_list)
        
        # Save to CSV
        df.to_csv('data/raw/fpl_players_2025_26.csv', index=False)
        
        print(f"\n Saved {len(df)} players to data/raw/fpl_players_2025_26.csv")
        
        return df
    else:
        print(f" Failed to fetch data. Status code: {response.status_code}")
        return None

def analyze_data(df):
    """
    Quick analysis of the fetched data
    """
    print("DATA SUMMARY")
    
    print(f"\nTotal players: {len(df)}")
    
    # Position breakdown
    position_map = {1: 'Goalkeeper', 2: 'Defender', 3: 'Midfielder', 4: 'Forward'}
    print("\nPlayers by position:")
    for pos_id, pos_name in position_map.items():
        count = len(df[df['position'] == pos_id])
        print(f"  {pos_name}: {count}")
    
    # Injury status breakdown
    print("\nInjury/Availability Status:")
    status_counts = df['status'].value_counts()
    status_map = {'a': 'Available', 'd': 'Doubtful', 'i': 'Injured', 'u': 'Unavailable'}
    for status_code, count in status_counts.items():
        print(f"  {status_map.get(status_code, status_code)}: {count}")
    
    # Players with injury news
    injured_players = df[df['news'] != ''].copy()
    print(f"\nPlayers with injury/availability news: {len(injured_players)}")
    
    if len(injured_players) > 0:
        print("\nSample injured/unavailable players:")
        for idx, player in injured_players.head(5).iterrows():
            print(f"  - {player['full_name']} ({player['team']})")
            print(f"    Status: {player['status']}")
    
    # Minutes played stats
    print(f"\nMinutes played stats:")
    print(f"  Average: {df['minutes'].mean():.0f}")
    print(f"  Max: {df['minutes'].max()}")
    print(f"  Players with 0 minutes: {len(df[df['minutes'] == 0])}")
    print(f"  Players with 1000+ minutes: {len(df[df['minutes'] >= 1000])}")
    
    # Top 5 players by minutes
    print("\nTop 5 most-played players (potential overwork risk):")
    top_players = df.nlargest(5, 'minutes')[['full_name', 'team', 'position', 'minutes', 'status']]
    for idx, player in top_players.iterrows():
        pos_name = position_map[player['position']]
        print(f"  - {player['full_name']} ({player['team']}, {pos_name}): {player['minutes']} mins - Status: {player['status']}")
    
    # Check data quality
    print(f"\nData Quality Check:")
    print(f"  Missing minutes data: {df['minutes'].isna().sum()}")
    print(f"  Missing status data: {df['status'].isna().sum()}")
    print(f"  Players with no games: {len(df[df['games_played'] == 0])}")

def show_sample_data(df):
    """
    Show sample of what the data looks like
    """
    print("SAMPLE DATA (First 5 Players)")
    
    sample = df.head(5)[['full_name', 'team', 'position', 'minutes', 'status', 'news']]
    print(sample.to_string(index=False))

def main():
    # Fetch data
    df = fetch_all_players()
    
    if df is not None:
        analyze_data(df)
        show_sample_data(df)
        
        print("DATA FETCHING COMPLETE")
        print("\nData Got:")
        print("  1.  Current season player data (2025/2026)")
        print("  2.  Injury status information")
        print("  3.  Minutes played")
        print("  4.  Form and performance metrics")
        print("  5.  700+ Premier League players")
        print("\n File saved: data/raw/fpl_players_2025_26.csv")
       

if __name__ == "__main__":
    main()