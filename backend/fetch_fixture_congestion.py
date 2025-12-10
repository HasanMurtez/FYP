import requests
import pandas as pd
from datetime import datetime, timedelta

def fetch_fixture_congestion():
    """
    Fetch upcoming fixtures and calculate congestion metrics per team
    """
    print(" FETCHING FIXTURE CONGESTION DATA")
    
    # Fetch fixtures
    url = "https://fantasy.premierleague.com/api/fixtures/"
    print(f"\n Fetching fixtures from API...")
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        fixtures = response.json()
        print(f" Fetched {len(fixtures)} fixtures")
    except Exception as e:
        print(f"Error fetching fixtures: {e}")
        return None
    
    # Get team data for team names
    base_url = "https://fantasy.premierleague.com/api/bootstrap-static/"
    base_response = requests.get(base_url)
    base_data = base_response.json()
    teams = base_data['teams']
    
    team_lookup = {team['id']: team['name'] for team in teams}
    
    # Separate finished and upcoming fixtures
    finished_fixtures = [f for f in fixtures if f['finished']]
    upcoming_fixtures = [f for f in fixtures if not f['finished']]
    
    print(f"\n Fixture Summary:")
    print(f"   Finished: {len(finished_fixtures)}")
    print(f"   Upcoming: {len(upcoming_fixtures)}")
    
    # Calculate congestion for each team
    team_congestion = {}
    
    for team_id, team_name in team_lookup.items():
        # Count upcoming fixtures for this team
        home_fixtures = [f for f in upcoming_fixtures if f['team_h'] == team_id]
        away_fixtures = [f for f in upcoming_fixtures if f['team_a'] == team_id]
        total_upcoming = len(home_fixtures) + len(away_fixtures)
        
        # Get next 5 fixtures
        next_5_fixtures = (home_fixtures + away_fixtures)[:5]
        
        # Calculate days until fixtures (if kickoff_time is available)
        fixture_dates = []
        for fixture in next_5_fixtures:
            if fixture.get('kickoff_time'):
                try:
                    kickoff = datetime.fromisoformat(fixture['kickoff_time'].replace('Z', '+00:00'))
                    fixture_dates.append(kickoff)
                except:
                    pass
        
        # Calculate average days between fixtures (congestion indicator)
        avg_days_between_fixtures = None
        if len(fixture_dates) >= 2:
            fixture_dates.sort()
            day_gaps = [(fixture_dates[i+1] - fixture_dates[i]).days 
                       for i in range(len(fixture_dates)-1)]
            avg_days_between_fixtures = sum(day_gaps) / len(day_gaps) if day_gaps else None
        
        # Categorize congestion level
        if avg_days_between_fixtures:
            if avg_days_between_fixtures < 4:
                congestion_level = 'High'
            elif avg_days_between_fixtures < 6:
                congestion_level = 'Medium'
            else:
                congestion_level = 'Low'
        else:
            congestion_level = 'Unknown'
        
        team_congestion[team_id] = {
            'team_id': team_id,
            'team_name': team_name,
            'total_upcoming_fixtures': total_upcoming,
            'next_5_fixtures_count': len(next_5_fixtures),
            'avg_days_between_fixtures': round(avg_days_between_fixtures, 1) if avg_days_between_fixtures else None,
            'congestion_level': congestion_level,
            'home_fixtures_upcoming': len(home_fixtures),
            'away_fixtures_upcoming': len(away_fixtures)
        }
    
    # Create DataFrame
    df_congestion = pd.DataFrame(list(team_congestion.values()))
    df_congestion = df_congestion.sort_values('avg_days_between_fixtures', na_position='last')
    
    # Save
    output_path = 'data/raw/fixture_congestion.csv'
    df_congestion.to_csv(output_path, index=False)
    
    print(f"\n Saved to: {output_path}")
    print(f"   Shape: {df_congestion.shape[0]} teams × {df_congestion.shape[1]} columns")
    
    # Display teams with highest congestion
    print("\n📋 Teams with Highest Fixture Congestion:")
    high_congestion = df_congestion[df_congestion['congestion_level'] == 'High']
    if not high_congestion.empty:
        print(high_congestion[['team_name', 'avg_days_between_fixtures', 
                               'total_upcoming_fixtures', 'congestion_level']].to_string(index=False))
    else:
        print("   No teams currently in high congestion period")
    
    # Summary by congestion level
    print("\n Congestion Level Distribution:")
    print(df_congestion['congestion_level'].value_counts().to_string())
    
    return df_congestion

def main():
    df = fetch_fixture_congestion()
    
    if df is not None:
        print("FIXTURE CONGESTION DATA COLLECTION COMPLETE")
if __name__ == "__main__":
    main()