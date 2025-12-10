import pandas as pd
from datetime import datetime

def merge_all_datasets():
    """
    Combine basic player data, history data, and fixture congestion
    """
    print(" MERGING ALL DATASETS")
    
    # Load all datasets
    print("\n Loading datasets...")
    
    try:
        df_basic = pd.read_csv('data/raw/fpl_players_basic.csv')
        print(f" Basic data: {df_basic.shape}")
        
        df_history = pd.read_csv('data/raw/fpl_player_history.csv')
        print(f" History data: {df_history.shape}")
        
        df_congestion = pd.read_csv('data/raw/fixture_congestion.csv')
        print(f" Congestion data: {df_congestion.shape}")
        
    except FileNotFoundError as e:
        print(f" Error: Missing required file - {e}")
        print("   Please run the previous data collection scripts first")
        return None
    
    # Merge datasets
    print("\n Merging datasets...")
    
    # Step 1: Merge basic + history on player_id
    df_merged = df_basic.merge(
        df_history,
        left_on='id',
        right_on='player_id',
        how='left',
        suffixes=('', '_hist')
    )
    print(f"   After basic + history: {df_merged.shape}")
    
    # Step 2: Merge with fixture congestion on team_id
    df_merged = df_merged.merge(
        df_congestion,
        left_on='team_id',
        right_on='team_id',
        how='left',
        suffixes=('', '_team')
    )
    print(f"   After adding congestion: {df_merged.shape}")
    
    # Clean up duplicate columns
    cols_to_drop = [col for col in df_merged.columns if col.endswith('_hist') or col.endswith('_team')]
    cols_to_drop.extend(['player_id', 'player_name'])  # Already have 'id' and 'full_name'
    df_merged = df_merged.drop(columns=[col for col in cols_to_drop if col in df_merged.columns])
    
    print(f"   After cleanup: {df_merged.shape}")
    
    # Fill missing values
    df_merged = df_merged.fillna({
        'recent_minutes_last_5': 0,
        'recent_games_last_5': 0,
        'avg_minutes_per_game_last_5': 0,
        'times_unavailable_last_10': 0,
        'max_consecutive_unavailable': 0,
        'chance_of_playing_next': 100,
        'chance_of_playing_this': 100,
        'news': '',
        'avg_days_between_fixtures': 7,
        'congestion_level': 'Unknown'
    })
    
    # Save merged dataset
    output_path = 'data/processed/fpl_merged_dataset.csv'
    df_merged.to_csv(output_path, index=False)
    
    print(f"\n Saved merged dataset to: {output_path}")
    print(f"   Final shape: {df_merged.shape[0]} players × {df_merged.shape[1]} features")
    
    # Display column summary
    print("\n Available Features:")
    feature_categories = {
        'Identity': ['id', 'full_name', 'position', 'team_name'],
        'Availability': ['status', 'status_description', 'chance_of_playing_next', 'news'],
        'Playing Time': ['minutes', 'starts', 'recent_minutes_last_5', 'avg_minutes_per_game_last_5'],
        'Physical Stats': ['yellow_cards', 'red_cards', 'goals_scored', 'assists'],
        'Injury History': ['times_unavailable_last_10', 'max_consecutive_unavailable'],
        'Fixture Load': ['congestion_level', 'avg_days_between_fixtures', 'total_upcoming_fixtures'],
        'Workload': ['workload_trend', 'total_minutes_last_10', 'games_started_last_10']
    }
    
    for category, features in feature_categories.items():
        available = [f for f in features if f in df_merged.columns]
        print(f"\n   {category}: {len(available)} features")
        for feat in available[:5]:  # Show first 5
            print(f"      - {feat}")
    
    # Data quality summary
    print("\n Data Quality Summary:")
    print(f"   Players with complete data: {df_merged.notna().all(axis=1).sum()}")
    print(f"   Players currently injured: {len(df_merged[df_merged['status'] == 'i'])}")
    print(f"   Players doubtful: {len(df_merged[df_merged['status'] == 'd'])}")
    print(f"   Players in high congestion teams: {len(df_merged[df_merged['congestion_level'] == 'High'])}")
    
    # Sample of high-risk players
    print("\n⚠️  Sample of Potentially High-Risk Players:")
    high_risk = df_merged[
        (df_merged['times_unavailable_last_10'] >= 3) | 
        (df_merged['status'].isin(['i', 'd'])) |
        (df_merged['recent_minutes_last_5'] > 400)
    ].head(5)
    
    if not high_risk.empty:
        print(high_risk[['full_name', 'team_name', 'status_description', 
                        'recent_minutes_last_5', 'times_unavailable_last_10', 
                        'workload_trend']].to_string(index=False))
    else:
        print("   No obvious high-risk players in sample")
    
    return df_merged

def main():
    df = merge_all_datasets()
    
    if df is not None:
        print("DATA COLLECTION & MERGING COMPLETE!")

if __name__ == "__main__":
    main()