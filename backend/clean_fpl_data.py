import pandas as pd

raw_path = "data/raw/fpl_players_2025_26.csv"
clean_path = "data/processed/fpl_cleaned.csv"

df = pd.read_csv(raw_path)

# Keep essential columns only
columns = [
    'id', 'full_name', 'team', 'position',
    'minutes', 'games_played',
    'goals', 'assists',
    'influence', 'creativity', 'threat', 'ict_index',
    'status', 'news'
]

df_clean = df[columns]

# Save cleaned CSV
df_clean.to_csv(clean_path, index=False)

print("Clean CSV saved to:", clean_path)
print("Rows:", len(df_clean))
print("Columns:", df_clean.columns)
