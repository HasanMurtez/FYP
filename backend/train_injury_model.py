import pandas as pd  # For working with data tables
import numpy as np  # For math operations
from sklearn.model_selection import train_test_split  # Split data into train/test
from sklearn.ensemble import RandomForestClassifier  # The ML algorithm
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix  # Check performance
from sklearn.preprocessing import StandardScaler  # Scale features to same range
from imblearn.over_sampling import SMOTE  # Balance classes
import matplotlib.pyplot as plt  # Create graphs
import joblib  # Save/load models
import os
import warnings
warnings.filterwarnings("ignore")  # Hide warning messages

def create_risk_labels(df):
    """
    Create injury risk labels based on multiple factors
    
    high risk (2): Injured, frequently unavailable, or very low chance of playing
    medium risk (1): Doubtful, overworked, some unavailability
    low risk(0): Available and not overworked
    """
    print("\n Creating risk labels...")
    
    def classify_risk(row):
        # high risk 
        if row['status'] == 'i':  # Actually injured
            return 2
        if row['times_unavailable_last_10'] >= 7:  
            return 2
        if row['chance_of_playing_next'] < 25:  
            return 2
        
        # medium risk
        if row['status'] in ['d', 'u']:
            return 1
        if 3 <= row['times_unavailable_last_10'] <= 6:  # range
            return 1
        if row['is_overworked'] == 1 and row['injury_prone'] == 1:  # Both must be true
            return 1
        
        # low risk
        return 0
    
    # Apply the classification to each player in the dataset
    # This creates a new column called 'risk_label
    df['risk_label'] = df.apply(classify_risk, axis=1)
    
        # Show the breakdown
    print(f"   Risk distribution:")
    print(df['risk_label'].value_counts().sort_index())
    print(f"\n   Percentages:")
    for risk, count in df['risk_label'].value_counts(normalize=True).sort_index().items():
        risk_name = ['Low', 'Medium', 'High'][risk]
        print(f"      {risk_name} Risk: {count*100:.1f}%")
    
    return df

def train_model():
    """Train injury risk prediction model"""
    print("INJURY RISK PREDICTION MODEL TRAINING")
    
    
    #Load Data
    
    
    print("\n Loading dataset...")
    try:
        df = pd.read_csv('data/processed/fpl_complete_dataset.csv')
        print(f" Loaded {df.shape[0]} players with {df.shape[1]} features")
    except FileNotFoundError:
        print(" Error: fpl_complete_dataset.csv not found!")
        print("   Run collect_fpl_data.py first")
        return None
    
    # Call the function from above to label each player
    df = create_risk_labels(df)
    
    # Select Features
    
    print("\n Selecting features.")

     # the columns used to predict
    features = [
        # Workload 
        'minutes',
        'starts',
        'recent_minutes_last_5',
        'total_minutes_last_10',
        'workload_intensity',
        'is_overworked',
        
        # Injury history
        'times_unavailable_last_10',
        'max_consecutive_unavailable',
        'availability_score',
        'injury_prone',
        
        # Physical demand
        'yellow_cards',
        'red_cards',
        'physical_demand',
        'position_risk',
        
        # Fixture congestion
        'total_upcoming_fixtures',
        'congestion_numeric',
        
        
        # Physical involvement
        'influence',
        'creativity',
        'threat',
        'ict_index'
    ]
    
    print(f"   Selected {len(features)} features")
    
  # X = inputs (the features/stats)
    X = df[features].copy()
    y = df['risk_label'].copy()# y = OUTPUT )
    X = X.fillna(0)
    
    #Train-Test Split
    
    print("\n  Splitting data (75% train, 25% test)...")
    
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.25 # Use 25% for testing
        , random_state=42, 
        stratify=y  # Keep same % of Low/Med/High in both sets
    )
    
    print(f"   Training: {len(X_train)} samples")
    print(f"   Testing: {len(X_test)} samples")
    
    # Handle class imbalance with SMOTE
    
    print("\n  Balancing classes with SMOTE...")
    
    smote = SMOTE(random_state=42 # Reproducible results
    , k_neighbors=3) 

    # 1. Finding a High Risk player (e.g., injured player)
    # 2. Finding 3 similar High Risk players
    # 3. Creating a "blend" of their statistics

    X_train_balanced, y_train_balanced = smote.fit_resample(X_train, y_train)
    
    print(f"   After SMOTE: {len(X_train_balanced)} samples")
    
    # SCALE FEATURES
    
    print("\n Scaling features...")
    
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train_balanced)
    X_test_scaled = scaler.transform(X_test)
    
    # TRAIN MODEL
    
    print("\n Training Random Forest Classifier...")
    
    model = RandomForestClassifier(
        n_estimators=200,# Create 200 decision trees.
        max_depth=12,  # Each tree can ask up to 12 questions.
        min_samples_split=10,  # Need 10 examples to make a split.
        min_samples_leaf=5, # Need 5 examples for final decision.
        class_weight='balanced',  # Treat all risk levels equally.
        random_state=42, # Reproducible results.
        n_jobs=-1
    )
    
    model.fit(X_train_scaled, y_train_balanced)
    print("Model trained")
    
    #EVALUATE

    
    print("MODEL EVALUATION")
    
    # test on the 150 players hidden during training
    y_pred = model.predict(X_test_scaled)
    
    accuracy = accuracy_score(y_test, y_pred)
    print(f"\nOverall Accuracy: {accuracy:.2%}")
    
    print("\n Classification Report:")
    print(classification_report(
        y_test,# y_test = what the risk actually is
     y_pred, # y_pred what the model thinks the risk is

        target_names=['Low Risk', 'Medium Risk', 'High Risk'],
        digits=3
    ))
    
    print(" Confusion Matrix:")
    cm = confusion_matrix(y_test, y_pred)
    print(cm)
    print("   Rows = Actual, Columns = Predicted")
    
    #FEATURE IMPORTANCE
    
    print("TOP 10 MOST IMPORTANT FEATURES")
    
    importances = pd.DataFrame({
        'feature': features,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    for idx, row in importances.head(10).iterrows():
        bar = '█' * int(row['importance'] * 100)
        print(f"   {row['feature']:<30} {row['importance']:.4f} {bar}")
    
    # Save plot
    os.makedirs('data/processed', exist_ok=True)
    plt.figure(figsize=(10, 6))
    top_features = importances.head(15)
    plt.barh(range(len(top_features)), top_features['importance'])
    plt.yticks(range(len(top_features)), top_features['feature'])
    plt.xlabel('Importance')
    plt.title('Top 15 Features for Injury Risk Prediction')
    plt.tight_layout()
    plt.savefig('data/processed/feature_importance.png', dpi=300)
    print("\n Plot saved: data/processed/feature_importance.png")
    plt.close()
    
    #SAVE MODEL
    
    print("\n Saving model...")
    os.makedirs('models', exist_ok=True)
    
    joblib.dump(model, 'models/injury_risk_model.pkl')
    joblib.dump(scaler, 'models/injury_scaler.pkl')
    
    with open('models/feature_list.txt', 'w') as f:
        f.write('\n'.join(features))
    
    print("    models/injury_risk_model.pkl")
    print("    models/injury_scaler.pkl")
    print("    models/feature_list.txt")
    
    # 11. SAMPLE PREDICTIONS
    
    print("SAMPLE PREDICTIONS")
    
    # Get diverse samples
    samples = []
    for risk in [0, 1, 2]:
        risk_players = df[df['risk_label'] == risk]
        if len(risk_players) > 0:
            samples.append(risk_players.sample(min(3, len(risk_players)), random_state=42))
    
    sample_df = pd.concat(samples)
    sample_X = sample_df[features].fillna(0)
    sample_X_scaled = scaler.transform(sample_X)
    sample_pred = model.predict(sample_X_scaled)
    sample_proba = model.predict_proba(sample_X_scaled)
    
    risk_names = ['Low Risk', 'Medium Risk', 'High Risk']
    
    for idx, (_, player) in enumerate(sample_df.iterrows()):
        actual = risk_names[player['risk_label']]
        predicted = risk_names[sample_pred[idx]]
        confidence = sample_proba[idx][sample_pred[idx]] * 100
        match = "Y" if actual == predicted else "X"
        
        print(f"{match} {player['full_name']} ({player['team_name']})")
        print(f"   Position: {player['position']} | Status: {player['status']}")
        print(f"   Actual: {actual} → Predicted: {predicted} ({confidence:.1f}%)")
        print(f"   Recent mins: {player['recent_minutes_last_5']} | Unavailable: {player['times_unavailable_last_10']}/10\n")
    
    return model, scaler, features

def main():
    result = train_model()
    
    if result is not None:
        print(" TRAINING COMPLETE")

if __name__ == "__main__":
    main()