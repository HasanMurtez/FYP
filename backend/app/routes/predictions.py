
from flask import Blueprint, jsonify
import joblib
import numpy as np
from datetime import datetime
import os
from app import db
from app.models import Player

# Create blueprint
predictions_bp = Blueprint('predictions', __name__)

# Global variables to cache the model
MODEL = None
SCALER = None
MODEL_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'models', 'injury_risk_model.pkl')
SCALER_PATH = os.path.join(os.path.dirname(__file__), '..', '..', 'models', 'injury_scaler.pkl')


def load_models():
    """Load the trained ML models"""
    global MODEL, SCALER
    
    if MODEL is None or SCALER is None:
        print("Loading ML models...")
        try:
            MODEL = joblib.load(MODEL_PATH)
            SCALER = joblib.load(SCALER_PATH)
            print("Models loaded successfully")
        except FileNotFoundError as e:
            print(f"Model files not found: {e}")
            raise Exception(f"Model files not found. Make sure injury_risk_model.pkl and injury_scaler.pkl exist in /models/")
    
    return MODEL, SCALER


@predictions_bp.route('/api/predict', methods=['POST'])
def predict_injuries():
    try:
        print("\n=== Starting Injury Risk Predictions ===")
        
        # Load models
        model, scaler = load_models()
        
        # Get all players from database
        players = Player.query.all()
        
        if not players:
            return jsonify({
                'success': False,
                'error': 'No players in database. Run /api/sync first.'
            }), 400
        
        print(f"Predicting for {len(players)} players...")
        
        # Risk labels
        risk_labels = ['Low', 'Medium', 'High']
        
        predictions_made = 0
        predictions_failed = 0
        risk_counts = {'Low': 0, 'Medium': 0, 'High': 0}
        
        for idx, player in enumerate(players):
            
            if (idx + 1) % 100 == 0:
                print(f"   Processed {idx + 1}/{len(players)} players...")
            
            try:
                # Extract features for this player
                features = player.get_features_for_prediction()
                
                # Validate features
                if None in features or any(f is None for f in features):
                    print(f"   ⚠ Skipping {player.web_name}: Missing feature data")
                    predictions_failed += 1
                    continue
                
                # Convert to numpy array and reshape for model
                features_array = np.array(features).reshape(1, -1)
                
                # Scale features
                features_scaled = scaler.transform(features_array)
                
                # Make prediction
                prediction = model.predict(features_scaled)[0]  # 0, 1, or 2
                probabilities = model.predict_proba(features_scaled)[0]  # [prob_low, prob_med, prob_high]
                
                # Get risk level and confidence
                risk_level = risk_labels[prediction]
                confidence = probabilities[prediction] * 100  # Convert to percentage
                
                # Update player in database
                player.injury_risk_level = risk_level
                player.injury_risk_score = round(confidence, 2)
                player.predicted_at = datetime.utcnow()
                
                # Count predictions
                risk_counts[risk_level] += 1
                predictions_made += 1
                
            except Exception as e:
                print(f"Error predicting for {player.web_name}: {str(e)}")
                predictions_failed += 1
                continue
        
        # Commit all predictions to database
        db.session.commit()
        
        print(f"\nPredictions complete!")
        print(f"   Low Risk: {risk_counts['Low']}")
        print(f"   Medium Risk: {risk_counts['Medium']}")
        print(f"   High Risk: {risk_counts['High']}")
        print(f"   Failed: {predictions_failed}\n")
        
        return jsonify({
            'success': True,
            'message': 'Injury risk predictions completed',
            'data': {
                'total_players': len(players),
                'predictions_made': predictions_made,
                'predictions_failed': predictions_failed,
                'risk_distribution': risk_counts,
                'timestamp': datetime.utcnow().isoformat()
            }
        }), 200
        
    except Exception as e:
        db.session.rollback()
        print(f"\nPrediction failed: {str(e)}\n")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@predictions_bp.route('/api/predict/status', methods=['GET'])
def prediction_status():
    """
    Check how many players have predictions and the distribution
    """
    try:
        total_players = Player.query.count()
        predicted_players = Player.query.filter(Player.injury_risk_level.isnot(None)).count()
        
        low_risk = Player.query.filter_by(injury_risk_level='Low').count()
        medium_risk = Player.query.filter_by(injury_risk_level='Medium').count()
        high_risk = Player.query.filter_by(injury_risk_level='High').count()
        
        # Get most recent prediction timestamp
        latest_player = Player.query.filter(Player.predicted_at.isnot(None)).order_by(Player.predicted_at.desc()).first()
        last_predicted = latest_player.predicted_at.isoformat() if latest_player else None
        
        return jsonify({
            'success': True,
            'data': {
                'total_players': total_players,
                'players_with_predictions': predicted_players,
                'players_without_predictions': total_players - predicted_players,
                'risk_distribution': {
                    'low': low_risk,
                    'medium': medium_risk,
                    'high': high_risk
                },
                'last_prediction_time': last_predicted,
                'predictions_up_to_date': predicted_players == total_players
            }
        }), 200
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@predictions_bp.route('/api/predict/high-risk', methods=['GET'])
def get_high_risk_players():
    try:
        high_risk_players = Player.query.filter_by(injury_risk_level='High').all()
        
        players_data = [player.to_dict(include_team=True) for player in high_risk_players]
        
        return jsonify({
            'success': True,
            'data': {
                'count': len(high_risk_players),
                'players': players_data
            }
        }), 200
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500
