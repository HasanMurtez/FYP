
import React, { useState, useEffect } from 'react';
import { getSimilarPlayers } from '../services/api';
import './ScoutingModal.css';

function ScoutingModal({ player, onClose }) {
  const [similarPlayers, setSimilarPlayers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    loadSimilarPlayers();
  }, [player.id]);

  const loadSimilarPlayers = async () => {
    try {
      setLoading(true);
      setError(null);
      const response = await getSimilarPlayers(player.id, 5);
      setSimilarPlayers(response.data.data.similar_players);
    } catch (err) {
      console.error('Error loading similar players:', err);
      setError('Failed to find similar players');
    } finally {
      setLoading(false);
    }
  };

  const getRiskColor = (risk) => {
    if (risk === 'High') return '#ef4444';
    if (risk === 'Medium') return '#f59e0b';
    return '#22c55e';
  };

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-content" onClick={(e) => e.stopPropagation()}>
        {/* Header */}
        <div className="modal-header">
          <div>
            <h2 className="modal-title">Similar Players to {player.web_name}</h2>
            <p className="modal-subtitle">
              Based on performance stats and playing style
            </p>
          </div>
          <button className="close-button" onClick={onClose}>✕</button>
        </div>

        {/* Original Player Info */}
        <div className="original-player">
          <div className="player-info">
            <span className="player-badge">{player.position}</span>
            <span className="player-team">{player.team?.name || 'Unknown Team'}</span>
          </div>
          <div className="player-stats-row">
            <div className="stat-box">
              <span className="stat-label">Minutes</span>
              <span className="stat-value">{player.minutes}</span>
            </div>
            <div className="stat-box">
              <span className="stat-label">Goals</span>
              <span className="stat-value">{player.goals_scored}</span>
            </div>
            <div className="stat-box">
              <span className="stat-label">Assists</span>
              <span className="stat-value">{player.assists}</span>
            </div>
            <div className="stat-box">
              <span className="stat-label">Risk</span>
              <span 
                className="stat-value"
                style={{ color: getRiskColor(player.injury_risk_level) }}
              >
                {player.injury_risk_level}
              </span>
            </div>
          </div>
        </div>

        {/* Similar Players */}
        <div className="similar-players-section">
          <h3 className="section-title">🔍 Replacement Options</h3>

          {loading && (
            <div className="loading-state">
              <div className="spinner" />
              <p>Finding similar players...</p>
            </div>
          )}

          {error && (
            <div className="error-state">
              <p>{error}</p>
              <button onClick={loadSimilarPlayers} className="retry-button">
                Try Again
              </button>
            </div>
          )}

          {!loading && !error && similarPlayers.length === 0 && (
            <div className="empty-state">
              <p>No similar players found</p>
            </div>
          )}

          {!loading && !error && similarPlayers.length > 0 && (
            <div className="similar-players-list">
              {similarPlayers.map((item, index) => (
                <div key={item.player.id} className="similar-player-card">
                  {/* Rank Badge */}
                  <div className="rank-badge">#{index + 1}</div>

                  {/* Similarity Score */}
                  <div className="similarity-bar">
                    <div className="similarity-label">
                      Match: {item.similarity_score.toFixed(1)}%
                    </div>
                    <div className="similarity-progress">
                      <div 
                        className="similarity-fill"
                        style={{ width: `${item.similarity_score}%` }}
                      />
                    </div>
                  </div>

                  {/* Player Info */}
                  <div className="similar-player-info">
                    <div className="similar-player-header">
                      <h4 className="similar-player-name">{item.player.web_name}</h4>
                      <span className="similar-player-team">
                        {item.player.team?.name || 'Unknown'}
                      </span>
                    </div>

                    <div className="similar-player-badges">
                      <span className="position-badge">{item.player.position}</span>
                      <span 
                        className="risk-badge"
                        style={{ backgroundColor: getRiskColor(item.player.injury_risk_level) }}
                      >
                        {item.player.injury_risk_level} Risk
                      </span>
                      <span 
                        className="status-badge"
                        style={{
                          backgroundColor: item.player.status === 'a' ? '#22c55e' :
                                         item.player.status === 'i' ? '#ef4444' : '#f59e0b'
                        }}
                      >
                        {item.player.status_description}
                      </span>
                    </div>
                  </div>

                  {/* Stats Comparison */}
                  <div className="comparison-stats">
                    <div className="comparison-stat">
                      <span className="comparison-label">Minutes</span>
                      <span className="comparison-value">{item.player.minutes}</span>
                      <span className={`comparison-diff ${
                        item.player.minutes > player.minutes ? 'positive' : 
                        item.player.minutes < player.minutes ? 'negative' : 'neutral'
                      }`}>
                        {item.player.minutes > player.minutes ? '↑' : 
                         item.player.minutes < player.minutes ? '↓' : '→'}
                      </span>
                    </div>

                    <div className="comparison-stat">
                      <span className="comparison-label">Goals</span>
                      <span className="comparison-value">{item.player.goals_scored}</span>
                      <span className={`comparison-diff ${
                        item.player.goals_scored > player.goals_scored ? 'positive' : 
                        item.player.goals_scored < player.goals_scored ? 'negative' : 'neutral'
                      }`}>
                        {item.player.goals_scored > player.goals_scored ? '↑' : 
                         item.player.goals_scored < player.goals_scored ? '↓' : '→'}
                      </span>
                    </div>

                    <div className="comparison-stat">
                      <span className="comparison-label">Assists</span>
                      <span className="comparison-value">{item.player.assists}</span>
                      <span className={`comparison-diff ${
                        item.player.assists > player.assists ? 'positive' : 
                        item.player.assists < player.assists ? 'negative' : 'neutral'
                      }`}>
                        {item.player.assists > player.assists ? '↑' : 
                         item.player.assists < player.assists ? '↓' : '→'}
                      </span>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

export default ScoutingModal;
