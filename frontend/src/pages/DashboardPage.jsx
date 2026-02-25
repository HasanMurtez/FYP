import React, { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { getTeam, getTeamPlayers } from '../services/api';
import './DashboardPage.css';

function DashboardPage() {
  const { teamId } = useParams();
  const navigate = useNavigate();
  const [team, setTeam] = useState(null);
  const [players, setPlayers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState('all');

  useEffect(() => {
    loadTeamData();
  }, [teamId]);

  const loadTeamData = async () => {
    try {
      setLoading(true);
      const [teamRes, playersRes] = await Promise.all([
        getTeam(teamId),
        getTeamPlayers(teamId)
      ]);
      setTeam(teamRes.data.data);
      setPlayers(playersRes.data.data.players);
    } catch (err) {
      console.error('Error loading team data:', err);
    } finally {
      setLoading(false);
    }
  };

  const filteredPlayers = players.filter(player => {
    if (filter === 'all') return true;
    if (filter === 'high-risk') return player.injury_risk_level === 'High';
    if (filter === 'injured') return player.status === 'i';
    if (filter === 'doubtful') return player.status === 'd';
    return true;
  });

  const getRiskColor = (risk) => {
    if (risk === 'High') return '#ef4444';
    if (risk === 'Medium') return '#f59e0b';
    return '#22c55e';
  };

  if (loading) {
    return (
      <div className="dashboard-container">
        <div className="spinner" />
        <p className="loading-text">Loading team data...</p>
      </div>
    );
  }

  return (
    <div className="dashboard-container">
      <div className="dashboard-header">
        <button onClick={() => navigate('/')} className="back-button">
          ← Back to Teams
        </button>
        <div>
          <h1 className="dashboard-title">{team?.name}</h1>
          <p className="dashboard-subtitle">{players.length} Players in Squad</p>
        </div>
      </div>

      <div className="filter-bar">
        {['all', 'high-risk', 'injured', 'doubtful'].map(f => (
          <button
            key={f}
            onClick={() => setFilter(f)}
            className={`filter-button ${filter === f ? 'filter-button-active' : ''}`}
          >
            {f === 'all' ? 'All Players' :
             f === 'high-risk' ? 'High Risk' :
             f === 'injured' ? 'Injured' : 'Doubtful'}
          </button>
        ))}
      </div>

      <div className="players-grid">
        {filteredPlayers.map(player => (
          <div key={player.id} className="player-card">
            <div
              className="risk-badge"
              style={{ backgroundColor: getRiskColor(player.injury_risk_level) }}
            >
              {player.injury_risk_level} Risk
            </div>

            <h3 className="player-name">{player.web_name}</h3>
            <p className="player-position">{player.position}</p>

            <div className="player-stats">
              <div className="player-stat">
                <span className="stat-label">Minutes</span>
                <span className="stat-value">{player.minutes}</span>
              </div>
              <div className="player-stat">
                <span className="stat-label">Goals</span>
                <span className="stat-value">{player.goals_scored}</span>
              </div>
              <div className="player-stat">
                <span className="stat-label">Assists</span>
                <span className="stat-value">{player.assists}</span>
              </div>
            </div>

            <div className="status-row">
              <span className="status-label">Status:</span>
              <span
                className="status-badge"
                style={{
                  backgroundColor: player.status === 'a' ? '#22c55e' :
                                   player.status === 'i' ? '#ef4444' : '#f59e0b'
                }}
              >
                {player.status_description}
              </span>
            </div>

            {player.news && (
              <p className="player-news">{player.news}</p>
            )}
          </div>
        ))}
      </div>

      {filteredPlayers.length === 0 && (
        <p className="no-results">No players match this filter</p>
      )}
    </div>
  );
}

export default DashboardPage;
