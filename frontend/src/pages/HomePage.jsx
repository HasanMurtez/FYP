import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { getTeams, getSyncStatus } from '../services/api';
import './HomePage.css';

function HomePage() {
  const [teams, setTeams] = useState([]);
  const [loading, setLoading] = useState(true);
  const [dbStatus, setDbStatus] = useState(null);
  const [search, setSearch] = useState('');
  const navigate = useNavigate();

  useEffect(() => {
    loadData();
  }, []);

  const loadData = async () => {
    try {
      setLoading(true);
      const [teamsRes, statusRes] = await Promise.all([
        getTeams(),
        getSyncStatus()
      ]);
      setTeams(teamsRes.data.data.teams);
      setDbStatus(statusRes.data.data);
    } catch (err) {
      console.error('Error loading data:', err);
    } finally {
      setLoading(false);
    }
  };

  const filteredTeams = teams.filter(team =>
    team.name.toLowerCase().includes(search.toLowerCase())
  );

  const handleTeamSelect = (team) => {
    navigate(`/dashboard/${team.id}`);
  };

  return (
    <div className="home-container">
      <div className="home-header">
        <h1 className="home-title">⚽ Football Club Manager</h1>
        <p className="home-subtitle">AI-Powered Injury Risk & Player Scouting</p>

        {dbStatus && (
          <div className="status-bar">
            <span className="status-item">🏟️ {dbStatus.teams_in_database} Teams</span>
            <span className="status-item">👤 {dbStatus.players_in_database} Players</span>
            <span className="status-item">🚑 {dbStatus.injured_players} Injured</span>
            <span className="status-item">⚠️ {dbStatus.doubtful_players} Doubtful</span>
          </div>
        )}
      </div>

      <div className="search-container">
        <input
          type="text"
          placeholder="🔍 Search for a team..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="search-input"
        />
      </div>

      {loading ? (
        <div className="loading-container">
          <div className="spinner" />
          <p className="loading-text">Loading teams...</p>
        </div>
      ) : (
        <div className="teams-grid">
          {filteredTeams.map(team => (
            <div
              key={team.id}
              className="team-card"
              onClick={() => handleTeamSelect(team)}
            >
              <div className="team-initial">{team.short_name}</div>
              <h3 className="team-name">{team.name}</h3>
              <p className="team-players">{team.player_count} Players</p>
              <div
                className="congestion-badge"
                style={{
                  backgroundColor: team.congestion_level === 'High' ? '#ef4444' :
                    team.congestion_level === 'Medium' ? '#f59e0b' : '#22c55e'
                }}
              >
                {team.congestion_level} Congestion
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

export default HomePage;
