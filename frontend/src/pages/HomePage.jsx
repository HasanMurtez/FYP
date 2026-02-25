
import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { getTeams, getSyncStatus } from '../services/api';

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
    <div style={styles.container}>
      {/* Header */}
      <div style={styles.header}>
        <h1 style={styles.title}>⚽ Football Club Manager</h1>
        <p style={styles.subtitle}>AI-Powered Injury Risk & Player Scouting</p>

        {/* DB Status */}
        {dbStatus && (
          <div style={styles.statusBar}>
            <span style={styles.statusItem}>🏟️ {dbStatus.teams_in_database} Teams</span>
            <span style={styles.statusItem}>👤 {dbStatus.players_in_database} Players</span>
            <span style={styles.statusItem}>🚑 {dbStatus.injured_players} Injured</span>
            <span style={styles.statusItem}>⚠️ {dbStatus.doubtful_players} Doubtful</span>
          </div>
        )}
      </div>

      {/* Search */}
      <div style={styles.searchContainer}>
        <input
          type="text"
          placeholder="🔍 Search for a team..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          style={styles.searchInput}
        />
      </div>

      {/* Teams Grid */}
      {loading ? (
        <div style={styles.loadingContainer}>
          <div className="spinner" />
          <p style={styles.loadingText}>Loading teams...</p>
        </div>
      ) : (
        <div style={styles.teamsGrid}>
          {filteredTeams.map(team => (
            <div
              key={team.id}
              style={styles.teamCard}
              onClick={() => handleTeamSelect(team)}
              onMouseEnter={e => e.currentTarget.style.transform = 'translateY(-4px)'}
              onMouseLeave={e => e.currentTarget.style.transform = 'translateY(0)'}
            >
              <div style={styles.teamInitial}>{team.short_name}</div>
              <h3 style={styles.teamName}>{team.name}</h3>
              <p style={styles.teamPlayers}>{team.player_count} Players</p>
              <div style={{
                ...styles.congestionBadge,
                backgroundColor: team.congestion_level === 'High' ? '#ef4444' :
                  team.congestion_level === 'Medium' ? '#f59e0b' : '#22c55e'
              }}>
                {team.congestion_level} Congestion
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

const styles = {
  container: {
    minHeight: '100vh',
    background: 'linear-gradient(135deg, #0f1923 0%, #1a2634 100%)',
    padding: '20px',
  },
  header: {
    textAlign: 'center',
    paddingBottom: '30px',
    borderBottom: '1px solid #1e3a4a',
    marginBottom: '30px',
  },
  title: {
    fontSize: '2.5rem',
    fontWeight: '800',
    background: 'linear-gradient(90deg, #38bdf8, #818cf8)',
    WebkitBackgroundClip: 'text',
    WebkitTextFillColor: 'transparent',
    marginBottom: '8px',
  },
  subtitle: {
    color: '#94a3b8',
    fontSize: '1rem',
    marginBottom: '20px',
  },
  statusBar: {
    display: 'flex',
    justifyContent: 'center',
    gap: '20px',
    flexWrap: 'wrap',
  },
  statusItem: {
    background: '#1e3a4a',
    padding: '6px 14px',
    borderRadius: '20px',
    fontSize: '0.85rem',
    color: '#38bdf8',
  },
  searchContainer: {
    maxWidth: '400px',
    margin: '0 auto 30px',
  },
  searchInput: {
    width: '100%',
    padding: '12px 16px',
    borderRadius: '10px',
    border: '1px solid #1e3a4a',
    background: '#1a2634',
    color: '#ffffff',
    fontSize: '1rem',
    outline: 'none',
  },
  teamsGrid: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fill, minmax(180px, 1fr))',
    gap: '16px',
    maxWidth: '1200px',
    margin: '0 auto',
  },
  teamCard: {
    background: '#1a2634',
    border: '1px solid #1e3a4a',
    borderRadius: '12px',
    padding: '20px',
    textAlign: 'center',
    cursor: 'pointer',
    transition: 'all 0.2s ease',
  },
  teamInitial: {
    width: '60px',
    height: '60px',
    borderRadius: '50%',
    background: 'linear-gradient(135deg, #38bdf8, #818cf8)',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    margin: '0 auto 12px',
    fontSize: '1rem',
    fontWeight: '800',
    color: '#0f1923',
  },
  teamName: {
    fontSize: '0.95rem',
    fontWeight: '600',
    color: '#ffffff',
    marginBottom: '6px',
  },
  teamPlayers: {
    fontSize: '0.8rem',
    color: '#94a3b8',
    marginBottom: '10px',
  },
  congestionBadge: {
    display: 'inline-block',
    padding: '3px 10px',
    borderRadius: '20px',
    fontSize: '0.7rem',
    fontWeight: '600',
    color: '#ffffff',
  },
  loadingContainer: {
    textAlign: 'center',
    paddingTop: '60px',
  },
  loadingText: {
    color: '#94a3b8',
    marginTop: '10px',
  }
};

export default HomePage;
