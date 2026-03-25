import React, { useState } from 'react';
import './PlayerStatsModal.css';

// Helper to get player initials for fallback
const getInitials = (name) => {
  if (!name) return '?';
  const parts = name.split(/[\s.]+/);
  if (parts.length === 1) return parts[0].charAt(0).toUpperCase();
  return (parts[0].charAt(0) + parts[parts.length - 1].charAt(0)).toUpperCase();
};

function PlayerPhoto({ code, name }) {
  const [imgFailed, setImgFailed] = useState(false);
  const photoUrl = code
    ? `https://resources.premierleague.com/premierleague/photos/players/250x250/p${code}.png`
    : null;

  if (!photoUrl || imgFailed) {
    return (
      <div className="hero-photo-fallback">
        {getInitials(name)}
      </div>
    );
  }

  return (
    <img
      src={photoUrl}
      alt={name}
      className="hero-photo"
      onError={() => setImgFailed(true)}
    />
  );
}

function PlayerStatsModal({ player, onClose, onFindReplacement }) {

  const getRiskColor = (risk) => {
    if (risk === 'High') return '#ef4444';
    if (risk === 'Medium') return '#f59e0b';
    return '#22c55e';
  };

  const stats = [
    { label: 'MINUTES', value: player.minutes, bar: Math.min((player.minutes / 2500) * 100, 100) },
    { label: 'GOALS', value: player.goals_scored, bar: Math.min((player.goals_scored / 20) * 100, 100) },
    { label: 'ASSISTS', value: player.assists, bar: Math.min((player.assists / 15) * 100, 100) },
    { label: 'ICT INDEX', value: parseFloat(player.ict_index).toFixed(1), bar: Math.min((player.ict_index / 300) * 100, 100) },
    { label: 'INFLUENCE', value: parseFloat(player.influence).toFixed(1), bar: Math.min((player.influence / 500) * 100, 100) },
    { label: 'CREATIVITY', value: parseFloat(player.creativity).toFixed(1), bar: Math.min((player.creativity / 500) * 100, 100) },
    { label: 'THREAT', value: parseFloat(player.threat).toFixed(1), bar: Math.min((player.threat / 500) * 100, 100) },
  ];

  const injuryStats = [
    { label: 'Availability Score', value: `${player.availability_score}/10` },
    { label: 'Times Unavailable (L10)', value: player.times_unavailable_last_10 },
    { label: 'Max Consecutive Out', value: player.max_consecutive_unavailable },
    { label: 'Workload Intensity', value: player.workload_intensity?.toFixed(1) },
    { label: 'Overworked', value: player.is_overworked ? 'YES' : 'No' },
    { label: 'Injury Prone', value: player.injury_prone ? 'YES' : 'No' },
  ];

  return (
    <div className="stats-overlay" onClick={onClose}>
      <div className="stats-modal" onClick={(e) => e.stopPropagation()}>
        <button className="stats-close" onClick={onClose}>✕</button>

        {/* Top section */}
        <div className="stats-hero">
          <div className="hero-photo-wrapper">
            <PlayerPhoto code={player.code} name={player.web_name} />
          </div>
          <div className="hero-center">
            <h2 className="hero-name">{player.web_name}</h2>
            <p className="hero-fullname">{player.full_name}</p>
            <div className="hero-badges">
              <span className="hero-pos-badge">{player.position}</span>
              <span
                className="hero-risk-badge"
                style={{ backgroundColor: getRiskColor(player.injury_risk_level) }}
              >
                {player.injury_risk_level} Risk
              </span>
              <span
                className="hero-status-badge"
                style={{
                  backgroundColor: player.status === 'a' ? '#22c55e' :
                    player.status === 'i' ? '#ef4444' : '#f59e0b',
                }}
              >
                {player.status_description}
              </span>
            </div>
            {player.news && (
              <p className="hero-news">{player.news}</p>
            )}
          </div>
          <div className="hero-right">
            <div className="hero-risk-score">
              {player.injury_risk_score
                ? `${player.injury_risk_score.toFixed(0)}%`
                : '—'}
            </div>
            <div className="hero-risk-label">CONF</div>
          </div>
        </div>

        {/* Performance Stats */}
        <div className="stats-section">
          <h3 className="stats-section-title">PERFORMANCE</h3>
          <div className="stat-bars">
            {stats.map((stat, i) => (
              <div key={i} className="stat-bar-row">
                <span className="stat-bar-label">{stat.label}</span>
                <div className="stat-bar-track">
                  <div className="stat-bar-fill" style={{ width: `${stat.bar}%` }} />
                </div>
                <span className="stat-bar-value">{stat.value}</span>
              </div>
            ))}
          </div>
        </div>

        {/* Injury Stats */}
        <div className="stats-section">
          <h3 className="stats-section-title">INJURY & WORKLOAD</h3>
          <div className="injury-grid">
            {injuryStats.map((stat, i) => (
              <div key={i} className="injury-stat">
                <span className="injury-label">{stat.label}</span>
                <span
                  className="injury-value"
                  data-warning={
                    stat.label === 'Overworked' && player.is_overworked ? 'true' :
                    stat.label === 'Injury Prone' && player.injury_prone ? 'true' : 'false'
                  }
                >
                  {stat.value}
                </span>
              </div>
            ))}
          </div>
        </div>

        {/* Discipline */}
        <div className="stats-section">
          <h3 className="stats-section-title">DISCIPLINE & FORM</h3>
          <div className="quick-stats-row">
            <div className="quick-stat">
              <div className="quick-val">{player.yellow_cards}</div>
              <div className="quick-label">Yellow Cards</div>
            </div>
            <div className="quick-stat">
              <div className="quick-val">{player.red_cards}</div>
              <div className="quick-label">Red Cards</div>
            </div>
            <div className="quick-stat">
              <div className="quick-val">{player.starts}</div>
              <div className="quick-label">Starts</div>
            </div>
            <div className="quick-stat">
              <div className="quick-val">{player.recent_minutes_last_5}</div>
              <div className="quick-label">Mins (L5)</div>
            </div>
          </div>
        </div>

        {/* Actions */}
        <div className="stats-actions">
          <button className="action-btn action-scout" onClick={onFindReplacement}>
            🔍 FIND REPLACEMENT
          </button>
        </div>
      </div>
    </div>
  );
}

export default PlayerStatsModal;
