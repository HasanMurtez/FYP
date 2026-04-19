import React, { useState, useEffect } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { getTeam, getTeamPlayers } from '../services/api';
import ScoutingModal from '../components/ScoutingModal';
import PlayerStatsModal from '../components/PlayerStatsModal';
import './DashboardPage.css';

// 4-3-3 formation positions
const FORMATION_433 = {
  GK: [{ top: 88, left: 50 }],
  DEF: [
    { top: 72, left: 12 },
    { top: 72, left: 36 },
    { top: 72, left: 64 },
    { top: 72, left: 88 },
  ],
  MID: [
    { top: 48, left: 25 },
    { top: 44, left: 50 },
    { top: 48, left: 75 },
  ],
  FWD: [
    { top: 20, left: 20 },
    { top: 14, left: 50 },
    { top: 20, left: 80 },
  ],
};

// Helper to get player initials for fallback
const getInitials = (name) => {
  if (!name) return '?';
  const parts = name.split(/[\s.]+/);
  if (parts.length === 1) return parts[0].charAt(0).toUpperCase();
  return (parts[0].charAt(0) + parts[parts.length - 1].charAt(0)).toUpperCase();
};

// Player photo URL — use 250x250 for better quality
const getPlayerPhotoUrl = (code) => {
  if (!code) return null;
  return `https://resources.premierleague.com/premierleague/photos/players/250x250/p${code}.png`;
};

// Photo component with initials fallback
function PlayerPhoto({ code, name, size = 54, className = '' }) {
  const [imgFailed, setImgFailed] = useState(false);
  const photoUrl = getPlayerPhotoUrl(code);

  if (!photoUrl || imgFailed) {
    return (
      <div
        className={`photo-fallback ${className}`}
        style={{ width: size, height: size, fontSize: size * 0.35 }}
      >
        {getInitials(name)}
      </div>
    );
  }

  return (
    <img
      src={photoUrl}
      alt={name}
      className={`player-photo-img ${className}`}
      style={{ width: size, height: size }}
      onError={() => setImgFailed(true)}
    />
  );
}

function DashboardPage() {
  const { teamId } = useParams();
  const navigate = useNavigate();
  const [team, setTeam] = useState(null);
  const [players, setPlayers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [selectedPlayer, setSelectedPlayer] = useState(null);
  const [scoutingPlayer, setScoutingPlayer] = useState(null);

  useEffect(() => {
    loadTeamData();
  // eslint-disable-next-line react-hooks/exhaustive-deps
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

  // Pick the best starting XI:
  const getStartingXI = () => {
    const byPosition = { GK: [], DEF: [], MID: [], FWD: [] };
    players.forEach(p => {
      if (byPosition[p.position]) {
        byPosition[p.position].push(p);
      }
    });

    // Sort: available first, then by recent minutes, then total minutes
    Object.keys(byPosition).forEach(pos => {
      byPosition[pos].sort((a, b) => {
        // Available players first
        const aAvail = a.status === 'a' ? 1 : 0;
        const bAvail = b.status === 'a' ? 1 : 0;
        if (bAvail !== aAvail) return bAvail - aAvail;

        // Then by recent minutes (last 5 games)
        const aRecent = a.recent_minutes_last_5 || 0;
        const bRecent = b.recent_minutes_last_5 || 0;
        if (bRecent !== aRecent) return bRecent - aRecent;

        // Then by total minutes
        return (b.minutes || 0) - (a.minutes || 0);
      });
    });

    const starters = [];
    const slots = { GK: 1, DEF: 4, MID: 3, FWD: 3 };

    Object.keys(slots).forEach(pos => {
      const count = slots[pos];
      const available = byPosition[pos];
      for (let i = 0; i < count && i < available.length; i++) {
        starters.push({ ...available[i], formationPos: pos, slotIndex: i });
      }
    });

    return starters;
  };

  const getBenchPlayers = () => {
    const starterIds = new Set(getStartingXI().map(p => p.id));
    return players.filter(p => !starterIds.has(p.id));
  };

  const getRiskColor = (risk) => {
    if (risk === 'High') return '#ef4444';
    if (risk === 'Medium') return '#f59e0b';
    return '#22c55e';
  };

  const getRiskGlow = (risk) => {
    if (risk === 'High') return 'rgba(239, 68, 68, 0.5)';
    if (risk === 'Medium') return 'rgba(245, 158, 11, 0.35)';
    return 'rgba(34, 197, 94, 0.3)';
  };

  const getCardBorder = (risk) => {
    if (risk === 'High') return '#ef4444';
    if (risk === 'Medium') return '#f59e0b';
    return '#22c55e';
  };

  if (loading) {
    return (
      <div className="dashboard-container">
        <div className="loading-screen">
          <div className="spinner" />
          <p className="loading-text">Loading squad...</p>
        </div>
      </div>
    );
  }

  const startingXI = getStartingXI();
  const bench = getBenchPlayers();

  return (
    <div className="dashboard-container">
      {/* Header */}
      <div className="dashboard-header">
        <button onClick={() => navigate('/')} className="back-button">
          ← Back
        </button>
        <div className="header-info">
          <h1 className="team-name-header">{team?.name}</h1>
          <div className="header-badges">
            <span className="header-badge">{players.length} Players</span>
            <span className="header-badge badge-formation">4-3-3</span>
            <span
              className="header-badge badge-congestion"
              data-level={team?.congestion_level}
            >
              {team?.congestion_level} Congestion
            </span>
          </div>
        </div>
      </div>

      {/* Pitch */}
      <div className="pitch-wrapper">
        <div className="pitch">
          {/* Pitch markings */}
          <div className="pitch-marking center-circle" />
          <div className="pitch-marking center-line" />
          <div className="pitch-marking center-dot" />
          <div className="pitch-marking penalty-box-top" />
          <div className="pitch-marking penalty-box-bottom" />
          <div className="pitch-marking penalty-arc-top" />
          <div className="pitch-marking penalty-arc-bottom" />
          <div className="pitch-marking goal-box-top" />
          <div className="pitch-marking goal-box-bottom" />

          {/* Players in formation */}
          {startingXI.map((player) => {
            const positions = FORMATION_433[player.formationPos];
            const pos = positions[player.slotIndex];
            if (!pos) return null;

            return (
              <div
                key={player.id}
                className="pitch-player"
                style={{ top: `${pos.top}%`, left: `${pos.left}%` }}
                onClick={() => setSelectedPlayer(player)}
              >
                <div
                  className="player-card-fifa"
                  style={{
                    borderColor: getCardBorder(player.injury_risk_level),
                    boxShadow: `0 4px 24px ${getRiskGlow(player.injury_risk_level)}`,
                  }}
                >
                  <div className="card-position-badge">{player.position}</div>
                  <div className="card-photo-wrapper">
                    <PlayerPhoto code={player.code} name={player.web_name} size={58} className="card-photo" />
                  </div>
                  <div className="card-name">{player.web_name}</div>
                  <div className="card-divider" />
                  <div className="card-stats-mini">
                    <div className="mini-stat">
                      <span className="mini-val">{player.goals_scored}</span>
                      <span className="mini-label">GOL</span>
                    </div>
                    <div className="mini-stat">
                      <span className="mini-val">{player.assists}</span>
                      <span className="mini-label">AST</span>
                    </div>
                    <div className="mini-stat">
                      <span className="mini-val">{Math.round(player.ict_index)}</span>
                      <span className="mini-label">ICT</span>
                    </div>
                  </div>
                  <div
                    className="card-risk-strip"
                    style={{ backgroundColor: getRiskColor(player.injury_risk_level) }}
                  />
                </div>
                {player.status !== 'a' && (
                  <div className="player-status-icon">
                    {player.status === 'i' ? '🚑' : '⚠️'}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </div>

      {/* Bench Section */}
      <div className="bench-section">
        <div className="bench-header">
          <h2 className="bench-title">BENCH & RESERVES</h2>
          <span className="bench-count">{bench.length} players</span>
        </div>
        <div className="bench-scroll">
          {bench.map(player => (
            <div
              key={player.id}
              className="bench-card"
              onClick={() => setSelectedPlayer(player)}
              style={{
                borderColor: getCardBorder(player.injury_risk_level),
                boxShadow: `0 2px 12px ${getRiskGlow(player.injury_risk_level)}`,
              }}
            >
              <div className="bench-pos-badge">{player.position}</div>
              <div className="bench-photo-wrapper">
                <PlayerPhoto code={player.code} name={player.web_name} size={44} className="bench-photo" />
              </div>
              <div className="bench-name">{player.web_name}</div>
              <div className="bench-stats">
                <span>{player.minutes}'</span>
                <span>{player.goals_scored}G</span>
                <span>{player.assists}A</span>
              </div>
              <div
                className="bench-risk-strip"
                style={{ backgroundColor: getRiskColor(player.injury_risk_level) }}
              />
              {player.status !== 'a' && (
                <div className="bench-status-flag">
                  {player.status === 'i' ? '🚑' : '⚠️'}
                </div>
              )}
            </div>
          ))}
        </div>
      </div>

      {/* Player Stats Modal */}
      {selectedPlayer && !scoutingPlayer && (
        <PlayerStatsModal
          player={selectedPlayer}
          onClose={() => setSelectedPlayer(null)}
          onFindReplacement={() => {
            setScoutingPlayer(selectedPlayer);
          }}
        />
      )}

      {/* Scouting Modal */}
      {scoutingPlayer && (
        <ScoutingModal
          player={scoutingPlayer}
          onClose={() => {
            setScoutingPlayer(null);
            setSelectedPlayer(null);
          }}
        />
      )}
    </div>
  );
}

export default DashboardPage;
