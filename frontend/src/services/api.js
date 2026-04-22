import axios from 'axios';

//Render backend
const BASE_URL = process.env.REACT_APP_API_URL || 'https://fcm-backend-lvcp.onrender.com';

// LOCAL development
// const BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:5000';

const api = axios.create({
  baseURL: BASE_URL,
  timeout: 30000,
});

// TEAMS
export const getTeams = () => api.get('/api/teams');
export const getTeam = (id) => api.get(`/api/teams/${id}`);
export const getTeamPlayers = (id, filters = {}) => api.get(`/api/teams/${id}/players`, { params: filters });

// PLAYERS
export const getPlayers = (filters = {}) => api.get('/api/players', { params: filters });
export const getPlayer = (id) => api.get(`/api/players/${id}`);
export const searchPlayers = (query) => api.get('/api/players/search', { params: { q: query } });
export const getStatsSummary = () => api.get('/api/stats/summary');

// PREDICTIONS
export const runPredictions = () => api.post('/api/predict');
export const getPredictionStatus = () => api.get('/api/predict/status');
export const getHighRiskPlayers = () => api.get('/api/predict/high-risk');

// SCOUTING
export const getSimilarPlayers = (playerId, limit = 5) =>
  api.get('/api/scouting/similar', { params: { player_id: playerId, limit } });
export const getReplacements = (playerId, maxRisk = 'Medium', limit = 5) =>
  api.get('/api/scouting/replacements', { params: { player_id: playerId, max_risk: maxRisk, limit } });

// SYNC
export const syncData = () => api.post('/api/sync');
export const getSyncStatus = () => api.get('/api/sync/status');

export default api;
