import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import HomePage from './pages/HomePage';
import DashboardPage from './pages/DashboardPage';
import ChatBot from './components/ChatBot';
import './App.css';

// Get the API base URL from api.js config
const API_BASE_URL = process.env.REACT_APP_API_URL || 'https://fcm-backend-lvcp.onrender.com';

function App() {
  return (
    <Router>
      <div className="App">
        <Routes>
          <Route path="/" element={<HomePage />} />
          <Route path="/dashboard/:teamId" element={<DashboardPage />} />
        </Routes>
        <ChatBot apiBaseUrl={API_BASE_URL} />
      </div>
    </Router>
  );
}

export default App;