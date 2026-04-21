import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import HomePage from './pages/HomePage';
import DashboardPage from './pages/DashboardPage';
import ChatBot from './components/ChatBot';
import './App.css';

// Get the API base URL from api.js config
const API_BASE_URL = process.env.REACT_APP_API_URL || 'https://automatic-yodel-x5566x9x5jw6265jg-5000.app.github.dev';

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