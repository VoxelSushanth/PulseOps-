import React, { useState, useEffect } from 'react';
import './App.css';

const API_BASE = 'http://localhost:8080';

function App() {
  const [stats, setStats] = useState({ total_events: 0, tps: 0 });
  const [successRate, setSuccessRate] = useState(98.5);
  const [anomalies, setAnomalies] = useState([]);
  const [gatewayHealth, setGatewayHealth] = useState([
    { name: 'HDFC', status: 'healthy', latency: 145, successRate: 98.2 },
    { name: 'ICICI', status: 'healthy', latency: 132, successRate: 98.5 },
    { name: 'SBI', status: 'healthy', latency: 178, successRate: 97.9 },
    { name: 'AXIS', status: 'healthy', latency: 156, successRate: 98.1 },
    { name: 'RAZORPAY', status: 'healthy', latency: 98, successRate: 99.1 },
  ]);

  useEffect(() => {
    const interval = setInterval(async () => {
      try {
        const response = await fetch(`${API_BASE}/stats`);
        const data = await response.json();
        setStats(data);
        
        // Simulate success rate fluctuation
        setSuccessRate(prev => Math.min(99.9, Math.max(95, prev + (Math.random() - 0.5))));
      } catch (error) {
        console.error('Failed to fetch stats:', error);
      }
    }, 5000);

    return () => clearInterval(interval);
  }, []);

  const getSuccessRateColor = (rate) => {
    if (rate >= 98) return '#22c55e';
    if (rate >= 95) return '#f59e0b';
    return '#ef4444';
  };

  const getStatusColor = (status) => {
    switch (status) {
      case 'healthy': return '#22c55e';
      case 'degraded': return '#f59e0b';
      case 'down': return '#ef4444';
      default: return '#6b7280';
    }
  };

  return (
    <div className="dashboard">
      <header className="header">
        <h1>💳 Payment Operations Monitor</h1>
        <div className="header-stats">
          <span className="stat">TPS: <strong>{stats.tps || 1000}</strong></span>
          <span className="stat">Total Events: <strong>{stats.total_events?.toLocaleString()}</strong></span>
        </div>
      </header>

      <main className="main-content">
        {/* Success Rate Gauge */}
        <section className="card gauge-section">
          <h2>Success Rate</h2>
          <div className="gauge-container">
            <svg viewBox="0 0 200 100" className="gauge">
              <path
                d="M 20 80 A 60 60 0 0 1 180 80"
                fill="none"
                stroke="#e5e7eb"
                strokeWidth="20"
                strokeLinecap="round"
              />
              <path
                d="M 20 80 A 60 60 0 0 1 180 80"
                fill="none"
                stroke={getSuccessRateColor(successRate)}
                strokeWidth="20"
                strokeLinecap="round"
                strokeDasharray={`${(successRate / 100) * 251.2} 251.2`}
                className="gauge-fill"
              />
              <text x="100" y="70" textAnchor="middle" className="gauge-value">
                {successRate.toFixed(1)}%
              </text>
            </svg>
          </div>
        </section>

        {/* Gateway Health Matrix */}
        <section className="card gateway-section">
          <h2>Gateway Health Matrix</h2>
          <div className="gateway-grid">
            {gatewayHealth.map((gateway) => (
              <div key={gateway.name} className="gateway-card">
                <div className="gateway-header">
                  <span className="gateway-name">{gateway.name}</span>
                  <span 
                    className="gateway-status"
                    style={{ backgroundColor: getStatusColor(gateway.status) }}
                  />
                </div>
                <div className="gateway-metrics">
                  <div className="metric">
                    <span className="metric-label">Latency</span>
                    <span className="metric-value">{gateway.latency}ms</span>
                  </div>
                  <div className="metric">
                    <span className="metric-label">Success</span>
                    <span className="metric-value">{gateway.successRate}%</span>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </section>

        {/* Live Anomaly Feed */}
        <section className="card anomaly-section">
          <h2>🔔 Live Anomaly Feed</h2>
          <div className="anomaly-list">
            {anomalies.length === 0 ? (
              <div className="no-anomalies">
                <span className="checkmark">✓</span>
                <p>No active anomalies</p>
              </div>
            ) : (
              anomalies.map((anomaly) => (
                <div key={anomaly.id} className={`anomaly-item severity-${anomaly.severity}`}>
                  <div className="anomaly-header">
                    <span className="severity-badge">{anomaly.severity}</span>
                    <span className="anomaly-time">{new Date(anomaly.detected_at).toLocaleTimeString()}</span>
                  </div>
                  <p className="anomaly-description">{anomaly.description}</p>
                  <div className="anomaly-details">
                    <span>Gateway: {anomaly.gateway}</span>
                    <span>Deviation: {anomaly.deviation}%</span>
                  </div>
                </div>
              ))
            )}
          </div>
        </section>

        {/* Transaction Counter */}
        <section className="card counter-section">
          <h2>Live Transactions</h2>
          <div className="counter-display">
            <span className="counter-number">
              {stats.total_events?.toLocaleString() || '0'}
            </span>
            <span className="counter-label">events processed</span>
          </div>
        </section>
      </main>

      {/* LLM Analysis Sidebar */}
      <aside className="sidebar">
        <h3>🤖 AI Incident Analysis</h3>
        <div className="analysis-placeholder">
          <p>No active incidents requiring analysis</p>
          <p className="hint">P0/P1 incidents will trigger automatic LLM analysis</p>
        </div>
      </aside>
    </div>
  );
}

export default App;
