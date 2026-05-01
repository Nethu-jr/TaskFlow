import React, { useEffect, useState } from "react";
import { api } from "../api/client.js";

export default function HistoryPanel() {
  const [rows, setRows] = useState([]);
  const [metrics, setMetrics] = useState(null);
  const [hours, setHours] = useState(24);

  async function refresh() {
    setRows(await api.history({ since_hours: hours, limit: 100 }));
    setMetrics(await api.metrics(hours));
  }
  useEffect(() => { refresh(); }, [hours]);     // eslint-disable-line

  return (
    <div className="card">
      <div className="row-between">
        <h3>History (last {hours}h)</h3>
        <select value={hours} onChange={e => setHours(Number(e.target.value))}>
          <option value={1}>1 hour</option>
          <option value={24}>24 hours</option>
          <option value={168}>7 days</option>
          <option value={720}>30 days</option>
        </select>
      </div>

      {metrics && (
        <div className="metrics-grid">
          {Object.entries(metrics.by_type).map(([type, m]) => {
            const total = (m.completed || 0) + (m.failed || 0);
            const successRate = total ? Math.round(100 * (m.completed || 0) / total) : 0;
            return (
              <div key={type} className="metric-card">
                <h4>{type}</h4>
                <div className="metric-row"><span>Completed</span><b>{m.completed || 0}</b></div>
                <div className="metric-row"><span>Failed</span><b>{m.failed || 0}</b></div>
                <div className="metric-row"><span>Success</span><b>{successRate}%</b></div>
                <div className="metric-row"><span>Avg ms</span><b>{m.avg_ms ? Math.round(m.avg_ms) : "—"}</b></div>
              </div>
            );
          })}
          {Object.keys(metrics.by_type).length === 0 && (
            <div className="empty">No completed tasks in this window yet.</div>
          )}
        </div>
      )}

      <table>
        <thead>
          <tr><th>Name</th><th>Type</th><th>Status</th><th>Pri</th><th>Duration</th><th>Retries</th><th>Completed</th></tr>
        </thead>
        <tbody>
          {rows.map(r => (
            <tr key={r.task_id}>
              <td title={r.task_id}>{r.name}</td>
              <td>{r.task_type}</td>
              <td>
                <span className="badge" style={{ background: r.status === "completed" ? "#10b981" : "#ef4444" }}>
                  {r.status}
                </span>
              </td>
              <td className="prio">P{r.priority}</td>
              <td>{r.duration_ms ? Math.round(r.duration_ms) + "ms" : "—"}</td>
              <td>{r.retries}</td>
              <td>{new Date(r.completed_at).toLocaleString()}</td>
            </tr>
          ))}
          {rows.length === 0 && (
            <tr><td colSpan={7} className="empty-row">No history yet.</td></tr>
          )}
        </tbody>
      </table>
    </div>
  );
}
