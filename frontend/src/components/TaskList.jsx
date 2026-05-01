import React from "react";
import { api } from "../api/client.js";

const STATUS_COLOR = {
  pending: "#888", queued: "#3b82f6", running: "#f59e0b",
  completed: "#10b981", failed: "#ef4444", retrying: "#a855f7",
};

export default function TaskList({ tasks, onChange }) {
  if (!tasks.length) return <div className="card empty">No tasks yet.</div>;

  return (
    <div className="card">
      <h3>Tasks ({tasks.length})</h3>
      <table>
        <thead>
          <tr>
            <th>Name</th><th>Type</th><th>Priority</th><th>Status</th>
            <th>Retries</th><th>Created</th><th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {tasks.map(t => (
            <tr key={t.id}>
              <td title={t.id}>{t.name}</td>
              <td>{t.task_type}</td>
              <td className="prio">P{t.priority}</td>
              <td>
                <span className="badge" style={{ background: STATUS_COLOR[t.status] }}>
                  {t.status}
                </span>
              </td>
              <td>{t.retries}/{t.max_retries}</td>
              <td>{new Date(t.created_at).toLocaleTimeString()}</td>
              <td>
                {t.status === "failed" &&
                  <button onClick={async () => { await api.retry(t.id); onChange(); }}>
                    Retry
                  </button>}
                {t.status === "pending" &&
                  <button onClick={async () => { await api.runNow(t.id); onChange(); }}>
                    Run now
                  </button>}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
