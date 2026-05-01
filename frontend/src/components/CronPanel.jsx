import React, { useEffect, useState } from "react";
import { api } from "../api/client.js";

const TASK_TYPES = ["email", "report", "data_sync", "ml_inference", "generic"];

const PRESETS = [
  { label: "Every minute",   expr: "* * * * *" },
  { label: "Every 15 min",   expr: "*/15 * * * *" },
  { label: "Hourly",         expr: "0 * * * *" },
  { label: "Daily 3am",      expr: "0 3 * * *" },
  { label: "Mondays 9am",    expr: "0 9 * * 1" },
];

export default function CronPanel() {
  const [crons, setCrons] = useState([]);
  const [name, setName] = useState("");
  const [expr, setExpr] = useState("0 * * * *");
  const [type, setType] = useState("generic");
  const [payload, setPayload] = useState("{}");
  const [error, setError] = useState("");

  async function refresh() { setCrons(await api.listCrons()); }
  useEffect(() => { refresh(); const t = setInterval(refresh, 5000); return () => clearInterval(t); }, []);

  async function submit(e) {
    e.preventDefault();
    setError("");
    let parsed;
    try { parsed = JSON.parse(payload); }
    catch { setError("Payload must be valid JSON"); return; }
    try {
      await api.createCron({ name, cron_expr: expr, task_type: type, payload: parsed });
      setName("");
      refresh();
    } catch (e) { setError(String(e)); }
  }

  async function toggle(c) { await api.patchCron(c.id, { enabled: !c.enabled }); refresh(); }
  async function remove(c) { if (confirm(`Delete cron ${c.name}?`)) { await api.deleteCron(c.id); refresh(); } }

  return (
    <div className="card">
      <h3>Recurring tasks (cron)</h3>

      <form onSubmit={submit} className="cron-form">
        <input placeholder="Name" value={name} onChange={e => setName(e.target.value)} required />
        <select value={type} onChange={e => setType(e.target.value)}>
          {TASK_TYPES.map(t => <option key={t}>{t}</option>)}
        </select>
        <input placeholder="Cron expression" value={expr} onChange={e => setExpr(e.target.value)}
               style={{ fontFamily: "monospace" }} required />
        <input placeholder='Payload JSON' value={payload} onChange={e => setPayload(e.target.value)} />
        <button>Add</button>
      </form>

      <div className="presets">
        {PRESETS.map(p => (
          <button key={p.expr} type="button" onClick={() => setExpr(p.expr)}>{p.label}</button>
        ))}
      </div>

      {error && <div className="err">{error}</div>}

      <table>
        <thead>
          <tr><th>Name</th><th>Expr</th><th>Type</th><th>Fires</th><th>Last fired</th><th>Enabled</th><th></th></tr>
        </thead>
        <tbody>
          {crons.map(c => (
            <tr key={c.id}>
              <td>{c.name}</td>
              <td><code>{c.cron_expr}</code></td>
              <td>{c.task_type}</td>
              <td>{c.fire_count}</td>
              <td>{c.last_fired_at ? new Date(c.last_fired_at).toLocaleString() : "—"}</td>
              <td>
                <button onClick={() => toggle(c)}>{c.enabled ? "Pause" : "Resume"}</button>
              </td>
              <td><button onClick={() => remove(c)}>Delete</button></td>
            </tr>
          ))}
          {crons.length === 0 && (
            <tr><td colSpan={7} className="empty-row">No recurring tasks. Add one above.</td></tr>
          )}
        </tbody>
      </table>
    </div>
  );
}
