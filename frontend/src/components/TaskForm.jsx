import React, { useState } from "react";
import { api } from "../api/client.js";

const TASK_TYPES = ["email", "report", "data_sync", "ml_inference", "generic"];

export default function TaskForm({ onCreated }) {
  const [name, setName] = useState("");
  const [type, setType] = useState("email");
  const [priority, setPriority] = useState("");        // empty = let ML decide
  const [delaySec, setDelaySec] = useState(0);
  const [payload, setPayload] = useState('{"to": "user@example.com"}');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");

  async function submit(e) {
    e.preventDefault();
    setError("");
    let parsed;
    try { parsed = JSON.parse(payload); }
    catch { setError("Payload must be valid JSON"); return; }

    setBusy(true);
    try {
      const body = {
        name, task_type: type, payload: parsed,
        ...(priority ? { priority: Number(priority) } : {}),
        ...(delaySec > 0
            ? { run_at: new Date(Date.now() + delaySec * 1000).toISOString() }
            : {}),
      };
      await api.create(body);
      setName("");
      onCreated?.();
    } catch (e) { setError(String(e)); }
    finally { setBusy(false); }
  }

  return (
    <form onSubmit={submit} className="card form">
      <h3>Create task</h3>
      <label>Name<input value={name} onChange={e => setName(e.target.value)} required /></label>
      <label>Type
        <select value={type} onChange={e => setType(e.target.value)}>
          {TASK_TYPES.map(t => <option key={t}>{t}</option>)}
        </select>
      </label>
      <label>Priority (1-10, blank = ML predicts)
        <input type="number" min="1" max="10" value={priority}
               onChange={e => setPriority(e.target.value)} />
      </label>
      <label>Delay (seconds)
        <input type="number" min="0" value={delaySec}
               onChange={e => setDelaySec(Number(e.target.value))} />
      </label>
      <label>Payload (JSON)
        <textarea rows={3} value={payload} onChange={e => setPayload(e.target.value)} />
      </label>
      {error && <div className="err">{error}</div>}
      <button disabled={busy}>{busy ? "Submitting…" : "Schedule task"}</button>
    </form>
  );
}
