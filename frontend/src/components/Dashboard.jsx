import React, { useCallback, useEffect, useState } from "react";
import TaskForm from "./TaskForm.jsx";
import TaskList from "./TaskList.jsx";
import CronPanel from "./CronPanel.jsx";
import HistoryPanel from "./HistoryPanel.jsx";
import { api } from "../api/client.js";
import { useTaskWebSocket } from "../hooks/useTaskWebSocket.js";

export default function Dashboard() {
  const [tab, setTab] = useState("tasks");
  const [tasks, setTasks] = useState([]);
  const [stats, setStats] = useState(null);

  const refresh = useCallback(async () => {
    setTasks(await api.list());
    setStats(await api.stats());
  }, []);

  useEffect(() => { refresh(); }, [refresh]);
  useEffect(() => {
    const t = setInterval(refresh, 5000);
    return () => clearInterval(t);
  }, [refresh]);

  useTaskWebSocket(useCallback((upd) => {
    setTasks(prev => prev.map(t =>
      t.id === upd.task_id
        ? { ...t, status: upd.status, last_error: upd.error, result: upd.result, worker_id: upd.worker_id }
        : t
    ));
  }, []));

  return (
    <div className="page">
      <header>
        <h1>Intelligent Task Scheduler</h1>
        {stats && (
          <div className="stats">
            <Stat label="Heap" value={stats.heap.heap_size} />
            <Stat label="Queue" value={stats.redis_queue_depth} />
            <Stat label="Running" value={stats.running_tasks} />
            <Stat label="Workers" value={stats.active_workers} />
          </div>
        )}
      </header>

      <nav className="tabs">
        <button className={tab === "tasks"   ? "tab active" : "tab"} onClick={() => setTab("tasks")}>Tasks</button>
        <button className={tab === "cron"    ? "tab active" : "tab"} onClick={() => setTab("cron")}>Recurring</button>
        <button className={tab === "history" ? "tab active" : "tab"} onClick={() => setTab("history")}>History</button>
      </nav>

      {tab === "tasks" && (
        <div className="grid">
          <TaskForm onCreated={refresh} />
          <TaskList tasks={tasks} onChange={refresh} />
        </div>
      )}
      {tab === "cron"    && <CronPanel />}
      {tab === "history" && <HistoryPanel />}
    </div>
  );
}

function Stat({ label, value }) {
  return (
    <div className="stat"><span>{value}</span><label>{label}</label></div>
  );
}
