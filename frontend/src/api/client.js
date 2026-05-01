// Centralized API calls. Vite proxies /api -> backend:8000
const BASE = "/api";

export const api = {
  // Tasks
  list:    ()              => fetch(`${BASE}/tasks`).then(r => r.json()),
  get:     (id)            => fetch(`${BASE}/tasks/${id}`).then(r => r.json()),
  create:  (body)          => fetch(`${BASE}/tasks`, {
                                method: "POST",
                                headers: { "Content-Type": "application/json" },
                                body: JSON.stringify(body),
                              }).then(r => r.json()),
  runNow:  (id)            => fetch(`${BASE}/tasks/${id}/run`,   { method: "POST" }).then(r => r.json()),
  retry:   (id)            => fetch(`${BASE}/tasks/${id}/retry`, { method: "POST" }).then(r => r.json()),
  stats:   ()              => fetch(`${BASE}/scheduler/stats`).then(r => r.json()),

  // Crons
  listCrons: ()            => fetch(`${BASE}/crons`).then(r => r.json()),
  createCron: (body)       => fetch(`${BASE}/crons`, {
                                method: "POST",
                                headers: { "Content-Type": "application/json" },
                                body: JSON.stringify(body),
                              }).then(r => r.json()),
  patchCron: (id, body)    => fetch(`${BASE}/crons/${id}`, {
                                method: "PATCH",
                                headers: { "Content-Type": "application/json" },
                                body: JSON.stringify(body),
                              }).then(r => r.json()),
  deleteCron: (id)         => fetch(`${BASE}/crons/${id}`, { method: "DELETE" }),

  // History
  history:    (params={})  => fetch(`${BASE}/history/completed?${new URLSearchParams(params)}`).then(r => r.json()),
  timeline:   (id)         => fetch(`${BASE}/history/tasks/${id}/events`).then(r => r.json()),
  metrics:    (hours=24)   => fetch(`${BASE}/history/metrics?since_hours=${hours}`).then(r => r.json()),
};
