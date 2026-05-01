import { useEffect, useRef } from "react";

/**
 * Subscribes to /ws/tasks and invokes onUpdate(taskUpdate) for each message.
 * Auto-reconnects with exponential backoff on disconnect — same algorithm as
 * the worker retry, capped at 30s.
 */
export function useTaskWebSocket(onUpdate) {
  const wsRef = useRef(null);
  const retryRef = useRef(0);

  useEffect(() => {
    let stopped = false;

    function connect() {
      if (stopped) return;
      const proto = location.protocol === "https:" ? "wss" : "ws";
      const ws = new WebSocket(`${proto}://${location.host}/ws/tasks`);
      wsRef.current = ws;

      ws.onopen   = () => { retryRef.current = 0; };
      ws.onmessage = (e) => {
        try { onUpdate(JSON.parse(e.data)); } catch { /* ignore malformed */ }
      };
      ws.onclose  = () => {
        if (stopped) return;
        const delay = Math.min(1000 * Math.pow(2, retryRef.current++), 30000);
        setTimeout(connect, delay);
      };
    }

    connect();
    return () => { stopped = true; wsRef.current?.close(); };
  }, [onUpdate]);
}
