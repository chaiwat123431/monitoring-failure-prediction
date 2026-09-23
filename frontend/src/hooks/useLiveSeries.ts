"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { getHistory } from "@/lib/api";
import { WS_BASE_URL } from "@/lib/config";
import type { ChartPoint, ConnectionState, LabeledWindow, LiveMessage } from "@/lib/types";

const RECONNECT_BASE_MS = 250;
const RECONNECT_MAX_MS = 8000;

export interface LiveSeriesState {
  points: ChartPoint[];
  labeledWindows: LabeledWindow[];
  modelLoaded: boolean | null;
  connectionState: ConnectionState;
  error: string | null;
}

/** AD-25/26/27/29: the one place this dashboard talks to the network. Fetches the initial history
 * bracket once, opens the live WebSocket for the tail, and on every *reconnect* (not the first
 * connect) re-fetches history from the last point already held to close the gap AD-23 says the
 * socket itself will never backfill — never a polling loop.
 *
 * Callers mount this behind a `key={seriesId}` (see page.tsx) rather than passing a *changing*
 * seriesId to a single long-lived instance — AD-25's "switching series is unmount the old hook
 * instance, mount a new one," which is also what keeps every state variable below starting fresh
 * per series with no manual reset step. */
export function useLiveSeries(seriesId: string): LiveSeriesState {
  const [points, setPoints] = useState<Map<string, ChartPoint>>(() => new Map());
  const [labeledWindows, setLabeledWindows] = useState<LabeledWindow[]>([]);
  const [modelLoaded, setModelLoaded] = useState<boolean | null>(null);
  const [connectionState, setConnectionState] = useState<ConnectionState>("connecting");
  const [error, setError] = useState<string | null>(null);

  const pointsRef = useRef(points);
  useEffect(() => {
    pointsRef.current = points;
  }, [points]);

  const mergeHistory = useCallback((history: ChartPoint[]) => {
    if (history.length === 0) return;
    setPoints((prev) => {
      const next = new Map(prev);
      for (const p of history) next.set(p.time, p);
      return next;
    });
  }, []);

  useEffect(() => {
    let cancelled = false;
    let isFirstConnect = true;
    let reconnectDelay = RECONNECT_BASE_MS;
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
    let ws: WebSocket | null = null;

    function lastKnownTime(): string | null {
      let max: string | null = null;
      for (const t of pointsRef.current.keys()) {
        if (max === null || t > max) max = t;
      }
      return max;
    }

    async function connect() {
      const reconnecting = !isFirstConnect;
      try {
        if (!reconnecting) {
          const data = await getHistory(seriesId);
          if (cancelled) return;
          mergeHistory(data.history);
          setLabeledWindows(data.labeled_windows);
          setModelLoaded(data.model_loaded);
        } else {
          // AD-27 gap-fill: pull everything from the last point already on the chart onward,
          // rather than accepting a silent hole for however long the socket was down.
          const since = lastKnownTime();
          if (since) {
            const data = await getHistory(seriesId, since);
            if (cancelled) return;
            mergeHistory(data.history);
            setModelLoaded(data.model_loaded);
          }
        }
      } catch (err) {
        if (!cancelled) setError(err instanceof Error ? err.message : String(err));
      }
      if (cancelled) return;

      isFirstConnect = false;
      ws = new WebSocket(`${WS_BASE_URL}/ws/series/${seriesId}/live`);

      ws.onopen = () => {
        if (cancelled) return;
        setConnectionState("open");
        setError(null);
        reconnectDelay = RECONNECT_BASE_MS;
      };

      ws.onmessage = (event) => {
        if (cancelled) return;
        const msg: LiveMessage = JSON.parse(event.data);
        setPoints((prev) => {
          const next = new Map(prev);
          next.set(msg.timestamp, { time: msg.timestamp, value: msg.value, is_anomaly: msg.is_anomaly });
          return next;
        });
        setModelLoaded(msg.model_loaded);
      };

      ws.onclose = () => {
        if (cancelled) return;
        setConnectionState("reconnecting");
        reconnectTimer = setTimeout(() => {
          reconnectDelay = Math.min(reconnectDelay * 2, RECONNECT_MAX_MS);
          connect();
        }, reconnectDelay);
      };

      // A socket error is always followed by close (per the WebSocket spec) — onclose alone
      // drives reconnection; this just forces the close promptly instead of waiting on the
      // browser's own timeout.
      ws.onerror = () => {
        ws?.close();
      };
    }

    connect();

    return () => {
      cancelled = true;
      if (reconnectTimer) clearTimeout(reconnectTimer);
      ws?.close();
    };
  }, [seriesId, mergeHistory]);

  const sorted = useMemo(
    () => Array.from(points.values()).sort((a, b) => (a.time < b.time ? -1 : a.time > b.time ? 1 : 0)),
    [points],
  );

  return { points: sorted, labeledWindows, modelLoaded, connectionState, error };
}
