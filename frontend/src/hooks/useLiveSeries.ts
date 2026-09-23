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

    // Incoming live messages are buffered here and flushed at most once per animation frame,
    // rather than cloning+re-sorting the whole `points` map on every single message — at
    // REPLAY_SPEED=0 the producer can emit thousands of messages/sec (PLANNING.md's own
    // measurements log), far faster than the UI can or should re-render.
    let pending: Map<string, ChartPoint> = new Map();
    let pendingModelLoaded: boolean | null = null;
    let flushRafId: number | null = null;

    function lastKnownTime(): string | null {
      let max: string | null = null;
      for (const t of pointsRef.current.keys()) {
        if (max === null || t > max) max = t;
      }
      for (const t of pending.keys()) {
        if (max === null || t > max) max = t;
      }
      return max;
    }

    function scheduleFlush() {
      if (flushRafId !== null) return;
      flushRafId = requestAnimationFrame(() => {
        flushRafId = null;
        if (cancelled || pending.size === 0) return;
        const toApply = pending;
        pending = new Map();
        setPoints((prev) => {
          const next = new Map(prev);
          for (const [k, v] of toApply) next.set(k, v);
          return next;
        });
        if (pendingModelLoaded !== null) setModelLoaded(pendingModelLoaded);
      });
    }

    function scheduleReconnect() {
      reconnectTimer = setTimeout(() => {
        reconnectDelay = Math.min(reconnectDelay * 2, RECONNECT_MAX_MS);
        connect();
      }, reconnectDelay);
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
        if (cancelled) return;
        setError(err instanceof Error ? err.message : String(err));
        // Don't open a socket over data we know may now be incomplete/stale — retry the whole
        // connect() (history fetch included) with the same backoff as a dropped socket, rather
        // than falling through to a WebSocket that would silently mask the failed fetch behind
        // an apparently-healthy "Live" badge.
        setConnectionState(reconnecting ? "reconnecting" : "connecting");
        scheduleReconnect();
        return;
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
        pending.set(msg.timestamp, { time: msg.timestamp, value: msg.value, is_anomaly: msg.is_anomaly });
        pendingModelLoaded = msg.model_loaded;
        scheduleFlush();
      };

      ws.onclose = (event) => {
        if (cancelled) return;
        // AD-23: the server closes with 1008 only for an unknown series_id — a value this
        // dashboard only ever sends from GET /api/series in the first place, but if the backend's
        // registry ever changes under a live session, retrying that exact request forever would
        // never succeed. Settle into "closed" instead of retrying indefinitely.
        if (event.code === 1008) {
          setConnectionState("closed");
          setError(event.reason || "server rejected the connection");
          return;
        }
        setConnectionState("reconnecting");
        scheduleReconnect();
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
      if (flushRafId !== null) cancelAnimationFrame(flushRafId);
      ws?.close();
    };
  }, [seriesId, mergeHistory]);

  const sorted = useMemo(
    () => Array.from(points.values()).sort((a, b) => (a.time < b.time ? -1 : a.time > b.time ? 1 : 0)),
    [points],
  );

  return { points: sorted, labeledWindows, modelLoaded, connectionState, error };
}
