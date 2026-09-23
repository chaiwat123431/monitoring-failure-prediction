// Mirrors the API's actual JSON shapes verbatim (backend/app/api/metrics.py, app/api/ws.py,
// app/live/feed_consumer.py) — not a re-shaped or renamed subset.

export interface SeriesListItem {
  series_id: string;
}

// GET /api/series/{id}/history — each row's own field name is "time".
export interface HistoryPoint {
  time: string; // ISO 8601, e.g. "2014-04-15T07:24:00+00:00"
  value: number;
  is_anomaly: boolean | null; // AD-24: null means "no model was available", never coerced to false
}

export interface LabeledWindow {
  window_start: string;
  window_end: string;
}

export interface HistoryResponse {
  series_id: string;
  model_loaded: boolean;
  history: HistoryPoint[];
  labeled_windows: LabeledWindow[];
}

// GET /api/model — returned verbatim from the joblib artifact's embedded metadata (AD-19), or a
// 503 body ({status: "not_trained", detail}) which callers of getModel() surface as a thrown error
// instead of modeling here — this dashboard never calls that endpoint (AD-29: model_loaded already
// rides on /history and every live message).

// WS /ws/series/{id}/live — deliberately a *different* field name ("timestamp", not "time"): this
// is the raw Kafka payload shape (AD-12/AD-22), not the history row shape. Normalized to the
// shared ChartPoint shape at the point the hook receives it.
export interface LiveMessage {
  series_id: string;
  timestamp: string;
  value: number;
  is_anomaly: boolean | null;
  model_loaded: boolean;
}

// The shape the chart/panel components actually consume, once history rows and live messages have
// both been normalized onto the same field names.
export type ChartPoint = HistoryPoint;

export type ConnectionState = "connecting" | "open" | "reconnecting" | "closed";
