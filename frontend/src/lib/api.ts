import { API_BASE_URL } from "./config";
import type { HistoryResponse, SeriesListItem } from "./types";

// Synthetic bracket, not real data bounds — see PLANNING.md AD-26. GET /api/series/{id}/history
// requires an explicit start/end (AD-20 gives it no server-side default), and this frontend has no
// way to learn either series' *real* min/max timestamp without a backend change out of this
// slice's scope. A wide, fixed bracket returns every row currently in raw_metrics for a series —
// the same practical result as "give me everything" — without hardcoding a specific dataset's
// specific calendar dates (AD-16) into frontend code, and without depending on client wall-clock
// time, which bears no relationship to the NAB events' 2014 timestamps (AD-12).
const HISTORY_RANGE_START = "2000-01-01T00:00:00Z";
const HISTORY_RANGE_END = "2100-01-01T00:00:00Z";

export async function getSeries(): Promise<SeriesListItem[]> {
  const res = await fetch(`${API_BASE_URL}/api/series`);
  if (!res.ok) {
    throw new Error(`GET /api/series failed: ${res.status}`);
  }
  return res.json();
}

// `since`, when given, replaces the bracket's start — used for the AD-27 reconnect gap-fill, which
// only needs "everything from the last known point onward", not the full bracket again.
export async function getHistory(seriesId: string, since?: string): Promise<HistoryResponse> {
  const start = since ?? HISTORY_RANGE_START;
  const query = new URLSearchParams({ start, end: HISTORY_RANGE_END });
  // seriesId is interpolated raw, not encodeURIComponent'd: it contains a literal "/"
  // (e.g. "realAWSCloudwatch/ec2_cpu_utilization_825cc2", AD-10) that the backend's {series_id:path}
  // route (AD-23) expects to see as real path separators, not a percent-encoded "%2F".
  const res = await fetch(`${API_BASE_URL}/api/series/${seriesId}/history?${query.toString()}`);
  if (!res.ok) {
    throw new Error(`GET /api/series/${seriesId}/history failed: ${res.status}`);
  }
  return res.json();
}
