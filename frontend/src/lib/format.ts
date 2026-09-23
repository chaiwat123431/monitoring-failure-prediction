// Single source of truth for how a point's time is displayed — both the alerts list and the chart
// tooltip need the full local date/time; the chart axis needs a compact tick label. All three
// render in the *viewer's* local timezone (Intl default), not UTC — see PLANNING.md Slice 5's
// empirical verification note for why that matters when cross-checking a displayed value against
// the API's UTC timestamps.

export function formatDateTime(iso: string): string {
  return new Date(iso).toLocaleString();
}

export function formatAxisTick(epochMs: number): string {
  return new Date(epochMs).toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}
