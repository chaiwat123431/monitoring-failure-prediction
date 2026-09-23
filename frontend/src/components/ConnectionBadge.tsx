"use client";

import type { ConnectionState } from "@/lib/types";

const LABEL: Record<ConnectionState, string> = {
  connecting: "Connecting…",
  open: "Live",
  reconnecting: "Reconnecting…",
  closed: "Disconnected",
};

// Status color conveys state, but never alone (dataviz skill: status colors ship with an
// icon/label, never color-only) — the dot is paired with the text label above.
const COLOR: Record<ConnectionState, string> = {
  connecting: "var(--text-muted)",
  open: "var(--status-good)",
  reconnecting: "var(--status-warning)",
  closed: "var(--status-critical)",
};

export function ConnectionBadge({ state }: { state: ConnectionState }) {
  return (
    <span className="badge" role="status">
      <span className="badge-dot" style={{ background: COLOR[state] }} aria-hidden />
      {LABEL[state]}
    </span>
  );
}
