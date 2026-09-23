"use client";

import { formatDateTime } from "@/lib/format";
import type { ChartPoint } from "@/lib/types";

const MAX_ALERTS = 50;

interface Props {
  points: ChartPoint[];
  modelLoaded: boolean | null;
}

export function AlertsPanel({ points, modelLoaded }: Props) {
  if (modelLoaded === false) {
    // AD-29: an explicit, stated-reason empty state — never a silently empty list, which is the
    // exact "masquer silencieusement les alertes" failure mode this slice was asked to avoid.
    return (
      <div className="panel">
        <h2 className="panel-title">Alerts</h2>
        <p className="empty-state">Anomaly detection unavailable — no model loaded (see banner above).</p>
      </div>
    );
  }

  const alerts = points
    .filter((p) => p.is_anomaly === true)
    .slice(-MAX_ALERTS)
    .reverse();

  return (
    <div className="panel">
      <h2 className="panel-title">Alerts</h2>
      {alerts.length === 0 ? (
        <p className="empty-state">No anomalies detected yet.</p>
      ) : (
        <ul className="alert-list">
          {alerts.map((p) => (
            <li key={p.time} className="alert-item">
              <span className="alert-item-time">{formatDateTime(p.time)}</span>
              <span className="alert-item-value">value: {p.value.toFixed(2)}</span>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
