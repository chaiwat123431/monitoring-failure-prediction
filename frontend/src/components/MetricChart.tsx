"use client";

import { useMemo } from "react";
import {
  CartesianGrid,
  ComposedChart,
  Line,
  ReferenceArea,
  ResponsiveContainer,
  Scatter,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import { formatAxisTick, formatDateTime } from "@/lib/format";
import type { ChartPoint, LabeledWindow } from "@/lib/types";

interface Props {
  points: ChartPoint[];
  labeledWindows: LabeledWindow[];
}

interface DotProps {
  cx?: number;
  cy?: number;
  payload?: ChartPoint;
}

// AD-28: is_anomaly is a 3-state field — three distinct marker styles, never a 2-state collapse of
// null into false. >=8px marker diameter (r>=4) per the dataviz skill's mark spec, a 2px
// surface-color ring on filled marks so they stay legible where they cross the line.
function AnomalyDot({ cx, cy, payload }: DotProps) {
  if (cx == null || cy == null || !payload) return null;
  const state = payload.is_anomaly;

  if (state === true) {
    return <circle cx={cx} cy={cy} r={5} fill="var(--status-critical)" stroke="var(--surface)" strokeWidth={2} />;
  }
  if (state === null) {
    // Hollow, not filled: visually distinct from both "normal" and "anomaly" — a point the model
    // never saw, not one it checked and cleared (AD-24's own distinction, carried into the chart).
    return <circle cx={cx} cy={cy} r={4} fill="var(--surface)" stroke="var(--text-muted)" strokeWidth={1.5} />;
  }
  return <circle cx={cx} cy={cy} r={3} fill="var(--series-line)" stroke="var(--surface)" strokeWidth={1} />;
}

function statusLabel(isAnomaly: boolean | null): string {
  if (isAnomaly === true) return "Anomaly detected";
  if (isAnomaly === false) return "Normal";
  return "No model available";
}

function ChartTooltip({ active, payload }: { active?: boolean; payload?: Array<{ payload: ChartPoint }> }) {
  if (!active || !payload || payload.length === 0) return null;
  const point = payload[0].payload;
  return (
    <div className="panel" style={{ padding: "8px 12px", fontSize: 12 }}>
      <div>{formatDateTime(point.time)}</div>
      <div>value: {point.value.toFixed(2)}</div>
      <div>{statusLabel(point.is_anomaly)}</div>
    </div>
  );
}

export function MetricChart({ points, labeledWindows }: Props) {
  const chartData = useMemo(() => points.map((p) => ({ ...p, t: new Date(p.time).getTime() })), [points]);
  const windowRanges = useMemo(
    () =>
      labeledWindows.map((w) => ({
        x1: new Date(w.window_start).getTime(),
        x2: new Date(w.window_end).getTime(),
      })),
    [labeledWindows],
  );

  if (chartData.length === 0) {
    return <p className="empty-state">Waiting for data…</p>;
  }

  return (
    <div>
      <ResponsiveContainer width="100%" height={420}>
        <ComposedChart data={chartData} margin={{ top: 8, right: 16, left: 0, bottom: 8 }}>
          <CartesianGrid stroke="var(--gridline)" vertical={false} />
          {windowRanges.map((w, i) => (
            <ReferenceArea key={i} x1={w.x1} x2={w.x2} fill="var(--window-band-fill)" stroke="none" />
          ))}
          <XAxis
            dataKey="t"
            type="number"
            domain={["dataMin", "dataMax"]}
            scale="time"
            tickFormatter={formatAxisTick}
            stroke="var(--baseline)"
            tick={{ fill: "var(--text-muted)", fontSize: 11 }}
          />
          <YAxis stroke="var(--baseline)" tick={{ fill: "var(--text-muted)", fontSize: 11 }} width={44} />
          <Tooltip content={<ChartTooltip />} />
          <Line
            type="monotone"
            dataKey="value"
            stroke="var(--series-line)"
            strokeWidth={2}
            dot={false}
            isAnimationActive={false}
          />
          <Scatter dataKey="value" shape={<AnomalyDot />} isAnimationActive={false} />
        </ComposedChart>
      </ResponsiveContainer>
      <div className="legend">
        <span className="legend-item">
          <span className="legend-swatch" style={{ background: "var(--series-line)" }} />
          Normal
        </span>
        <span className="legend-item">
          <span className="legend-swatch" style={{ background: "var(--status-critical)" }} />
          Anomaly detected
        </span>
        <span className="legend-item">
          <span className="legend-swatch legend-swatch-hollow" />
          No model available
        </span>
        <span className="legend-item">
          <span className="legend-swatch-band" />
          NAB labeled window (ground truth)
        </span>
      </div>
    </div>
  );
}
