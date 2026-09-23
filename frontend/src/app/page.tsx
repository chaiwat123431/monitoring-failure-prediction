"use client";

import { useEffect, useState } from "react";
import { AlertsPanel } from "@/components/AlertsPanel";
import { ConnectionBadge } from "@/components/ConnectionBadge";
import { MetricChart } from "@/components/MetricChart";
import { ModelStatusBanner } from "@/components/ModelStatusBanner";
import { SeriesSelector } from "@/components/SeriesSelector";
import { useLiveSeries } from "@/hooks/useLiveSeries";
import { getSeries } from "@/lib/api";
import type { SeriesListItem } from "@/lib/types";

// AD-25: switching series unmounts this whole subtree and mounts a fresh one (the `key` on the
// call site below) rather than a single long-lived useLiveSeries instance reacting to a changing
// seriesId — every piece of per-series state starts genuinely fresh, no manual reset logic needed.
function Dashboard({ seriesId }: { seriesId: string }) {
  const { points, labeledWindows, modelLoaded, connectionState } = useLiveSeries(seriesId);

  return (
    <>
      <ModelStatusBanner modelLoaded={modelLoaded} />
      <div className="dashboard-body">
        <div className="panel">
          <div className="dashboard-header" style={{ marginBottom: 0 }}>
            <h2 className="panel-title" style={{ margin: 0 }}>
              {seriesId}
            </h2>
            <ConnectionBadge state={connectionState} />
          </div>
          <MetricChart points={points} labeledWindows={labeledWindows} />
        </div>
        <AlertsPanel points={points} modelLoaded={modelLoaded} />
      </div>
    </>
  );
}

export default function Home() {
  const [series, setSeries] = useState<SeriesListItem[]>([]);
  const [selectedSeriesId, setSelectedSeriesId] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    getSeries().then((list) => {
      if (cancelled) return;
      setSeries(list);
      setSelectedSeriesId((current) => current ?? list[0]?.series_id ?? null);
    });
    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <main className="dashboard">
      <div className="dashboard-header">
        <h1 className="dashboard-title">Monitoring &amp; Failure Prediction</h1>
        <SeriesSelector series={series} selected={selectedSeriesId} onSelect={setSelectedSeriesId} />
      </div>

      {selectedSeriesId ? (
        <Dashboard key={selectedSeriesId} seriesId={selectedSeriesId} />
      ) : (
        <p className="empty-state">Loading…</p>
      )}
    </main>
  );
}
