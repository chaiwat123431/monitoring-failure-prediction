"use client";

import type { SeriesListItem } from "@/lib/types";

interface Props {
  series: SeriesListItem[];
  selected: string | null;
  onSelect: (seriesId: string) => void;
}

export function SeriesSelector({ series, selected, onSelect }: Props) {
  return (
    <select
      aria-label="Series"
      value={selected ?? ""}
      onChange={(e) => onSelect(e.target.value)}
      disabled={series.length === 0}
    >
      {series.length === 0 && <option value="">Loading series…</option>}
      {series.map((s) => (
        <option key={s.series_id} value={s.series_id}>
          {s.series_id}
        </option>
      ))}
    </select>
  );
}
