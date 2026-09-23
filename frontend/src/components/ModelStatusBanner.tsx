"use client";

// AD-29: persistent, not a toast — visible the entire time model_loaded is false, and reflects the
// reactive state useLiveSeries derives from both /history and every live WS message.
export function ModelStatusBanner({ modelLoaded }: { modelLoaded: boolean | null }) {
  if (modelLoaded !== false) return null;

  return (
    <div className="banner" role="alert">
      <span className="banner-icon" aria-hidden>
        ⚠
      </span>
      <span>
        No trained model detected — anomaly detection is unavailable, all points are shown as
        unscored. Run <code>scripts/train.py</code> to enable detection.
      </span>
    </div>
  );
}
