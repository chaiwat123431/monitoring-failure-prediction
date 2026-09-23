"use client";

// Surfaces useLiveSeries' `error` (a failed /history fetch, initial or reconnect gap-fill, or a
// permanent WebSocket rejection) — previously computed but never rendered anywhere, so a fetch
// failure was invisible to the user despite the connection badge still looking healthy.
export function ErrorBanner({ message }: { message: string | null }) {
  if (!message) return null;

  return (
    <div className="banner banner-critical" role="alert">
      <span className="banner-icon" aria-hidden>
        ⚠
      </span>
      <span>Connection problem: {message}</span>
    </div>
  );
}
