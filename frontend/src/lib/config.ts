// The browser reaches the API directly at this URL (NEXT_PUBLIC_*, baked in at build time for
// client code) — never the in-compose-network `http://backend:8000` a server-side Next fetch would
// use. See PLANNING.md AD-7 for why the two are deliberately different.
export const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000";

// Derived, not a second env var: the live feed is the same origin as the REST API, just a
// different scheme (ws/wss instead of http/https).
export const WS_BASE_URL = API_BASE_URL.replace(/^http/, "ws");
