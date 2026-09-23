import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  // PLANNING.md Slice 6 AD-32: the prod Docker image copies only this standalone server output
  // (plus .next/static) rather than the full node_modules tree — no effect on `next dev`.
  output: "standalone",
};

export default nextConfig;
