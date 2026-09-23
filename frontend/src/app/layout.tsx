import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "monitoring-failure-prediction",
  description: "Real-time monitoring failure prediction dashboard.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
