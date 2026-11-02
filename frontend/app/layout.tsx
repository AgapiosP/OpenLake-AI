import type { Metadata } from 'next';
import './globals.css';
export const metadata: Metadata = { title: 'OpenLake AI — Intelligent Data Workspace', description: 'Self-hosted analytics and AI for ambitious teams.' };
export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return <html lang="en"><body>{children}</body></html>;
}
