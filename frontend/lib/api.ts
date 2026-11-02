export const API = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';
export async function request<T>(path: string, opts: RequestInit = {}): Promise<T> {
  const form = opts.body instanceof FormData;
  const res = await fetch(`${API}/api${path}`, { credentials: 'include', cache: 'no-store', ...opts,
    headers: { ...(form ? {} : { 'Content-Type': 'application/json' }), 'X-Requested-With': 'openlake-web', ...opts.headers } });
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(typeof body.detail === 'string' ? body.detail : `Request failed (${res.status})`);
  }
  return res.json() as Promise<T>;
}
export type Dataset = { id: string; name: string; filename: string; rows: number; columns: {name:string;type:string}[]; created_at:string };
export type QueryResult = { columns: string[]; rows: (string|number|null|boolean)[][]; count:number; truncated_at:number; engine?:string; cached?:boolean };
export type AIConfig = {provider:string;model:string;configured:boolean};
