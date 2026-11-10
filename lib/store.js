// Token persistence - data/token.json
import { readFile, writeFile, mkdir, unlink } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_DIR = path.join(__dirname, '..', 'data');
const TOKEN_FILE = path.join(DATA_DIR, 'token.json');

export async function ensureDataDir() {
  await mkdir(DATA_DIR, { recursive: true });
}

export async function loadToken() {
  try {
    const raw = await readFile(TOKEN_FILE, 'utf8');
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

export async function saveToken(token) {
  await ensureDataDir();
  const existing = (await loadToken()) || {};
  const merged = { ...existing, ...token, savedAt: new Date().toISOString() };
  delete merged.valid; // recompute on demand
  await writeFile(TOKEN_FILE, JSON.stringify(merged, null, 2), 'utf8');
  return merged;
}

export async function clearToken() {
  try {
    await unlink(TOKEN_FILE);
    return true;
  } catch {
    return false;
  }
}

export function maskToken(installationId) {
  if (!installationId) return null;
  const t = String(installationId);
  if (t.length <= 12) return `${t.slice(0, 4)}****`;
  return `${t.slice(0, 8)}...${t.slice(-6)}`;
}

// Public view of the stored token (no secrets in full)
export function tokenStatus(token) {
  if (!token || !token.installationId) {
    return { configured: false, message: 'Koi token configured nahi hai. Login karo ya manual set karo.' };
  }
  return {
    configured: true,
    installationId: maskToken(token.installationId),
    source: token.source || 'unknown',
    userId: token.userId ?? null,
    phoneNumber: token.phoneNumber ?? null,
    savedAt: token.savedAt || null,
    lastVerifiedAt: token.lastVerifiedAt || null,
    ttl: token.ttl ?? null,
  };
}
