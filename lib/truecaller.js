// Truecaller unofficial API client (app-flow endpoints, same as Android app uses)
import { normalizePhone, getCountry } from './countries.js';

const USER_AGENT = 'Truecaller/26.43.5 (Android;10)';
const CLIENT_SECRET = 'lvc22mp3l1sfv6ujg83rd17btt'; // public client secret used by the app

const BASE = {
  sendOtp: 'https://account-asia-south1.truecaller.com/v2/sendOnboardingOtp',
  verifyOtp: 'https://account-asia-south1.truecaller.com/v1/verifyOnboardingOtp',
  search: 'https://search5-noneu.truecaller.com/v2/search',
  bulk: 'https://search5-noneu.truecaller.com/v2/bulk',
};

function randomDeviceId(length = 16) {
  const chars = 'abcdefghijklmnopqrstuvwxyz0123456789';
  let out = '';
  for (let i = 0; i < length; i++) out += chars[Math.floor(Math.random() * chars.length)];
  return out;
}

function randomUuid() {
  return crypto.randomUUID();
}

async function jsonFetch(url, options = {}) {
  const res = await fetch(url, {
    ...options,
    headers: {
      'content-type': 'application/json; charset=UTF-8',
      'accept-encoding': 'gzip',
      'user-agent': USER_AGENT,
      ...(options.headers || {}),
    },
  });
  let body = null;
  const text = await res.text();
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = text;
  }
  return { status: res.status, ok: res.ok, body };
}

/* ------------------------------------------------------------------ */
/*  AUTH - OTP login (installationId token generate karne ke liye)     */
/* ------------------------------------------------------------------ */

/**
 * Step 1: OTP bhejo. Response mein `requestId` aata hai.
 * @param {{ phoneNumber: string, countryCode?: string }} input
 */
export async function sendOtp({ phoneNumber, countryCode = 'IN' }) {
  const pn = normalizePhone(phoneNumber, countryCode);
  const country = getCountry(countryCode);
  if (!country) {
    return { ok: false, error: `Unsupported country code: ${countryCode}` };
  }

  const payload = {
    countryCode: pn.countryCode,
    dialingCode: pn.dialingCode,
    installationDetails: {
      app: { buildVersion: 5, majorVersion: 11, minorVersion: 7, store: 'GOOGLE_PLAY' },
      device: {
        deviceId: randomDeviceId(),
        language: 'en',
        manufacturer: 'Xiaomi',
        model: 'M2010J19SG',
        osName: 'Android',
        osVersion: '10',
        mobileServices: ['GMS'],
      },
      language: 'en',
    },
    phoneNumber: pn.national,
    region: 'region-2',
    sequenceNo: 2,
  };

  const { status, body } = await jsonFetch(BASE.sendOtp, {
    method: 'POST',
    headers: { clientsecret: CLIENT_SECRET },
    body: JSON.stringify(payload),
  });

  if (status === 200 && (body?.status === 1 || body?.status === 9)) {
    return {
      ok: true,
      status: body.status,
      message: `OTP sent to +${country.dial} ${pn.national}`,
      requestId: body.requestId,
      tokenTtl: body.tokenTtl,
      raw: body,
    };
  }

  return {
    ok: false,
    status: body?.status ?? status,
    message: body?.message || 'OTP bhejne mein problem aayi (number/country check karo)',
    raw: body,
  };
}

/**
 * Step 2: OTP verify karo. Success pe installationId (token) milta hai.
 * @param {{ phoneNumber: string, countryCode?: string, requestId: string, otp: string }} input
 */
export async function verifyOtp({ phoneNumber, countryCode = 'IN', requestId, otp }) {
  const pn = normalizePhone(phoneNumber, countryCode);
  const country = getCountry(countryCode);
  if (!country) {
    return { ok: false, error: `Unsupported country code: ${countryCode}` };
  }

  const payload = {
    countryCode: pn.countryCode,
    dialingCode: pn.dialingCode,
    phoneNumber: pn.national,
    requestId,
    token: String(otp),
  };

  const { status, body } = await jsonFetch(BASE.verifyOtp, {
    method: 'POST',
    headers: { clientsecret: CLIENT_SECRET },
    body: JSON.stringify(payload),
  });

  if (status === 200 && body?.status === 2 && body?.installationId && !body?.suspended) {
    return {
      ok: true,
      message: 'Login successful! Token saved.',
      installationId: body.installationId,
      userId: body.userId,
      ttl: body.ttl,
      phones: body.phones,
      raw: body,
    };
  }

  const msg =
    body?.status === 11 ? 'Invalid OTP' :
    body?.status === 7 ? 'OTP retries limit exceed ho gayi' :
    body?.suspended ? 'Account suspended hai' :
    body?.message || 'OTP verification fail ho gayi';

  return { ok: false, status: body?.status ?? status, message: msg, raw: body };
}

/* ------------------------------------------------------------------ */
/*  SEARCH                                                             */
/* ------------------------------------------------------------------ */

/**
 * Number search. 401/403 = token invalid/expired.
 * @param {{ q: string, countryCode?: string, installationId: string, type?: number }} input
 */
export async function searchNumber({ q, countryCode = 'IN', installationId, type = 4 }) {
  const pn = normalizePhone(q, countryCode);
  const url = new URL(BASE.search);
  url.searchParams.set('q', pn.national);
  url.searchParams.set('countryCode', pn.countryCode);
  url.searchParams.set('type', String(type));
  url.searchParams.set('locAddr', '');
  url.searchParams.set('placement', 'SEARCHRESULTS,HISTORY,DETAILS');
  url.searchParams.set('encoding', 'json');

  const { status, body } = await jsonFetch(url.toString(), {
    headers: {
      Authorization: `Bearer ${installationId}`,
      Accept: 'application/json',
    },
  });

  if (status === 401 || status === 403) {
    return { ok: false, code: 'TOKEN_EXPIRED', status, message: 'Token invalid ya expired hai. Re-login karo (POST /auth/send-otp).' };
  }
  if (!status || status >= 500) {
    return { ok: false, code: 'UPSTREAM_ERROR', status, message: 'Truecaller server error', raw: body };
  }
  if (status !== 200) {
    return { ok: false, code: 'SEARCH_FAILED', status, message: 'Search fail hui', raw: body };
  }

  return { ok: true, status, data: body };
}

/**
 * Bulk search - comma separated numbers.
 */
export async function bulkSearch({ q, countryCode = 'IN', installationId }) {
  const numbers = String(q)
    .split(',')
    .map((n) => n.trim())
    .filter(Boolean);
  if (!numbers.length) return { ok: false, code: 'BAD_REQUEST', status: 400, message: 'q mein comma-separated numbers do' };
  if (numbers.length > 10) return { ok: false, code: 'BAD_REQUEST', status: 400, message: 'Max 10 numbers per request' };

  // e164 with + form (proven from app traffic)
  const normalized = numbers.map((n) => normalizePhone(n, countryCode).e164).join(',');
  const pn0 = normalizePhone(numbers[0], countryCode);

  const url = new URL(BASE.bulk);
  url.searchParams.set('q', normalized);
  url.searchParams.set('countryCode', pn0.countryCode);
  url.searchParams.set('type', '14');
  url.searchParams.set('placement', 'SEARCHRESULTS,HISTORY,DETAILS');
  url.searchParams.set('encoding', 'json');

  const { status, body } = await jsonFetch(url.toString(), {
    headers: {
      Authorization: `Bearer ${installationId}`,
      Accept: 'application/json',
    },
  });

  if (status === 401 || status === 403) {
    return { ok: false, code: 'TOKEN_EXPIRED', status, message: 'Token invalid ya expired hai. Re-login karo (POST /auth/send-otp).' };
  }
  if (status !== 200) {
    return { ok: false, code: 'SEARCH_FAILED', status, message: 'Bulk search fail hui', raw: body };
  }

  return { ok: true, status, data: body };
}

/* ------------------------------------------------------------------ */
/*  TOKEN VALIDITY / REFRESH                                           */
/* ------------------------------------------------------------------ */

/**
 * Stored token ko Truecaller pe ek light search se validate karta hai.
 * 200 => token valid, 401/403 => expired (re-login chahiye).
 * Note: Truecaller app-flow mein koi silent refresh endpoint nahi hai;
 * token expire hone pe dobara OTP login karna padta hai.
 */
export async function validateToken(installationId) {
  const { status } = await jsonFetch(BASE.search + '?q=1234567890&countryCode=IN&type=4&encoding=json', {
    headers: {
      Authorization: `Bearer ${installationId}`,
      Accept: 'application/json',
    },
  });
  if (status === 401 || status === 403) {
    return { valid: false, status };
  }
  if (status === 200) {
    return { valid: true, status };
  }
  return { valid: null, status, message: 'Validate nahi kar paye (network/upstream)' };
}

export { randomUuid };
