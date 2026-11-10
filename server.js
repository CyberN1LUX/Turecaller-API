// Truecaller API Server - localhost:8217 (PORT env se override kar sakte ho)
// Zero-dependency Node.js server (node server.js)
import http from 'node:http';
import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  sendOtp, verifyOtp, searchNumber, bulkSearch, validateToken,
} from './lib/truecaller.js';
import { loadToken, saveToken, clearToken, tokenStatus } from './lib/store.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT || 8217);
const PUBLIC_DIR = path.join(__dirname, 'public');

/* ---------------- helpers ---------------- */

function json(res, status, data) {
  const body = JSON.stringify(data, null, 2);
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Access-Control-Allow-Origin': '*',
  });
  res.end(body);
}

async function readBody(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  if (!chunks.length) return {};
  try {
    return JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}');
  } catch {
    throw Object.assign(new Error('Invalid JSON body'), { status: 400 });
  }
}

// Search result ko clean/simple shape mein convert karta hai
function simplify(raw) {
  const d = raw?.data?.[0];
  const base = { found: false, searchedAt: new Date().toISOString() };
  if (!d) return base;

  const phone = d.phones?.[0] || {};
  const address = d.addresses?.[0] || {};
  const email = (d.internetAddresses || []).find((i) => (i.service || '').toLowerCase() === 'email');

  return {
    ...base,
    found: true,
    id: d.id || null,
    name: d.name || null,
    altName: d.altName || null,
    about: d.about || null,
    image: d.image || null,
    gender: d.gender || null,
    score: d.score ?? null,
    access: d.access || null,
    enhanced: d.enhanced ?? null,
    spam: {
      isFraud: d.isFraud ?? false,
      feedback: d.feedback || null,
      spamScore: d.spamInfo?.spamScore ?? phone.spamScore ?? null,
      spamType: d.spamInfo?.spamType ?? phone.spamType ?? null,
      reports: d.spamInfo?.spamStats?.numReports ?? null,
    },
    phone: {
      e164: phone.e164Format || null,
      national: phone.nationalFormat || null,
      numberType: phone.numberType || null,
      carrier: phone.carrier || null,
      dialingCode: phone.dialingCode ?? null,
      countryCode: phone.countryCode || null,
    },
    address: {
      city: address.city || null,
      country: address.countryCode || null,
      timeZone: address.timeZone || null,
      full: address.address || null,
    },
    email: email?.id || null,
    internetAddresses: d.internetAddresses || [],
    badges: d.badges || [],
    electedTags: d.electedTags || [],
    imId: d.imId || null,
  };
}

// Stored token lo; nahi hai to error
async function requireToken() {
  const token = await loadToken();
  if (!token?.installationId) {
    return {
      error: {
        code: 'NO_TOKEN',
        message: 'Pehle login karo: POST /auth/send-otp → POST /auth/verify-otp, ya manual token set karo: POST /auth/token',
      },
    };
  }
  return { token };
}

/* ---------------- routes ---------------- */

async function handleApi(req, res, url) {
  const { pathname } = url;
  const method = req.method;

  /* ---- health ---- */
  if (method === 'GET' && pathname === '/health') {
    const token = await loadToken();
    return json(res, 200, {
      ok: true,
      service: 'truecaller-api',
      uptimeSec: Math.round(process.uptime()),
      tokenConfigured: Boolean(token?.installationId),
    });
  }

  /* ---- home / docs ---- */
  if (method === 'GET' && (pathname === '/' || pathname === '/api')) {
    return json(res, 200, {
      ok: true,
      service: 'truecaller-api',
      endpoints: {
        'GET  /health': 'Server status',
        'GET  /': 'Ye JSON docs (UI ke liye /ui kholo)',
        'GET  /ui': 'Web search UI',
        'POST /auth/send-otp': '{ "phoneNumber": "+919334120373", "countryCode": "IN" } → requestId',
        'POST /auth/verify-otp': '{ "phoneNumber", "countryCode", "requestId", "otp": "123456" } → installationId save',
        'GET  /auth/token': 'Token status (masked)',
        'POST /auth/token': '{ "installationId": "..." } manual set',
        'POST /auth/refresh': 'Token validity check + update',
        'DELETE /auth/token': 'Token delete (logout)',
        'GET  /search?q=9334120373&countryCode=IN': 'Number search (format=full|simple)',
        'GET  /search/bulk?q=+91...,+91...&countryCode=IN': 'Bulk search (max 10 numbers)',
      },
    });
  }

  /* ---- auth: send otp ---- */
  if (method === 'POST' && pathname === '/auth/send-otp') {
    const body = await readBody(req);
    if (!body.phoneNumber) return json(res, 400, { ok: false, message: 'phoneNumber required hai' });
    const result = await sendOtp({ phoneNumber: body.phoneNumber, countryCode: body.countryCode || 'IN' });
    return json(res, result.ok ? 200 : 400, result);
  }

  /* ---- auth: verify otp ---- */
  if (method === 'POST' && pathname === '/auth/verify-otp') {
    const body = await readBody(req);
    if (!body.phoneNumber || !body.requestId || !body.otp) {
      return json(res, 400, { ok: false, message: 'phoneNumber, requestId aur otp required hain' });
    }
    const result = await verifyOtp({
      phoneNumber: body.phoneNumber,
      countryCode: body.countryCode || 'IN',
      requestId: body.requestId,
      otp: body.otp,
    });
    if (result.ok) {
      await saveToken({
        installationId: result.installationId,
        userId: result.userId,
        ttl: result.ttl,
        phoneNumber: body.phoneNumber,
        source: 'otp-login',
      });
    }
    return json(res, result.ok ? 200 : 400, result);
  }

  /* ---- auth: token status ---- */
  if (method === 'GET' && pathname === '/auth/token') {
    const token = await loadToken();
    const withLive = url.searchParams.get('live') === 'true';
    const status = tokenStatus(token);
    if (withLive && token?.installationId) {
      const v = await validateToken(token.installationId);
      status.valid = v.valid;
      status.checkedAt = new Date().toISOString();
      if (v.valid) {
        await saveToken({ lastVerifiedAt: new Date().toISOString() });
      }
    }
    return json(res, 200, { ok: true, token: status });
  }

  /* ---- auth: manual token set ---- */
  if (method === 'POST' && pathname === '/auth/token') {
    const body = await readBody(req);
    if (!body.installationId) {
      return json(res, 400, { ok: false, message: 'installationId required hai' });
    }
    // set karne se pehle validate karo
    const v = await validateToken(body.installationId);
    if (v.valid === false) {
      return json(res, 400, { ok: false, message: 'Ye token Truecaller pe valid nahi hai (401)', code: 'TOKEN_INVALID' });
    }
    await saveToken({
      installationId: body.installationId,
      source: body.source || 'manual',
      lastVerifiedAt: new Date().toISOString(),
    });
    return json(res, 200, { ok: true, message: 'Token set ho gaya', token: tokenStatus(await loadToken()) });
  }

  /* ---- auth: refresh / validate ---- */
  if (method === 'POST' && pathname === '/auth/refresh') {
    const { token, error } = await requireToken();
    if (error) return json(res, 400, { ok: false, ...error });

    const v = await validateToken(token.installationId);
    if (v.valid === true) {
      await saveToken({ lastVerifiedAt: new Date().toISOString(), valid: true });
      return json(res, 200, {
        ok: true,
        valid: true,
        message: 'Token abhi valid hai, refresh ki zaroorat nahi',
        token: tokenStatus(await loadToken()),
      });
    }
    if (v.valid === false) {
      return json(res, 401, {
        ok: false,
        valid: false,
        code: 'TOKEN_EXPIRED',
        message: 'Token expire ho gaya. Fresh token ke liye dobara login karo: POST /auth/send-otp',
      });
    }
    return json(res, 502, { ok: false, valid: null, message: 'Truecaller se validate nahi kar paye, thodi der baad try karo' });
  }

  /* ---- auth: logout ---- */
  if (method === 'DELETE' && pathname === '/auth/token') {
    const removed = await clearToken();
    return json(res, 200, { ok: true, message: removed ? 'Token delete ho gaya' : 'Koi token tha hi nahi' });
  }

  /* ---- search ---- */
  if (method === 'GET' && pathname === '/search') {
    const q = url.searchParams.get('q');
    if (!q) return json(res, 400, { ok: false, message: 'q parameter required hai (phone number)' });
    const countryCode = (url.searchParams.get('countryCode') || 'IN').toUpperCase();
    const format = url.searchParams.get('format') || 'simple';

    let { token, error } = await requireToken();
    if (error) return json(res, 400, { ok: false, ...error });

    let result = await searchNumber({ q, countryCode, installationId: token.installationId });

    // auto-retry: agar token 401 aaya aur file mein token update hua ho (external refresh),
    // ek baar naya token load karke phir se try karo
    if (result.code === 'TOKEN_EXPIRED') {
      const fresh = await loadToken();
      if (fresh?.installationId && fresh.installationId !== token.installationId) {
        result = await searchNumber({ q, countryCode, installationId: fresh.installationId });
      }
    }

    if (!result.ok) {
      const status = result.code === 'TOKEN_EXPIRED' ? 401 : 502;
      return json(res, status, { ok: false, code: result.code, message: result.message });
    }

    const payload = {
      ok: true,
      query: { q, countryCode },
      provider: result.data?.provider || null,
    };
    if (format === 'full') payload.result = result.data;
    else payload.result = simplify(result.data);
    return json(res, 200, payload);
  }

  /* ---- bulk search ---- */
  if (method === 'GET' && pathname === '/search/bulk') {
    const q = url.searchParams.get('q');
    if (!q) return json(res, 400, { ok: false, message: 'q parameter required hai (comma-separated numbers)' });
    const countryCode = (url.searchParams.get('countryCode') || 'IN').toUpperCase();
    const format = url.searchParams.get('format') || 'simple';

    const { token, error } = await requireToken();
    if (error) return json(res, 400, { ok: false, ...error });

    const result = await bulkSearch({ q, countryCode, installationId: token.installationId });
    if (!result.ok) {
      const status = result.code === 'TOKEN_EXPIRED' ? 401 : result.status >= 400 ? result.status : 502;
      return json(res, status, { ok: false, code: result.code, message: result.message });
    }

    const payload = { ok: true, query: { q, countryCode }, provider: result.data?.provider || null };
    if (format === 'full') payload.result = result.data;
    else payload.result = (result.data?.data || []).map((d) => {
      const s = simplify({ data: [d.value ?? d] });
      s.key = d.key || null;
      return s;
    });
    return json(res, 200, payload);
  }

  /* ---- 404 ---- */
  return json(res, 404, { ok: false, message: `Route nahi mila: ${method} ${pathname}` });
}

/* ---------------- static ui ---------------- */

const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.css': 'text/css', '.png': 'image/png', '.svg': 'image/svg+xml' };

async function serveStatic(res, file) {
  try {
    const content = await readFile(path.join(PUBLIC_DIR, file));
    res.writeHead(200, {
      'Content-Type': MIME[path.extname(file)] || 'text/plain; charset=utf-8',
      'Cache-Control': 'no-store',
    });
    res.end(content);
  } catch {
    json(res, 404, { ok: false, message: 'File nahi mili' });
  }
}

/* ---------------- server ---------------- */

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, `http://localhost:${PORT}`);
  try {
    if (req.method === 'OPTIONS') {
      res.writeHead(204, {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET,POST,DELETE,OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
      });
      return res.end();
    }
    if (url.pathname === '/ui' || url.pathname === '/index.html') return serveStatic(res, 'index.html');
    if (url.pathname.startsWith('/api/') || url.pathname.startsWith('/auth') || url.pathname.startsWith('/search') || url.pathname === '/health' || url.pathname === '/' || url.pathname === '/api') {
      return await handleApi(req, res, url);
    }
    return json(res, 404, { ok: false, message: `Route nahi mila: ${req.method} ${url.pathname}` });
  } catch (err) {
    if (err.status) return json(res, err.status, { ok: false, message: err.message });
    json(res, 500, { ok: false, message: err.message || 'Server error' });
  }
});

server.listen(PORT, () => {
  console.log('┌─────────────────────────────────────────────┐');
  console.log('│  Truecaller API Server                      │');
  console.log(`│  UI      :  http://localhost:${PORT}/ui        │`);
  console.log(`│  Docs    :  http://localhost:${PORT}/          │`);
  console.log(`│  Health  :  http://localhost:${PORT}/health   │`);
  console.log(`│  Search  :  http://localhost:${PORT}/search?q=9334120373&countryCode=IN`);
  console.log('└─────────────────────────────────────────────┘');
});
