# Truecaller API (Local)

Local Truecaller API server — number search, OTP login, token management & refresh. Zero dependencies (pure Node.js).

> **Port:** `8217` (80217 valid nahi hai, max port 65535 hota hai) — `PORT` env se change kar sakte ho.

## Quick Start

```bash
node server.js
```

| URL | Kya hai |
|---|---|
| http://localhost:8217/ui | Web UI (search + token management) |
| http://localhost:8217/ | JSON docs |
| http://localhost:8217/health | Health check |

## Endpoints

### Search (number details)

```bash
# simple (default)
curl "http://localhost:8217/search?q=9334120373&countryCode=IN"

# full raw response
curl "http://localhost:8217/search?q=+919334120373&countryCode=IN&format=full"
```

Number formats jo chalte hain: `9334120373`, `09334120373`, `+919334120373`, `919334120373`, `00919334120373`

Response (simple):

```json
{
  "ok": true,
  "query": { "q": "9334120373", "countryCode": "IN" },
  "provider": "ss-nu",
  "result": {
    "found": true,
    "name": "Ardnivak Ramuk",
    "phone": { "e164": "+919334120373", "carrier": "Jio", "numberType": "MOBILE" },
    "address": { "city": "Bihar", "country": "IN", "timeZone": "+05:30" },
    "spam": { "isFraud": false, "spamScore": null, "spamType": null, "reports": null },
    "badges": ["user"],
    "email": null,
    "image": null
  }
}
```

### Bulk Search

```bash
curl "http://localhost:8217/search/bulk?q=9334120373,919876543210&countryCode=IN"
```

Max 10 numbers, comma-separated.

### Auth — OTP Login (naya token)

```bash
# 1. OTP bhejo (requestId milega)
curl -X POST http://localhost:8217/auth/send-otp \
  -H "Content-Type: application/json" \
  -d '{"phoneNumber": "+919999999999", "countryCode": "IN"}'

# 2. OTP verify karo (installationId token save ho jayega)
curl -X POST http://localhost:8217/auth/verify-otp \
  -H "Content-Type: application/json" \
  -d '{"phoneNumber": "+919999999999", "requestId": "<step-1 ka requestId>", "otp": "123456"}'
```

### Token Management

```bash
# status (masked)
curl http://localhost:8217/auth/token

# live validity check ke saath
curl "http://localhost:8217/auth/token?live=true"

# manual token set (HAR file se mila hua installationId)
curl -X POST http://localhost:8217/auth/token \
  -H "Content-Type: application/json" \
  -d '{"installationId": "a2i0t-xxxx..."}'

# refresh / validate
curl -X POST http://localhost:8217/auth/refresh

# logout (token delete)
curl -X DELETE http://localhost:8217/auth/token
```

**Token expiry:** Truecaller app-flow mein koi silent refresh endpoint nahi hai. Token expire hone par (`401 TOKEN_EXPIRED`) dobara OTP login karo — UI mein Login button ya `POST /auth/send-otp`.

## Token kahan se milega

1. **OTP login** (recommended) — `/auth/send-otp` → `/auth/verify-otp`
2. **HAR capture** — HTTPToolkit se Truecaller app ka traffic capture karo, `Authorization: Bearer xxx` header se token copy karo, phir `POST /auth/token` se set karo
3. Token `data/token.json` mein save hota hai (gitignored)

## Files

```
server.js            # HTTP server + routes
lib/truecaller.js   # Truecaller client (login/search/bulk/validate)
lib/countries.js    # Country codes + phone normalizer
lib/store.js        # Token persistence (data/token.json)
public/index.html   # Web UI
data/token.json     # Saved token (auto-created)
```

## Notes

- Ye **unofficial** API hai — Truecaller ke Android app endpoints use hoti hain. Heavy use pe account block ho sakta hai.
- Response `Accept: application/json` header ke saath jata hai (protobuf nahi).
- `search5-noneu.truecaller.com/v2/search` wahi endpoint hai jo app use karta hai.
