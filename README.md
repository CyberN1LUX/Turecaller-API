# HITEK + Truecaller Data Gateway

Single-file Python API (`main.py`) — FastAPI + DuckDB over the public HuggingFace
ICMR/HITEK bucket, merged with the Truecaller caller-id search.

## Quick start (one command)

```bash
chmod +x install.sh
./install.sh
```

`install.sh` ek hi baar mein:

1. `python3` check karta hai
2. `.venv` banata hai
3. `pip` upgrade + `requirements.txt` install karta hai
4. root par **systemd service** (`Restart=always`) install karke non-stop deploy karta hai
5. normal user par foreground **auto-restart loop** chalata hai

Default: `0.0.0.0:17712` (code ka port). Override:

```bash
PORT=8080 HOST=0.0.0.0 ./install.sh
INSTALL_SERVICE=0 ./install.sh     # systemd skip, foreground loop
```

Manual run:

```bash
pip install -r requirements.txt
python main.py
```

## Endpoints

| URL | Kya hai |
|---|---|
| `GET /` | Landing page + search box |
| `GET /FetchData?Number=9334120373` | DB records + Truecaller, ek hi JSON |
| `GET /search?q=...` | Unified search (phone / aadhar / name) |
| `POST /search/parallel` | Batch search |
| `GET /callapp?Number=...` | CallApp lookup (capture ke bina disabled) |
| `GET /health` | Status |
| `GET /docs` | Swagger UI |

### Merged `/FetchData` response

`Data` (raw DB rows) pehle aata hai, uske baad `Truecaller` — ek hi response
mein, koi alag section/endpoint nahi:

```json
{
  "status": "success",
  "phone": "9334120373",
  "Data": {
    "Main_Records": [ ... ],
    "Alt_Records": [ ... ]
  },
  "Truecaller": {
    "ok": true,
    "status": 200,
    "data": { "data": [ { "name": "..." } ] }
  },
  "meta": { "...": "..." },
  "Developer": "cybern1lux"
}
```

Truecaller part band karna ho: `?truecaller=false`.

## Truecaller token

Token `main.py` ke andar `TRUECALLER_TOKEN` mein embedded hai. Expire hone par
(401/403) naya token daalna padega — Truecaller app-flow mein silent refresh
endpoint nahi hota. Env override: `TRUECALLER_TOKEN`.

## CallApp

Diya gaya HAR sirf ad-SDK traffic contain karta hai (CallApp ka search request
capture nahi hua). Jab asli request mil jaye to `CALLAPP_SEARCH_URL` (template
with `{number}` / `{e164}` / `{countryCode}`) aur `CALLAPP_HEADERS` (JSON) set
karo — `/callapp` activate ho jayega.

## Non-stop deploy (systemd)

```bash
sudo ./install.sh
systemctl status hitek-truecaller
journalctl -u hitek-truecaller -f
```

## Notes

- Ye unofficial endpoints hain; heavy use pe Truecaller account block ho sakta hai.
- Remote DuckDB lookup pehli baar 30–90s le sakta hai, phir cache hota hai.
- Port/host env se change hote hain: `PORT`, `HOST`.
