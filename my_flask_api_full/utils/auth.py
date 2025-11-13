# utils/auth.py
import os, time
from functools import wraps
from typing import Dict, Any, Optional
import requests
from requests.adapters import HTTPAdapter, Retry
import jwt
from jwcrypto import jwk
from flask import request, jsonify, g

GOOGLE_JWKS_URL  = os.getenv("GOOGLE_JWKS_URL", "https://www.googleapis.com/oauth2/v3/certs")
# Accept either issuer form (Google uses both in the wild)
GOOGLE_ISSUERS   = {
    "https://accounts.google.com",
    "accounts.google.com",
}
# Comma-separated list of allowed client IDs (audiences)
_GOOGLE_AUDIENCES = os.getenv("GOOGLE_AUDIENCES") or os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_AUDIENCES  = [a.strip() for a in _GOOGLE_AUDIENCES.split(",") if a.strip()]

# Optional demo/static token for local tests (bypasses Google)
DEMO_BEARER = os.getenv("API_BEARER_TOKEN")

_JWKS_CACHE: Dict[str, Any] = {"keys": {}, "fetched_at": 0, "ttl": 300}

# --- HTTP session with retries ---
_session = requests.Session()
_retries = Retry(
    total=3,
    backoff_factor=0.3,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)
_session.mount("https://", HTTPAdapter(max_retries=_retries))

def _load_keys(force: bool = False) -> Dict[str, jwk.JWK]:
    now = time.time()
    if not force and _JWKS_CACHE["keys"] and (now - _JWKS_CACHE["fetched_at"] < _JWKS_CACHE["ttl"]):
        return _JWKS_CACHE["keys"]

    resp = _session.get(GOOGLE_JWKS_URL, timeout=5)
    resp.raise_for_status()
    payload = resp.json()

    keys = {}
    for k in payload.get("keys", []):
        keys[k["kid"]] = jwk.JWK(**k)

    # respect Cache-Control max-age if present
    ttl = 300
    cc = resp.headers.get("Cache-Control", "")
    # crude parse for max-age
    for part in cc.split(","):
        part = part.strip().lower()
        if part.startswith("max-age="):
            try:
                ttl = max(60, int(part.split("=", 1)[1]))
            except ValueError:
                pass

    _JWKS_CACHE.update({"keys": keys, "fetched_at": now, "ttl": ttl})
    return keys

def _verify_google_jwt(token: str) -> Dict[str, Any]:
    header = jwt.get_unverified_header(token)
    if header.get("alg") != "RS256":
        raise jwt.InvalidAlgorithmError("Invalid alg; expected RS256")

    keys = _load_keys()
    key = keys.get(header.get("kid"))
    if not key:
        # key rotation: force refresh once
        keys = _load_keys(force=True)
        key = keys.get(header.get("kid"))
        if not key:
            raise jwt.InvalidKeyError("Signing key not found (kid)")

    public_pem = key.export_to_pem(private_key=False, password=None)

    # Validate with small leeway for clock skew
    claims = jwt.decode(
        token,
        public_pem,
        algorithms=["RS256"],
        audience=GOOGLE_AUDIENCES or None,  # if not set, PyJWT will not enforce aud
        issuer=list(GOOGLE_ISSUERS),
        options={"require": ["exp", "iat"], "leeway": 10},
    )

    # Optional: enforce email_verified or domain restrictions
    # if os.getenv("REQUIRE_EMAIL_VERIFIED") == "1" and not claims.get("email_verified"):
    #     raise jwt.InvalidTokenError("Email not verified")
    # if (hd := os.getenv("GOOGLE_HD")) and claims.get("hd") != hd:
    #     raise jwt.InvalidTokenError("Hosted domain mismatch")

    return claims

def require_auth(fn):
    @wraps(fn)
    def wrapped(*args, **kwargs):
        auth = request.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            return jsonify({"error": True, "message": "Missing bearer token"}), 401

        token = auth.split(" ", 1)[1].strip()

        # Demo/static token path (useful in local/dev or during class demo)
        if DEMO_BEARER and token == DEMO_BEARER:
            g.user = {"sub": "demo", "iss": "local", "aud": "demo", "scopes": ["*"]}
            return fn(*args, **kwargs)

        try:
            claims = _verify_google_jwt(token)
            g.user = claims
            return fn(*args, **kwargs)
        except requests.RequestException as e:
            # network problem fetching JWKS -> treat as 503 auth provider unavailable
            return jsonify({"error": True, "message": "IdP unavailable", "detail": str(e)}), 503
        except jwt.ExpiredSignatureError:
            return jsonify({"error": True, "message": "Token expired"}), 401
        except jwt.InvalidAudienceError:
            return jsonify({"error": True, "message": "Invalid audience"}), 401
        except jwt.InvalidIssuerError:
            return jsonify({"error": True, "message": "Invalid issuer"}), 401
        except jwt.PyJWTError as e:
            # any other JWT error
            return jsonify({"error": True, "message": "Invalid token", "detail": str(e)}), 401
        except Exception as e:
            return jsonify({"error": True, "message": "Auth error", "detail": str(e)}), 401

    return wrapped
