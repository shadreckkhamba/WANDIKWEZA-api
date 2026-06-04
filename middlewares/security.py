import hmac
import logging

from flask import jsonify, request
from werkzeug.middleware.proxy_fix import ProxyFix

from utils.config import as_bool


logger = logging.getLogger(__name__)

DEFAULT_API_KEY_HEADER = "X-API-Key"
DEFAULT_EXEMPT_PATHS = ["/rr/health/"]


def _normalize_exempt_paths(value):
    if isinstance(value, list):
        return value
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return list(DEFAULT_EXEMPT_PATHS)


def _path_is_exempt(path, exempt_paths):
    for exempt_path in exempt_paths:
        if not exempt_path:
            continue

        if exempt_path.endswith("*"):
            if path.startswith(exempt_path[:-1]):
                return True
            continue

        if path == exempt_path:
            return True

    return False


def _extract_supplied_api_key(header_name):
    raw_value = request.headers.get(header_name, "").strip()

    if header_name.lower() == "authorization" and raw_value.lower().startswith("bearer "):
        return raw_value[7:].strip()

    if raw_value:
        return raw_value

    authorization = request.headers.get("Authorization", "").strip()
    if authorization.lower().startswith("bearer "):
        return authorization[7:].strip()

    return ""

def configure_security(app, full_config):
    app_config = full_config.get("app", {})
    security_config = full_config.get("security", {})

    if as_bool(app_config.get("TRUST_PROXY_HEADERS"), False):
        app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

    require_api_key = as_bool(security_config.get("require_api_key"), False)
    enforce_https = as_bool(security_config.get("enforce_https"), False)
    expected_api_key = (security_config.get("api_key") or "").strip()
    api_key_header = security_config.get("api_key_header", DEFAULT_API_KEY_HEADER)
    exempt_paths = _normalize_exempt_paths(security_config.get("exempt_paths"))

    @app.before_request
    def enforce_security_controls():
        if request.method == "OPTIONS":
            return None

        if _path_is_exempt(request.path, exempt_paths):
            return None

        if enforce_https and not request.is_secure:
            logger.warning("Rejected insecure request for path %s", request.path)
            return jsonify({"error": "HTTPS is required"}), 403

        if require_api_key:
            if not expected_api_key:
                logger.error("API key enforcement is enabled but no API key is configured.")
                return jsonify({"error": "Server security is misconfigured"}), 500

            supplied_api_key = _extract_supplied_api_key(api_key_header)
            if not hmac.compare_digest(supplied_api_key, expected_api_key):
                logger.warning("Rejected unauthorized request for path %s", request.path)
                return jsonify({"error": "Unauthorized"}), 401

        return None

    @app.after_request
    def set_security_headers(response):
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("X-Frame-Options", "DENY")

        if enforce_https and request.is_secure:
            response.headers.setdefault(
                "Strict-Transport-Security",
                "max-age=31536000; includeSubDomains"
            )

        return response
