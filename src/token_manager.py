
import os
import json
from utils.paths import SRC_DIR
import time
import base64
import requests
from utils.logging import get_logger

def _load_tokens() -> dict:
    try:
        with open(SRC_DIR / "tokens.json", "r") as f:
            data = json.load(f)

    except json.JSONDecodeError as e:
        raise RuntimeError("Failed to parse JSON in tokens.json") from e

    except FileNotFoundError as e:
        raise FileNotFoundError("tokens.json not found - run auth.py first") from e

    required = {"access_token", "refresh_token", "expires_at"}
    for key in required:
        if key not in data:
            raise ValueError(f"Missing required key: {key}")

    access_token = data["access_token"]
    refresh_token = data["refresh_token"]
    expires_at = data["expires_at"]

    if not isinstance(access_token, str) or not access_token.strip():
        raise ValueError("Invalid access_token (must be non-empty string)")

    if not isinstance(refresh_token, str) or not refresh_token.strip():
        raise ValueError("Invalid refresh_token (must be non-empty string)")

    if not isinstance(expires_at, (int, float)) or isinstance(expires_at, bool) or expires_at <= 0:
        raise ValueError("Invalid expires_at (must be numeric timestamp)")

    validated_data = {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "expires_at": expires_at
    }

    return validated_data

def _create_request_params(token_data: dict) -> tuple[dict, dict]:
    refresh_token = token_data["refresh_token"]
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }

    credentials = f"{os.environ.get('CLIENT_ID')}:{os.environ.get('CLIENT_SECRET')}"
    encoded = base64.b64encode(credentials.encode()).decode()
    headers = {"Authorization": f"Basic {encoded}"}

    return data, headers

def _write_tokens(tokens_json: dict) -> None:
    try:
        with open(SRC_DIR / "tokens.json.tmp", "w") as f:
            json.dump(tokens_json, f)
    except IOError as e:
        try:
            os.remove(SRC_DIR / "tokens.json.tmp")
        except IOError:
            pass
        raise IOError("Failed to write tokens.json.tmp - cleanup attempted") from e

    try:
        os.replace(SRC_DIR / "tokens.json.tmp", SRC_DIR / "tokens.json")
    except IOError as e:
        try:
            os.remove(SRC_DIR / "tokens.json.tmp")
        except IOError:
            pass
        raise IOError("Atomic swap failed - tokens.json.tmp has correct data") from e


def _process_token_response(response: requests.Response, token_data: dict) -> str:
    try:
        updated_token_data = response.json()

    except requests.exceptions.JSONDecodeError as e:
        raise RuntimeError("Malformed JSON response - cannot be parsed") from e

    required = {"access_token", "expires_in"}
    for key in required:
        if key not in updated_token_data:
            raise RuntimeError(f"Token refresh failed: Required key ({key}) missing from Spotify response")

    access_token = updated_token_data["access_token"]
    refresh_token = updated_token_data.get("refresh_token", token_data["refresh_token"])
    expires_in = updated_token_data["expires_in"]

    if not isinstance(access_token, str) or not access_token.strip():
        raise ValueError("Invalid access_token (must be non-empty string)")

    if not isinstance(refresh_token, str) or not refresh_token.strip():
        raise ValueError("Invalid refresh_token (must be non-empty string)")

    if not isinstance(expires_in, (int, float)) or isinstance(expires_in, bool) or expires_in <= 0:
        raise ValueError("Invalid expires_in (must be numeric timestamp)")

    expires_at = time.time() + updated_token_data["expires_in"]

    tokens_json = {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "expires_at": expires_at
    }

    _write_tokens(tokens_json)

    return access_token

def _get_access_token(run_id: str) -> str:
    logger = get_logger(__name__, run_id)

    token_data = _load_tokens()
    if time.time() > token_data["expires_at"] - 60:
        logger.info("Requesting new access token...")
        data, headers = _create_request_params(token_data)

        try:
            response = requests.post(
                "https://accounts.spotify.com/api/token",
                data=data,
                headers=headers,
                timeout=5
            )

            response.raise_for_status()

        except requests.exceptions.ConnectionError as e:
            raise RuntimeError("Token refresh request failed") from e

        except requests.exceptions.Timeout as e:
            raise RuntimeError("Token refresh timed out after 5 seconds") from e

        except requests.exceptions.HTTPError as e:
            raise RuntimeError(f"Token refresh failed with HTTP error: {response.status_code}") from e

        except requests.exceptions.RequestException as e:
            raise RuntimeError("Token refresh request failed: unexpected network error") from e

        access_token = _process_token_response(response, token_data)
        return access_token

    else:
        logger.info("Using current access token...")
        access_token = token_data["access_token"]
        return access_token

def get_auth_headers(run_id: str) -> dict:
    access_token = _get_access_token(run_id)
    headers = {"Authorization": f"Bearer {access_token}"}
    return headers