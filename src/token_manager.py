
import os
import base64
import requests
from utils.logging import get_logger

def _create_request_params(refresh_token: str) -> tuple[dict, dict]:
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }

    credentials = f"{os.environ.get('CLIENT_ID')}:{os.environ.get('CLIENT_SECRET')}"
    encoded = base64.b64encode(credentials.encode()).decode()
    headers = {"Authorization": f"Basic {encoded}"}

    return data, headers

def _process_token_response(response: requests.Response) -> str:
    try:
        updated_token_data = response.json()

    except requests.exceptions.JSONDecodeError as e:
        raise RuntimeError("Malformed JSON response - cannot be parsed") from e

    if "access_token" not in updated_token_data:
        raise RuntimeError(f"Token refresh failed: Access token missing from Spotify response")

    access_token = updated_token_data["access_token"]

    if not isinstance(access_token, str) or not access_token.strip():
        raise ValueError("Invalid access_token (must be non-empty string)")

    return access_token

def _get_access_token(run_id: str, refresh_token: str) -> str:
    logger = get_logger(__name__, run_id)

    logger.info("Requesting new access token...")
    data, headers = _create_request_params(refresh_token)

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

    access_token = _process_token_response(response)
    return access_token

def get_auth_headers(run_id: str, refresh_token: str) -> dict:
    access_token = _get_access_token(run_id, refresh_token)
    headers = {"Authorization": f"Bearer {access_token}"}
    return headers