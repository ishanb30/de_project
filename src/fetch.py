"""
fetch.py — Fetches recently played tracks from the Spotify Web API

Assumptions:
    1. Valid auth headers are passed in by the caller (via token_manager).

    2. The Spotify API returns a JSON object (dict) at the top level. If the
       response cannot be parsed or is not a dict, a RuntimeError is raised
       before any field access is attempted.

    3. Each item in the response must contain: track, played_at, context.
       track and played_at must be non-null. context may be null — expected
       when tracks are played directly from search with no playlist or album
       context.

    4. The Retry-After header, when present on a 429 response, is assumed to
       be a parseable numeric string (e.g. "30" or "1.5"). If absent or a
       non-numeric string is received, falls back to exponential backoff.
"""

import requests
from utils.helpers import delay_retry
from src.token_manager import get_auth_headers
import time
import uuid
from utils.logging import get_logger
from src.connector import get_connection
import snowflake.connector
import yaml
from utils.paths import DBT_DIR
from datetime import datetime, timezone, timedelta

def _validate_watermark(watermark: datetime) -> None:
    now = datetime.now(timezone.utc)
    if watermark > now:
        raise RuntimeError("Corrupted watermark: watermark is ahead of current time "
                           f"by {watermark - now}")

def _validate_recently_played(response: requests.Response) -> dict:
    try:
        data = response.json()

    except requests.exceptions.JSONDecodeError as e:
        raise RuntimeError("Malformed JSON response - cannot be parsed") from e

    if not isinstance(data, dict):
        raise RuntimeError("JSON response is not a dict - cannot be validated")

    if "items" not in data:
        raise RuntimeError("Required key (items) missing from Spotify response")

    if not data["items"]:
        return data

    items = data["items"]
    required = ["track", "played_at", "context"]
    non_null = ["track", "played_at"]
    for item in items:
        for key in required:
            if key not in item:
                raise RuntimeError(f"Required key ({key}) missing from Spotify response")
            elif key in non_null:
                if not item[key]:
                    raise ValueError(f"Missing data from {key}")

    return data

def _get_last_watermark(run_id: str, max_retries: int=3) -> datetime | None:
    logger = get_logger(__name__, run_id)

    last_exception = None
    for i in range(max_retries + 1):
        try:
            with get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "SELECT WATERMARK_TIMESTAMP FROM SPOTIFY_PIPELINE.METADATA.PIPELINE_RUNS "
                        "WHERE RUN_STATUS = 'COMPLETED' AND WATERMARK_TIMESTAMP IS NOT NULL "
                        "ORDER BY WATERMARK_TIMESTAMP DESC LIMIT 1"
                    )

                    data = cursor.fetchone()
                    if data:
                        watermark = data[0]
                        _validate_watermark(watermark)
                        logger.info(f"Last watermark: {watermark}")
                        return watermark

                    else:
                        logger.info("No previous watermark found - full fetch")

        except snowflake.connector.errors.OperationalError as e:
            delay_retry(i)
            last_exception = e
            logger.warning(f"Snowflake OperationalError on attempt {i + 1}: {last_exception}")

        except snowflake.connector.errors.DatabaseError as e:
            raise RuntimeError("Failed to read watermark from PIPELINE_RUNS via Snowflake connector") from e

        except OSError as e:
            delay_retry(i)
            last_exception = e
            logger.warning(f"OS failure on attempt {i + 1}: {last_exception}")

    else:
        if isinstance(last_exception, snowflake.connector.errors.OperationalError):
            raise RuntimeError("Max retries exhausted: Failed to read watermark "
                               "from PIPELINE_RUNS via Snowflake connector") from last_exception

        elif isinstance(last_exception, OSError):
            raise RuntimeError("Max retries exhausted: Failed to read "
                               "from Snowflake due to network error") from last_exception

        else:
            raise RuntimeError("Max retries exhausted: Unrecognised exception "
                               "type whilst retrieving watermark") from last_exception

def _get_lookback_window_mins(run_id: str) -> int:
    logger = get_logger(__name__, run_id)

    try:
        with open(DBT_DIR / 'dbt_project.yml') as f:
            lookback_window_mins = yaml.safe_load(f)["vars"]["lookback_window_mins"]
            logger.debug(f"Lookback window: {lookback_window_mins} minutes")

            return lookback_window_mins

    except KeyError as e:
        raise RuntimeError("Missing key: either vars or lookback_window_mins") from e

def get_api_data(run_id: str, refresh_token: str, max_retries: int=3) -> list:
    logger = get_logger(__name__, run_id)

    headers = get_auth_headers(run_id, refresh_token)

    watermark = _get_last_watermark(run_id)
    lookback_window_mins = _get_lookback_window_mins(run_id)

    after = None
    if watermark:
        watermark_with_lookback = watermark - timedelta(minutes=lookback_window_mins)
        after = int(watermark_with_lookback.timestamp() * 1000)
        logger.debug(f"after parameter used (pre-conversion): {watermark_with_lookback}")

    last_exception = None
    remaining_sleep_budget = 3600
    for i in range(max_retries + 1):
        try:
            response = requests.get(
                "https://api.spotify.com/v1/me/player/recently-played",
                headers=headers,
                params={"after": after,"limit": 50} if after else {"limit": 50},
                timeout=5
            )

            response.raise_for_status()
            data = _validate_recently_played(response)

            items = data["items"]
            if items:
                logger.info(f"Fetch completed with {len(items)} play events")
                return items
            else:
                logger.info("items is empty - no tracks listened to recently")
                return items

        except requests.exceptions.ConnectionError as e:
            delay_retry(i)
            last_exception = e
            logger.warning(f"ConnectionError on attempt {i + 1}: {last_exception}")

        except requests.exceptions.Timeout as e:
            delay_retry(i)
            last_exception = e
            logger.warning(f"Timeout on attempt {i + 1}: {last_exception}")

        except requests.exceptions.HTTPError as e:
            if response.status_code >= 500:
                delay_retry(i)
                last_exception = e
                logger.warning(f"Server error (5xx) on attempt {i + 1}: {last_exception}")

            elif response.status_code == 429:
                try:
                    wait = float(response.headers.get("Retry-After") or 2 ** (i + 1))
                except ValueError:
                    wait = 2 ** (i + 1)

                if wait < remaining_sleep_budget:
                    remaining_sleep_budget = remaining_sleep_budget - wait
                    time.sleep(wait)
                    last_exception = e
                    logger.warning(f"Rate limited (429) on attempt {i + 1}: {last_exception}")

                else:
                    raise RuntimeError(f"Rate limited (429) on attempt {i + 1}. No more retries "
                                   f"attempted due to excessive sleep time: {last_exception}") from e

            else:
                raise RuntimeError(f"Get request failed with status code: {response.status_code}") from e

        except requests.exceptions.RequestException as e:
            raise RuntimeError("Get request failed") from e

    else:
        if isinstance(last_exception, requests.exceptions.ConnectionError):
            raise RuntimeError(f"Max retries exhausted: "
                               f"Get request failed due to connection failure") from last_exception

        elif isinstance(last_exception, requests.exceptions.Timeout):
            raise RuntimeError(f"Max retries exhausted: "
                               f"Get request timed out") from last_exception

        elif isinstance(last_exception, requests.exceptions.HTTPError):
            raise RuntimeError(f"Max retries exhausted: "
                               f"Get request failed due to server errors or rate limits") from last_exception

        else:
            raise RuntimeError("Max retries exhausted: Unrecognised exception "
                               "type whilst retrieving Spotify API response") from last_exception

if __name__ == "__main__":
      run_id = str(uuid.uuid4())
      data = get_api_data(run_id)








