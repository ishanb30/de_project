"""
run.py - orchestrates each pipeline run

Assumptions:
    1. The table PIPELINE_RUNS exists to be written to

    2. Required credentials are present in the process environment - written
       there by load_dotenv from .env locally, or by the workflow env: block
       from repository secrets in CI. _check_env_vars validates all of them
       before any connection is opened or any row is written, and callers
       read os.environ directly rather than being passed the values

Known Limitations:
    1. If the UPDATE query in the main except block in run_pipeline fails,
       then RUN_STATUS will not be updated to FAILED in PIPELINE_RUNS.
       Therefore, it is possible that a failed pipeline run could have a
       RUN_STATUS of STARTED. However, the failure is caught and logged to
       pipeline.log
"""

import uuid
import sys
from src.load import load
from src.fetch import get_api_data
from utils.logging import get_logger
from src.connector import get_connection
from datetime import datetime, timezone
from dotenv import load_dotenv
import os
from utils.paths import ENV_PATH
import requests

def _check_env_vars() -> None:
    """Validates all required env vars are present in the process environment,
    raising ValueError listing any that are missing."""
    var_names = [
        "CLIENT_ID",
        "CLIENT_SECRET",
        "ACCOUNT_IDENTIFIER",
        "USERNAME",
        "ROLE",
        "PRIVATE_KEY",
        "DATA_WAREHOUSE",
        "DATABASE"
    ]

    load_dotenv(ENV_PATH)

    missing_vars = []
    for var in var_names:
        var_value = os.environ.get(var)
        if not var_value:
            missing_vars.append(var)

    if missing_vars:
        raise ValueError(f"Variable name(s) ({missing_vars}) not set in environment "
                         "(expected from .env locally or repo secrets in CI)")

def _insert_pipeline_run(cursor, run_id: str, run_status: str):
    """Inserts run_id and run_status into PIPELINE_RUNS"""
    run_start = datetime.now(timezone.utc)

    cursor.execute(
        "INSERT INTO SPOTIFY_PIPELINE.METADATA.PIPELINE_RUNS (RUN_ID, RUN_START, RUN_STATUS) "
        "VALUES (%s, %s, %s)",
        (run_id, run_start, run_status)
    )

def _get_refresh_token(cursor) -> str:
    cursor.execute("SELECT REFRESH_TOKEN FROM SPOTIFY_PIPELINE.AUTH.TOKENS")
    row = cursor.fetchone()

    if row is not None:
        refresh_token = row[0]
        return refresh_token
    else:
        raise RuntimeError("Missing refresh token from AUTH.TOKENS table")

def _send_heartbeat_ping(logger) -> None:
    """Pings the heartbeat URL to signal successful ingestion. Never raises."""
    try:
        heartbeat_url = os.environ.get("HEARTBEAT_URL")
        if not heartbeat_url:
            logger.warning(
                "HEARTBEAT_URL not set - heartbeat ping skipped. "
                "Set it in .env locally or as a repo secret in CI"
            )
            return

        response = requests.get(heartbeat_url, timeout=5)
        response.raise_for_status()
        logger.info(f"Heartbeat ping sent (HTTP {response.status_code})")

    except Exception as e:
        logger.warning(f"Heartbeat ping failed: {e}")

def _update_pipeline_run(
        cursor, run_id: str, run_status: str, watermark: datetime | None = None
) -> None:
    """Updates the PIPELINE_RUNS table once pipeline run status has been determined"""
    run_end = datetime.now(timezone.utc)

    cursor.execute(
        "UPDATE SPOTIFY_PIPELINE.METADATA.PIPELINE_RUNS "
        "SET RUN_END = %s, WATERMARK_TIMESTAMP = %s, RUN_STATUS = %s "
        "WHERE RUN_ID = %s",
        (run_end, watermark, run_status, run_id)
    )

def run_pipeline(run_id: str, logger) -> None:
    """Main execution block for pipeline run logic"""
    run_status = "STARTED"

    try:
        _check_env_vars()
        with get_connection() as conn:
            with conn.cursor() as cursor:
                _insert_pipeline_run(cursor, run_id, run_status)
                conn.commit()

                refresh_token = _get_refresh_token(cursor)

                logger.info("Starting the process to fetch data from API...")
                data = get_api_data(run_id, refresh_token)
                logger.info("Loading data to Snowflake...")
                watermark = load(run_id, data)

                _send_heartbeat_ping(logger)

                # logger.info("Transforming data...")
                # TODO: trigger dbt transformation layer

                try:
                    run_status = "COMPLETED"
                    _update_pipeline_run(cursor, run_id, run_status, watermark)
                    conn.commit()
                    logger.info(f"Pipeline executed successfully")

                except Exception as db_err:
                    logger.error(f"Could not log COMPLETED status to Snowflake: {db_err}")

                    raise

    except Exception as e:
        logger.critical(f"Pipeline crashed during execution: {e}", exc_info=True)

        try:
            with get_connection() as conn:
                with conn.cursor() as cursor:
                    run_status = "FAILED"
                    _update_pipeline_run(cursor, run_id, run_status)
                    conn.commit()
                    logger.info("Successfully updated pipeline status to FAILED in Snowflake")

        except Exception as db_err:
            logger.error(f"Could not log FAILED status to Snowflake: {db_err}")

        raise

def main():
    run_id = str(uuid.uuid4())
    logger = get_logger(__name__, run_id)

    try:
        logger.info(f"Pipeline initialised")
        run_pipeline(run_id, logger)

    except Exception as e:
        sys.exit(1)

if __name__ == "__main__":
    main()

