"""
load.py - connects to Snowflake database and loads data

Assumptions:
    1. get_api_data() either returns an empty list or a list of dicts
    2. RECENTLY_PLAYED table exists in the schema configured in .env with
       a RAW_DATA VARIANT column
    3. Zero rows loaded is highly unlikely, because the lookback guarantees
       the watermark row returns. The only way zero rows occurs is if the API
       response is empty or there's a parsing error
"""

from src.connector import get_connection
import json
import snowflake.connector
from utils.helpers import delay_retry
import uuid
from src.fetch import get_api_data
from utils.logging import get_logger
from datetime import datetime

def load(run_id: str, data: list, max_retries: int=3) -> datetime:
    logger = get_logger(__name__, run_id)

    if data:
        last_exception = None
        for i in range(max_retries + 1):
            try:
                with get_connection() as conn:
                    with conn.cursor() as cursor:
                        for d in data:
                            cursor.execute(
                                "INSERT INTO SPOTIFY_PIPELINE.RAW.RECENTLY_PLAYED (RAW_DATA, RUN_ID) "
                                "SELECT PARSE_JSON(%s), %s",
                                (json.dumps(d), run_id)
                            )
                        conn.commit()

                        break

            except snowflake.connector.errors.OperationalError as e:
                delay_retry(i)
                last_exception = e
                logger.warning(f"Snowflake OperationalError on attempt {i + 1}: {last_exception}")

            except snowflake.connector.errors.DatabaseError as e:
                raise RuntimeError("Failed to load to RECENTLY_PLAYED via Snowflake connector") from e

            except OSError as e:
                delay_retry(i)
                last_exception = e
                logger.warning(f"OS failure on attempt {i + 1}: {last_exception}")

        else:
            if isinstance(last_exception, snowflake.connector.errors.OperationalError):
                raise RuntimeError("Max retries exhausted: "
                                   "Failed to load to RECENTLY_PLAYED via Snowflake connector") from last_exception

            elif isinstance(last_exception, OSError):
                raise RuntimeError("Max retries exhausted: "
                                   "Failed to load to Snowflake due to network error") from last_exception

            else:
                raise RuntimeError("Max retries exhausted: Unrecognised exception "
                                   "type whilst loading data into RECENTLY_PLAYED table "
                                   "via Snowflake connector") from last_exception

        logger.info(f"Load completed with {len(data)} play events")

        watermark = datetime.fromisoformat(max([d["played_at"] for d in data]))
        return watermark

    else:
        raise RuntimeError("Zero play events returned - the lookback window guarantees at least the "
                           "watermark row, so the API returned nothing or parsing dropped the rows")


if __name__ == "__main__":
    run_id = str(uuid.uuid4())
    data = get_api_data(run_id)
    load(run_id, data)







