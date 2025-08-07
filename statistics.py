import os
import logging
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_batch
from tqdm import tqdm

load_dotenv()

logging.basicConfig(
    filename="weather_data_statistics_log.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

QUERY_SELECT = """
SELECT
    station_id,
    EXTRACT(YEAR FROM date) AS year,
    AVG(max_temp)            AS avg_max_temp,
    AVG(min_temp)            AS avg_min_temp,
    SUM(precipitation)       AS total_precipitation
FROM weather_data
WHERE max_temp IS NOT NULL
  AND min_temp IS NOT NULL
  AND precipitation IS NOT NULL
GROUP BY station_id, EXTRACT(YEAR FROM date)
"""

QUERY_UPSERT = """
INSERT INTO statistics_data (
    station_id,
    year,
    avg_maximum_temperature,
    avg_minimum_temperature,
    total_precipitation
)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (station_id, year) DO UPDATE
SET avg_maximum_temperature       = EXCLUDED.avg_maximum_temperature,
    avg_minimum_temperature       = EXCLUDED.avg_minimum_temperature,
    total_precipitation = EXCLUDED.total_precipitation
"""


def db_connect():
    return psycopg2.connect()


def ingest_statistics():
    conn = db_connect()
    start = datetime.now()
    logging.info("Starting statistics calculation")
    try:
        with conn.cursor() as cur:
            cur.execute(QUERY_SELECT)
            results = cur.fetchall()
            for row in tqdm(results, desc="Updating statistics"):
                execute_batch(cur, QUERY_UPSERT, [row], page_size=1_000)
            conn.commit()
    except psycopg2.Error as e:
        logging.error(f"Database error: {e}")
    finally:
        conn.close()
        end = datetime.now()
        logging.info(f"Completed. Start: {start}, End: {end}, Duration: {end - start}")

if __name__ == "__main__":
    ingest_statistics()
