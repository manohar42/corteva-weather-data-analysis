import os
import logging
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_batch
from tqdm import tqdm
load_dotenv()

logging.basicConfig(
    filename="weather_data_log.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)


def db_conn():
    try:
        return psycopg2.connect()
    except psycopg2.Error as err:
        logging.error(f"DB connection error: {err}")
        return None


INSERT_SQL = """
INSERT INTO weather_data (date, max_temp, min_temp, precipitation, station_id)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (date, station_id) DO UPDATE
SET max_temp = EXCLUDED.max_temp,
    min_temp = EXCLUDED.min_temp,
    precipitation = EXCLUDED.precipitation;
"""


def process_file(path, conn):
    total = 0
    sid = os.path.splitext(os.path.basename(path))[0]
    recs = []
    with open(path, "r") as fh:
        for line in fh:
            date, max_t, min_t, prcp = line.strip().split()
            date = f"{date[:4]}-{date[4:6]}-{date[6:]}"
            max_t = None if max_t == "-9999" else float(max_t) / 10
            min_t = None if min_t == "-9999" else float(min_t) / 10
            prcp = None if prcp == "-9999" else float(prcp) / 10
            recs.append((date, max_t, min_t, prcp, sid))
    if recs:
        with conn.cursor() as cur:
            execute_batch(cur, INSERT_SQL, recs, page_size=1_000)
        conn.commit()
        total += len(recs)
    return total


def ingest(dir_path):
    conn = db_conn()
    if not conn:
        return
    total = 0
    start = datetime.now()
    logging.info(f"Start: {start:%F %T}")
    files = [f for f in os.listdir(dir_path) if f.endswith(".txt")]
    with tqdm(total=len(files), desc="Ingesting", unit="file") as bar:
        for fname in files:
            full = os.path.join(dir_path, fname)
            total += process_file(full, conn)
            bar.update(1)
    conn.close()
    end = datetime.now()
    logging.info(f"End: {end:%F %T}")
    logging.info(f"Duration: {end - start}")
    logging.info(f"Total rows: {total}")


if __name__ == "__main__":
    ingest("data/wx_data/")
