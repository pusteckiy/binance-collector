import sqlite3
import time
import yaml
from statistics import mean

from utils import measure_execution_time

with open("config.yaml", "r") as stream:
    config = yaml.safe_load(stream)


def connect_db():
    conn = sqlite3.connect("market_data.db")
    return conn


def create_db():
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS orderbook_volumes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pair TEXT,
            timestamp REAL,
            price REAL,
            volume_above REAL,
            volume_below REAL
        )
    """
    )
    conn.commit()


@measure_execution_time
def save_to_database(conn, pair, price, volume_above, volume_below):
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO orderbook_volumes (pair, timestamp, price, volume_above, volume_below) VALUES (?, ?, ?, ?, ?)",
        (pair, time.time(), price, volume_above, volume_below),
    )
    conn.commit()
    cursor.close()


@measure_execution_time
def get_average_volume(conn, pair):
    cursor = conn.cursor()
    cursor.execute(
        "SELECT volume_above, volume_below FROM orderbook_volumes WHERE pair = ? AND timestamp >= ? ORDER BY timestamp DESC",
        (pair, time.time() - config["monitoring"]["period"]),
    )
    rows = cursor.fetchall()
    avg_volume_above = mean([row[0] for row in rows])
    avg_volume_below = mean([row[1] for row in rows])
    cursor.close()
    return avg_volume_above, avg_volume_below


@measure_execution_time
def is_records_older_than_n_seconds(conn, pair, seconds=300):
    cursor = conn.cursor()
    current_time = time.time()
    cutoff_time = current_time - seconds

    cursor.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM orderbook_volumes
            WHERE pair = ? AND timestamp < ?
        )
        """,
        (pair, cutoff_time),
    )

    result = cursor.fetchone()[0]
    return bool(result)
