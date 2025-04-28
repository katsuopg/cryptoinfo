#!/usr/bin/env python3
"""
worker_dify.py – 未処理ツイートを Dify Workflow に投入
"""

import os, time, sqlite3, requests, json
from dotenv import load_dotenv

load_dotenv(".env")

DB_FILE = "data/tweets.sqlite"
DIFY_URL = os.getenv("DIFY_URL")
DIFY_KEY = os.getenv("DIFY_KEY")
WORKFLOW_ID = "fe6fb3a0-53d4-47ea-adc9-995cdeeeffe1"  # ここに「長いほう」セット固定

HEADERS = {
    "Authorization": f"Bearer {DIFY_KEY}",
    "Content-Type": "application/json"
}

def push_to_dify(row):
    """row = (id, username, url, original) from DB"""
    tweet_id, username, url, text = row
    payload = {
        "inputs": {
            "tweet_id": str(tweet_id),
            "tweet_text": text,
            "tweet_url": url,
            "usename": username
        },
        "response_mode": "blocking",
        "user": str(tweet_id)
    }
    r = requests.post(DIFY_URL, headers=HEADERS, json=payload, timeout=60)
    r.raise_for_status()
    return r.json()

def worker():
    conn = sqlite3.connect(DB_FILE, timeout=30)
    cur = conn.cursor()

    while True:
        rows = cur.execute(
            "SELECT id, username, url, original FROM tweets WHERE processed = 0 LIMIT 10"
        ).fetchall()

        if not rows:
            time.sleep(30)
            continue

        for row in rows:
            tweet_id = row[0]
            try:
                print(f"Pushing to Dify: {tweet_id}")
                resp = push_to_dify(row)
                cur.execute("UPDATE tweets SET processed = 1 WHERE id = ?", (tweet_id,))
            except Exception as e:
                print(f"Error pushing {tweet_id}: {e}")
                cur.execute(
                    "UPDATE tweets SET retries = retries + 1, last_error = ? "
                    "WHERE id = ? AND retries < 5",
                    (str(e)[:250], tweet_id)
                )

        conn.commit()
        time.sleep(1)

if __name__ == "__main__":
    worker()
