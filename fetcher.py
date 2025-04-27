#!/usr/bin/env python3
"""
fetcher_db.py  –  指定ユーザーのツイートを取得し
 1) リツイート／リポストを除外
 2) SQLite に UPSERT（id PRIMARY KEY で重複ゼロ）
"""

import asyncio, json, os, pathlib, sqlite3, time
from datetime import timezone
from typing import Dict, List

from twscrape import API
from twscrape.logger import set_log_level

# ────────────── 設定 ────────────── #
USERNAMES = [
    "WuBlockchain", "PANewsCN", "ChainCatcher_", "WatcherGuru",
    "TheBlock__", "lookonchain"
]

COOKIE_PATH = pathlib.Path("cookies/accounts.json")
DB_SQLITE   = pathlib.Path("data/tweets.sqlite")
DB_ACCOUNTS = "data/accounts.db"        # twscrape が使う DB
POLL_INTERVAL = 45                      # 秒

set_log_level("INFO")
DB_SQLITE.parent.mkdir(exist_ok=True)

SQL_INIT = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS tweets(
  id          INTEGER PRIMARY KEY,   -- Tweet ID、一意
  username    TEXT,
  url         TEXT,
  original    TEXT,
  created_at  TEXT,
  processed   INTEGER DEFAULT 0      -- 0=未処理 1=完了
);
"""

# ────────── ユーティリティ ────────── #
async def load_accounts(api: API, accounts: List[dict]) -> None:
    for acc in accounts:
        await api.pool.add_account(
            username=acc["username"],
            password=acc.get("password", ""),
            email=acc.get("email", ""),
            email_password=acc.get("email_password", ""),
            cookies=acc["cookies"],
        )

async def get_user_ids(api: API, usernames: List[str]) -> Dict[str, int]:
    return {u: (await api.user_by_login(u)).id for u in usernames}

def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_SQLITE, check_same_thread=False)
    conn.executescript(SQL_INIT)
    return conn

# ─────────────── 本体 ─────────────── #
async def main() -> None:
    conn = init_db()

    api = API(DB_ACCOUNTS)
    accounts = json.loads(COOKIE_PATH.read_text())
    await load_accounts(api, accounts)
    await api.pool.login_all()

    user_ids = await get_user_ids(api, USERNAMES)

    # DB から since_id をロード（起動後も重複ゼロ）
    since = {uid: conn.execute(
        "SELECT COALESCE(MAX(id),0) FROM tweets WHERE username=?",
        (uname,)).fetchone()[0] for uname, uid in user_ids.items()}

    while True:
        loop_start = time.time()
        for uname, uid in user_ids.items():
            async for tw in api.user_tweets(uid, limit=15):
                if tw.id <= since[uid]:
                    break
                if tw.retweetedTweet is not None:      # リツイート／リポスト除外
                    continue
                conn.execute("""INSERT OR IGNORE INTO tweets
                    (id, username, url, original, created_at)
                    VALUES (?,?,?,?,?)""",
                    (tw.id, uname,
                     f"https://twitter.com/{uname}/status/{tw.id}",
                     tw.rawContent,
                     tw.date.replace(tzinfo=timezone.utc).isoformat()))
                since[uid] = max(since[uid], tw.id)
        conn.commit()
        await asyncio.sleep(max(0, POLL_INTERVAL - (time.time() - loop_start)))

if __name__ == "__main__":
    asyncio.run(main())
