#!/usr/bin/env python3
"""
fetcher.py ─ 指定ユーザーのツイートを取得して
・リツイート／リポストは除外
・{url, text, created_at} を JSON 保存
"""

import asyncio, json, pathlib, time, uuid
from datetime import timezone
from typing import Dict, List

from twscrape import API
from twscrape.logger import set_log_level

# ────────────── 設定 ────────────── #
USERNAMES = [
    "WuBlockchain",
    "PANewsCN",
    "ChainCatcher_",
    "WatcherGuru",
    "TheBlock__",
    "lookonchain"
]

COOKIE_PATH = pathlib.Path("cookies/accounts.json")
DB_PATH = "data/accounts.db"
OUT_DIR = pathlib.Path("data")
POLL_INTERVAL = 30  # 秒

set_log_level("INFO")
OUT_DIR.mkdir(parents=True, exist_ok=True)

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

def tweet_to_dict(tw) -> dict:
    return {
        "id": tw.id,
        "url": f"https://twitter.com/{tw.user.username}/status/{tw.id}",
        "username": tw.user.username,
        "text": tw.rawContent,
        "created_at": tw.date.replace(tzinfo=timezone.utc).isoformat(),
    }

def save_json(obj: dict) -> None:
    fp = OUT_DIR / f"{obj['id']}_{uuid.uuid4().hex}.json"
    fp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

# ─────────────── 本体 ─────────────── #
async def main() -> None:
    api = API(DB_PATH)
    accounts = json.loads(COOKIE_PATH.read_text())
    await load_accounts(api, accounts)
    await api.pool.login_all()

    user_ids = await get_user_ids(api, USERNAMES)
    since: Dict[int, int] = {uid: 0 for uid in user_ids.values()}

    while True:
        loop_start = time.time()

        for uname, uid in user_ids.items():
            async for tw in api.user_tweets(uid, limit=10):
                if tw.id <= since[uid]:
                    break

                # リツイート／リポストは除外
                if tw.retweetedTweet is not None:
                    continue

                save_json(tweet_to_dict(tw))
                since[uid] = max(since[uid], tw.id)

        await asyncio.sleep(max(0, POLL_INTERVAL - (time.time() - loop_start)))

if __name__ == "__main__":
    asyncio.run(main())
