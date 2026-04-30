"""模型（主播/频道）仓储"""
from __future__ import annotations

import time
from typing import Optional

from streamvideo.infrastructure.database.repositories.base import BaseRepository


class ModelRepository(BaseRepository):
    """主播模型仓储"""

    def list_all(self) -> list[dict]:
        """查询所有模型，按创建时间倒序"""
        return self.fetch_all(
            "SELECT * FROM models ORDER BY created_at DESC"
        )

    def get(self, username: str) -> Optional[dict]:
        """按 username 查询单条"""
        return self.fetch_one(
            "SELECT * FROM models WHERE username = ?", (username,)
        )

    def upsert(self, username: str, url: str, **kwargs) -> None:
        """插入或更新模型

        只接受表中实际存在的列，避免与未迁移的旧库 schema 冲突。
        """
        # 通过 PRAGMA 探测当前表的列集合
        with self._conn_manager.transaction() as conn:
            cur = conn.execute("PRAGMA table_info(models)")
            existing_cols = {row[1] for row in cur.fetchall()}

        defaults = {
            "platform": "",
            "identifier": "",
            "display_name": "",
            "live_url": "",
            "schedule": "",
            "quality": "best",
            "auto_merge": 1,
            "enabled": 1,
            "priority": 5,
            "last_online": 0,
            "total_recordings": 0,
            "created_at": time.time(),
        }
        defaults.update(kwargs)
        # 过滤不存在的列
        defaults = {k: v for k, v in defaults.items() if k in existing_cols or k in ("username", "url")}
        cols = ["username", "url"] + [k for k in defaults if k not in ("username", "url")]
        vals = [username, url] + [defaults[k] for k in cols if k not in ("username", "url")]
        placeholders = ",".join("?" * len(cols))
        col_str = ",".join(cols)
        update_str = ",".join(f"{c}=excluded.{c}" for c in cols if c != "username")
        sql = (
            f"INSERT INTO models ({col_str}) VALUES ({placeholders}) "
            f"ON CONFLICT(username) DO UPDATE SET {update_str}"
        )
        self.execute(sql, tuple(vals))

    def delete(self, username: str) -> None:
        """删除模型（级联清理 sessions）"""
        self.execute("DELETE FROM models WHERE username = ?", (username,))

    def update_fields(self, username: str, **kwargs) -> None:
        """更新指定字段"""
        if not kwargs:
            return
        set_clause = ",".join(f"{k}=?" for k in kwargs)
        params = tuple(kwargs.values()) + (username,)
        self.execute(f"UPDATE models SET {set_clause} WHERE username = ?", params)

    def increment_recording_count(self, username: str) -> None:
        """录制计数 +1"""
        self.execute(
            "UPDATE models SET total_recordings = total_recordings + 1, last_online = ? WHERE username = ?",
            (time.time(), username),
        )
