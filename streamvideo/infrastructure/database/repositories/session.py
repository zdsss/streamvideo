"""录制会话仓储"""
from __future__ import annotations

import json
from typing import Optional

from streamvideo.infrastructure.database.repositories.base import BaseRepository


class SessionRepository(BaseRepository):
    """录制会话仓储"""

    def list_by_username(self, username: str) -> list[dict]:
        """按用户名查询全部会话"""
        rows = self.fetch_all(
            "SELECT * FROM sessions WHERE username = ? ORDER BY started_at DESC",
            (username,),
        )
        return [self._deserialize(r) for r in rows]

    def get(self, session_id: str) -> Optional[dict]:
        """按 ID 获取单个会话"""
        row = self.fetch_one(
            "SELECT * FROM sessions WHERE session_id = ?", (session_id,)
        )
        return self._deserialize(row) if row else None

    def list_by_status(self, status: str) -> list[dict]:
        """按状态查询"""
        rows = self.fetch_all(
            "SELECT * FROM sessions WHERE status = ? ORDER BY started_at DESC",
            (status,),
        )
        return [self._deserialize(r) for r in rows]

    def upsert(self, session: dict) -> None:
        """插入或更新会话"""
        s = dict(session)
        # JSON 字段序列化
        for key in ("segments", "original_segments"):
            if key in s and not isinstance(s[key], str):
                s[key] = json.dumps(s[key], ensure_ascii=False)
        cols = list(s.keys())
        col_str = ",".join(cols)
        placeholders = ",".join("?" * len(cols))
        update_str = ",".join(f"{c}=excluded.{c}" for c in cols if c != "session_id")
        sql = (
            f"INSERT INTO sessions ({col_str}) VALUES ({placeholders}) "
            f"ON CONFLICT(session_id) DO UPDATE SET {update_str}"
        )
        self.execute(sql, tuple(s.values()))

    def update_status(self, session_id: str, status: str, **kwargs) -> None:
        """更新会话状态"""
        fields = {"status": status, **kwargs}
        # JSON 序列化
        for key in ("segments", "original_segments"):
            if key in fields and not isinstance(fields[key], str):
                fields[key] = json.dumps(fields[key], ensure_ascii=False)
        set_clause = ",".join(f"{k}=?" for k in fields)
        params = tuple(fields.values()) + (session_id,)
        self.execute(
            f"UPDATE sessions SET {set_clause} WHERE session_id = ?", params
        )

    @staticmethod
    def _deserialize(row: Optional[dict]) -> Optional[dict]:
        """反序列化 JSON 字段"""
        if not row:
            return row
        for key in ("segments", "original_segments"):
            val = row.get(key, "[]")
            if isinstance(val, str):
                try:
                    row[key] = json.loads(val) if val else []
                except json.JSONDecodeError:
                    row[key] = []
        return row
