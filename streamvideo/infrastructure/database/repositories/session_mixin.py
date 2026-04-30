"""Database SessionMixin — session mixin"""
from typing import Optional


class SessionMixin:
    def get_sessions(self, username: str) -> list[dict]:
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT * FROM sessions WHERE username = ? ORDER BY started_at DESC", (username,)
            ).fetchall()
            result = []
            for r in rows:
                d = dict(r)
                d["segments"] = json.loads(d["segments"]) if d["segments"] else []
                d.setdefault("retry_count", 0)
                d.setdefault("merge_started_at", 0)
                d.setdefault("stream_end_reason", "")
                d.setdefault("merge_type", "")
                d.setdefault("rollback_deadline", 0)
                raw_orig = d.get("original_segments", "[]")
                d["original_segments"] = json.loads(raw_orig) if raw_orig else []
                result.append(d)
            return result
        finally:
            conn.close()

    def get_sessions_by_id(self, session_id: str) -> list[dict]:
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT * FROM sessions WHERE session_id = ?", (session_id,)
            ).fetchall()
            result = []
            for r in rows:
                d = dict(r)
                d["segments"] = json.loads(d["segments"]) if d["segments"] else []
                d.setdefault("merge_type", "")
                d.setdefault("rollback_deadline", 0)
                raw_orig = d.get("original_segments", "[]")
                d["original_segments"] = json.loads(raw_orig) if raw_orig else []
                result.append(d)
            return result
        finally:
            conn.close()

    def upsert_session(self, session: dict):
        conn = self._conn()
        try:
            conn.execute("""
                INSERT INTO sessions (session_id, username, started_at, ended_at, segments, status,
                    merged_file, merge_error, retry_count, merge_started_at, stream_end_reason,
                    merge_type, rollback_deadline, original_segments)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                    ended_at=excluded.ended_at,
                    segments=excluded.segments,
                    status=excluded.status,
                    merged_file=excluded.merged_file,
                    merge_error=excluded.merge_error,
                    retry_count=excluded.retry_count,
                    merge_started_at=excluded.merge_started_at,
                    stream_end_reason=excluded.stream_end_reason,
                    merge_type=excluded.merge_type,
                    rollback_deadline=excluded.rollback_deadline,
                    original_segments=excluded.original_segments
            """, (
                session["session_id"], session["username"],
                session.get("started_at", 0), session.get("ended_at", 0),
                json.dumps(session.get("segments", [])),
                session.get("status", "active"),
                session.get("merged_file", ""),
                session.get("merge_error", ""),
                session.get("retry_count", 0),
                session.get("merge_started_at", 0),
                session.get("stream_end_reason", ""),
                session.get("merge_type", ""),
                session.get("rollback_deadline", 0),
                json.dumps(session.get("original_segments", [])),
            ))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def update_session_status(self, session_id: str, status: str, **kwargs):
        conn = self._conn()
        try:
            sets = ["status = ?"]
            vals = [status]
            for k in ("merged_file", "merge_error", "ended_at", "retry_count",
                      "merge_started_at", "stream_end_reason", "segments",
                      "merge_type", "rollback_deadline", "original_segments"):
                if k in kwargs:
                    sets.append(f"{k} = ?")
                    v = kwargs[k]
                    vals.append(json.dumps(v) if k in ("segments", "original_segments") else v)
            vals.append(session_id)
            conn.execute(f"UPDATE sessions SET {', '.join(sets)} WHERE session_id = ?", vals)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_all_sessions_by_status(self, status: str) -> list[dict]:
        conn = self._conn()
        try:
            rows = conn.execute("SELECT * FROM sessions WHERE status = ?", (status,)).fetchall()
            return [dict(r) | {"segments": json.loads(r["segments"])} for r in rows]
        finally:
            conn.close()

    # ========== Merge History ==========

    def insert_merge_history(self, username: str, session_id: str = "",
                             input_files: list[str] = None, input_size: int = 0,
                             output_file: str = "", output_size: int = 0,
                             savings_bytes: int = 0, status: str = "done", error: str = ""):
        conn = self._conn()
        try:
            conn.execute("""
                INSERT INTO merge_history (username, session_id, input_files, input_size,
                    output_file, output_size, savings_bytes, completed_at, status, error)
                VALUES (?, ?, ?, ?, ?, ?, ?, strftime('%s','now'), ?, ?)
            """, (
                username, session_id, json.dumps(input_files or []), input_size,
                output_file, output_size, savings_bytes, status, error,
            ))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_merge_history(self, username: str, limit: int = 50) -> list[dict]:
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT * FROM merge_history WHERE username = ? ORDER BY completed_at DESC LIMIT ?",
                (username, limit)
            ).fetchall()
            result = []
            for r in rows:
                d = dict(r)
                d["input_files"] = json.loads(d["input_files"]) if d["input_files"] else []
                result.append(d)
            return result
        finally:
            conn.close()

    def get_all_merge_history(self, limit: int = 100) -> list[dict]:
        """获取全局合并历史（不限主播）"""
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT * FROM merge_history ORDER BY completed_at DESC LIMIT ?",
                (limit,)
            ).fetchall()
            result = []
            for r in rows:
                d = dict(r)
                d["input_files"] = json.loads(d["input_files"]) if d["input_files"] else []
                result.append(d)
            return result
        finally:
            conn.close()

    # ========== Daily Stats ==========

    def get_daily_stats(self, days: int = 30) -> list[dict]:
        conn = self._conn()
        try:
            rows = conn.execute("""
                SELECT date(started_at, 'unixepoch', 'localtime') as day,
                       COUNT(*) as sessions,
                       COALESCE(SUM(ended_at - started_at), 0) as total_duration
                FROM sessions
                WHERE started_at > strftime('%s','now') - ? * 86400
                  AND ended_at > 0
                GROUP BY day ORDER BY day
            """, (days,)).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    # ========== Stats ==========

    def get_stats(self, username: Optional[str] = None) -> dict:
        conn = self._conn()
        try:
            if username:
                row = conn.execute("""
                    SELECT COUNT(*) as session_count,
                           SUM(CASE WHEN status='merged' THEN 1 ELSE 0 END) as merged_sessions,
                           SUM(ended_at - started_at) as total_duration
                    FROM sessions WHERE username = ? AND ended_at > 0
                """, (username,)).fetchone()
            else:
                row = conn.execute("""
                    SELECT COUNT(*) as session_count,
                           SUM(CASE WHEN status='merged' THEN 1 ELSE 0 END) as merged_sessions,
                           SUM(ended_at - started_at) as total_duration
                    FROM sessions WHERE ended_at > 0
                """).fetchone()
            return dict(row) if row else {}
        finally:
            conn.close()

    # ========== Danmaku ==========

