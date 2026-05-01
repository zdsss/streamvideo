"""Database MediaMixin — media mixin"""
import json
from typing import Optional


class MediaMixin:
    def upsert_danmaku(self, session_id: str, username: str, file_path: str,
                       message_count: int = 0, peak_density: float = 0, keywords_found: dict = None):
        conn = self._conn()
        try:
            conn.execute("""
                INSERT INTO danmaku (session_id, username, file_path, message_count, peak_density, keywords_found)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id, username) DO UPDATE SET
                    file_path=excluded.file_path, message_count=excluded.message_count,
                    peak_density=excluded.peak_density, keywords_found=excluded.keywords_found
            """, (session_id, username, file_path, message_count, peak_density,
                  json.dumps(keywords_found or {})))
            conn.commit()
        except Exception:
            conn.rollback()
            raise


    def get_danmaku(self, session_id: str) -> Optional[dict]:
        conn = self._conn()
        try:
            row = conn.execute("SELECT * FROM danmaku WHERE session_id = ?", (session_id,)).fetchone()
            if row:
                d = dict(row)
                d["keywords_found"] = json.loads(d["keywords_found"]) if d["keywords_found"] else {}
                return d
            return None


    def get_danmaku_by_username(self, username: str, limit: int = 20) -> list[dict]:
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT * FROM danmaku WHERE username = ? ORDER BY created_at DESC LIMIT ?",
                (username, limit)).fetchall()
            result = []
            for r in rows:
                d = dict(r)
                d["keywords_found"] = json.loads(d["keywords_found"]) if d["keywords_found"] else {}
                result.append(d)
            return result


    # ========== Highlights ==========

    def insert_highlight(self, highlight_id: str, session_id: str, username: str,
                         video_file: str, start_time: float, end_time: float,
                         score: float = 0, category: str = "", signals: list = None, title: str = ""):
        conn = self._conn()
        try:
            conn.execute("""
                INSERT OR REPLACE INTO highlights
                (highlight_id, session_id, username, video_file, start_time, end_time, score, category, signals, title)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (highlight_id, session_id, username, video_file, start_time, end_time,
                  score, category, json.dumps(signals or []), title))
            conn.commit()
        except Exception:
            conn.rollback()
            raise


    def get_highlights(self, username: str, limit: int = 50) -> list[dict]:
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT * FROM highlights WHERE username = ? ORDER BY created_at DESC LIMIT ?",
                (username, limit)).fetchall()
            result = []
            for r in rows:
                d = dict(r)
                d["signals"] = json.loads(d["signals"]) if d["signals"] else []
                result.append(d)
            return result


    def get_highlight(self, highlight_id: str) -> Optional[dict]:
        conn = self._conn()
        try:
            row = conn.execute("SELECT * FROM highlights WHERE highlight_id = ?", (highlight_id,)).fetchone()
            if row:
                d = dict(row)
                d["signals"] = json.loads(d["signals"]) if d["signals"] else []
                return d
            return None


    def update_highlight_status(self, highlight_id: str, status: str):
        conn = self._conn()
        try:
            conn.execute("UPDATE highlights SET status = ? WHERE highlight_id = ?", (status, highlight_id))
            conn.commit()
        except Exception:
            conn.rollback()
            raise


    def delete_highlight(self, highlight_id: str):
        conn = self._conn()
        try:
            conn.execute("DELETE FROM highlights WHERE highlight_id = ?", (highlight_id,))
            conn.commit()
        except Exception:
            conn.rollback()
            raise


    def get_all_highlights(self, limit: int = 100) -> list[dict]:
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT * FROM highlights ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
            result = []
            for r in rows:
                d = dict(r)
                d["signals"] = json.loads(d["signals"]) if d["signals"] else []
                result.append(d)
            return result


    # ========== Clips ==========

    def insert_clip(self, clip_id: str, highlight_id: str, username: str,
                    output_file: str = "", resolution: str = "", duration: float = 0,
                    format: str = "", size: int = 0, status: str = "done"):
        conn = self._conn()
        try:
            conn.execute("""
                INSERT OR REPLACE INTO clips
                (clip_id, highlight_id, username, output_file, resolution, duration, format, size, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (clip_id, highlight_id, username, output_file, resolution, duration, format, size, status))
            conn.commit()
        except Exception:
            conn.rollback()
            raise


    def get_clips(self, username: str, limit: int = 50) -> list[dict]:
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT * FROM clips WHERE username = ? ORDER BY created_at DESC LIMIT ?",
                (username, limit)).fetchall()
            return [dict(r) for r in rows]


    def get_clip(self, clip_id: str) -> Optional[dict]:
        conn = self._conn()
        try:
            row = conn.execute("SELECT * FROM clips WHERE clip_id = ?", (clip_id,)).fetchone()
            return dict(row) if row else None


    def update_clip_status(self, clip_id: str, status: str, **kwargs):
        conn = self._conn()
        try:
            sets = ["status = ?"]
            vals = [status]
            for k in ("export_url", "output_file", "size", "title", "description"):
                if k in kwargs:
                    sets.append(f"{k} = ?")
                    vals.append(kwargs[k])
            if "tags" in kwargs:
                sets.append("tags = ?")
                vals.append(json.dumps(kwargs["tags"]) if isinstance(kwargs["tags"], list) else kwargs["tags"])
            vals.append(clip_id)
            conn.execute(f"UPDATE clips SET {', '.join(sets)} WHERE clip_id = ?", vals)
            conn.commit()
        except Exception:
            conn.rollback()
            raise


    def delete_clip(self, clip_id: str):
        conn = self._conn()
        try:
            conn.execute("DELETE FROM clips WHERE clip_id = ?", (clip_id,))
            conn.commit()
        except Exception:
            conn.rollback()
            raise


    def get_all_clips(self, limit: int = 100) -> list[dict]:
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT * FROM clips ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
            return [dict(r) for r in rows]


    def get_clip_stats(self, username: str = "") -> dict:
        conn = self._conn()
        try:
            if username:
                row = conn.execute("""
                    SELECT COUNT(*) as total, SUM(size) as total_size, SUM(duration) as total_duration
                    FROM clips WHERE username = ? AND status = 'done'
                """, (username,)).fetchone()
            else:
                row = conn.execute("""
                    SELECT COUNT(*) as total, SUM(size) as total_size, SUM(duration) as total_duration
                    FROM clips WHERE status = 'done'
                """).fetchone()
            return dict(row) if row else {}


    # ========== Highlight Rules ==========

    def get_highlight_rules(self, username: str = "") -> list[dict]:
        conn = self._conn()
        try:
            if username:
                rows = conn.execute(
                    "SELECT * FROM highlight_rules WHERE username = ? OR username = '' ORDER BY rule_id",
                    (username,)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM highlight_rules ORDER BY rule_id").fetchall()
            result = []
            for r in rows:
                d = dict(r)
                d["keywords"] = json.loads(d["keywords"]) if d["keywords"] else []
                d["weights"] = json.loads(d["weights"]) if d["weights"] else {}
                d["enabled"] = bool(d["enabled"])
                result.append(d)
            return result


    def upsert_highlight_rule(self, rule_id: Optional[int] = None, **kwargs):
        conn = self._conn()
        try:
            if rule_id:
                sets, vals = [], []
                for k in ("username", "min_score", "min_duration", "max_duration", "enabled"):
                    if k in kwargs:
                        sets.append(f"{k} = ?")
                        vals.append(1 if k == "enabled" and kwargs[k] else kwargs[k])
                for k in ("keywords", "weights"):
                    if k in kwargs:
                        sets.append(f"{k} = ?")
                        vals.append(json.dumps(kwargs[k]))
                if sets:
                    vals.append(rule_id)
                    conn.execute(f"UPDATE highlight_rules SET {', '.join(sets)} WHERE rule_id = ?", vals)
            else:
                conn.execute("""
                    INSERT INTO highlight_rules (username, keywords, min_score, min_duration, max_duration, weights, enabled)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    kwargs.get("username", ""),
                    json.dumps(kwargs.get("keywords", [])),
                    kwargs.get("min_score", 0.6),
                    kwargs.get("min_duration", 15),
                    kwargs.get("max_duration", 60),
                    json.dumps(kwargs.get("weights", {})),
                    1 if kwargs.get("enabled", True) else 0,
                ))
            conn.commit()
        except Exception:
            conn.rollback()
            raise


    def delete_highlight_rule(self, rule_id: int):
        conn = self._conn()
        try:
            conn.execute("DELETE FROM highlight_rules WHERE rule_id = ?", (rule_id,))
            conn.commit()
        except Exception:
            conn.rollback()
            raise


    # ========== Distribute Tasks ==========

