"""Database ModelMixin — model mixin"""
from typing import Optional


import json
class ModelMixin:
    def get_models(self) -> list[dict]:
        conn = self._conn()
        try:
            rows = conn.execute("SELECT * FROM models ORDER BY created_at").fetchall()
            result = []
            for r in rows:
                d = dict(r)
                d["schedule"] = json.loads(d["schedule"]) if d["schedule"] else None
                d["auto_merge"] = bool(d["auto_merge"])
                d["enabled"] = bool(d["enabled"])
                result.append(d)
            return result
        finally:
            conn.close()

    def upsert_model(self, username: str, url: str, **kwargs):
        conn = self._conn()
        try:
            schedule = kwargs.get("schedule")
            conn.execute("""
                INSERT INTO models (username, url, platform, display_name, quality, auto_merge, schedule)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(username) DO UPDATE SET
                    url=excluded.url,
                    platform=COALESCE(excluded.platform, platform),
                    display_name=COALESCE(excluded.display_name, display_name),
                    quality=COALESCE(excluded.quality, quality),
                    auto_merge=COALESCE(excluded.auto_merge, auto_merge),
                    schedule=COALESCE(excluded.schedule, schedule)
            """, (
                username, url,
                kwargs.get("platform", ""),
                kwargs.get("display_name", username),
                kwargs.get("quality", "best"),
                1 if kwargs.get("auto_merge", True) else 0,
                json.dumps(schedule) if schedule else "",
            ))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def delete_model(self, username: str):
        """删除主播及其所有关联数据"""
        conn = self._conn()
        try:
            conn.execute("DELETE FROM models WHERE username = ?", (username,))
            # sessions 有 CASCADE，但显式清理更安全
            conn.execute("DELETE FROM sessions WHERE username = ?", (username,))
            conn.execute("DELETE FROM highlights WHERE username = ?", (username,))
            conn.execute("DELETE FROM clips WHERE username = ?", (username,))
            conn.execute("DELETE FROM danmaku WHERE username = ?", (username,))
            conn.execute("DELETE FROM merge_history WHERE username = ?", (username,))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def update_model(self, username: str, **kwargs):
        _ALLOWED_COLUMNS = {"quality", "platform", "display_name", "live_url", "url", "last_online", "total_recordings"}
        conn = self._conn()
        try:
            sets = []
            vals = []
            for k, v in kwargs.items():
                if k == "schedule":
                    sets.append("schedule = ?")
                    vals.append(json.dumps(v) if v else "")
                elif k == "auto_merge":
                    sets.append("auto_merge = ?")
                    vals.append(1 if v else 0)
                elif k in _ALLOWED_COLUMNS:
                    sets.append(f"{k} = ?")
                    vals.append(v)
            if sets:
                vals.append(username)
                conn.execute(f"UPDATE models SET {', '.join(sets)} WHERE username = ?", vals)
                conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ========== Sessions ==========

