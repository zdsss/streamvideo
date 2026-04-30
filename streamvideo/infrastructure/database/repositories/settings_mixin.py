"""Database SettingsMixin — settings mixin"""
from typing import Optional


class SettingsMixin:
    def get_settings(self) -> dict:
        conn = self._conn()
        try:
            rows = conn.execute("SELECT key, value FROM settings").fetchall()
            result = {}
            for r in rows:
                try:
                    result[r["key"]] = json.loads(r["value"])
                except (json.JSONDecodeError, TypeError):
                    result[r["key"]] = r["value"]
            return result
        finally:
            conn.close()

    def set_setting(self, key: str, value):
        conn = self._conn()
        try:
            conn.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                (key, json.dumps(value))
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def set_settings(self, settings: dict):
        conn = self._conn()
        try:
            for k, v in settings.items():
                conn.execute(
                    "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                    (k, json.dumps(v))
                )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ========== Models ==========

