"""Database DistributeMixin — distribute mixin"""
from typing import Optional


class DistributeMixin:
    def upsert_distribute_task(self, task: dict):
        conn = self._conn()
        try:
            tags = json.dumps(task.get("tags", []), ensure_ascii=False) if isinstance(task.get("tags"), list) else task.get("tags", "[]")
            conn.execute("""
                INSERT INTO distribute_tasks (task_id, clip_id, username, platform, file_path,
                    title, description, tags, status, remote_id, remote_url, error,
                    retry_count, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(task_id) DO UPDATE SET
                    status=excluded.status, remote_id=excluded.remote_id,
                    remote_url=excluded.remote_url, error=excluded.error,
                    retry_count=excluded.retry_count, updated_at=excluded.updated_at
            """, (task["task_id"], task.get("clip_id", ""), task.get("username", ""),
                  task.get("platform", ""), task.get("file_path", ""),
                  task.get("title", ""), task.get("description", ""), tags,
                  task.get("status", "pending"), task.get("remote_id", ""),
                  task.get("remote_url", ""), task.get("error", ""),
                  task.get("retry_count", 0), task.get("created_at", 0),
                  task.get("updated_at", 0)))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_distribute_tasks(self, username: str = "", platform: str = "", limit: int = 50) -> list[dict]:
        conn = self._conn()
        try:
            sql = "SELECT * FROM distribute_tasks WHERE 1=1"
            params = []
            if username:
                sql += " AND username = ?"
                params.append(username)
            if platform:
                sql += " AND platform = ?"
                params.append(platform)
            sql += " ORDER BY created_at DESC LIMIT ?"
            params.append(limit)
            rows = conn.execute(sql, params).fetchall()
            cols = [d[0] for d in conn.execute("SELECT * FROM distribute_tasks LIMIT 0").description]
            result = []
            for row in rows:
                d = dict(zip(cols, row))
                try:
                    d["tags"] = json.loads(d.get("tags", "[]"))
                except Exception:
                    d["tags"] = []
                result.append(d)
            return result
        finally:
            conn.close()

    def get_distribute_task(self, task_id: str) -> Optional[dict]:
        conn = self._conn()
        try:
            row = conn.execute("SELECT * FROM distribute_tasks WHERE task_id = ?", (task_id,)).fetchone()
            if not row:
                return None
            cols = [d[0] for d in conn.execute("SELECT * FROM distribute_tasks LIMIT 0").description]
            d = dict(zip(cols, row))
            try:
                d["tags"] = json.loads(d.get("tags", "[]"))
            except Exception:
                d["tags"] = []
            return d
        finally:
            conn.close()

    # ========== Migration ==========

    def _migrate_from_json(self):
        """从 JSON 文件迁移数据到 SQLite（仅首次运行）"""
        config_path = Path("config.json")
        if not config_path.exists():
            return

        conn = self._conn()
        # 检查是否已迁移
        count = conn.execute("SELECT COUNT(*) FROM settings").fetchone()[0]
        if count > 0:
            conn.close()
            return

        logger.info("Migrating from JSON to SQLite...")
        try:
            with open(config_path) as f:
                config = json.load(f)

            # 迁移设置
            for key in ("auto_merge", "merge_gap_minutes", "auto_delete_originals",
                         "min_segment_size_kb", "smart_rename", "webhooks"):
                if key in config:
                    conn.execute(
                        "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                        (key, json.dumps(config[key]))
                    )

            # 迁移主播
            for item in config.get("models", []):
                if isinstance(item, dict):
                    url = item.get("url", "")
                    name = item.get("name", "")
                    schedule = item.get("schedule")
                    quality = item.get("quality", "best")
                else:
                    url = item
                    name = ""
                    schedule = None
                    quality = "best"
                if url:
                    conn.execute("""
                        INSERT OR IGNORE INTO models (username, url, display_name, quality, schedule)
                        VALUES (?, ?, ?, ?, ?)
                    """, (name or url, url, name, quality, json.dumps(schedule) if schedule else ""))

            conn.commit()

            # 迁移 sessions.json
            recordings_dir = Path("recordings")
            if recordings_dir.exists():
                for d in recordings_dir.iterdir():
                    if not d.is_dir() or d.name in ("thumbs", "logs"):
                        continue
                    sessions_path = d / "sessions.json"
                    if sessions_path.exists():
                        try:
                            with open(sessions_path) as f:
                                sessions = json.load(f)
                            for s in sessions:
                                conn.execute("""
                                    INSERT OR IGNORE INTO sessions
                                    (session_id, username, started_at, ended_at, segments, status, merged_file, merge_error)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                """, (
                                    s.get("session_id", ""), s.get("username", d.name),
                                    s.get("started_at", 0), s.get("ended_at", 0),
                                    json.dumps(s.get("segments", [])),
                                    s.get("status", ""), s.get("merged_file", ""),
                                    s.get("merge_error", ""),
                                ))
                            conn.commit()
                        except Exception as e:
                            logger.warning(f"Failed to migrate sessions for {d.name}: {e}")

            logger.info("Migration complete")
        except Exception as e:
            logger.error(f"Migration failed: {e}")
        finally:
            conn.close()

    # ========== Platform Credentials ==========

    def save_credential(self, user_id: str, platform: str, access_token: str,
                        refresh_token: str = "", openid: str = "", display_name: str = "",
                        expires_at: float = 0):
        """保存或更新平台 OAuth 凭据"""
        import time as _time
        conn = self._conn()
        try:
            conn.execute("""
                INSERT INTO platform_credentials
                    (user_id, platform, access_token, refresh_token, openid, display_name, expires_at, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id, platform) DO UPDATE SET
                    access_token=excluded.access_token,
                    refresh_token=excluded.refresh_token,
                    openid=COALESCE(NULLIF(excluded.openid,''), openid),
                    display_name=COALESCE(NULLIF(excluded.display_name,''), display_name),
                    expires_at=excluded.expires_at,
                    updated_at=excluded.updated_at
            """, (user_id, platform, access_token, refresh_token, openid, display_name,
                  expires_at, _time.time(), _time.time()))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_credential(self, user_id: str, platform: str) -> Optional[dict]:
        """获取指定用户的平台凭据，不存在返回 None"""
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT * FROM platform_credentials WHERE user_id=? AND platform=?",
                (user_id, platform)
            ).fetchone()
            return {k: row[k] for k in row.keys()} if row else None
        finally:
            conn.close()

    def delete_credential(self, user_id: str, platform: str):
        """删除平台凭据"""
        conn = self._conn()
        try:
            conn.execute(
                "DELETE FROM platform_credentials WHERE user_id=? AND platform=?",
                (user_id, platform)
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

