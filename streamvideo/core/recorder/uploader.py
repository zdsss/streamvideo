import asyncio
import json
import logging
import os
import re
import shutil
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Callable, Optional
from urllib.parse import urlparse

import aiohttp

logger = logging.getLogger("recorder")


class CloudUploader:
    """可选的云存储上传（S3 兼容 / 阿里云 OSS）"""

    def __init__(self):
        self.config: Optional[dict] = None  # {"type":"s3","bucket":"...","prefix":"...","access_key":"...","secret_key":"...","endpoint":"...","region":"..."}

    async def upload(self, file_path: Path, username: str) -> Optional[str]:
        """上传文件到云存储，返回远程 URL 或 None"""
        if not self.config or not self.config.get("type"):
            return None
        try:
            import subprocess
            cloud_type = self.config["type"]
            bucket = self.config.get("bucket", "")
            prefix = self.config.get("prefix", "streamvideo")
            remote_key = f"{prefix}/{username}/{file_path.name}"

            if cloud_type in ("s3", "oss"):
                # 使用 AWS CLI 或 ossutil（需要预先配置）
                endpoint = self.config.get("endpoint", "")
                access_key = self.config.get("access_key", "")
                secret_key = self.config.get("secret_key", "")
                region = self.config.get("region", "us-east-1")

                env = os.environ.copy()
                env["AWS_ACCESS_KEY_ID"] = access_key
                env["AWS_SECRET_ACCESS_KEY"] = secret_key
                env["AWS_DEFAULT_REGION"] = region

                cmd = ["aws", "s3", "cp", str(file_path), f"s3://{bucket}/{remote_key}"]
                if endpoint:
                    cmd += ["--endpoint-url", endpoint]

                proc = await asyncio.create_subprocess_exec(
                    *cmd, env=env,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                )
                _, stderr = await asyncio.wait_for(proc.communicate(), timeout=600)
                if proc.returncode == 0:
                    url = f"s3://{bucket}/{remote_key}"
                    logger.info(f"[{username}] Uploaded to cloud: {url}")
                    return url
                else:
                    logger.warning(f"[{username}] Cloud upload failed: {(stderr.decode() if stderr else '')[:200]}")
            elif cloud_type == "rclone":
                # 使用 rclone（通用方案，支持所有云存储）
                remote = self.config.get("remote", "")
                cmd = ["rclone", "copy", str(file_path), f"{remote}:{bucket}/{prefix}/{username}/"]
                proc = await asyncio.create_subprocess_exec(
                    *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
                )
                _, stderr = await asyncio.wait_for(proc.communicate(), timeout=600)
                if proc.returncode == 0:
                    url = f"{remote}:{bucket}/{prefix}/{username}/{file_path.name}"
                    logger.info(f"[{username}] Uploaded via rclone: {url}")
                    return url
                else:
                    logger.warning(f"[{username}] rclone upload failed: {(stderr.decode() if stderr else '')[:200]}")
            else:
                logger.warning(f"[{username}] Unknown cloud type: {cloud_type}")
        except Exception as e:
            logger.warning(f"[{username}] Cloud upload error: {e}")
        return None


# ========== Webhook 通知 ==========
