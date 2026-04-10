"""
HLS Mouflon 代理服务器
用 asyncio HTTP server 实现，将 Stripchat 的 Mouflon HLS playlist 重写为标准格式。
"""

import asyncio
import base64
import logging
import re
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread
from urllib.request import Request, urlopen
from urllib.error import URLError

logger = logging.getLogger("hls_proxy")

PROXY_HOST = "http://127.0.0.1:7890"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


def _encode_url(url: str) -> str:
    return base64.urlsafe_b64encode(url.encode()).decode().rstrip("=")

def _decode_url(encoded: str) -> str:
    padding = 4 - len(encoded) % 4
    if padding != 4:
        encoded += "=" * padding
    return base64.urlsafe_b64decode(encoded.encode()).decode()


def fetch_url(url: str, proxy: str = PROXY_HOST) -> tuple[int, bytes]:
    """通过代理获取 URL 内容（使用 curl）"""
    import subprocess
    try:
        result = subprocess.run(
            ["curl", "-sL", "--connect-timeout", "10", "--max-time", "20",
             "-x", proxy,
             "-H", f"User-Agent: {USER_AGENT}",
             "-H", "Referer: https://stripchat.com/",
             "-w", "\n%{http_code}",
             url],
            capture_output=True, timeout=25,
        )
        # 最后一行是 HTTP 状态码
        parts = result.stdout.rsplit(b"\n", 1)
        if len(parts) == 2:
            body = parts[0]
            code = int(parts[1].strip())
        else:
            body = result.stdout
            code = 200 if result.returncode == 0 else 502
        return code, body
    except Exception as e:
        logger.error(f"Fetch failed [{url[:80]}]: {e}")
        return 502, b""


class HLSProxyHandler(BaseHTTPRequestHandler):
    proxy_port = 9876
    cdn_proxy = PROXY_HOST

    def log_message(self, format, *args):
        pass  # 静默日志

    def do_GET(self):
        # 路径就是 base64 编码的 URL（去掉开头的 /）
        encoded = self.path.lstrip("/")
        if not encoded:
            self.send_error(400, "Empty path")
            return

        try:
            target_url = _decode_url(encoded)
        except Exception as e:
            self.send_error(400, f"Invalid URL: {e}")
            return

        status, body = fetch_url(target_url, self.cdn_proxy)
        if status != 200 or not body:
            self.send_error(status or 502, "Upstream error")
            return

        # Playlist rewriting
        if ".m3u8" in target_url:
            text = body.decode("utf-8", errors="ignore")
            if "#EXT-X-STREAM-INF" in text:
                text = rewrite_master(text, self.proxy_port)
            else:
                text = rewrite_media(text, self.proxy_port)
            body = text.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/vnd.apple.mpegurl")
            self.send_header("Cache-Control", "no-cache")
        else:
            self.send_response(200)
            self.send_header("Content-Type", "video/mp4")

        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)


def _local_url(remote_url: str, port: int) -> str:
    return f"http://127.0.0.1:{port}/{_encode_url(remote_url)}"


def rewrite_master(content: str, port: int) -> str:
    lines = content.split("\n")
    result = []

    # 提取 MOUFLON PSCH 参数（CDN 认证必需）
    psch_params = ""
    for line in lines:
        m = re.match(r'#EXT-X-MOUFLON:PSCH:(\w+):(\S+)', line.strip())
        if m:
            psch_params = f"psch={m.group(1)}&pkey={m.group(2)}"
            break

    for line in lines:
        s = line.strip()
        if s.startswith("https://") and ".m3u8" in s:
            # 附加 psch/pkey 参数
            if psch_params:
                sep = "&" if "?" in s else "?"
                s = s + sep + psch_params
            result.append(_local_url(s, port))
        else:
            result.append(line)
    return "\n".join(result)


def rewrite_media(content: str, port: int) -> str:
    lines = content.split("\n")
    result = []
    mouflon_uri = None

    for line in lines:
        s = line.strip()

        if s.startswith("#EXT-X-MOUFLON:URI:"):
            mouflon_uri = s[len("#EXT-X-MOUFLON:URI:"):]
            continue
        if s.startswith("#EXT-X-MOUFLON:EXT-REF:"):
            continue
        if s.startswith("#EXT-X-SERVER-CONTROL:"):
            continue
        if s.startswith("#EXT-X-PART-INF:"):
            continue
        if s.startswith("#EXT-X-PART:"):
            mouflon_uri = None
            continue

        # Init segment
        map_match = re.match(r'#EXT-X-MAP:URI="([^"]+)"', s)
        if map_match:
            result.append(f'#EXT-X-MAP:URI="{_local_url(map_match.group(1), port)}"')
            continue

        # Segment URL
        if s and not s.startswith("#"):
            if mouflon_uri:
                result.append(_local_url(mouflon_uri, port))
                mouflon_uri = None
            elif s.startswith("http"):
                result.append(_local_url(s, port))
            else:
                result.append(line)
            continue

        result.append(line)

    return "\n".join(result)


class HLSProxy:
    def __init__(self, proxy: str = PROXY_HOST, port: int = 9876):
        self.proxy = proxy
        self.port = port
        self._server = None
        self._thread = None

    def start(self):
        HLSProxyHandler.proxy_port = self.port
        HLSProxyHandler.cdn_proxy = self.proxy
        self._server = HTTPServer(("127.0.0.1", self.port), HLSProxyHandler)
        self._thread = Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        logger.info(f"HLS 代理已启动: http://127.0.0.1:{self.port}")

    def stop(self):
        if self._server:
            self._server.shutdown()
            self._thread.join(timeout=3)
            logger.info("HLS 代理已停止")

    def get_proxied_master_url(self, original_url: str) -> str:
        return _local_url(original_url, self.port)
