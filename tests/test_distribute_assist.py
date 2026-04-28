"""
测试 distribute.py 的辅助投稿功能（B站、微信视频号）
"""
import pytest
import asyncio
from distribute import (
    BilibiliAssistPublisher, WeixinVideoPublisher,
    DouyinPublisher, KuaishouPublisher,
    DistributeTask, DistributeStatus, DistributeManager, MockPublisher
)
import time


def make_task(**kwargs):
    defaults = dict(
        task_id="t_test",
        clip_id="c_test",
        username="streamer1",
        platform="bilibili",
        file_path="/tmp/test.mp4",
        title="精彩片段",
        description="直播高光",
        tags=["直播", "精彩"],
        created_at=time.time(),
        updated_at=time.time(),
    )
    defaults.update(kwargs)
    return DistributeTask(**defaults)


# ===== BilibiliAssistPublisher =====

@pytest.mark.asyncio
async def test_bilibili_authenticate():
    pub = BilibiliAssistPublisher()
    assert await pub.authenticate({}) is True
    assert await pub.authenticate({"token": "anything"}) is True


@pytest.mark.asyncio
async def test_bilibili_upload_returns_assist_mode():
    pub = BilibiliAssistPublisher()
    task = make_task(platform="bilibili")
    result = await pub.upload(task, {})
    assert result["assist_mode"] is True
    assert "bilibili" in result["open_url"]
    assert result["title"] == "精彩片段"
    assert result["tags"] == ["直播", "精彩"]


@pytest.mark.asyncio
async def test_bilibili_upload_contains_file_path():
    pub = BilibiliAssistPublisher()
    task = make_task(platform="bilibili", file_path="/recordings/clip.mp4")
    result = await pub.upload(task, {})
    assert result["file_path"] == "/recordings/clip.mp4"


@pytest.mark.asyncio
async def test_bilibili_check_status():
    pub = BilibiliAssistPublisher()
    result = await pub.check_status("any_id", {})
    assert result["status"] == "published"


# ===== WeixinVideoPublisher =====

@pytest.mark.asyncio
async def test_weixin_authenticate():
    pub = WeixinVideoPublisher()
    assert await pub.authenticate({}) is True


@pytest.mark.asyncio
async def test_weixin_upload_returns_assist_mode():
    pub = WeixinVideoPublisher()
    task = make_task(platform="weixinvideo")
    result = await pub.upload(task, {})
    assert result["assist_mode"] is True
    assert "channels.weixin.qq.com" in result["open_url"]
    assert result["title"] == "精彩片段"


@pytest.mark.asyncio
async def test_weixin_upload_remote_id_unique():
    pub = WeixinVideoPublisher()
    task1 = make_task(task_id="t1", platform="weixinvideo")
    task2 = make_task(task_id="t2", platform="weixinvideo")
    r1 = await pub.upload(task1, {})
    r2 = await pub.upload(task2, {})
    assert r1["remote_id"] != r2["remote_id"]


@pytest.mark.asyncio
async def test_weixin_check_status():
    pub = WeixinVideoPublisher()
    result = await pub.check_status("any_id", {})
    assert result["status"] == "published"


# ===== DistributeManager 集成 =====

@pytest.mark.asyncio
async def test_manager_registers_assist_publishers():
    dm = DistributeManager()
    dm.register_publisher("bilibili", BilibiliAssistPublisher())
    dm.set_credentials("bilibili", {})
    dm.register_publisher("weixinvideo", WeixinVideoPublisher())
    dm.set_credentials("weixinvideo", {})
    platforms = dm.get_available_platforms()
    assert "bilibili" in platforms
    assert "weixinvideo" in platforms


@pytest.mark.asyncio
async def test_manager_execute_bilibili_assist():
    dm = DistributeManager()
    dm.register_publisher("bilibili", BilibiliAssistPublisher())
    dm.set_credentials("bilibili", {})
    task = await dm.create_task(
        clip_id="c1", username="user1", platform="bilibili",
        file_path="/tmp/test.mp4", title="测试"
    )
    result = await dm.execute_task(task.task_id)
    assert result.status == DistributeStatus.PUBLISHED
    assert result.remote_url and "bilibili" in result.remote_url


@pytest.mark.asyncio
async def test_manager_execute_weixinvideo_assist():
    dm = DistributeManager()
    dm.register_publisher("weixinvideo", WeixinVideoPublisher())
    dm.set_credentials("weixinvideo", {})
    task = await dm.create_task(
        clip_id="c2", username="user1", platform="weixinvideo",
        file_path="/tmp/test.mp4", title="视频号测试"
    )
    result = await dm.execute_task(task.task_id)
    assert result.status == DistributeStatus.PUBLISHED
    assert result.remote_url and "weixin" in result.remote_url


# ===== DouyinPublisher / KuaishouPublisher 错误处理 =====

@pytest.mark.asyncio
async def test_douyin_upload_no_token_raises_valueerror():
    pub = DouyinPublisher()
    task = make_task(platform="douyin")
    with pytest.raises(ValueError, match="未授权"):
        await pub.upload(task, {})


@pytest.mark.asyncio
async def test_douyin_upload_with_token_raises_not_implemented():
    pub = DouyinPublisher()
    task = make_task(platform="douyin")
    with pytest.raises(ValueError, match="开发中"):
        await pub.upload(task, {"access_token": "tok123"})


@pytest.mark.asyncio
async def test_kuaishou_upload_no_token_raises_valueerror():
    pub = KuaishouPublisher()
    task = make_task(platform="kuaishou")
    with pytest.raises(ValueError, match="未授权"):
        await pub.upload(task, {})


@pytest.mark.asyncio
async def test_douyin_check_status_returns_unknown():
    pub = DouyinPublisher()
    result = await pub.check_status("any_id", {})
    assert result["status"] == "unknown"


@pytest.mark.asyncio
async def test_manager_execute_douyin_no_token_fails_immediately():
    """无 token 时直接 FAILED，不重试"""
    dm = DistributeManager()
    dm.register_publisher("douyin", DouyinPublisher())
    dm.set_credentials("douyin", {})
    task = await dm.create_task(
        clip_id="c1", username="user1", platform="douyin",
        file_path="/tmp/test.mp4", title="测试"
    )
    result = await dm.execute_task(task.task_id)
    assert result.status == DistributeStatus.FAILED
    assert result.retry_count == 0  # ValueError 不触发重试
