"""
StreamVideo 核心模块测试
运行: pytest tests/ -v
"""

import os
import sys
import time
import pytest
import tempfile
import sqlite3

# 确保项目根目录在 path 中
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def tmp_db():
    """创建临时数据库"""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    from database import Database
    db = Database(db_path)
    yield db
    os.unlink(db_path)


# ========== Auth Tests ==========

class TestAuth:
    def test_register(self, tmp_db):
        from auth import AuthManager
        am = AuthManager(tmp_db)
        user = am.register("test@example.com", "password123", "Test")
        assert user["email"] == "test@example.com"
        assert user["display_name"] == "Test"
        assert user["role"] == "user"
        assert user["user_id"].startswith("u_")

    def test_register_duplicate(self, tmp_db):
        from auth import AuthManager
        am = AuthManager(tmp_db)
        am.register("dup@example.com", "pass123")
        with pytest.raises(ValueError, match="已注册"):
            am.register("dup@example.com", "pass456")

    def test_register_short_password(self, tmp_db):
        from auth import AuthManager
        am = AuthManager(tmp_db)
        with pytest.raises(ValueError, match="至少 6 位"):
            am.register("short@example.com", "123")

    def test_login_success(self, tmp_db):
        from auth import AuthManager
        am = AuthManager(tmp_db)
        am.register("login@example.com", "mypassword")
        result = am.login("login@example.com", "mypassword")
        assert "session_token" in result
        assert result["user"]["email"] == "login@example.com"
        assert result["expires_at"] > time.time()

    def test_login_wrong_password(self, tmp_db):
        from auth import AuthManager
        am = AuthManager(tmp_db)
        am.register("wrong@example.com", "correct")
        with pytest.raises(ValueError, match="密码错误"):
            am.login("wrong@example.com", "incorrect")

    def test_login_nonexistent(self, tmp_db):
        from auth import AuthManager
        am = AuthManager(tmp_db)
        with pytest.raises(ValueError, match="密码错误"):
            am.login("nobody@example.com", "pass")

    def test_session_validate(self, tmp_db):
        from auth import AuthManager
        am = AuthManager(tmp_db)
        am.register("sess@example.com", "pass123")
        result = am.login("sess@example.com", "pass123")
        user = am.validate_session(result["session_token"])
        assert user is not None
        assert user["email"] == "sess@example.com"

    def test_session_invalid_token(self, tmp_db):
        from auth import AuthManager
        am = AuthManager(tmp_db)
        assert am.validate_session("bogus_token") is None
        assert am.validate_session("") is None

    def test_logout(self, tmp_db):
        from auth import AuthManager
        am = AuthManager(tmp_db)
        am.register("out@example.com", "pass123")
        result = am.login("out@example.com", "pass123")
        token = result["session_token"]
        assert am.validate_session(token) is not None
        am.logout(token)
        assert am.validate_session(token) is None

    def test_password_hashing(self):
        from auth import _hash_password, _verify_password
        h = _hash_password("secret")
        assert _verify_password("secret", h)
        assert not _verify_password("wrong", h)
        # 不同调用产生不同 salt
        h2 = _hash_password("secret")
        assert h != h2
        assert _verify_password("secret", h2)

    def test_get_users(self, tmp_db):
        from auth import AuthManager
        am = AuthManager(tmp_db)
        am.register("a@example.com", "pass123")
        am.register("b@example.com", "pass123")
        users = am.get_users()
        assert len(users) == 2


# ========== Quota Tests ==========

class TestQuota:
    def test_default_tier(self, tmp_db):
        from quota import QuotaManager
        qm = QuotaManager(tmp_db)
        assert qm.get_tier("new_user") == "free"

    def test_set_tier(self, tmp_db):
        from quota import QuotaManager
        qm = QuotaManager(tmp_db)
        qm.set_tier("user1", "pro")
        assert qm.get_tier("user1") == "pro"

    def test_invalid_tier(self, tmp_db):
        from quota import QuotaManager
        qm = QuotaManager(tmp_db)
        with pytest.raises(ValueError, match="无效套餐"):
            qm.set_tier("user1", "diamond")

    def test_tier_expiry(self, tmp_db):
        from quota import QuotaManager
        qm = QuotaManager(tmp_db)
        # 设置已过期的 pro
        qm.set_tier("user2", "pro", expires_at=time.time() - 100)
        assert qm.get_tier("user2") == "free"

    def test_check_quota_free(self, tmp_db):
        from quota import QuotaManager
        qm = QuotaManager(tmp_db)
        allowed, used, limit = qm.check_quota("free_user")
        assert allowed is True
        assert used == 0
        assert limit == 3

    def test_consume_quota(self, tmp_db):
        from quota import QuotaManager
        qm = QuotaManager(tmp_db)
        qm.consume_quota("user3", 2)
        allowed, used, limit = qm.check_quota("user3")
        assert used == 2
        assert allowed is True
        qm.consume_quota("user3", 1)
        allowed, used, limit = qm.check_quota("user3")
        assert used == 3
        assert allowed is False

    def test_watermark(self, tmp_db):
        from quota import QuotaManager
        qm = QuotaManager(tmp_db)
        assert qm.should_watermark("free_user") is True
        qm.set_tier("pro_user", "pro")
        assert qm.should_watermark("pro_user") is False

    def test_check_feature(self, tmp_db):
        from quota import QuotaManager
        qm = QuotaManager(tmp_db)
        assert qm.check_feature("free_user", "h265_transcode") is False
        assert qm.check_feature("free_user", "cloud_upload") is False
        qm.set_tier("pro_user", "pro")
        assert qm.check_feature("pro_user", "h265_transcode") is True
        assert qm.check_feature("pro_user", "api_access") is False
        qm.set_tier("team_user", "team")
        assert qm.check_feature("team_user", "api_access") is True

    def test_tier_info(self, tmp_db):
        from quota import QuotaManager
        qm = QuotaManager(tmp_db)
        info = qm.get_tier_info("new_user")
        assert info["tier"] == "free"
        assert info["tier_name"] == "免费版"
        assert info["daily_limit"] == 3
        assert info["remaining"] == 3
        assert "features" in info

    def test_tier_definitions(self):
        from quota import QuotaManager
        tiers = QuotaManager.get_tier_definitions()
        assert "free" in tiers
        assert "pro" in tiers
        assert "team" in tiers
        assert tiers["free"]["watermark"] is True
        assert tiers["pro"]["watermark"] is False


# ========== Highlight Tests ==========

class TestHighlight:
    def test_keyword_weights(self):
        from highlight import KEYWORD_WEIGHTS, KEYWORDS_HIGH, KEYWORDS_MEDIUM
        assert len(KEYWORD_WEIGHTS) > 40
        assert KEYWORD_WEIGHTS["秒杀"] == 1.0
        assert KEYWORD_WEIGHTS["666"] == 0.6
        assert KEYWORD_WEIGHTS["加油"] == 0.3

    def test_detector_init_default(self):
        from highlight import HighlightDetector
        d = HighlightDetector()
        assert d.min_score == 0.6
        assert d.min_duration == 15
        assert d.max_duration == 60
        assert "gift_spike" in d.weights

    def test_detector_init_custom(self):
        from highlight import HighlightDetector
        d = HighlightDetector({"highlight_min_score": 0.8, "highlight_keywords": ["自定义"]})
        assert d.min_score == 0.8
        assert "自定义" in d.keyword_weights
        assert d.keyword_weights["自定义"] == 0.8  # 用户自定义默认权重

    def test_score_and_merge_empty(self):
        from highlight import HighlightDetector
        d = HighlightDetector()
        assert d._score_and_merge([], 100) == []

    def test_determine_category(self):
        from highlight import HighlightDetector
        d = HighlightDetector()
        signals = [{"type": "danmaku_peak", "strength": 0.9}, {"type": "audio_peak", "strength": 0.3}]
        assert d._determine_category(signals) == "engagement_spike"
        signals2 = [{"type": "keyword_match", "strength": 0.8}]
        assert d._determine_category(signals2) == "keyword_trigger"
        signals3 = [{"type": "gift_spike", "strength": 0.7}]
        assert d._determine_category(signals3) == "gift_spike"

    def test_auto_title(self):
        from highlight import HighlightDetector, Highlight
        d = HighlightDetector()
        h = Highlight(start_time=125.0, end_time=140.0, score=0.8, category="engagement_spike")
        title = d._auto_title(h, 1)
        assert "高光 #1" in title
        assert "弹幕爆发" in title
        assert "02:05" in title

    def test_merge_overlapping(self):
        from highlight import HighlightDetector, Highlight
        d = HighlightDetector()
        h1 = Highlight(start_time=10, end_time=30, score=0.7, category="audio_peak")
        h2 = Highlight(start_time=25, end_time=50, score=0.9, category="engagement_spike")
        h3 = Highlight(start_time=100, end_time=120, score=0.6, category="keyword_trigger")
        merged = d._merge_overlapping([h1, h2, h3])
        assert len(merged) == 2
        assert merged[0].end_time == 50
        assert merged[0].score == 0.9


# ========== Subtitle Tests ==========

class TestSubtitle:
    def test_punctuation_restore(self):
        from subtitle_gen import _restore_punctuation
        assert _restore_punctuation("你好啊") == "你好啊。"
        assert _restore_punctuation("你好啊。") == "你好啊。"
        assert _restore_punctuation("你好啊！") == "你好啊！"
        assert _restore_punctuation("") == ""

    def test_split_long_segments(self):
        from subtitle_gen import _split_long_segments
        segs = [{"start": 0, "end": 10, "text": "短句"}]
        result = _split_long_segments(segs, max_chars=20)
        assert len(result) == 1

        segs2 = [{"start": 0, "end": 10, "text": "这是一个非常非常长的句子，需要被拆分成多行才能正常显示在屏幕上面"}]
        result2 = _split_long_segments(segs2, max_chars=20)
        assert len(result2) >= 2
        # 时间应该连续
        for i in range(1, len(result2)):
            assert abs(result2[i]["start"] - result2[i-1]["end"]) < 0.01

    def test_format_srt_time(self):
        from subtitle_gen import SubtitleGenerator
        assert SubtitleGenerator._format_srt_time(0) == "00:00:00,000"
        assert SubtitleGenerator._format_srt_time(3661.5) == "01:01:01,500"

    def test_format_vtt_time(self):
        from subtitle_gen import SubtitleGenerator
        assert SubtitleGenerator._format_vtt_time(0) == "00:00:00.000"
        assert SubtitleGenerator._format_vtt_time(3661.5) == "01:01:01.500"

    def test_format_ass_time(self):
        from subtitle_gen import SubtitleGenerator
        assert SubtitleGenerator._format_ass_time(0) == "0:00:00.00"
        assert SubtitleGenerator._format_ass_time(3661.5) == "1:01:01.50"

    def test_to_srt(self):
        from subtitle_gen import SubtitleGenerator
        segs = [{"start": 1.0, "end": 3.5, "text": "Hello"}, {"start": 4.0, "end": 6.0, "text": "World"}]
        srt = SubtitleGenerator._to_srt(segs)
        assert "1\n00:00:01,000 --> 00:00:03,500\nHello" in srt
        assert "2\n00:00:04,000 --> 00:00:06,000\nWorld" in srt

    def test_to_vtt(self):
        from subtitle_gen import SubtitleGenerator
        segs = [{"start": 1.0, "end": 3.0, "text": "Test"}]
        vtt = SubtitleGenerator._to_vtt(segs)
        assert vtt.startswith("WEBVTT")
        assert "00:00:01.000 --> 00:00:03.000" in vtt

    def test_to_ass(self):
        from subtitle_gen import SubtitleGenerator
        segs = [{"start": 1.0, "end": 3.0, "text": "Test"}]
        ass = SubtitleGenerator._to_ass(segs)
        assert "[Script Info]" in ass
        assert "Dialogue:" in ass


# ========== Cover Tests ==========

class TestCover:
    def test_sizes(self):
        from cover_gen import SIZES
        assert SIZES["vertical"] == (1080, 1920)
        assert SIZES["horizontal"] == (1920, 1080)
        assert SIZES["square"] == (1080, 1080)

    def test_pick_seek_time_with_highlight(self):
        from cover_gen import CoverGenerator
        g = CoverGenerator()
        assert g._pick_seek_time(300, 100) == 102  # highlight + 2s
        assert g._pick_seek_time(300, 298) == 299  # clamped to duration - 1

    def test_pick_seek_time_no_highlight(self):
        from cover_gen import CoverGenerator
        g = CoverGenerator()
        assert g._pick_seek_time(300, None) == 90.0  # 30% of 300
        assert g._pick_seek_time(10, None) == 3.0


# ========== Distribute Tests ==========

class TestDistribute:
    def test_create_manager(self):
        from distribute import DistributeManager
        dm = DistributeManager()
        assert dm.get_available_platforms() == []

    def test_register_publisher(self):
        from distribute import DistributeManager, MockPublisher
        dm = DistributeManager()
        dm.register_publisher("mock", MockPublisher())
        dm.set_credentials("mock", {"token": "test"})
        assert "mock" in dm.get_available_platforms()

    @pytest.mark.asyncio
    async def test_create_and_execute_task(self):
        from distribute import DistributeManager, MockPublisher
        dm = DistributeManager()
        dm.register_publisher("mock", MockPublisher())
        dm.set_credentials("mock", {"token": "test"})

        task = await dm.create_task(
            clip_id="c_test", username="testuser", platform="mock",
            file_path="/tmp/test.mp4", title="Test Video"
        )
        assert task.task_id.startswith("dist_")
        assert task.status == "pending"

        result = await dm.execute_task(task.task_id)
        assert result.status == "processing"
        assert result.remote_id.startswith("mock_")
        assert "example.com" in result.remote_url

    @pytest.mark.asyncio
    async def test_check_status(self):
        from distribute import DistributeManager, MockPublisher
        dm = DistributeManager()
        dm.register_publisher("mock", MockPublisher())
        dm.set_credentials("mock", {"token": "test"})

        task = await dm.create_task("c1", "user", "mock", "/tmp/t.mp4")
        await dm.execute_task(task.task_id)
        result = await dm.check_task_status(task.task_id)
        assert result.status == "published"

    def test_unsupported_platform(self):
        from distribute import DistributeManager
        dm = DistributeManager()
        with pytest.raises(ValueError, match="not supported"):
            import asyncio
            asyncio.run(dm.create_task("c1", "user", "tiktok", "/tmp/t.mp4"))

    def test_get_tasks(self):
        from distribute import DistributeManager, MockPublisher
        dm = DistributeManager()
        dm.register_publisher("mock", MockPublisher())
        dm.set_credentials("mock", {"token": "test"})
        import asyncio
        asyncio.run(dm.create_task("c1", "user1", "mock", "/tmp/1.mp4"))
        asyncio.run(dm.create_task("c2", "user2", "mock", "/tmp/2.mp4"))
        all_tasks = dm.get_tasks()
        assert len(all_tasks) == 2
        user1_tasks = dm.get_tasks(username="user1")
        assert len(user1_tasks) == 1


# ========== Database Tests ==========

class TestDatabase:
    def test_init(self, tmp_db):
        assert tmp_db is not None

    def test_settings(self, tmp_db):
        tmp_db.set_setting("test_key", "test_value")
        settings = tmp_db.get_settings()
        assert settings.get("test_key") == "test_value"

    def test_distribute_tasks(self, tmp_db):
        task = {
            "task_id": "dist_test1",
            "clip_id": "c1",
            "username": "user1",
            "platform": "mock",
            "file_path": "/tmp/test.mp4",
            "title": "Test",
            "tags": ["tag1", "tag2"],
            "status": "pending",
            "created_at": time.time(),
            "updated_at": time.time(),
        }
        tmp_db.upsert_distribute_task(task)
        tasks = tmp_db.get_distribute_tasks()
        assert len(tasks) == 1
        assert tasks[0]["task_id"] == "dist_test1"
        assert tasks[0]["tags"] == ["tag1", "tag2"]

        # Update
        task["status"] = "published"
        tmp_db.upsert_distribute_task(task)
        t = tmp_db.get_distribute_task("dist_test1")
        assert t["status"] == "published"
