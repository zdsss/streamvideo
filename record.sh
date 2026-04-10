#!/bin/bash
#
# Stripchat 直播自动监控录制脚本（健壮版）
#
# 特性:
#   - 自动检测主播在线状态并开始录制
#   - 网络波动/短暂断线自动重连（宽限期内不认为直播结束）
#   - API 走代理，HLS 流直连（CDN 不被墙）
#   - 支持 ffmpeg / streamlink / yt-dlp 多种录制后端
#   - 同一场直播的多个片段自动归入同一目录
#   - macOS 通知提醒（开播/下播）
#   - 日志文件持久化
#
# 用法:
#   ./record.sh [用户名] [输出目录]
#   ./record.sh nina520 ~/Videos/streams
#
# 依赖: curl, jq, ffmpeg
# 可选: streamlink, yt-dlp, terminal-notifier
#

set -uo pipefail

# ===================== 配置 =====================
MODEL_NAME="${1:-nina520}"
OUTPUT_DIR="${2:-$(pwd)/recordings}"

# 代理设置（ClashX 默认端口）
PROXY="http://127.0.0.1:7890"

# 时间间隔（秒）
POLL_INTERVAL_OFFLINE=30       # 离线时轮询间隔
POLL_INTERVAL_ONLINE=10        # 在线但录制未启动时的轮询间隔
GRACE_PERIOD=60                # 录制中断后的宽限期（秒），在此期间认为可能是短暂断线
RECONNECT_DELAY=3              # 重连前等待（秒）
MAX_CONSECUTIVE_FAILS=10       # 连续失败次数上限，超过后进入冷却
COOLDOWN=120                   # 冷却时间（秒）

# API
API_URL="https://stripchat.com/api/front/models/username"
HLS_CDN="https://edge-hls.doppiocdn.com/hls"
USER_AGENT="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# ===================== 颜色 & 日志 =====================
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; DIM='\033[2m'; NC='\033[0m'

LOG_DIR="${OUTPUT_DIR}/logs"
LOG_FILE=""

init_log() {
    mkdir -p "$LOG_DIR"
    LOG_FILE="${LOG_DIR}/${MODEL_NAME}_$(date '+%Y%m%d').log"
}

_log() {
    local color="$1" level="$2"; shift 2
    local ts
    ts=$(date '+%Y-%m-%d %H:%M:%S')
    echo -e "${color}[${ts}] [${level}]${NC} $*"
    echo "[${ts}] [${level}] $*" >> "$LOG_FILE" 2>/dev/null
}

log_info()  { _log "$CYAN"   "INFO"  "$@"; }
log_ok()    { _log "$GREEN"  " OK "  "$@"; }
log_warn()  { _log "$YELLOW" "WARN"  "$@"; }
log_err()   { _log "$RED"    " ERR"  "$@"; }
log_debug() { _log "$DIM"    "DEBG"  "$@"; }

# ===================== macOS 通知 =====================
notify() {
    local title="$1" msg="$2"
    if command -v terminal-notifier &>/dev/null; then
        terminal-notifier -title "$title" -message "$msg" -sound default &>/dev/null &
    elif command -v osascript &>/dev/null; then
        osascript -e "display notification \"$msg\" with title \"$title\"" &>/dev/null &
    fi
}

# ===================== 依赖检查 =====================
check_deps() {
    local missing=()
    for cmd in curl jq ffmpeg; do
        command -v "$cmd" &>/dev/null || missing+=("$cmd")
    done
    if [[ ${#missing[@]} -gt 0 ]]; then
        log_err "缺少必要依赖: ${missing[*]}"
        log_err "请运行: brew install ${missing[*]}"
        exit 1
    fi
    # 可选依赖提示
    command -v streamlink &>/dev/null || log_info "提示: 安装 streamlink 可获得更好的录制体验 (brew install streamlink)"
    command -v terminal-notifier &>/dev/null || log_info "提示: 安装 terminal-notifier 可获得桌面通知 (brew install terminal-notifier)"
}

# ===================== 代理检测 =====================
check_proxy() {
    if curl -s --connect-timeout 3 -x "$PROXY" -o /dev/null "https://stripchat.com" 2>/dev/null; then
        log_ok "代理连接正常 ($PROXY)"
        return 0
    else
        log_warn "代理不可用 ($PROXY)，尝试直连..."
        if curl -s --connect-timeout 5 -o /dev/null "https://stripchat.com" 2>/dev/null; then
            PROXY=""
            log_ok "直连可用，不使用代理"
            return 0
        fi
        log_err "无法连接 stripchat.com（代理和直连均失败）"
        return 1
    fi
}

# ===================== API 请求封装 =====================
api_curl() {
    local url="$1"
    local proxy_args=()
    [[ -n "$PROXY" ]] && proxy_args=(-x "$PROXY")
    curl -s --connect-timeout 10 --max-time 15 \
        "${proxy_args[@]}" \
        -H "User-Agent: $USER_AGENT" \
        -H "Accept: application/json" \
        "$url" 2>/dev/null
}

# ===================== 获取主播信息 =====================
# 返回: JSON 字符串，失败返回空
get_model_info() {
    local response
    response=$(api_curl "${API_URL}/${MODEL_NAME}")
    if [[ -n "$response" ]] && echo "$response" | jq -e '.id' &>/dev/null; then
        echo "$response"
        return 0
    fi
    return 1
}

# ===================== 检查在线状态 =====================
# 返回: 0=在线, 1=离线/错误
# 输出: model_id（如果在线）
check_online() {
    local info
    info=$(get_model_info) || return 1

    local status is_live model_id
    status=$(echo "$info" | jq -r '.status // "offline"')
    is_live=$(echo "$info" | jq -r '.isLive // false')
    model_id=$(echo "$info" | jq -r '.id // empty')

    if [[ "$is_live" == "true" && ("$status" == "public" || "$status" == "p2p") ]]; then
        echo "$model_id"
        return 0
    fi
    return 1
}

# ===================== 构建 HLS 流地址 =====================
get_hls_url() {
    local model_id="$1"
    local proxy_args=()
    [[ -n "$PROXY" ]] && proxy_args=(-x "$PROXY")

    # 尝试主地址和备用地址（CDN 也可能被墙，统一走代理）
    local urls=(
        "${HLS_CDN}/${model_id}/master/${model_id}_auto.m3u8"
        "${HLS_CDN}/${model_id}/master/${model_id}.m3u8"
    )

    for url in "${urls[@]}"; do
        local code
        code=$(curl -s --connect-timeout 5 --max-time 8 "${proxy_args[@]}" \
            -o /dev/null -w "%{http_code}" \
            -H "User-Agent: $USER_AGENT" \
            -H "Referer: https://stripchat.com/" \
            "$url" 2>/dev/null)
        if [[ "$code" == "200" ]]; then
            echo "$url"
            return 0
        fi
    done

    return 1
}

# ===================== 录制: ffmpeg =====================
record_ffmpeg() {
    local stream_url="$1" output_file="$2"
    log_info "录制引擎: ffmpeg"
    log_debug "流地址: $stream_url"

    # CDN 被墙，ffmpeg 也需要走代理（通过 http_proxy 环境变量）
    local proxy_env=()
    if [[ -n "$PROXY" ]]; then
        proxy_env=(env http_proxy="$PROXY" https_proxy="$PROXY")
    fi

    "${proxy_env[@]}" ffmpeg -y -hide_banner -loglevel warning -stats \
        -user_agent "$USER_AGENT" \
        -headers "Referer: https://stripchat.com/\r\n" \
        -reconnect 1 \
        -reconnect_streamed 1 \
        -reconnect_delay_max 10 \
        -reconnect_at_eof 1 \
        -i "$stream_url" \
        -c copy \
        -movflags +faststart \
        "$output_file"
    return $?
}

# ===================== 录制: yt-dlp（首选）=====================
record_ytdlp() {
    local output_file="$1"
    if ! command -v yt-dlp &>/dev/null; then
        return 1
    fi

    log_info "录制引擎: yt-dlp"
    local proxy_args=()
    [[ -n "$PROXY" ]] && proxy_args=(--proxy "$PROXY")

    local tmp_log
    tmp_log=$(mktemp /tmp/ytdlp_XXXXXX.log)

    yt-dlp \
        "${proxy_args[@]}" \
        --no-part \
        --no-overwrites \
        --hls-use-mpegts \
        --live-from-start \
        --wait-for-video 5 \
        -o "$output_file" \
        "https://stripchat.com/${MODEL_NAME}" 2>&1 | tee "$tmp_log" | while IFS= read -r line; do
            log_debug "[yt-dlp] $line"
        done
    local rc="${PIPESTATUS[0]}"

    # 检查是否是私密秀（不应回退到 ffmpeg）
    if grep -q "private show\|private\|Model is offline" "$tmp_log" 2>/dev/null; then
        log_warn "主播在私密秀或已离线，跳过录制"
        rm -f "$tmp_log"
        return 2  # 返回 2 表示不需要回退
    fi

    rm -f "$tmp_log"
    return $rc
}

# ===================== 录制调度 =====================
# 为同一场直播创建一个 session 目录，多个片段放在一起
SESSION_DIR=""
SEGMENT_NUM=0

start_session() {
    local ts
    ts=$(date '+%Y%m%d_%H%M%S')
    SESSION_DIR="${OUTPUT_DIR}/${MODEL_NAME}/${ts}"
    SEGMENT_NUM=0
    mkdir -p "$SESSION_DIR"
    log_ok "新录制会话: ${SESSION_DIR}"
}

next_segment_file() {
    ((SEGMENT_NUM++))
    echo "${SESSION_DIR}/seg_$(printf '%03d' $SEGMENT_NUM).mp4"
}

do_record() {
    local model_id="$1"
    local output_file
    output_file=$(next_segment_file)

    log_ok "录制片段 #${SEGMENT_NUM} → $(basename "$output_file")"

    # 优先 yt-dlp（内置 Stripchat 提取器，自动处理 token），回退到 ffmpeg
    record_ytdlp "$output_file"
    local rc=$?

    if [[ $rc -eq 0 ]]; then
        return 0
    elif [[ $rc -eq 2 ]]; then
        # 私密秀/离线，不回退到 ffmpeg（ffmpeg 只能录到广告预览）
        return 1
    fi

    log_warn "yt-dlp 失败，使用 ffmpeg 直录..."
    local hls_url
    hls_url=$(get_hls_url "$model_id")
    if [[ -z "$hls_url" ]]; then
        log_err "无法获取 HLS 流地址"
        return 1
    fi

    record_ffmpeg "$hls_url" "$output_file"
    return $?
}

# ===================== 清理空文件 =====================
cleanup_empty_files() {
    if [[ -n "$SESSION_DIR" && -d "$SESSION_DIR" ]]; then
        find "$SESSION_DIR" -name "*.mp4" -size 0 -delete 2>/dev/null || true
        # 如果目录为空也删除
        rmdir "$SESSION_DIR" 2>/dev/null || true
    fi
}

# ===================== 统计信息 =====================
print_session_stats() {
    if [[ -n "$SESSION_DIR" && -d "$SESSION_DIR" ]]; then
        local count total_size
        count=$(find "$SESSION_DIR" -name "*.mp4" -type f 2>/dev/null | wc -l | tr -d ' ')
        total_size=$(du -sh "$SESSION_DIR" 2>/dev/null | cut -f1)
        if [[ "$count" -gt 0 ]]; then
            log_info "本次会话: ${count} 个片段, 总大小 ${total_size}"
        fi
    fi
}

# ===================== 主循环 =====================
main() {
    init_log
    check_deps

    log_info "========================================="
    log_info "  直播监控录制 - ${MODEL_NAME}"
    log_info "  输出目录: ${OUTPUT_DIR}"
    log_info "  日志文件: ${LOG_FILE}"
    log_info "  代理: ${PROXY:-直连}"
    log_info "  宽限期: ${GRACE_PERIOD}s"
    log_info "========================================="

    mkdir -p "$OUTPUT_DIR"

    # 检查代理
    check_proxy || exit 1

    local consecutive_fails=0
    local in_session=false
    local last_online_time=0

    while true; do
        local model_id
        model_id=$(check_online 2>/dev/null)

        if [[ -n "$model_id" ]]; then
            # ---- 主播在线 ----
            last_online_time=$(date +%s)
            consecutive_fails=0

            if [[ "$in_session" == false ]]; then
                # 新的直播会话
                log_ok "${MODEL_NAME} 开播了！(ID: ${model_id})"
                notify "开播提醒" "${MODEL_NAME} 正在直播！"
                start_session
                in_session=true
            fi

            # 开始录制（阻塞直到录制结束）
            do_record "$model_id"
            local rc=$?

            cleanup_empty_files

            if [[ $rc -ne 0 ]]; then
                ((consecutive_fails++))
                log_warn "录制中断 (连续失败: ${consecutive_fails}/${MAX_CONSECUTIVE_FAILS})"

                if [[ $consecutive_fails -ge $MAX_CONSECUTIVE_FAILS ]]; then
                    log_err "连续失败过多，冷却 ${COOLDOWN}s..."
                    sleep "$COOLDOWN"
                    consecutive_fails=0
                    continue
                fi
            fi

            # 录制结束后，进入宽限期检查
            # 短暂等待后确认是否真的下播
            log_info "录制结束，进入宽限期检查 (${GRACE_PERIOD}s)..."
            local grace_start grace_now elapsed
            grace_start=$(date +%s)

            while true; do
                sleep "$RECONNECT_DELAY"
                grace_now=$(date +%s)
                elapsed=$((grace_now - grace_start))

                if [[ $elapsed -ge $GRACE_PERIOD ]]; then
                    log_info "宽限期结束，确认下播"
                    break
                fi

                # 在宽限期内反复检查
                local check_id
                check_id=$(check_online 2>/dev/null)
                if [[ -n "$check_id" ]]; then
                    log_ok "宽限期内检测到重新上线 (${elapsed}s)，继续录制！"
                    consecutive_fails=0
                    # 跳出宽限期循环，回到主循环继续录制
                    break
                fi

                log_debug "宽限期检查 ${elapsed}/${GRACE_PERIOD}s - 仍离线"
            done

            # 再次检查，如果在线则继续主循环（会重新录制）
            check_id=$(check_online 2>/dev/null)
            if [[ -n "$check_id" ]]; then
                continue
            fi

            # 确认下播
            if [[ "$in_session" == true ]]; then
                log_warn "${MODEL_NAME} 已下播"
                notify "下播提醒" "${MODEL_NAME} 直播结束"
                print_session_stats
                in_session=false
                SESSION_DIR=""
                consecutive_fails=0
            fi

        else
            # ---- 主播离线 ----
            if [[ "$in_session" == true ]]; then
                # 刚从在线变为离线，可能是短暂断线
                local now elapsed_since_online
                now=$(date +%s)
                elapsed_since_online=$((now - last_online_time))

                if [[ $elapsed_since_online -lt $GRACE_PERIOD ]]; then
                    log_debug "离线但在宽限期内 (${elapsed_since_online}s)，快速重检..."
                    sleep "$RECONNECT_DELAY"
                    continue
                fi

                # 超过宽限期，确认下播
                log_warn "${MODEL_NAME} 已下播"
                notify "下播提醒" "${MODEL_NAME} 直播结束"
                print_session_stats
                in_session=false
                SESSION_DIR=""
                consecutive_fails=0
            fi

            log_info "${MODEL_NAME} 离线，${POLL_INTERVAL_OFFLINE}s 后再检查"
        fi

        sleep "$POLL_INTERVAL_OFFLINE"
    done
}

# ===================== 信号处理 =====================
cleanup() {
    echo ""
    log_warn "收到退出信号，正在停止录制..."
    cleanup_empty_files
    print_session_stats
    # 终止所有子进程（ffmpeg/streamlink）
    jobs -p | xargs -r kill 2>/dev/null
    wait 2>/dev/null
    log_info "已退出"
    exit 0
}
trap cleanup SIGINT SIGTERM

# ===================== 启动 =====================
main
