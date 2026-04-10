#!/bin/bash
#
# 多主播并行监控管理脚本
#
# 用法:
#   ./monitor.sh start    - 启动所有主播监控
#   ./monitor.sh stop     - 停止所有监控
#   ./monitor.sh status   - 查看运行状态
#   ./monitor.sh restart  - 重启所有监控
#   ./monitor.sh log <名字> - 查看某个主播的实时日志
#

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RECORD_SCRIPT="${SCRIPT_DIR}/record.sh"
OUTPUT_BASE="${SCRIPT_DIR}/recordings"
PID_DIR="${SCRIPT_DIR}/.pids"
LOG_DIR="${OUTPUT_BASE}/logs"

# ========== 监控列表 ==========
MODELS=(
    "nina520"
    "Linh_2004"
    "Xiaozuzongo1"
    "jiajia_L"
    "Krismil3"
)

# ========== 颜色 ==========
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

# ========== 工具函数 ==========
pid_file() { echo "${PID_DIR}/${1}.pid"; }
log_file() { echo "${LOG_DIR}/${1}.log"; }

is_running() {
    local pf
    pf=$(pid_file "$1")
    if [[ -f "$pf" ]]; then
        local pid
        pid=$(cat "$pf")
        if kill -0 "$pid" 2>/dev/null; then
            return 0
        fi
        rm -f "$pf"
    fi
    return 1
}

# ========== 启动 ==========
do_start() {
    mkdir -p "$PID_DIR" "$LOG_DIR"

    echo -e "${BOLD}启动监控...${NC}"
    echo ""

    local started=0 skipped=0

    for model in "${MODELS[@]}"; do
        if is_running "$model"; then
            local pid
            pid=$(cat "$(pid_file "$model")")
            echo -e "  ${YELLOW}[跳过]${NC} ${model} - 已在运行 (PID: ${pid})"
            ((skipped++))
            continue
        fi

        local model_output="${OUTPUT_BASE}"
        local lf
        lf=$(log_file "$model")

        # 启动后台录制进程
        nohup bash "$RECORD_SCRIPT" "$model" "$model_output" >> "$lf" 2>&1 &
        local pid=$!

        echo "$pid" > "$(pid_file "$model")"
        echo -e "  ${GREEN}[启动]${NC} ${model} (PID: ${pid}) → ${model_output}/${model}/"
        ((started++))
    done

    echo ""
    echo -e "${BOLD}完成:${NC} 启动 ${started} 个, 跳过 ${skipped} 个"
    echo -e "日志目录: ${LOG_DIR}"
    echo -e "录制目录: ${OUTPUT_BASE}/<主播名>/"
    echo ""
    echo -e "查看状态: ${CYAN}./monitor.sh status${NC}"
    echo -e "查看日志: ${CYAN}./monitor.sh log <主播名>${NC}"
    echo -e "停止所有: ${CYAN}./monitor.sh stop${NC}"
}

# ========== 停止 ==========
do_stop() {
    echo -e "${BOLD}停止监控...${NC}"
    echo ""

    local stopped=0

    for model in "${MODELS[@]}"; do
        local pf
        pf=$(pid_file "$model")

        if [[ ! -f "$pf" ]]; then
            echo -e "  ${YELLOW}[跳过]${NC} ${model} - 未在运行"
            continue
        fi

        local pid
        pid=$(cat "$pf")

        if kill -0 "$pid" 2>/dev/null; then
            # 先发 SIGTERM，让脚本优雅退出
            kill "$pid" 2>/dev/null

            # 同时杀掉该进程的所有子进程（ffmpeg/streamlink）
            pkill -P "$pid" 2>/dev/null

            # 等待最多 5 秒
            local wait_count=0
            while kill -0 "$pid" 2>/dev/null && [[ $wait_count -lt 10 ]]; do
                sleep 0.5
                ((wait_count++))
            done

            # 如果还没死，强制杀
            if kill -0 "$pid" 2>/dev/null; then
                kill -9 "$pid" 2>/dev/null
                pkill -9 -P "$pid" 2>/dev/null
            fi

            echo -e "  ${RED}[停止]${NC} ${model} (PID: ${pid})"
            ((stopped++))
        else
            echo -e "  ${YELLOW}[已停]${NC} ${model} (进程已不存在)"
        fi

        rm -f "$pf"
    done

    echo ""
    echo -e "${BOLD}已停止 ${stopped} 个监控进程${NC}"
}

# ========== 状态 ==========
do_status() {
    echo -e "${BOLD}监控状态${NC}"
    echo -e "────────────────────────────────────────────"
    printf "  ${BOLD}%-20s %-10s %-10s %s${NC}\n" "主播" "状态" "PID" "录制文件"
    echo -e "────────────────────────────────────────────"

    for model in "${MODELS[@]}"; do
        local status_str pid_str files_str

        if is_running "$model"; then
            local pid
            pid=$(cat "$(pid_file "$model")")
            status_str="${GREEN}运行中${NC}"
            pid_str="$pid"

            # 统计录制文件
            local model_dir="${OUTPUT_BASE}/${model}"
            if [[ -d "$model_dir" ]]; then
                local count size
                count=$(find "$model_dir" -name "*.mp4" -type f 2>/dev/null | wc -l | tr -d ' ')
                size=$(du -sh "$model_dir" 2>/dev/null | cut -f1)
                files_str="${count} 个 (${size})"
            else
                files_str="暂无"
            fi
        else
            status_str="${RED}未运行${NC}"
            pid_str="-"
            files_str="-"
        fi

        printf "  %-20s ${status_str}%-1s %-10s %s\n" "$model" "" "$pid_str" "$files_str"
    done

    echo -e "────────────────────────────────────────────"
    echo ""

    # 磁盘使用
    if [[ -d "$OUTPUT_BASE" ]]; then
        local total
        total=$(du -sh "$OUTPUT_BASE" 2>/dev/null | cut -f1)
        echo -e "  总磁盘占用: ${BOLD}${total}${NC}"
    fi
    echo ""
}

# ========== 查看日志 ==========
do_log() {
    local model="$1"

    # 查找匹配的日志文件（最新的）
    local lf
    lf=$(log_file "$model")

    if [[ ! -f "$lf" ]]; then
        echo -e "${RED}未找到 ${model} 的日志文件${NC}"
        echo "可用的主播: ${MODELS[*]}"
        exit 1
    fi

    echo -e "${CYAN}查看 ${model} 的实时日志 (Ctrl+C 退出)${NC}"
    echo -e "────────────────────────────────────────────"
    tail -f "$lf"
}

# ========== 主入口 ==========
case "${1:-}" in
    start)
        do_start
        ;;
    stop)
        do_stop
        ;;
    restart)
        do_stop
        echo ""
        sleep 1
        do_start
        ;;
    status|st)
        do_status
        ;;
    log|logs)
        if [[ -z "${2:-}" ]]; then
            echo "用法: $0 log <主播名>"
            echo "可用: ${MODELS[*]}"
            exit 1
        fi
        do_log "$2"
        ;;
    *)
        echo "用法: $0 {start|stop|restart|status|log <名字>}"
        echo ""
        echo "  start    启动所有主播监控"
        echo "  stop     停止所有监控"
        echo "  restart  重启所有监控"
        echo "  status   查看运行状态"
        echo "  log <名> 查看某主播实时日志"
        echo ""
        echo "监控列表:"
        for m in "${MODELS[@]}"; do
            echo "  - $m"
        done
        exit 1
        ;;
esac
