#!/usr/bin/env bash
# measure_resources.sh —— 测量某个进程运行时的资源占用（Linux /proc 采样）
#
# 通过周期性读取 /proc/<pid>/{status,stat,fd} 采集内存(RSS)、线程数、文件描述符、
# 以及由 CPU 时间差分算出的瞬时 CPU 占用率，最后汇总峰值/均值。
#
# 用法:
#   tools/measure_resources.sh [-i 间隔秒] [-d 最长秒] -- <command> [args...]
#
# 示例:
#   tools/measure_resources.sh -i 0.2 -d 6 -- ./build/.../emq_stress soak -d 5
#   tools/measure_resources.sh -i 0.5 -d 4 -- ./build/.../emqtop monitor '#' --no-udp
#
# 说明:
#   - CPU% 以单核 100% 为基准（多线程满载可 >100%，机器有 $(nproc) 核）。
#   - RSS = 物理驻留内存；VmHWM = 进程生命周期内的 RSS 峰值（内核统计）。

set -u

INTERVAL=0.2
MAXDUR=10
CLK=$(getconf CLK_TCK 2>/dev/null || echo 100)

while [[ $# -gt 0 ]]; do
    case "$1" in
        -i) INTERVAL="$2"; shift 2;;
        -d) MAXDUR="$2"; shift 2;;
        --) shift; break;;
        *) echo "未知参数: $1" >&2; exit 2;;
    esac
done

if [[ $# -eq 0 ]]; then
    echo "用法: $0 [-i 间隔秒] [-d 最长秒] -- <command> [args...]" >&2
    exit 2
fi

CMD=("$@")
echo "=== 测量目标: ${CMD[*]} ==="
echo "=== 采样间隔: ${INTERVAL}s  最长: ${MAXDUR}s  CLK_TCK=${CLK}  cores=$(nproc) ==="

# 后台启动目标进程（stderr/stdout 丢弃，避免日志刷屏）
"${CMD[@]}" >/dev/null 2>&1 &
PID=$!
sleep 0.05
if ! kill -0 "$PID" 2>/dev/null; then
    echo "进程未能启动或瞬间退出 (pid=$PID)" >&2
    exit 1
fi

read_cpu_ticks() {  # 输出 utime+stime（时钟滴答）
    awk '{print $14 + $15}' "/proc/$1/stat" 2>/dev/null || echo 0
}
read_rss_kb()    { awk '/^VmRSS:/{print $2}'  "/proc/$1/status" 2>/dev/null || echo 0; }
read_hwm_kb()    { awk '/^VmHWM:/{print $2}'  "/proc/$1/status" 2>/dev/null || echo 0; }
read_threads()   { awk '/^Threads:/{print $2}' "/proc/$1/status" 2>/dev/null || echo 0; }
read_fds()       { ls "/proc/$1/fd" 2>/dev/null | wc -l; }

printf "\n%8s | %9s | %7s | %4s | %4s\n" "t(s)" "RSS(KB)" "CPU%" "THR" "FD"
printf -- "---------+-----------+---------+------+------\n"

peak_rss=0; sum_cpu=0; n_cpu=0; peak_thr=0; peak_fd=0
prev_ticks=$(read_cpu_ticks "$PID")
t0=$(date +%s.%N)
elapsed=0

while kill -0 "$PID" 2>/dev/null; do
    sleep "$INTERVAL"
    # 进程可能在本次睡眠期间退出：此时 /proc 读数已失效，跳过该帧避免污染统计
    kill -0 "$PID" 2>/dev/null || break
    now=$(date +%s.%N)
    elapsed=$(awk -v a="$t0" -v b="$now" 'BEGIN{printf "%.2f", b-a}')
    rss=$(read_rss_kb "$PID");   [[ -z "$rss" ]] && rss=0
    thr=$(read_threads "$PID");  [[ -z "$thr" ]] && thr=0
    fd=$(read_fds "$PID")
    cur_ticks=$(read_cpu_ticks "$PID")
    # CPU% = Δticks / CLK / 间隔 * 100
    cpu=$(awk -v d="$((cur_ticks - prev_ticks))" -v clk="$CLK" -v iv="$INTERVAL" \
          'BEGIN{printf "%.1f", (d/clk)/iv*100}')
    prev_ticks=$cur_ticks

    printf "%8s | %9s | %7s | %4s | %4s\n" "$elapsed" "$rss" "$cpu" "$thr" "$fd"

    (( rss > peak_rss )) && peak_rss=$rss
    (( thr > peak_thr )) && peak_thr=$thr
    (( fd  > peak_fd  )) && peak_fd=$fd
    sum_cpu=$(awk -v s="$sum_cpu" -v c="$cpu" 'BEGIN{print s+c}')
    n_cpu=$((n_cpu + 1))

    if awk -v e="$elapsed" -v m="$MAXDUR" 'BEGIN{exit !(e>=m)}'; then
        kill "$PID" 2>/dev/null
        break
    fi
done

hwm=$(read_hwm_kb "$PID" 2>/dev/null)
wait "$PID" 2>/dev/null
avg_cpu=$(awk -v s="$sum_cpu" -v n="$n_cpu" 'BEGIN{printf "%.1f", (n>0)?s/n:0}')

echo ""
echo "=== 汇总 ==="
printf "  峰值 RSS      : %s KB (%.2f MB)\n" "$peak_rss" "$(awk -v k="$peak_rss" 'BEGIN{print k/1024}')"
[[ -n "${hwm:-}" && "$hwm" != "0" ]] && \
printf "  内核 VmHWM    : %s KB (%.2f MB)\n" "$hwm" "$(awk -v k="$hwm" 'BEGIN{print k/1024}')"
printf "  平均 CPU      : %s%% (单核基准, 共 %d 采样)\n" "$avg_cpu" "$n_cpu"
printf "  峰值线程数    : %s\n" "$peak_thr"
printf "  峰值 FD 数    : %s\n" "$peak_fd"
