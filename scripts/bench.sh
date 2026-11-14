#!/usr/bin/env bash
set -euo pipefail

started=$(date +%s)
root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"

os=$(uname -s)

load_average() {
  if [ -r /proc/loadavg ]; then
    cut -d' ' -f1-3 /proc/loadavg
  else
    sysctl -n vm.loadavg 2>/dev/null | tr -d '{}' | awk '{print $1, $2, $3}' || echo unknown
  fi
}
load_at_start=$(load_average)

if [ "$os" = Darwin ]; then
  os_version="macOS $(sw_vers -productVersion 2>/dev/null || echo unknown) (Darwin $(uname -r))"
  cpu=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || echo unknown)
  cores=$(sysctl -n hw.ncpu 2>/dev/null || echo unknown)
  ram_bytes=$(sysctl -n hw.memsize 2>/dev/null || echo 0)
else
  pretty=$( (. /etc/os-release 2>/dev/null && echo "${PRETTY_NAME:-}") || true)
  os_version="${pretty:-$os} (kernel $(uname -r))"
  cpu=$(lscpu 2>/dev/null | sed -n 's/^Model name:[[:space:]]*//p' | head -1)
  [ -n "$cpu" ] && [ "$cpu" != - ] || cpu=$(sed -n 's/^model name[[:space:]]*:[[:space:]]*//p' /proc/cpuinfo 2>/dev/null | head -1)
  [ -n "$cpu" ] || cpu="$(lscpu 2>/dev/null | sed -n 's/^Vendor ID:[[:space:]]*//p' | head -1) $(uname -m)"
  [ -n "${cpu// /}" ] || cpu=unknown
  cores=$(nproc 2>/dev/null || getconf _NPROCESSORS_ONLN 2>/dev/null || echo unknown)
  ram_kb=$(awk '/^MemTotal:/ {print $2}' /proc/meminfo 2>/dev/null || echo 0)
  ram_bytes=$((ram_kb * 1024))
fi
if [ "$os" = Darwin ]; then
  port_first=$(sysctl -n net.inet.ip.portrange.first 2>/dev/null || echo 0)
  port_last=$(sysctl -n net.inet.ip.portrange.last 2>/dev/null || echo 0)
  time_wait=$(( $(sysctl -n net.inet.tcp.msl 2>/dev/null || echo 0) * 2 / 1000 ))
  tw_reuse=0
else
  read -r port_first port_last < /proc/sys/net/ipv4/ip_local_port_range 2>/dev/null || { port_first=0; port_last=0; }
  time_wait=60
  tw_reuse=$(cat /proc/sys/net/ipv4/tcp_tw_reuse 2>/dev/null || echo 0)
fi
ports=$((port_last - port_first + 1))
jvm_props=()
if [ "$tw_reuse" = 0 ] && [ "$ports" -gt 1 ] && [ "$time_wait" -gt 0 ]; then
  connections_per_second=$((ports / time_wait))
  ports_note="$ports ephemeral ports, ${time_wait} s TIME_WAIT, no reuse: about $connections_per_second new connections/s sustained"
  jvm_props+=("-Dbench.connectionsPerSecond=$connections_per_second" "-Dbench.portCooldownSeconds=$((time_wait + 2))")
else
  ports_note="$ports ephemeral ports, TIME_WAIT reuse $tw_reuse"
fi

time_wait_sockets() {
  if command -v ss >/dev/null 2>&1; then
    ss -tan state time-wait 2>/dev/null | tail -n +2 | wc -l | tr -d ' '
  else
    netstat -an 2>/dev/null | grep -c TIME_WAIT || true
  fi
}

ram=$(awk -v b="$ram_bytes" 'BEGIN { if (b > 0) printf "%.1f GiB", b / 1073741824; else print "unknown" }')

if [ -n "${JAVA_HOME:-}" ]; then
  java="$JAVA_HOME/bin/java"
else
  java=$(command -v java)
fi
jdk=$("$java" -XshowSettings:properties -version 2>&1 \
  | awk -F' = ' '/^ *java.vendor = / {v=$2} /^ *java.runtime.version = / {r=$2} END {print v, r}')

sha=$(git rev-parse HEAD)
short_sha=$(git rev-parse --short HEAD)
if [ -n "$(git status --porcelain)" ]; then dirty=yes; else dirty=no; fi
date_utc=$(date -u +%Y-%m-%d)
timestamp_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)

echo "building"
mvn -B -q test-compile dependency:build-classpath \
  -Dmdep.includeScope=test -Dmdep.outputFile=target/bench-classpath.txt >/dev/null

work=$(mktemp -d "${TMPDIR:-/tmp}/nanokafka-bench.XXXXXX")
trap 'rm -rf "$work"' EXIT

device=$(df -P "$work" | awk 'NR == 2 {print $1}')
if [ "$os" = Darwin ]; then
  filesystem=$(mount | awk -v d="$device" '$1 == d {sub(/^\(/, "", $4); sub(/,$/, "", $4); print $4; exit}')
else
  filesystem=$(df -PT "$work" 2>/dev/null | awk 'NR == 2 {print $2}')
fi
filesystem=${filesystem:-unknown}

if [ "${#jvm_props[@]}" -gt 0 ]; then
  for _ in $(seq 1 "$((time_wait + 5))"); do
    [ "$(time_wait_sockets)" -lt 1000 ] && break
    [ "$_" = 1 ] && echo "waiting for TIME_WAIT sockets from earlier runs to expire"
    sleep 1
  done
fi

classpath="$root/target/classes:$root/target/test-classes:$(cat target/bench-classpath.txt)"
set +e
(cd "$work" && "$java" -Djava.io.tmpdir="$work" ${jvm_props[@]+"${jvm_props[@]}"} -cp "$classpath" com.simplekafka.bench.Bench "$work/body.md") 2>&1 \
  | tee "$work/bench.log" | grep --line-buffered '^bench: '
status=${PIPESTATUS[0]}
set -e
if [ ! -s "$work/body.md" ]; then
  echo "benchmark failed before writing results (exit $status). Last log lines:" >&2
  tail -40 "$work/bench.log" >&2
  exit 1
fi

runtime=$(($(date +%s) - started))
mkdir -p bench/results
report="bench/results/$date_utc-$short_sha.md"
{
  echo "# NanoKafka benchmark, $date_utc, $short_sha"
  echo
  echo "| Field | Value |"
  echo "| --- | --- |"
  echo "| Date (UTC) | $timestamp_utc |"
  echo "| Git SHA | \`$sha\` |"
  echo "| Working tree dirty | $dirty |"
  echo "| OS | $os_version |"
  echo "| CPU | $cpu |"
  echo "| Logical cores | $cores |"
  echo "| RAM | $ram |"
  echo "| JDK | $jdk |"
  echo "| JVM flags | defaults, no tuning flags |"
  echo "| Data directory filesystem | $filesystem (a fresh temp dir, deleted after the run) |"
  echo "| Loopback connections | $ports_note |"
  echo "| Load average at start (1, 5, 15 min) | $load_at_start |"
  echo "| Harness exit status | $status |"
  echo "| Total scripts/bench.sh runtime | $runtime s (build included) |"
  echo
  echo "Reproduce with \`scripts/bench.sh\`. See bench/README.md for what each scenario measures."
  echo
  cat "$work/body.md"
} >"$report"

awk '/^## / {print; next} /^\|/ {print} /^$/ {print}' "$work/body.md" | cat -s
echo "report: $report (${runtime}s)"
exit "$status"
