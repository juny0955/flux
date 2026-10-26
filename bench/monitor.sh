#!/bin/bash

PID=$1
OUTPUT=${2:-metrics.csv}

echo "timestamp,cpu_percent,rss_mb" > "$OUTPUT"

while kill -0 "$PID" 2>/dev/null; do
    ps -p "$PID" -o %cpu=,rss= | awk -v ts="$(date +%s)" \
    '{printf "%s,%.2f,%.2f\n", ts, $1, $2/1024}' >> "$OUTPUT"

    sleep 1
done