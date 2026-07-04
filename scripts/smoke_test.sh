#!/usr/bin/env bash
set -euo pipefail

API=${API_URL:-http://localhost:8080}
QUEUE="default"
COUNT=1000

echo "Submitting $COUNT jobs to queue '$QUEUE'..."
for i in $(seq 1 $COUNT); do
  curl -s -X POST "$API/jobs" \
    -H "Content-Type: application/json" \
    -d "{\"queue\":\"$QUEUE\",\"payload\":\"smoke-job-$i\",\"priority\":1}" > /dev/null
done

echo "Waiting for jobs to drain..."
for i in $(seq 1 60); do
  STATS=$(curl -s "$API/queues/$QUEUE/stats")
  PENDING=$(echo "$STATS" | jq '.pending')
  PROCESSING=$(echo "$STATS" | jq '.processing')
  echo "  pending=$PENDING processing=$PROCESSING"
  if [[ "$PENDING" == "0" && "$PROCESSING" == "0" ]]; then
    echo "All jobs drained."
    exit 0
  fi
  sleep 1
done

echo "FAILED: jobs still stuck after 60s"
exit 1