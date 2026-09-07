#!/bin/sh
# Single node: replicas MUST be 0, or every index sits yellow forever
# waiting for a replica shard that has nowhere to go.
set -e
OS=http://opensearch:9200

for name in sre-logs sre-traces; do
  curl -fsS -X PUT "$OS/_index_template/${name}-template" \
    -H 'Content-Type: application/json' -d "{
      \"index_patterns\": [\"${name}*\"],
      \"template\": {
        \"settings\": {
          \"number_of_shards\": 1,
          \"number_of_replicas\": 0,
          \"refresh_interval\": \"5s\"
        }
      }
    }" > /dev/null
  echo "index template ready: ${name}"
done

# ponytail: no ISM rollover policy. Add one when logs outgrow the disk —
# at simulator rates that is roughly a week.
