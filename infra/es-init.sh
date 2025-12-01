#!/usr/bin/env bash
set -euo pipefail

ES=${1:-http://localhost:9200}

echo "[1/3] create index: chunks"
curl -s -X PUT "$ES/chunks" -H 'Content-Type: application/json' -d @- <<'JSON' > /dev/null
{
  "settings": {
    "index": {
      "number_of_shards": 1,
      "number_of_replicas": 0,
      "knn": true
    }
  },
  "mappings": {
    "properties": {
      "course_id":   {"type":"keyword"},
      "material_id": {"type":"keyword"},
      "filename":    {"type":"keyword"},
      "page":        {"type":"integer"},
      "content":     {"type":"text"},
      "vector":      {"type":"dense_vector","dims":384,"index":true,"similarity":"cosine"}
    }
  }
}
JSON

echo "[2/3] create index: quizzes"
curl -s -X PUT "$ES/quizzes" -H 'Content-Type: application/json' -d @- <<'JSON' > /dev/null
{
  "settings": {"index":{"number_of_shards":1,"number_of_replicas":0}},
  "mappings": {
    "properties": {
      "quiz_id":{"type":"keyword"},
      "course_id":{"type":"keyword"},
      "question":{"type":"text"},
      "options":{"type":"keyword"},
      "answer":{"type":"keyword"},
      "source":{"type""keyword"}
    }
  }
}
JSON

echo "[3/3] create index: attempts"
curl -s -X PUT "$ES/attempts" -H 'Content-Type: application/json' -d @- <<'JSON' > /dev/null
{
  "settings": {"index":{"number_of_shards":1,"number_of_replicas":0}},
  "mappings": {
    "properties": {
      "attempt_id":{"type":"keyword"},
      "quiz_id":{"type":"keyword"},
      "user_id":{"type":"keyword"},
      "score":{"type":"float"},
      "max_score":{"type":"float"},
      "graded_at":{"type":"date"},
      "details":{"type":"nested"}
    }
  }
}
JSON

echo "done."