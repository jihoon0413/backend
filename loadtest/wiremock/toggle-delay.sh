#!/bin/bash
# Toss confirm 스텁의 응답 지연/상태코드를 실행 중에 바꾼다 (WireMock 재시작 없이).
#
# 사용법:
#   ./toggle-delay.sh 50          -> 정상 상황 (지연 50ms, 200 OK)
#   ./toggle-delay.sh 8000        -> PG 장애 상황 (지연 8000ms, 200 OK)
#   ./toggle-delay.sh 8000 500    -> PG 장애 + 5xx 응답까지 재현
set -e

DELAY_MS="${1:-50}"
STATUS="${2:-200}"
WIREMOCK_URL="${WIREMOCK_URL:-http://localhost:8089}"
STUB_ID="a1b2c3d4-0001-4000-8000-000000000001"

curl -s -X PUT "${WIREMOCK_URL}/__admin/mappings/${STUB_ID}" \
  -H "Content-Type: application/json" \
  -d @- <<EOF
{
  "id": "${STUB_ID}",
  "priority": 1,
  "request": {
    "method": "POST",
    "urlPath": "/brandpay/payments/confirm"
  },
  "response": {
    "status": ${STATUS},
    "fixedDelayMilliseconds": ${DELAY_MS},
    "headers": { "Content-Type": "application/json" },
    "jsonBody": {
      "paymentKey": "wiremock-pay-key",
      "orderId": "k6-stub-order",
      "orderName": "k6 load test",
      "status": "DONE",
      "requestedAt": "2026-01-01T00:00:00+09:00",
      "approvedAt": "2026-01-01T00:00:00+09:00",
      "totalAmount": 1000,
      "balanceAmount": 1000,
      "type": "BRANDPAY"
    }
  }
}
EOF

echo "Toss confirm stub -> delay=${DELAY_MS}ms, status=${STATUS}"
