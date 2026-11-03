#!/bin/bash
# Toss 취소(cancel) 스텁의 응답 지연/상태코드를 실행 중에 바꾼다 (WireMock 재시작 없이).
#
# 사용법:
#   ./toggle-cancel-delay.sh 50          -> 정상 상황
#   ./toggle-cancel-delay.sh 8000        -> PG 장애 상황
set -e

DELAY_MS="${1:-50}"
STATUS="${2:-200}"
WIREMOCK_URL="${WIREMOCK_URL:-http://localhost:8089}"
STUB_ID="a1b2c3d4-0002-4000-8000-000000000002"

curl -s -X PUT "${WIREMOCK_URL}/__admin/mappings/${STUB_ID}" \
  -H "Content-Type: application/json" \
  -d @- <<EOF
{
  "id": "${STUB_ID}",
  "priority": 1,
  "request": {
    "method": "POST",
    "urlPathPattern": "/payments/.*/cancel"
  },
  "response": {
    "status": ${STATUS},
    "fixedDelayMilliseconds": ${DELAY_MS},
    "headers": { "Content-Type": "application/json" },
    "jsonBody": {
      "paymentKey": "wiremock-pay-key",
      "orderId": "k6-stub-order",
      "orderName": "k6 load test",
      "status": "CANCELED",
      "cancels": [
        {
          "cancellationKey": "wiremock-cancel-key",
          "cancelAmount": 1000,
          "cancelReason": "k6 load test",
          "canceledAt": "2026-01-01T00:00:00+09:00",
          "transactionKey": "wiremock-tx-key",
          "receiptKey": "wiremock-receipt-key"
        }
      ]
    }
  }
}
EOF

echo "Toss cancel stub -> delay=${DELAY_MS}ms, status=${STATUS}"
