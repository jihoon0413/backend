// payment-service의 cancel 경로(이제 Outbox+Kafka로 비동기 분리됨)가 Toss 장애 시
// 다른 요청까지 물고 들어가지 않는지 측정하는 부하 테스트 - AFTER 버전.
// facade-isolation-test.js(confirm)와 동일한 accept+폴링 구조.
//
// 지표가 두 개로 나뉜다:
//   - accept  : POST /cancel 자체의 응답시간 - 이제 즉시(202) 와야 한다. before의 "pg_slow_cancel"
//               http_req_duration과 직접 비교할 대상.
//   - complete: 접수 후 GET /status 폴링으로 종결 상태까지 걸린 시간 (참고 지표, before와 직접 비교 안 함).
//
// 실행 전 준비:
//   1. loadtest/sql/seed-cancel-payments.sql 로 PAID 상태의 결제 row를 미리 시딩
//      -> 출력되는 cancel_id_start 값을 CANCEL_ID_START 로 사용
//   2. payment-service를 TOSS_BASE_URL이 WireMock(http://localhost:8089)을 보도록 띄움
//      (-Dspring.profiles.active=local,loadtest)
//   3. ./wiremock/toggle-cancel-delay.sh 로 원하는 지연 설정
//
// 실행 예:
//   BATCH=20261008 CANCEL_ID_START=1234 CANCEL_RATE=20 DURATION=60s k6 run facade-isolation-cancel-test.js

import http from 'k6/http';
import { check, sleep } from 'k6';
import exec from 'k6/execution';
import { Trend } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8300';
const BATCH = __ENV.BATCH;
const DURATION = __ENV.DURATION || '60s';
const CANCEL_ID_START = Number(__ENV.CANCEL_ID_START);
const USER_ID = __ENV.USER_ID || '999001';
const POLL_INTERVAL_S = Number(__ENV.POLL_INTERVAL_S || 0.3);
const POLL_MAX_ATTEMPTS = Number(__ENV.POLL_MAX_ATTEMPTS || 40); // 0.3s * 40 = 12s 상한

if (!BATCH) {
  throw new Error('BATCH 환경변수가 필요합니다.');
}
if (!CANCEL_ID_START) {
  throw new Error('CANCEL_ID_START 환경변수가 필요합니다. seed-cancel-payments.sql 실행 결과의 cancel_id_start 값을 넣으세요.');
}

const cancelCompletionTime = new Trend('cancel_completion_duration', true);

export const options = {
  scenarios: {
    pg_slow_cancel: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.CANCEL_RATE || 20),
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: Number(__ENV.CANCEL_VUS || 50),
      maxVUs: Number(__ENV.CANCEL_MAX_VUS || 400),
      exec: 'cancelPayment',
    },
    control_traffic: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.CONTROL_RATE || 20),
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: 10,
      maxVUs: 50,
      exec: 'hitControlEndpoint',
    },
  },
  thresholds: {
    'http_req_duration{scenario:control_traffic}': ['p(95)<999999'],
    'http_req_duration{scenario:pg_slow_cancel,name:accept}': ['p(95)<999999'],
    'http_req_failed{scenario:control_traffic}': ['rate<=1'],
    'http_req_failed{scenario:pg_slow_cancel}': ['rate<=1'],
  },
};

export function cancelPayment() {
  const idx = exec.scenario.iterationInTest;
  const paymentId = CANCEL_ID_START + idx;
  const start = Date.now();

  const payload = JSON.stringify({
    paymentId,
    cancelAmount: 1000,
    cancelReason: 'k6 load test',
    idempotencyKey: `k6-cancel-idem-${BATCH}-${idx}`,
  });

  const res = http.post(`${BASE_URL}/api/payments/cancel`, payload, {
    headers: {
      'Content-Type': 'application/json',
      'X-User-Id': USER_ID,
    },
    timeout: '30s',
    tags: { name: 'accept' },
  });

  const accepted = check(res, { 'accept 202': (r) => r.status === 202 || r.status === 200 });
  if (!accepted) {
    return;
  }

  const paymentId2 = JSON.parse(res.body).data;
  // cancel accept는 Payment.status를 안 건드린다(confirm과 달리 PROCESSING 마커가 없음) -
  // 시작 상태가 'PAID'이므로, 그 상태를 벗어날 때까지(=CANCELLED/PARTIALLY_REFUNDED/CANCEL_UNKNOWN_HOLD) 기다려야 한다.
  let status = 'PAID';

  for (let i = 0; i < POLL_MAX_ATTEMPTS && status === 'PAID'; i++) {
    sleep(POLL_INTERVAL_S);
    const statusRes = http.get(`${BASE_URL}/api/payments/${paymentId2}/status`, {
      timeout: '10s',
      tags: { name: 'status_poll' },
    });
    const body = JSON.parse(statusRes.body);
    status = body.data ? body.data.status : status;
  }

  cancelCompletionTime.add(Date.now() - start);
  check(null, { 'cancel completed': () => status !== 'PAID' });
}

export function hitControlEndpoint() {
  const res = http.get(`${BASE_URL}/actuator/health`, { timeout: '30s' });
  check(res, { 'control 200': (r) => r.status === 200 });
}
