// payment-service의 confirm 경로(이제 Outbox+Kafka로 비동기 분리됨)가 Toss 장애 시
// 다른 요청까지 물고 들어가지 않는지(스레드 격리) 측정하기 위한 부하 테스트 - AFTER 버전.
//
// before(facade-isolation-test.js의 원래 동기 버전)와 비교할 때 지표가 두 개로 나뉜다:
//   - accept  : POST /confirm/brandpay 자체의 응답시간 - 이제 Toss 호출과 무관하게 즉시(202) 와야 한다.
//               before의 "pg_slow_confirm" http_req_duration과 직접 비교할 대상.
//   - complete: 접수 후 GET /status를 폴링해서 PAID/FAILED 등 종결 상태가 될 때까지 걸린 시간.
//               Toss 지연을 그대로 반영하므로 before의 숫자와 비교하는 지표가 아니라
//               "스레드 대신 어디서/얼마나 기다리는지"를 보여주는 참고 지표.
//
// control_traffic은 이전과 동일 - 이게 0% 에러/낮은 p95를 유지하는지가 핵심 비교 포인트.
//
// 실행 전 준비: facade-isolation-test.js(원본) 상단 주석과 동일.
//
// 실행 예:
//   BATCH=20261008 CONFIRM_RATE=20 DURATION=60s k6 run facade-isolation-test.js
//
// 주의: 환경변수 이름을 K6_BATCH로 쓰면 안 된다 - k6는 K6_ 접두사가 붙은 이름을
// 자기 내부 설정(예: K6_BATCH -> http.batch 동시성)에 매핑해서 충돌/에러가 난다.

import http from 'k6/http';
import { check, sleep } from 'k6';
import exec from 'k6/execution';
import { Trend } from 'k6/metrics';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8300';
const BATCH = __ENV.BATCH;
const DURATION = __ENV.DURATION || '60s';
const POLL_INTERVAL_S = Number(__ENV.POLL_INTERVAL_S || 0.3);
const POLL_MAX_ATTEMPTS = Number(__ENV.POLL_MAX_ATTEMPTS || 40); // 0.3s * 40 = 12s 상한

if (!BATCH) {
  throw new Error('BATCH 환경변수가 필요합니다. seed-confirm-payments.sql 실행 시 쓴 batch 값과 동일해야 합니다.');
}

const confirmCompletionTime = new Trend('confirm_completion_duration', true);

export const options = {
  scenarios: {
    pg_slow_confirm: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.CONFIRM_RATE || 20),
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: Number(__ENV.CONFIRM_VUS || 50),
      maxVUs: Number(__ENV.CONFIRM_MAX_VUS || 400),
      exec: 'confirmPayment',
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
  // 실패시키려는 게 아니라, k6 요약에 시나리오별 서브메트릭을 강제로 분리해서 찍기 위한 용도
  thresholds: {
    'http_req_duration{scenario:control_traffic}': ['p(95)<999999'],
    'http_req_duration{scenario:pg_slow_confirm,name:accept}': ['p(95)<999999'],
    'http_req_failed{scenario:control_traffic}': ['rate<=1'],
    'http_req_failed{scenario:pg_slow_confirm}': ['rate<=1'],
  },
};

export function confirmPayment() {
  const idx = exec.scenario.iterationInTest;
  const orderId = `k6-${BATCH}-${idx}`;
  const start = Date.now();

  const payload = JSON.stringify({
    paymentKey: null,
    orderId,
    amount: 1000,
    customerKey: 'k6-customer',
  });

  const res = http.post(`${BASE_URL}/api/payments/confirm/brandpay`, payload, {
    headers: { 'Content-Type': 'application/json' },
    timeout: '30s',
    tags: { name: 'accept' },
  });

  const accepted = check(res, { 'accept 202': (r) => r.status === 202 });
  if (!accepted) {
    return;
  }

  const paymentId = JSON.parse(res.body).data;
  let status = 'PROCESSING';

  for (let i = 0; i < POLL_MAX_ATTEMPTS && status === 'PROCESSING'; i++) {
    sleep(POLL_INTERVAL_S);
    const statusRes = http.get(`${BASE_URL}/api/payments/${paymentId}/status`, {
      timeout: '10s',
      tags: { name: 'status_poll' },
    });
    const body = JSON.parse(statusRes.body);
    status = body.data ? body.data.status : status;
  }

  confirmCompletionTime.add(Date.now() - start);
  check(null, { 'confirm completed': () => status !== 'PROCESSING' });
}

export function hitControlEndpoint() {
  const res = http.get(`${BASE_URL}/actuator/health`, { timeout: '30s' });
  check(res, { 'control 200': (r) => r.status === 200 });
}
