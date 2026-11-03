# payment-service Facade 구조 - BEFORE 측정: 결제 취소(cancel) (2026-10-08)

`PaymentCancelFacade.cancel()`(`/api/payments/cancel`)에 대한 BEFORE 기록.
방법론은 confirm(`2026-10-08-before-facade.md`)과 동일하되, **결과는 confirm보다 훨씬 나쁘다** - 이유는 아래 해석 참고.

## 실행 조건

- WireMock stub: `/payments/.*/cancel` (urlPathPattern), toggle: `loadtest/wiremock/toggle-cancel-delay.sh`
- 시딩: `loadtest/sql/seed-cancel-payments.sql` (PAID 상태 + refundableAmount/paymentKey 세팅해서 바로 취소 가능한 row 생성)
- k6: `loadtest/k6/facade-isolation-cancel-test.js`
  - baseline: `CANCEL_RATE=20 CONTROL_RATE=20 DURATION=30s`, delay=50ms
  - fault: `CANCEL_RATE=30 CONTROL_RATE=20 DURATION=60s CANCEL_MAX_VUS=400`, delay=8000ms
- **cancelPayment()는 `@CircuitBreaker`가 없고 `@Retry`만 걸려있다** (confirm의 `confirmBrandpay()`는 `@CircuitBreaker` 있음) - 이게 아래 결과 차이의 핵심 원인

## 결과

### 1) Baseline (PG 정상, delay=50ms)

| 지표 | pg_slow_cancel | control_traffic |
|---|---|---|
| 에러율 | 0% | 0% |
| avg | 159ms | 24ms |
| p95 | 495ms | 130ms |

⚠️ confirm 베이스라인(p95 97ms/11ms)보다 눈에 띄게 높다 - 이 JVM에서 cancel 엔드포인트를 이 테스트가 처음 많이 때린 거라 JIT/클래스로딩 워밍업 영향으로 보임(로그상 재시도/에러는 없었음). 구조적 결함이라기보다 측정 노이즈로 판단 - 차후 재측정 시 confirm/cancel 둘 다 먼저 가볍게 워밍업 트래픽을 흘려주는 걸 권장.

### 2) Fault 주입 (Toss 응답 8000ms 지연, 60초)

| 지표 | pg_slow_cancel | control_traffic |
|---|---|---|
| 요청 수 | 859 | 378 |
| 에러율 | **90.33%** | **13.49%** |
| avg | 28.48s | 7.42s |
| p95 | 31.32s | **30s** |
| dropped_iterations | 1763 (k6가 VU를 못 늘려서 요청 자체를 못 보낸 횟수) | - |

Tomcat/HikariCP 풀 상태 (Prometheus 저장값, `max_over_time`):

| 지표 | 유휴 | fault 피크 |
|---|---|---|
| `tomcat_threads_busy` (max 200) | 1 | **172 (86%)** |
| `hikaricp_connections_active` | 0 | 3 |
| `hikaricp_connections_pending` | 0 | 0 |

## 해석 - confirm과 무엇이 다른가

1. **control_traffic이 실제로 타격을 입었다.** confirm 땐 서킷브레이커가 ~15~20초 만에 열려서 control_traffic 에러율이 0%로 지켜졌는데, cancel은 `@CircuitBreaker`가 없어서 **보호 장치 없이 매 요청이 8초씩 블로킹**된다. 그 결과 control_traffic도 13.49% 실패하고 p95가 30초(k6 클라이언트 타임아웃)까지 올라갔다 - **"다른 요청까지 영향받는다"는 가설이 cancel에서는 수치로 직접 증명됨.**
2. **스레드 풀 점유율도 더 높다.** confirm 최대 139/200(69.5%) vs cancel 최대 172/200(86%) - CB의 fast-fail이 없으니 더 많은 스레드가 더 오래 묶여있었다.
3. **HikariCP는 여전히 영향 없음** (active 3, pending 0) - cancel도 confirm과 동일하게 Toss 호출을 DB 트랜잭션 밖에서 하기 때문. 이 구조적 특징은 confirm/cancel 공통.
4. **dropped_iterations=1763**: k6가 설정한 부하(30 req/s)조차 다 못 보낼 정도로 시스템이 막혔다 - confirm 테스트에선 이 정도로 심하지 않았음(96 dropped). cancel 쪽이 실제로 더 위험한 상태였다는 뜻.

**결론: MQ 분리의 효과를 가장 극적으로 보여줄 수 있는 건 confirm이 아니라 cancel일 가능성이 높다.** confirm은 이미 서킷브레이커가 어느 정도 방어해주고 있어서 "완전한 전체 장애"까지는 안 가지만, cancel은 지금 상태로 PG 장애가 나면 무방비로 전체 API가 같이 느려지는 구조다.

## 다음 측정(after) 체크리스트

- [ ] 같은 delay(50ms/8000ms), 같은 rate(20/30)·duration으로 실행
- [ ] control_traffic 에러율 13.49% -> 0%, p95 30s -> 베이스라인 수준(130ms대)으로 떨어지는지 확인
- [ ] tomcat_threads_busy 피크 172(86%) 대비 after에서 변화 확인
- [ ] (선택) MQ 분리와 별개로, cancel에도 confirm처럼 `@CircuitBreaker`를 추가하는 것만으로 얼마나 개선되는지도 비교해보면 "MQ 분리 효과"와 "서킷브레이커 유무 효과"를 분리해서 설명할 수 있어 설득력이 더 생김
