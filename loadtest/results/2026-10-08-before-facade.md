# payment-service Facade 구조 - BEFORE 측정 (2026-10-08)

리팩토링(MQ 분리) 전, 현재 동기 Facade 구조의 기준선(baseline) 기록.
나중에 MQ 분리 후 똑같은 조건으로 재실행해서 이 파일과 비교할 것.

**범위: `PaymentConfirmFacade.confirm()`(일반 결제, `/api/payments/confirm/brandpay`)만 측정함.**
`PaymentCancelFacade`(취소), `BillingPaymentFacade`(정기결제)는 아직 측정 안 함 - 필요하면 동일한 방식(WireMock 스텁 경로만 바꿔서)으로 추가 측정 필요.

k6의 `pg_slow_confirm`/`control_traffic` 두 시나리오는 **동시에(병렬로)** 실행됨 (`startTime` 미지정 시 k6 기본값이 동시 시작) - 순차 실행이면 장애 전파 자체를 측정할 수 없어서 이건 의도된 설계.

## 실행 조건 (재현/비교용 - 반드시 동일하게 맞출 것)

- 코드: commit `c850b28` 기준 working tree (facade/서비스 로직 자체는 이 커밋과 동일, 모니터링/부하테스트 인프라만 추가됨)
- payment-service: `-Dspring.profiles.active=local,loadtest` (Toss base-url -> WireMock `localhost:8089`)
- HikariCP: `maximum-pool-size` 기본값 10 (명시적 설정 없음)
- Tomcat: `server.tomcat.threads.max` 기본값 200
- resilience4j `brandpayCircuitBreaker`: slidingWindowSize=10, minimumNumberOfCalls=5, failureRateThreshold=50%, slowCallDurationThreshold=3000ms, waitDurationInOpenState=20000ms
- k6: `loadtest/k6/facade-isolation-test.js`
  - baseline: `CONFIRM_RATE=20 CONTROL_RATE=20 DURATION=30s`, WireMock delay=50ms
  - fault: `CONFIRM_RATE=30 CONTROL_RATE=20 DURATION=60s CONFIRM_MAX_VUS=400`, WireMock delay=8000ms (status 200)
- 측정 전 payment-service를 재시작해서 서킷브레이커를 CLOSED 상태로 리셋한 뒤 측정 (이전 fault 테스트의 OPEN 상태가 다음 측정에 섞여 들어가는 걸 확인하고 재시작으로 제거함)

## 결과

### 1) Baseline (PG 정상, delay=50ms)

| 지표 | pg_slow_confirm | control_traffic |
|---|---|---|
| 요청 수 | 600 | 601 |
| 에러율 | 0% | 0% |
| p95 latency | 97.38ms | 11.14ms |
| p90 latency | 89.17ms | 9.18ms |

→ 정상 상황에서는 둘 다 깨끗함. 사고 실험 자체가 유효하다는 sanity check.

### 2) Fault 주입 (Toss 응답 8000ms 지연, 60초)

| 지표 | pg_slow_confirm | control_traffic |
|---|---|---|
| 요청 수 | 1704 | 1201 |
| 에러율 | 96.12% | 0% (전체 60초 평균) |
| p95 latency | 29.99s (타임아웃) | 6.53ms (전체 60초 평균) |
| avg latency | 2.17s | 4.17ms |

Tomcat/HikariCP 풀 상태 (3초 간격 직접 폴링 + 종료 후 Prometheus 저장값으로 재검증):

| 시점 | tomcat_threads_busy (max 200) | hikari_active | hikari_pending |
|---|---|---|---|
| 테스트 시작 ~10초 | **139 (69.5%)** (Prometheus `max_over_time`로 재확인) | 0~2 | 0 |
| ~15~20초 (CB open 추정) | 95 -> 20 -> 2 로 급감 | 0~2 | 0 |
| 20초 이후 ~ 종료 | 1~5 (거의 유휴) | 0~2 | 0 |
| 유휴 상태(idle, 비교용) | 1 | 0 | 0 |

**주의 - 지표 혼동 주의**: Grafana 기본 JVM 대시보드의 "Threads(Daemon/Live/Peak)" 패널은 `jvm_threads_live_threads` 등 **JVM 전체 스레드 수**이고, 이건 GC/Kafka consumer poll/Eureka heartbeat/스케줄러 등 Tomcat과 무관한 백그라운드 스레드(유휴 상태 기준 약 45개, `jvm_threads_live`=46 vs `tomcat_threads_busy`=1)까지 다 포함한다. fault 구간에서 `jvm_threads_live`는 183까지 올라가는데, 이건 "45(배경) + 139(Tomcat busy)"와 거의 정확히 일치한다. **Facade가 블로킹하는 건 정확히 Tomcat 워커 스레드이므로, 비교 지표는 `tomcat_threads_busy`(139/200)를 써야 하고 `jvm_threads_live`(183)는 보조 지표일 뿐 메인 비교 대상이 아니다.**

## 해석 (중요 - 지금 구조에 대한 정확한 설명)

1. **스레드 풀이 핵심 병목이다, HikariCP가 아니다.** confirm 플로우는 Toss 호출을 DB 트랜잭션 밖에서 하기 때문에(블로킹 구간 동안 DB 커넥션을 쥐고 있지 않음) `hikaricp_connections_pending`은 테스트 내내 0이었다. 반면 `tomcat_threads_busy`는 최대 **139/200(69.5%)**까지 치솟았다 - Facade의 동기 호출이 공유 Tomcat 워커 스레드를 블로킹한다는 직접 증거.
2. **control_traffic이 "전체 평균"으로는 괜찮아 보이는 이유**: resilience4j 서킷브레이커(`brandpayCircuitBreaker`)가 장애 지속 ~15~20초 뒤 OPEN으로 전환되면서 이후 요청을 즉시 실패시켜(fast-fail) 스레드를 더 이상 붙잡지 않게 됐다. 그래서 60초 전체로 평균을 내면 초반 스파이크가 희석돼 보인다. **진짜 위험 구간은 CB가 열리기 전까지의 첫 15~20초, 스레드 사용률이 69%까지 올라가는 그 구간이다.**
3. 즉 지금 시스템은 이미 서킷브레이커라는 부분적 안전장치가 있어서 "완전한 전체 장애"까지는 안 가지만, **CB가 열리기 전 짧은 창(window) 동안은 공유 스레드 풀이 상당히 소모되고, confirm 트래픽이 더 많거나 CB 임계값이 지금보다 느슨했다면 그 창에서 실제로 다른 요청까지 영향받을 수 있는 상태**다. MQ로 분리하면 이 창 자체가 사라지는지(스레드를 전혀 안 쓰므로) 를 after 측정에서 비교하면 된다.
4. pg_slow_confirm의 96% 에러율은 CB 트립 이후 즉시 실패 + 일부 타임아웃(p95=30s)이 섞인 것 - 예상된 결과, 메인 비교 지표는 아님.

## 다음 측정(after) 시 체크리스트

- [ ] 같은 WireMock delay(50ms / 8000ms), 같은 k6 rate/duration으로 실행
- [ ] 실행 전 서비스 재시작으로 CB/풀 상태 초기화
- [ ] tomcat_threads_busy 피크값과 피크까지 걸린 시간을 이 파일의 69.5%/139와 직접 비교 (Grafana에서 볼 땐 jvm_threads_live가 아니라 tomcat_threads_busy 패널로 봐야 함)
- [ ] control_traffic의 "CB open 전 초반 구간" 체감 영향이 사라졌는지 확인 (가능하면 구간별 latency를 raw json 출력으로 다시 잡는 걸 고려 - 이번엔 cumulative trend라 초반 스파이크가 뭉개져서 안 보였음)
