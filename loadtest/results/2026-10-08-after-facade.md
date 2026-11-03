# payment-service Outbox+Kafka 분리 - AFTER 측정: confirm (2026-10-08)

`2026-10-08-before-facade.md`과 직접 비교. 코드: Outbox+Kafka 비동기 분리 구현 완료 커밋(이 대화 세션).

## 실행 조건

- payment-service: `-Dspring.profiles.active=local,loadtest`
- k6: `loadtest/k6/facade-isolation-test.js` (accept+폴링 버전)
  - baseline: `CONFIRM_RATE=20 CONTROL_RATE=20 DURATION=30s`, delay=50ms
  - fault: `CONFIRM_RATE=20 CONTROL_RATE=20 DURATION=60s CONFIRM_MAX_VUS=800`, delay=8000ms
- 측정 전 payment-service 재시작(CB 초기화) + Kafka 토픽을 10파티션으로 명시 설정(`KafkaTopicConfig`) 완료 후 측정

## 결과

### Baseline (PG 정상, delay=50ms)

| 지표 | accept (POST) | control_traffic |
|---|---|---|
| 에러율 | 0% | 0% |
| p95 | 38.05ms | 17.96ms |

`confirm_completion_duration`(폴링으로 잰 실제 완료시간)은 p95=7.67s로 50ms 지연치고 비정상적으로 높음 - PG 지연이 아니라 **기존 outbox 스케줄러(500ms 주기, LIMIT 50, 발행 순차 블로킹)의 처리량 한계** 때문. confirm 1건당 outbox 쓰기가 3건(요청 1 + 결과통지 2) 발생하는데, 이 환경에서 relay 처리량이 그걸 못 따라가서 뒤로 갈수록 발행이 밀림. 아래 "중요 발견" 참고.

### Fault 주입 (Toss 응답 8000ms 지연, 60초) — 핵심 비교

| 지표 | accept (POST) | control_traffic |
|---|---|---|
| 에러율 | **0%** | **0%** |
| p95 | **91.65ms** | **84.82ms** |
| avg | 33.62ms | 23.58ms |

Tomcat/HikariCP 풀 (Prometheus `max_over_time`):

| 지표 | before 피크 | after 피크 |
|---|---|---|
| `tomcat_threads_busy` (max 200) | 139 (69.5%) | **6 (3%)** |
| `hikaricp_connections_active` | 0~2 | 10 (= 컨슈머 동시성과 일치, 정상) |

`confirm_completion_duration`: avg=12.69s, p95=19.2s (8초 PG 지연 + relay 백로그 합산 - before의 pg_slow_confirm p95=29.99s와 직접 비교하는 지표 아님, 참고용)

## Before vs After 핵심 비교

| 지표 | Before | After |
|---|---|---|
| 요청(confirm/accept) p95 | 29.99s | **91.65ms** |
| 요청 에러율 | 96% | **0%** |
| control_traffic p95 | 6.53ms | 84.82ms |
| control_traffic 에러율 | 0% | 0% |
| tomcat_threads_busy 피크 | 139/200 (69.5%) | **6/200 (3%)** |

→ **스레드 풀 점유가 69.5%에서 3%로, 요청 자체의 p95가 30초에서 91ms로 떨어졌다.** 핵심 가설(Toss 장애가 Tomcat 스레드를 블로킹한다) 그대로 증명됨 - MQ 분리로 그 블로킹 구간 자체가 사라졌다.

## 중요 발견 - Outbox 릴레이 처리량 한계

confirm을 outbox 경로에 태우면서 기존 relay(`PaymentOutboxScheduler`)가 처리해야 할 이벤트가 늘었다(요청 접수 1건 + 결과통지 2건 = 1건당 outbox 3건). 이 relay는 500ms마다 최대 50건을 **순차적으로** Kafka에 발행하는데(`kafkaTemplate.send().get()`이 건별 블로킹), 테스트에서 쓴 초당 20건 페이로드에선 가끔 처리량 한계에 걸려 발행 자체가 수 초씩 밀리는 걸 관찰했다. 이게 `confirm_completion_duration`을 부풀리는 주 원인이지, Toss 응답 지연 자체가 아니다.

**중요: 이게 accept/control_traffic의 핵심 비교 수치엔 영향 없다** (Tomcat 스레드는 안 건드리는 별도 스케줄러 스레드라서). 다만 "사용자가 결제 완료를 체감하는 시간"엔 영향을 주므로, confirm/cancel 트래픽이 커지면 relay 처리량(배치 크기, 주기, 비동기 발행)을 별도로 튜닝해야 한다는 걸 시사한다 - 이번 범위에선 건드리지 않았다(기존 production 경로라 리스크 있는 변경).

## Relay 처리량 개선 (2026-10-11 추가)

"중요 발견"에 적은 relay 처리량 한계를 2단계로 고쳤다.

**1단계(싼 수정)**: `fixedDelay 500→100ms`, `LIMIT 50→200`. baseline에서 publish delay가 ~7s→~3~5s로 줄었지만(DB 타임스탬프로 직접 측정), fault에선 거의 개선이 없었다(19.2s→19.77s) - 건별 `.get()` 순차 블로킹 자체는 안 건드렸고, fault는 테스트가 60초로 더 길어서 그만큼 백로그가 쌓일 시간도 늘어났기 때문.

**2단계(구조 수정)**: `OutboxEventProcessor`를 배치 내 건별 블로킹 발행에서 **전부 동시에 발행 후 콜백에서 상태 갱신**(`CompletableFuture` 기반, 전용 스레드풀 `outboxPublishExecutor` 10개)으로 바꿨다. `PaymentOutboxProcessor.postEvent()`는 배치 전체가 끝날 때까지만 기다리고(`CompletableFuture.allOf(...).join()`), 실패한 건만 개별적으로 PENDING으로 되돌아간다(배치 전체를 롤백하던 기존 로직 제거).

| 지표 | 1단계 전 | 1단계 후 | 2단계 후 |
|---|---|---|---|
| baseline publish delay | ~7s | ~3~5s | **<1s급** (completion p95=657ms) |
| fault `confirm_completion_duration` p95 | 19.2s | 19.77s | **16.38s** |
| fault accept p95 | 91.65ms | ~790ms(폴링 경쟁 혼입) | 458.85ms |
| fault tomcat_threads_busy 피크 | 6/200 | 170/200(폴링 아티팩트) | 110/200(폴링 영향 일부 남음, 0.3s→1.5s로 줄였는데도) |

**baseline은 확실히 해결됐다**(수백 ms대로 떨어짐 - relay가 더 이상 정상 상황에서 병목이 아님). **fault는 개선됐지만(19.2s→16.38s) 여전히 느리다** - 이유는 confirm 실패/보류(HOLD)도 결과통지 outbox 2건을 그대로 쓰기 때문에, 장애 중엔 outbox 수요 자체가 안 줄어서 relay가 여전히 빡빡하게 돈다. 완전히 잡으려면 "요청 접수"용 outbox와 "결과 통지"용 outbox를 분리하는 3단계(구조 분리)가 필요하다 - 지금 범위에선 거기까진 안 들어갔다.

## 3단계: 요청 접수용 outbox를 결과통지용과 분리 (2026-10-11 추가)

`PaymentRequestOutbox`(신규 테이블/엔티티) + `PaymentRequestOutboxRepository` + `RequestOutboxEventProcessor` + `PaymentRequestOutboxProcessor` + `PaymentRequestOutboxScheduler`를 confirm/cancel 접수 전용으로 새로 만들고, 기존 `PaymentOutbox`/`PaymentOutboxScheduler`는 결제 결과통지(`PaymentPostProcessor`)만 쓰도록 되돌렸다. 둘은 이제 테이블, 스케줄러, 콜백 스레드풀(`outboxPublishExecutor` vs `requestOutboxPublishExecutor`)까지 완전히 분리돼서 서로의 처리량에 전혀 영향을 안 준다.

| 지표 | 2단계(병렬 발행)까지 | **3단계(relay 분리)까지** |
|---|---|---|
| fault `confirm_completion_duration` p95 | 16.38s | **10.69s** |
| fault accept p95 | 458.85ms | 378.75ms |
| fault control_traffic p95 | 551.61ms | 91.91ms |
| fault tomcat_threads_busy 피크 | 110/200 | **8/200 (4%)** |

예상대로 맞아떨어졌다 - 분리 전엔 confirm이 실패/보류돼도 결과통지 outbox 2건이 그대로 쓰여서 장애 중에도 relay 수요가 안 줄었는데, 분리 후엔 접수 전용 relay가 confirm 1건당 딱 1건만 처리하면 되니 장애 상황에서도 여유가 생겼다. 남은 10.69s는 이제 거의 순수하게 "Toss 8초 지연 + 서킷브레이커가 열리기 전까지 실제로 기다리는 일부 요청들"을 반영하는 숫자에 가깝다.

## 다음 할 일

- [ ] cancel 쪽도 1~3단계 전체 효과 측정 - `2026-10-08-after-cancel.md` 참고
