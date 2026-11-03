# payment-service Outbox+Kafka 분리 - AFTER 측정: cancel (2026-10-08)

`2026-10-08-before-cancel.md`과 직접 비교.

## 실행 조건

confirm과 동일 (`2026-10-08-after-facade.md` 참고). k6: `loadtest/k6/facade-isolation-cancel-test.js`
- baseline: `CANCEL_RATE=20 CONTROL_RATE=20 DURATION=30s`, delay=50ms
- fault: `CANCEL_RATE=20 CONTROL_RATE=20 DURATION=60s CANCEL_MAX_VUS=800`, delay=8000ms

**측정 중 폴링 로직 버그 하나 발견/수정함**: cancel accept는 confirm과 달리 `Payment.status`를 `PROCESSING`으로 바꾸지 않는다(기존 `readyCancel()`을 그대로 재사용했고, 거기선 `PaymentCancel.status=REQUEST`만 바뀜). 그래서 폴링 종료조건을 confirm과 똑같이 "status != PROCESSING"으로 짰더니 접수 직후(아직 PAID인 상태)를 바로 "완료"로 오판했다. "status != PAID"로 고쳐서 재측정함 - 이 문서의 수치는 수정 후 버전.

## 결과

### Baseline (PG 정상, delay=50ms)

| 지표 | accept (POST) | control_traffic |
|---|---|---|
| 에러율 | 0% | 0% |
| p95 | 122.59ms | 232.71ms |

`cancel_completion_duration` p95=12.04s - confirm과 동일한 이유(outbox relay 처리량 한계), PG 지연이랑 무관.

### Fault 주입 (Toss 응답 8000ms 지연, 60초) — 핵심 비교

| 지표 | accept (POST) | control_traffic |
|---|---|---|
| 에러율 | **0%** | **0%** |
| p95 | **402.31ms** | 491.96ms ⚠ 아래 주의 참고 |
| avg | 116.98ms | 126.13ms |

Tomcat 풀 (Prometheus `max_over_time`): **200/200 (100%) ⚠**

## ⚠ 중요 - Tomcat 200/200은 격리 실패가 아니라 테스트 설계 결함

이 수치만 보면 "MQ 분리했는데도 스레드 풀이 꽉 찼다"로 오해하기 쉬운데, 원인을 파보니 아니었다. 요청 유형별 건수를 직접 뽑아봤다:

| 요청 종류 | 건수 |
|---|---|
| `status_poll` (GET /status 폴링) | **42,429** |
| `accept` (POST /cancel) | 717 |
| `control_traffic` (GET /health) | 1,084 |

cancel은 완료까지 24~30초가 걸리는데(= 10개 컨슈머가 8초짜리 Toss 호출을 처리하는 처리량 한계, 아래 참고), k6 폴링 주기가 0.3초라서 VU 하나가 완료될 때까지 최대 100번 가까이 폴링을 반복했다. 그 결과 `/status` 폴링 요청만 초당 500건 넘게 쏟아져서 **그 폴링 트래픽 자체가 Tomcat을 채운 것**이지, cancel의 Toss 블로킹이 전파된 게 아니다. 증거: 같은 상황에서 `accept`(진짜 측정 대상)의 p95는 402ms로 여전히 양호했다 - 진짜로 스레드가 장시간 고갈됐다면 accept도 같이 수 초 이상 걸렸어야 한다.

confirm 쪽은 완료가 더 빨라서(12~19s) 폴링이 덜 쌓였고 Tomcat 피크가 6/200에 그쳤다 - 같은 아키텍처인데 이 차이가 나는 이유가 바로 폴링 볼륨 차이다.

**결론: accept/control_traffic 에러율·응답시간 수치는 그대로 신뢰해도 된다 (둘 다 0%/수백ms). Tomcat 200/200 수치만 폴링 설계 결함으로 무효 처리한다.**

**재확인 완료**: 폴링 주기를 0.3초 → 1.5초로 늘려서 같은 fault 시나리오(delay=8000ms, CANCEL_RATE=20)를 다시 돌렸더니 `tomcat_threads_busy` 피크가 **58/200 (29%)**로 나왔다 - 200/200이 순수 폴링 아티팩트였다는 게 확인됨. confirm(6/200)만큼 깨끗하진 않은데, 이건 cancel의 완료 시간 자체가 더 길어서(아래 완료시간 지표 참고) 그만큼 동시에 대기 중인 폴링 요청 수가 여전히 confirm보다 많기 때문 - 폴링 주기를 더 늘리거나 SSE/웹훅으로 바꾸면 더 내려갈 여지가 있다.

## Before vs After 핵심 비교

| 지표 | Before | After |
|---|---|---|
| 요청(cancel/accept) p95 | 31.32s | **402.31ms** |
| 요청 에러율 | 90.33% | **0%** |
| control_traffic p95 | 30s (타임아웃) | **491.96ms** |
| control_traffic 에러율 | 13.49% | **0%** |
| tomcat_threads_busy 피크 | 172/200 (86%) | **58/200 (29%)** (폴링 주기 1.5초로 재확인) |

→ cancel은 before에서 서킷브레이커가 없어 confirm보다 훨씬 심하게 터졌던 케이스인데, after에서는 confirm과 거의 같은 수준으로 개선됐다(accept p95 31초 → 0.4초, control_traffic 에러율 13.49% → 0%). **cancel이야말로 MQ 분리 효과가 가장 극적으로 드러나는 케이스.**

## 다음 할 일

- [ ] 폴링 주기를 1~2초로 늘려서 Tomcat 피크를 재측정 (confirm처럼 한 자릿수가 나올 것으로 예상)
- [ ] relay 처리량 튜닝 (confirm 문서와 동일 이슈)
