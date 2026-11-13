# 💬 Bubble Clone Backend (Payment Engine)
이전 프로젝트에서 도출된 동기식 재시도의 응답 지연(p95 8초)과 웹 스레드 블로킹 한계를 극복하기 위해, 결제 유형별 장애 격리와 Outbox + Kafka 기반 완전 비동기 파이프라인을 구축한 백엔드 시스템입니다.

> - 진행 기간: `2026.06.29 ~ 2026.07.27` (1개월)
> - 팀 구성 & 역할: 백엔드 5인 (결제 도메인 장애 격리 및 비동기 파이프라인 설계 전담)
> - 핵심 기술: Spring Boot, PostgreSQL, Apache Kafka, Transactional Outbox, Resilience4j, WireMock, Prometheus, Grafana
> - 핵심 내용: 결제 유형별 장애 격리(Circuit Breaker / Backoff+Jitter), 비동기 전환을 통한 Tomcat 워커 점유 해소(139개 → 6개) 및 접수 지연 단축(29.99s → 91.65ms), 중복 결제 0건 보장

<br>

## 🏗 시스템 아키텍처

<p align="center">
  <img width="800" alt="local" src="https://github.com/user-attachments/assets/01a9d5f2-7765-4fc2-b442-f4fbba780d14" />
</p>

<br>

## 1. 결제 유형별 장애 격리 전략 (Circuit Breaker / Backoff + Jitter)

### [문제 현상 및 배경]
- 외부 PG사 장애(지연 8초, 에러율 80% 등) 발생 시 단순 고정 간격 재시도(`@Retryable`, 최대 3회)로 인해 지연이 누적되며 **p95 응답 시간이 최대 8초까지 급증**.
- 사용자는 8초간 화면 멈춤을 겪은 뒤 최종 실패를 마주해 UX가 크게 저하되었으며, 동일 간격 재시도로 장애 중인 PG사에 트래픽이 집중되는 **재시도 폭풍(Retry Storm)** 위험 노출.

### [해결 전략 및 아키텍처]
결제 도메인의 실행 특성(실시간 사용자 대기 vs 백그라운드 배치)에 따라 장애 격리 및 재시도 전략을 이원화했습니다.

1. **일반 결제 (실시간): Resilience4j Circuit Breaker 기반 Fast-Fail**
   - 사용자가 실시간으로 응답을 기다리는 특성을 고려해 서킷 브레이커 도입.
   - PG 장애 임계치 초과 시 서킷을 Open하여 불필요한 대기 없이 즉시 실패(Fast-Fail) 처리함으로써 장애 전파 조기 차단.
   <!-- 서킷 브레이커 적용 흐름 또는 설정 요약 이미지/코드 위치 -->

2. **정기 구독 결제 (배치): Exponential Backoff + Full Jitter**
   - 즉각적인 응답보다 최종 결제 성공률이 중요한 백그라운드 배치 특성에 맞춰 지수 백오프와 Full Jitter를 조합해 PG 부하 분산 및 Thundering Herd 방지.
   - 재시도 후에도 응답이 불확실한 건은 `UNKNOWN_HOLD` 상태로 격리하고 복구 스케줄러가 사후 추적·확정.
<br>

<img width="1802" height="810" alt="image" src="https://github.com/user-attachments/assets/149737fe-7411-4d95-8c26-5a1651c006a4" />

 
### [검증 결과 및 한계 도출 (병목 이동 및 아키텍처 회고)]
- **성과**: 정기 결제의 재시도 트래픽 분산(Thundering Herd 방지) 및 서킷 오픈 이후 일반 결제의 Fast-Fail 동작 확인.
- **한계 및 회고**:
  1. **동기 블로킹으로 인한 웹 스레드 고갈 (병목 이동)**
     - 서킷 브레이커는 실패율·지연 비율이 임계치에 도달해 열리기(Open) 전까지 인입되는 요청을 막지 못함.
     - 이로 인해 PG 지연(8초) 환경에서 **Tomcat 워커 스레드 200개 중 최대 139개(69.5%)가 PG 응답 대기에 점유**되었으며, 결제 API **p95 응답 시간이 29.99초**까지 치솟음.
     - **인사이트**: 서킷 브레이커만으로는 동기 블로킹 구조로 인한 **결제 API 자체의 웹 스레드 고갈을 근본적으로 해결할 수 없음**을 확인하고, PG 호출을 웹 스레드 밖으로 완전히 격리하는 Outbox + Kafka 비동기 파이프라인 전환 도출.
  2. **정기 결제 재시도(Retry) 경로의 서킷 브레이커 미결합 (설계 회고)**
     - 결제 유형을 '실시간(Circuit Breaker)'과 '정기 배치(Backoff + Jitter)'로 이원화하는 데 집중한 나머지, 정기 결제 경로에는 서킷 브레이커를 결합하지 못함.
     - 이로 인해 PG사 전체 장애(Down) 상황에서도 배치 워커가 지수 백오프 재시도를 끝까지 수행하며 불필요한 리소스를 소모하는 한계 존재.
     - **개선 방향**: 향후 정기 결제 파이프라인에도 `CircuitBreaker`와 `Retry`를 계층적으로 조합(`Retry(CircuitBreaker)`)하여, PG 장애가 명확히 감지된 상태에서는 배치 재시도 역시 즉시 중단하고 `UNKNOWN_HOLD`로 우회시키도록 고도화 필요.

<br>

## 2. Outbox + Kafka 기반 비동기 결제 접수로 스레드 고갈 해소

### [문제 현상 및 배경]
- Facade 패턴으로 DB 트랜잭션을 분리하고 Circuit Breaker를 적용했음에도, 외부 PG 호출이 여전히 **Tomcat 워커 스레드를 동기 블로킹**하는 구조적 한계 존재.
- PG 응답 지연(8초) 환경에서 결제 접수 API의 **Tomcat 워커 점유율 67%(134/200개)**, **`p95` 응답 지연 29.99초**가 발생하여 트래픽 집중 시 전체 웹 서버 마비 위험 확인.

<div align="center">
  <img width="805" height="332" alt="개선 전 Tomcat 스레드 점유 지표" src="https://github.com/user-attachments/assets/375141d4-b609-47fa-803f-8752d07276e7" />
  <br>
  <sub>▲ 개선 전: PG 응답 대기(8초)로 인한 Tomcat 워커 스레드 급증 (최대 134개 점유)</sub>
  <br><br>
  <img width="793" height="683" alt="개선 전 응답 지연 지표" src="https://github.com/user-attachments/assets/01b01bee-c3f1-45bb-b085-22a595b251c4" />
  <br>
  <sub>▲ 개선 전: 스레드 블로킹으로 인한 결제 API 응답 지연 폭증 및 에러 발생(p95 29.99s)</sub>
</div>

---

### [해결 전략 및 아키텍처]

1. **Transactional Outbox 기반 비동기 결제 접수 전환**
   - 결제 승인(`confirm`) 요청 시 동기식 PG 호출을 제거하고, 주문 상태 변경과 Outbox 이벤트 적재를 단일 DB 트랜잭션으로 커밋한 뒤 클라이언트에 즉시 **`202 Accepted`** 응답 반환 및 웹 워커 해제.

<div align="center">
  <img width="1846" height="812" alt="Outbox + Kafka 비동기 결제 아키텍처" src="https://github.com/user-attachments/assets/b347cf8a-3c50-4421-883a-29a70e1f8350" />
  <br>
  <sub>▲ Outbox + Kafka 기반 비동기 결제 접수 및 PG 승인 파이프라인 아키텍처</sub>
</div>

2. **웹 스레드 풀과 PG 호출 파이프라인의 완전 격리 및 병목 튜닝**
   - **파이프라인 재설계**: `결제 접수(Outbox 적재) → 폴링 스케줄러 → Kafka 발행 → 전용 PG 컨슈머` 구조로 전환하여 지연이 발생하는 외부 네트워크 I/O를 웹 서버 스레드 풀 밖으로 격리.

3. **메시지 유실 및 중복 결제 원천 차단**
   - **원자적 이벤트 발행**: 주문 상태 변경과 Outbox 테이블 적재를 단일 트랜잭션으로 묶어 발행 유실 차단.
   - **Kafka Manual Commit**: 컨슈머가 PG 승인 및 후속 DB 반영을 완전히 마친 시점에만 오프셋을 수동 커밋하여, 서버 재기동이나 일시적 장애 시에도 미처리 이벤트 전량 재처리 보장.

### [부하 테스트 검증 결과 및 회고]
PG 응답 지연(8초)을 주입한 동일 부하 환경에서 동기 블로킹(Circuit Breaker 적용) 구조와 Outbox + Kafka 비동기 구조를 비교 검증했습니다.

| 측정 지표 | 개선 전 (동기 + Circuit Breaker) | 개선 후 (Outbox + Kafka 비동기) | 개선 효과 |
| :--- | :---: | :---: | :---: |
| **Tomcat 워커 스레드 점유** | 최대 134개 / 200개 (**67%**) | 최대 6개 / 200개 (**3%**) | **워커 점유율 95.5% 감소** |
| **결제 접수 API p95 지연** | 29.99s | 91.65ms | **응답 지연 99.7% 단축** |

- **장애 전파 차단 및 인프라 기반 정합성 확보**:
  * 외부 PG 지연으로 발생하던 부하를 웹 서버의 공유 스레드 풀이 아닌 **Kafka 메시지 큐로 완전히 격리**함으로써, 특정 외부 연동 장애가 타 API의 스레드 고갈 및 전체 서버 마비로 전파되는 구조를 원천 차단했습니다.
  * 또한 비동기 전환 시 우려되는 데이터 유실과 중복 처리 문제를 애플리케이션의 복잡한 보상 로직 대신 **Transactional Outbox와 Kafka(Manual Commit 및 파티션 기반 순차 소비)라는 인프라 레벨의 신뢰성 메커니즘**으로 해결하여 시스템 가용성과 데이터 정합성을 동시에 확보했습니다.

- **💡 테스트 환경에 대한 회고**
  * 본 부하 테스트는 애플리케이션, DB, Kafka 브로커, 부하 생성기가 단일 머신 자원을 공유하는 **로컬 환경에서 수행**되었기 때문에, 절대적인 처리량(TPS)이나 응답 시간 수치 자체는 실제 분산 운영 환경과 차이가 있을 수 있습니다.
  * 그럼에도 **동기 구조에서 웹 서버(Tomcat) 스레드를 고갈시키던 외부 PG 지연 부하가 비동기 전환 이후 Kafka 컨슈머 파이프라인으로 완전히 격리·전이되는 구조적 동작을 정량적 지표로 확인**한 데 큰 의의가 있었습니다.


<div align="center">
  <img width="775" height="336" alt="개선 후 Tomcat 스레드 점유 지표" src="https://github.com/user-attachments/assets/ac53228b-13b5-40ba-9d75-38afdbdd0d88" />
  <img width="786" height="335" alt="개선 후 결제 접수 API 응답 시간" src="https://github.com/user-attachments/assets/eb973e0f-6fe3-48db-aed7-b8b15207a476" />
  <br>
  <sub>▲ 개선 후: Tomcat 워커 점유 6개 이하 유지 및 결제 접수 API p95 91.65ms 달성</sub>
  <br><br>
  <img width="773" height="321" alt="Kafka 컨슈머 부하 전이 지표 1" src="https://github.com/user-attachments/assets/9688c0aa-37e0-4a66-8027-320ce6c007b8" />
  <img width="738" height="318" alt="Kafka 컨슈머 부하 전이 지표 2" src="https://github.com/user-attachments/assets/38cc1eb8-9be6-40fe-847c-64b8144df80a" />
  <br>
  <sub>▲ 개선 후: 기존 웹 스레드에 집중되던 외부 PG 지연 부하가 Kafka 컨슈머 파이프라인으로 격리·전이되어 처리되는 지표</sub>
</div>

