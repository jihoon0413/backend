# 🧋 BUBBLETEA Backend

> **커머스+메신저: 구독형 팬&아티스트 소통 플랫폼**
> 월간 구독권 및 아티스트별 상품 로직을 결합한 커머스-메신저 통합 플랫폼

> **🗓️ MVP 개발 기간:**  `2026.06.29 ~ 2026.07.27`
> **https://github.com/twogetter/backend**

<br>


## 🤝 팀원
  
|<img width=150 alt="문시원 (팀장)" src="https://avatars.githubusercontent.com/u/105481797?v=4" />|<img width=150 alt="김재우 (부팀장)" src="https://avatars.githubusercontent.com/u/134791226?v=4" />|<img width=130 alt="신지훈" src="https://avatars.githubusercontent.com/u/119675297?v=4" />|<img width=150 alt="유창민" src="https://avatars.githubusercontent.com/u/268832835?v=4" />|<img width=150 alt="윤수현" src="https://avatars.githubusercontent.com/u/141344997?v=4" />|<img width=150 alt="차민혁" src="https://avatars.githubusercontent.com/u/143821560?v=4" />|
| :---: | :---: | :---: | :---: | :---: | :---:|
| 문시원 (팀장) | 김재우 (부팀장) | 신지훈 | 유창민 | 윤수현 | 차민혁 |
| [@muncool39](https://github.com/muncool39) | [@J4E-mik](https://github.com/J4E-mik) | [@jihoon0413](https://github.com/jihoon0413) | [@dnwn3295-lgtm](https://github.com/dnwn3295-lgtm) | [@yS2h](https://github.com/yS2h) | [@Dev-Rutin](https://github.com/Dev-Rutin) |



<br>

## 🛠 기술 스택


### Application
| Category |	Stack |
|:-----|:-----|
| Language	| Java 25 | 
| Framework	| Spring Boot 4.0.7 |
| Build	| Gradle (Multi-module) |
| Spring Cloud	| Eureka, Config Server, Gateway, OpenFeign |
| Messaging	| Apache Kafka (Confluent 7.8.0) |
| Library	| Lombok, Spring Boot Actuator |

### Infrastructure
| Category	| Stack  |
|:-----|:-----|
| Container| 	Docker, Docker Compose |
| Database	| PostgreSQL 17, MongoDB 8 |
| Cache| 	Redis 7 |
| Monitoring	| Kafka UI |

<br>

## 🏗 시스템 아키텍처

<p align="center">
  <img width="800" alt="local" src="https://github.com/user-attachments/assets/01a9d5f2-7765-4fc2-b442-f4fbba780d14" />
</p>

<br>

<br>

## 🧩 주요 기능
| 서비스 | 주요 기능 | 
|:-----|:-----|
| auth-service	| 회원가입, 로그인, JWT 토큰 발급, 로그아웃 | 
| user-service	| 사용자 프로필 조회 및 수정, 회원 탈퇴 | 
| product-service	| 상품 및 예약 스케줄 관리 | 
| payment-service	| 결제 요청, 결제 이력 조회 | 
| order-service	| 주문 생성, 주문 상태 관리 | 
| notification-service	| 알림 발송, 알림 이력 조회 | 
| chat-service	| 실시간 채팅, 채팅방 관리, 메시지 이력 조회 | 

<br>


## 🚀 실행 방법
### 전체 서비스 기본 실행 방법
```bash
# 1. 저장소 클론
git clone https://github.com/twogetter/backend.git bubbletea-backend
cd bubbletea-backend

# 2. .env 파일 설정
cp .env.example .env

# 3. 전체 서비스 빌드 및 실행
docker compose up --build -d

# 4. 실행 상태 확인
docker compose ps
```
### 시드 데이터 포함 방법
local 프로파일을 사용해 MongoDB 테스트용 초기 데이터 삽입이 가능합니다.
```bash
docker compose --profile local up --build -d
```
### 서비스 시작 순서
```scss
1단계 (인프라):    PostgreSQL, MongoDB, Redis, Kafka
2단계 (설정/등록): Config Server → Eureka Server
3단계 (애플리케이션): API Gateway, 각 *-service
```
### 특정 서비스만 재빌드
```bash
# 예: auth-service만 재빌드
docker compose up --build -d auth-service
```

<br>

## 📦 패키지 구성
```
bubbletea/                        # 루트 프로젝트
│
├── common-lib/                   # 공통 유틸, DTO, 예외 등 공유 라이브러리
├── common-test/                  # 공통 테스트 픽스처 및 유틸
│
├── config-server/                # Spring Cloud Config Server (설정 중앙화)
├── eureka-server/                # Netflix Eureka Server (서비스 디스커버리)
├── gateway-server/               # Spring Cloud Gateway (API 게이트웨이, JWT 필터)
│
├── auth-service/                 # 인증/인가 (로그인, 토큰 발급) | PostgreSQL, Redis
├── user-service/                 # 사용자 관리 | PostgreSQL
├── product-service/              # 상품/컨텐츠 관리 | MongoDB
├── payment-service/              # 결제 처리 | PostgreSQL, Redis
├── order-service/                # 주문 관리 | PostgreSQL
├── notification-service/         # 알림 발송 | PostgreSQL
├── chat-service/                 # 실시간 채팅 | PostgreSQL, Redis
│
├── docker/
│   ├── postgres-init/            # PostgreSQL 초기화 스크립트
│   ├── mongo-init/               # MongoDB 사용자/DB 초기화 스크립트
│   └── mongo-seed/               # MongoDB 로컬 테스트 데이터 시드
│
├── docker-compose.yml
├── .env.example
└── build.gradle
```

<br>
