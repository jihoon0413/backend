-- payment-service 부하 테스트용 결제 데이터 시딩
-- confirm API는 ready() 단계를 거친 READY 상태의 payments row가 있어야 동작하는데,
-- ready()는 브랜드페이 연동/카드등록 같은 온보딩 선행 조건이 많아 k6에서 직접 거치기 어렵다.
-- 그래서 온보딩은 건너뛰고 필요한 row를 SQL로 바로 만든다.
--
-- 사용법 (매 테스트 실행마다 새 batch 값을 줄 것 - 같은 batch 재사용 시 toss_order_id 충돌):
--   psql -h localhost -U payment_user -d paymentdb \
--     -v batch="$(date +%s)" -v count=5000 -f seed-confirm-payments.sql

INSERT INTO user_brandpay_auth (user_id, customer_key, access_token, refresh_token, is_billing_agreed, created_at, updated_at)
VALUES (999001, 'k6-customer', 'k6-access-token', 'k6-refresh-token', true, now(), now())
ON CONFLICT (user_id) DO NOTHING;

INSERT INTO payment_methods (id, user_id, provider, type, toss_method_id, toss_method_key, display_name, masked_number, status, is_default, created_at, updated_at)
VALUES (999001, 999001, 'TOSS', 'NORMAL', 'k6-toss-method', 'k6-toss-method-key', 'k6 load test card', '**** 0000', 'ACTIVE', true, now(), now())
ON CONFLICT (id) DO NOTHING;

INSERT INTO payments (payment_method_id, order_id, toss_order_id, user_id, total_amount, currency, status, idempotency_key, created_at, updated_at)
SELECT
    999001,
    gs,
    'k6-' || :'batch' || '-' || gs,
    999001,
    1000.0000,
    'KRW',
    'READY',
    'k6-idem-' || :'batch' || '-' || gs,
    now(),
    now()
FROM generate_series(0, :count - 1) AS gs;

-- 정리하고 싶으면 (이 batch로 시딩한 것만):
-- DELETE FROM payments WHERE toss_order_id LIKE 'k6-<batch>-%';
