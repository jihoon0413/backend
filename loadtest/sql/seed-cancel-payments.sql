-- payment-service 취소(cancel) 부하 테스트용 데이터 시딩
-- cancel API는 PAID 상태 + refundableAmount/paymentKey가 있는 payments row가 있어야 동작한다.
-- confirm 테스트와 같은 유저/결제수단(999001)을 재사용하고, 이번엔 바로 PAID 상태로 심는다.
--
-- 사용법 (매 테스트 실행마다 새 batch 값을 줄 것):
--   psql -h localhost -U payment_user -d paymentdb \
--     -v batch="$(date +%s)" -v count=3000 -f seed-cancel-payments.sql
--
-- 실행 후 출력되는 min(id)를 k6의 CANCEL_ID_START 환경변수로 넘길 것.

INSERT INTO user_brandpay_auth (user_id, customer_key, access_token, refresh_token, is_billing_agreed, created_at, updated_at)
VALUES (999001, 'k6-customer', 'k6-access-token', 'k6-refresh-token', true, now(), now())
ON CONFLICT (user_id) DO NOTHING;

INSERT INTO payment_methods (id, user_id, provider, type, toss_method_id, toss_method_key, display_name, masked_number, status, is_default, created_at, updated_at)
VALUES (999001, 999001, 'TOSS', 'NORMAL', 'k6-toss-method', 'k6-toss-method-key', 'k6 load test card', '**** 0000', 'ACTIVE', true, now(), now())
ON CONFLICT (id) DO NOTHING;

INSERT INTO payments (payment_method_id, order_id, toss_order_id, user_id, payment_key, total_amount, refundable_amount, currency, status, idempotency_key, created_at, updated_at)
SELECT
    999001,
    gs,
    'k6-cancel-' || :'batch' || '-' || gs,
    999001,
    'wiremock-pay-key',
    1000.0000,
    1000.0000,
    'KRW',
    'PAID',
    'k6-cancel-src-idem-' || :'batch' || '-' || gs,
    now(),
    now()
FROM generate_series(0, :count - 1) AS gs;

-- k6의 CANCEL_ID_START로 넘길 값
SELECT min(id) AS cancel_id_start, max(id) AS cancel_id_end
FROM payments
WHERE toss_order_id LIKE 'k6-cancel-' || :'batch' || '-%';

-- 정리하고 싶으면:
-- DELETE FROM payments WHERE toss_order_id LIKE 'k6-cancel-<batch>-%';
