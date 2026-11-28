-- Public TON messages from Dune query 9119480, independently decoded with
-- pytoniq-core, plus one synthetic empty-dictionary case. Covers referral
-- dictionaries, addr_none, nonempty sender addresses and signed amounts.
-- depends_on: {{ ref('stormtrade_ton_trade_notification_v2') }}
WITH fixtures AS (
    SELECT * FROM (VALUES
        ('UxHkQADjg9k0Pny2bECvSJpuHp+9dZAettbkA/YEtSs=', from_base64('te6cckEBAwEAvQAC0CjTZJH4O8YVuYVN8gA7AAAAATmgS4EAAAAAAAAAAAAAABXf68XgAAAAAqAOWKYAAAAAhmlE7gAAAADgBMg3AAAAAAAAAAAcAMVCcbJE7uPh6sVS0/Q1InOSgCWG3nhv93oFTQHsNEDKAQIAR9IAdctUsF3RmTy0jETjQ3sIk1nuk6dWFcRGPVzLNj5D6R6yQABRGAcr5MnM6gBe9VQNQKD7egDKR716ozBEP0km08zI1WlJMd/WTxxIE1Dz5DzQ'), '17887108141641190898', '59', '5261773697', '0', '93951084000', '11275229350', '2255045870', '3758409783', '0', '1', '0:C54271B244EEE3E1EAC552D3F435227392802586DE786FF77A054D01EC3440CA', 'addr_none', '1971275854650', '0:BDEAA81A8141F6F401948F7AF54660887E924DA79991AAD29263BFAC9E389026'),
        ('JViNPOb1UMlXuTIEpigKucPGKBjoZyiFcfGhatQ8npk=', from_base64('te6cckEBAwEAtwAC0CjTZJFmWmyyuq0WMAAEAAAAAIPPYlAAAAAAAAAAAP////ywU0MzAAAAAQeexJ8AAAAAAAAAAAAAAACDz2JPAAAAAAAAAAAcAE0OI/84TrW4tBkyTq5VJbktLx4eRIOiP3JONdaJnZeLAQIAR9IAdctUsF3RmTy0jETjQ3sIk1nuk6dWFcRGPVzLNj5D6R6yQABFAgBznBkcrevwDa10Y6sSTCBUVJo9P3N+u2Lc9Io7GTy4k5Ce96ot'), '7375326854664492592', '4', '2211406416', '0', '-14221622477', '4422812831', '0', '2211406415', '0', '1', '0:4D0E23FF384EB5B8B419324EAE5525B92D2F1E1E4483A23F724E35D6899D978B', 'addr_none', '0', '0:E73832395BD7E01B5AE8C756249840A8A9347A7EE6FD76C5B9E9147632797127'),
        ('synthetic-empty-referrals', from_base64('te6ccgEBAgEAsgAB0CjTZJEAAAAAAAAAAAAD//////////8AAAAAAAAAAv/////////9AAAAAAAAAAAAAAAAAAAABwAAAAAAAAAIAAAAAAAAAAAEABERERERERERERERERERERERERERERERERERERERERERAQCJgAZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmZmYhMACIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIig'), '0', '3', '-1', '2', '-3', '0', '7', '8', '0', '0', '0:1111111111111111111111111111111111111111111111111111111111111111', '0:3333333333333333333333333333333333333333333333333333333333333333', '9', '0:2222222222222222222222222222222222222222222222222222222222222222')
    ) AS t(tx_hash, body_boc, expected_query_id, expected_asset_id, expected_free_amount, expected_locked_amount, expected_exchange_amount, expected_withdraw_locked_amount, expected_fee_to_stakers, expected_executor_amount, expected_total_referrer_amount, expected_ref_count, expected_trader_addr, expected_origin_addr, expected_balance_delta, expected_amm)
),
decoded AS (
    SELECT tx_hash, {{ stormtrade_ton_decode_trade_notification_v2('body_boc') }} AS result,
        expected_query_id, expected_asset_id, expected_free_amount, expected_locked_amount, expected_exchange_amount, expected_withdraw_locked_amount, expected_fee_to_stakers, expected_executor_amount, expected_total_referrer_amount, expected_ref_count, expected_trader_addr, expected_origin_addr, expected_balance_delta, expected_amm
    FROM fixtures
)
SELECT tx_hash FROM decoded
WHERE CAST(result.query_id AS varchar) IS DISTINCT FROM expected_query_id
    OR CAST(result.asset_id AS varchar) IS DISTINCT FROM expected_asset_id
    OR CAST(result.free_amount AS varchar) IS DISTINCT FROM expected_free_amount
    OR CAST(result.locked_amount AS varchar) IS DISTINCT FROM expected_locked_amount
    OR CAST(result.exchange_amount AS varchar) IS DISTINCT FROM expected_exchange_amount
    OR CAST(result.withdraw_locked_amount AS varchar) IS DISTINCT FROM expected_withdraw_locked_amount
    OR CAST(result.fee_to_stakers AS varchar) IS DISTINCT FROM expected_fee_to_stakers
    OR CAST(result.executor_amount AS varchar) IS DISTINCT FROM expected_executor_amount
    OR CAST(result.total_referrer_amount AS varchar) IS DISTINCT FROM expected_total_referrer_amount
    OR CAST(result.ref_count AS varchar) IS DISTINCT FROM expected_ref_count
    OR CAST(result.trader_addr AS varchar) IS DISTINCT FROM expected_trader_addr
    OR CAST(result.origin_addr AS varchar) IS DISTINCT FROM expected_origin_addr
    OR CAST(result.balance_delta AS varchar) IS DISTINCT FROM expected_balance_delta
    OR CAST(result.amm AS varchar) IS DISTINCT FROM expected_amm
