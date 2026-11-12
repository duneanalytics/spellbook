{{ config(
    schema = 'stormtrade_ton',
    alias = 'trade_notification_v2',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    unique_key = ['tx_hash', 'block_date'],
    filtering_columns = ['block_date'],
    post_hook='{{ expose_spells(\'["ton"]\', "project", "stormtrade", \'[]\') }}'
) }}

-- V2 has a different layout from trade_notification#3475fdd2.
-- https://github.com/ton-blockchain/abis/blob/7dc57067d85c95dc25c09ccaaf108eaba62f6303/data/storm/vault/types/storm_vault.types.tolk
-- Keep V1's table contract unchanged. Consumers must include this table for V2.

WITH stormtrade_vaults AS (
    SELECT vault, vault_token FROM (VALUES
    (upper('0:33e9e84d7cbefff0d23b395875420e3a1ecb82e241692be89c7ea2bd27716b77'), 'USDT'),
    (upper('0:f29d17a209e2bcc652916d802ba69e23cb366e17afad61f8453343b9ba53ace4'), 'jUSDT'),
    (upper('0:e926764ff3d272c73ddeb836975c5521c025ad68e7919a25094e2de3198805f1'), 'TON'),
    (upper('0:06f3f073c255a49aa6fdcc89abf512638e065908b30e4173fd3d1d01d4f607bd'), 'NOT')
    ) AS T(vault, vault_token)
),
parsed_boc AS (
    SELECT M.block_date, M.tx_hash, M.trace_id, M.tx_now, M.tx_lt,
        vault, vault_token, M.source AS message_source, M.opcode, M.body_boc
    FROM {{ source('ton', 'messages') }} M
    JOIN stormtrade_vaults ON M.destination = vault
    JOIN {{ source('ton', 'transactions') }} T
        ON M.block_date = T.block_date AND M.tx_hash = T.hash
        AND T.block_date > DATE '2023-10-01'
        {% if is_incremental() -%}
        AND {{ incremental_predicate('T.block_date') }}
        {% endif -%}
    WHERE M.block_date > DATE '2023-10-01'
        AND M.direction = 'in'
        AND M.opcode = 684942481 -- trade_notification_v2#28d36491
        AND T.compute_exit_code = 0 AND T.action_result_code = 0
        {% if is_incremental() -%}
        AND {{ incremental_predicate('M.block_date') }}
        {% endif -%}
),
parse_output AS (
    SELECT CASE WHEN opcode = 684942481 THEN {{ stormtrade_ton_decode_trade_notification_v2('body_boc') }} END AS result,
    block_date, tx_hash, trace_id, tx_now, tx_lt, vault, vault_token, message_source
    FROM parsed_boc
)
SELECT block_date, tx_hash, trace_id, tx_now, tx_lt, vault, vault_token,
    message_source, result.query_id, result.asset_id,
    result.free_amount, result.locked_amount, result.exchange_amount,
    result.withdraw_locked_amount, result.fee_to_stakers,
    result.executor_amount, result.total_referrer_amount, result.ref_count,
    result.trader_addr, result.origin_addr, result.balance_delta, result.amm
FROM parse_output
