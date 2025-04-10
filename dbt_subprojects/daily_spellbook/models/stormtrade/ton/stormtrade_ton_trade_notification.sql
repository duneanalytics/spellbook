{{ config(
       schema = 'stormtrade_ton'
       , alias = 'trade_notification'
       , materialized = 'incremental'
       , file_format = 'delta'
       , incremental_strategy = 'merge'
       , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
       , unique_key = ['tx_hash', 'block_date']
       , post_hook='{{ expose_spells(\'["ton"]\',
                                   "project",
                                   "stormtrade",
                                   \'["pshuvalov"]\') }}'
   )
 }}


-- trade_notification#3475fdd2 is sent from the AMM to the vault
-- https://github.com/Tsunami-Exchange/storm-contracts-specs/blob/3da4852/scheme.tlb#L143-L148

WITH stormtrade_vaults AS (
    SELECT vault, vault_token FROM (VALUES 
    (upper('0:33e9e84d7cbefff0d23b395875420e3a1ecb82e241692be89c7ea2bd27716b77'), 'USDT'),
    (upper('0:f29d17a209e2bcc652916d802ba69e23cb366e17afad61f8453343b9ba53ace4'), 'jUSDT'),
    (upper('0:e926764ff3d272c73ddeb836975c5521c025ad68e7919a25094e2de3198805f1'), 'TON'),
    (upper('0:06f3f073c255a49aa6fdcc89abf512638e065908b30e4173fd3d1d01d4f607bd'), 'NOT')
    ) AS T(vault, vault_token)
),
parsed_boc AS (
    SELECT M.block_date, M.tx_hash, M.trace_id, M.tx_now, M.tx_lt, vault, vault_token, M.source as amm, body_boc
    FROM {{ source('ton', 'messages') }} M
    JOIN stormtrade_vaults ON M.destination = vault
    JOIN {{ source('ton', 'transactions') }} T ON M.block_date = T.block_date AND M.tx_hash = T.hash AND M.direction = 'in'
    WHERE M.block_date > TIMESTAMP '2023-10-01'
    {% if is_incremental() %}
        AND {{ incremental_predicate('M.block_date') }}
    {% endif %}
    AND opcode = 880147922 -- trade_notification#3475fdd2
    AND T.compute_exit_code = 0 AND T.action_result_code = 0 -- only successful transactions
), parse_output as (
select {{ ton_from_boc('body_boc', [
    ton_begin_parse(),
    ton_skip_bits(32),
    ton_load_uint(16, 'asset_id'),
    ton_load_int(64, 'free_amount'),
    ton_load_int(64, 'locked_amount'),
    ton_load_int(64, 'exchange_amount'),
    ton_load_uint(64, 'withdraw_locked_amount'),
    ton_load_uint(64, 'fee_to_stakers'),
    ton_load_uint(64, 'withdraw_amount'),
    ton_load_address('trader_addr'),
    ton_load_address('origin_addr'),

    ton_load_maybe_ref(),
    ton_begin_parse(),
    ton_load_coins('referral_amount'),
    ton_load_address('referral_addr'),

    ton_restart_parse(),
    ton_begin_parse(),
    ton_skip_bits(32 + 16 + 64 * 6),
    ton_load_address('trader_addr_2'),
    ton_load_address('origin_addr_2'),
    ton_skip_maybe_ref(),
    ton_load_maybe_ref(),
    ton_begin_parse(),
    ton_load_uint(1, 'split_executor_reward'),
    ton_load_coins('executor_amount'),
    ton_load_uint(32, 'executor_index')

    ]) }} as result, * from parsed_boc
)
select block_date, tx_hash, trace_id, tx_now, tx_lt,
vault, vault_token, amm, result.asset_id, result.free_amount, result.locked_amount,
result.exchange_amount, result.withdraw_locked_amount, result.fee_to_stakers,
result.withdraw_amount, result.trader_addr, result.origin_addr,
result.referral_amount, result.referral_addr,
result.split_executor_reward, result.executor_amount, result.executor_index
from parse_output