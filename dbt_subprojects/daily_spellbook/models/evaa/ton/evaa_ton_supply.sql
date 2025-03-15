{{ config(
       schema = 'evaa_ton'
       , alias = 'supply'
       , materialized = 'incremental'
       , file_format = 'delta'
       , incremental_strategy = 'merge'
       , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
       , unique_key = ['tx_hash']
       , post_hook='{{ expose_spells(\'["ton"]\',
                                   "project",
                                   "evaa",
                                   \'["pshuvalov"]\') }}'
   )
 }}


-- op::supply_succes message is sent from the user contract to the pool contract, message layout is defined here:
-- https://github.com/evaafi/contracts/blob/d5a6bf889f8bbfa8bcc82671c17e65a3b2b360cd/contracts/messages/supply-message.fc#L61-L71
-- note that contract was updated on May 18 2024 17:00:31 GMT and new field user_new_principal was added
WITH evaa_pools AS (




    SELECT pool_address FROM (VALUES 
    ('0:BCAD466A47FA565750729565253CD073CA24D856804499090C2100D95C809F9E'), -- Main pool
    ('0:489595F65115A45C24A0DD0176309654FB00B95E40682F0C3E85D5A4D86DFB25'), -- LP pool
    ('0D511552DDF8413BD6E2BE2837E22C89422F7B16131BA62BE8D5A504012D8661') -- Alts pool
    ) AS T(pool_address)
),
 parsed_boc AS (
    SELECT T.block_date, M.tx_hash, T.trace_id, T.now AS tx_now, T.lt AS tx_lt, pool_address, {{ ton_boc_begin_parse('body_boc') }} AS boc
    FROM {{ source('ton', 'transactions') }} T
    JOIN  {{ source('ton', 'messages') }} M ON T.block_date = M.block_date AND T.hash = M.tx_hash AND direction = 'in'
    JOIN evaa_pools ON M.destination = pool_address
    WHERE compute_exit_code = 0 AND action_result_code = 0 -- only successful operations validated by the pool
    AND T.block_date >= TIMESTAMP '2023-10-09' -- protocol launch
    {% if is_incremental() %}
        AND {{ incremental_predicate('T.block_date') }}
    {% endif %}
    AND opcode = 282 -- op::supply_succes, 0x11a
), parsed_cell AS (
    SELECT block_date, tx_hash, trace_id, tx_now, tx_lt, pool_address, {{ ton_cell_load_cell('boc') }} -- cell, cell_cursor
    FROM parsed_boc
), parse_step_one AS (
    SELECT block_date, tx_hash, trace_id, tx_now, tx_lt, pool_address, cell,
    format('0x%x',CAST({{ ton_cell_preload_uint('cell', ton_cell_skip_bits('cell_cursor', 32), 64) }} as bigint)) AS query_id,
    {{ ton_cell_load_address('cell', ton_cell_skip_bits('cell_cursor', 32 + 64)) }} AS cell_cursor
    FROM parsed_cell
), parse_step_two AS (
    SELECT block_date, tx_hash, trace_id, tx_now, tx_lt, pool_address, query_id, cell_cursor.address AS owner_address,
    {{ ton_cell_preload_uint('cell', 'cell_cursor', 256) }} as asset_id,
    {{ ton_cell_preload_uint('cell', ton_cell_skip_bits('cell_cursor', 256), 64) }} as amount,
    cell,  {# XUL1eBYPbMd2fHydBGgojhqYEHsNi1bmSDS1xGGrwPo contract update time #}
    CAST(IF (tx_now > 1716051631, 
        {{ ton_cell_load_int('cell', ton_cell_skip_bits('cell_cursor', 256 + 64), 64, cast_row=false) }},
        {{ ton_cell_load_int('cell', ton_cell_skip_bits('cell_cursor', 256 + 64), 0, cast_row=false) }}
    ) AS ROW(bit_offset bigint, ref_offset bigint, value INT256)) AS cell_cursor
    FROM parse_step_one
), parse_step_three AS (
    SELECT block_date, tx_hash, trace_id, tx_now, tx_lt, pool_address, query_id, owner_address, asset_id,
    amount, cell_cursor.value as user_new_principal,
    {{ ton_cell_preload_int('cell', 'cell_cursor', 64) }} AS repay_amount_principal,
    {{ ton_cell_preload_int('cell', ton_cell_skip_bits('cell_cursor', 64), 64) }} AS supply_amount_principal
    FROM parse_step_two
)
SELECT block_date, tx_hash, trace_id, tx_now, tx_lt, pool_address, query_id, owner_address, asset_id,
    amount, user_new_principal, repay_amount_principal, 
    supply_amount_principal from parse_step_three

