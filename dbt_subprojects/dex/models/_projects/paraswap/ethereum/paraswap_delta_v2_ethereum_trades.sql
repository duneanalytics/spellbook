{{ config(
    schema = 'paraswap_delta_v2_ethereum',
    alias = 'trades',

    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.call_block_time')],
    unique_key = ['method', 'call_tx_hash', 'call_trace_address', 'order_index'],
    post_hook='{{ expose_spells(blockchains = \'["ethereum"]\',
                                spell_type = "project",
                                spell_name = "paraswap_delta_v2",
                                contributors = \'["eptighte"]\') }}'
    )
}}

{% set project_start_date = '2024-05-01' %}

with
{{ delta_v2_swap_settle('ethereum') }}
select 
    'delta_v2_swap_settle_model' as method,
    0 as order_index,
    call_trace_address,
    call_block_number,
    call_block_time,
    date_trunc('month', call_block_time) AS block_month,
    call_tx_hash,
    -- parsed_order_data,
    feeAmount as fee_amount,
    -- orderWithSig as order_with_sig,
    executorData as calldata_to_execute,
    -- "order",
    signature,
    order_owner,
    src_token,
    dest_token,
    src_amount,
    dest_amount,
    src_token_for_joining,
    dest_token_for_joining,
    fee_token,
    src_token_price_usd,
    dest_token_price_usd,
    gas_fee_usd,
    src_token_order_usd,
    dest_token_order_usd,
    contract_address
 from delta_v2_swap_settle_model