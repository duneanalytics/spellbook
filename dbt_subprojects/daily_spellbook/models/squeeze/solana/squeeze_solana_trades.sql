{{ config(
    alias = 'trades',
    schema = 'squeeze_solana',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_month', 'tx_id', 'outer_instruction_index', 'inner_instruction_index']
) }}

{% set project_start_date = '2025-04-15' %}
{% set platform_id = 'FpKUW9vDSRPTByNu4MerR2SU4YPkJU9pLWQTnChGAW3h' %}

SELECT
    b.block_time
    , date_trunc('day', b.block_time) AS block_date
    , date_trunc('month', b.block_time) AS block_month
    , 'solana' AS blockchain
    , 'squeeze' AS project
    , 'raydium_launchlab' AS dex_project
    , b.pool_id
    , b.base_token_mint
    , b.quote_token_mint
    , b.is_buy
    , b.platform_config AS platform_id
    , b.platform_name
    , b.tx_id
    , b.tx_signer AS trader_id
    , b.outer_instruction_index
    , b.inner_instruction_index
    , b.tx_index
    , b.block_slot
FROM {{ ref('raydium_launchlab_v1_base_trades') }} b
WHERE b.platform_config = '{{ platform_id }}'
  {% if is_incremental() %}
  AND {{ incremental_predicate('b.block_time') }}
  {% else %}
  AND b.block_time >= TIMESTAMP '{{ project_start_date }}'
  {% endif %}
