{{
    config(
        schema = 'zigzag_arbitrum',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

with dexs as (
    select
      call_block_time as block_time,
      from_hex(json_extract_scalar(zzmo.makerOrder, '$.sellToken')) as token_sold_address,
      from_hex(json_extract_scalar(zzmo.makerOrder, '$.buyToken')) as token_bought_address,
      cast(json_extract_scalar(zzmo.output_matchedFillResults, '$.takerSellFilledAmount') as UINT256) as token_bought_amount_raw,
      cast(json_extract_scalar(zzmo.output_matchedFillResults, '$.makerSellFilledAmount') as UINT256) as token_sold_amount_raw,
      from_hex(json_extract_scalar(zzmo.makerOrder, '$.user')) as maker,
      from_hex(json_extract_scalar(zzmo.takerOrder, '$.user')) as taker,
      call_tx_hash as tx_hash,
      call_block_number as block_number,
      row_number() OVER(PARTITION BY call_tx_hash ORDER BY zzmo.makerOrder) AS evt_index, --prevent duplicates
      contract_address as project_contract_address
    from
    {{ source('zigzag_test_v6_arbitrum', 'zigzag_settelment_call_matchOrders') }} zzmo
    where
        call_success = true
        {% if is_incremental() %}
        AND {{ incremental_predicate('zzmo.call_block_time') }}
        {% endif %}
)

SELECT
    'arbitrum' AS blockchain
    , 'zigzag' AS project
    , '1' AS version
    , CAST(date_trunc('month', dexs.block_time) AS date) AS block_month
    , CAST(date_trunc('day', dexs.block_time) AS date) AS block_date
    , dexs.block_time
    , dexs.block_number
    , dexs.token_bought_amount_raw
    , dexs.token_sold_amount_raw
    , dexs.token_bought_address
    , dexs.token_sold_address
    , dexs.taker
    , dexs.maker
    , dexs.project_contract_address
    , dexs.tx_hash
    , dexs.evt_index
FROM
    dexs