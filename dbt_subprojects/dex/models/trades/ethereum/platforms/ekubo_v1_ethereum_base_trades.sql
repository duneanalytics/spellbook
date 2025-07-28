{{
    config(
        schema = 'ekubo_v1_ethereum',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set project_deployed_on = '2025-03-14 23:49' %}
{% set contract_addresses = [
    '0xe0e0e08a6a4b9dc7bd67bcb7aade5cf48157d444'
] %}


/* mev transaction (0xbd6f5765d7bcf3cb3a4d6ae71225c3a9bb50354d5544f1e8aef95aad2f6215aa), so here the user removed liquidity + collected fees+ swapped, giving the total token0 as 1,86,73,09,51,582 */
/* tough one to verify 0x802b84d9900f577d3b535b982717c3c2acf8d45d45b0b0629cceb810291c49c6 */
with trace_trades as 
(
    SELECT
        'ethereum' AS blockchain
        , 'ekubo' AS project
        , '1' AS version
        , call_block_time as block_time
        , call_block_number AS block_number
        , CASE 
            WHEN isToken1 = TRUE THEN ABS(output_delta0)
            WHEN isToken1 = FALSE THEN ABS(output_delta1)
        END AS token_bought_amount_raw
        , CASE
            WHEN isToken1 = TRUE THEN ABS(output_delta1)
            WHEN isToken1 = FALSE THEN ABS(output_delta0)
        END AS token_sold_amount_raw
        , CASE
            WHEN isToken1 = TRUE THEN cast(JSON_EXTRACT(poolKey, '$.token0') as varchar)
            WHEN isToken1 = FALSE THEN cast(JSON_EXTRACT(poolKey, '$.token1') as varchar)
        END AS token_bought_address
        , CASE
            WHEN isToken1 = TRUE THEN cast(JSON_EXTRACT(poolKey, '$.token1') as varchar)
            WHEN isToken1 = FALSE THEN cast(JSON_EXTRACT(poolKey, '$.token0') as varchar)
        END AS token_sold_address
        , contract_address AS project_contract_address
        , call_tx_hash AS tx_hash
        , call_tx_from AS taker
        , cast(null as varbinary) AS maker
        -- , cast(JSON_EXTRACT(poolKey, '$.config') as varchar) as config  
        , row_number() over (partition by call_tx_hash order by call_trace_address asc) as swap_number

    FROM {{ source('ekubo_ethereum', 'ekubo_core_call_swap_611415377') }}
    WHERE 1=1
        AND call_success
        {% if is_incremental() %}
        and {{ incremental_predicate('call_block_time') }}
        {% endif %}
)
, evt_trades as 
(
    select
        block_time
        , tx_hash
        , index as evt_index
        , tx_index 
        -- , substr(data,21,32)  as pool_id 
        , row_number() over (partition by tx_hash order by tx_index asc) as swap_number
    from {{ source('ethereum', 'logs') }}
    where 1=1
        and cast(contract_address as varchar) in 
        (
            {% for contract_address in contract_addresses %}
                '{{ contract_address }}'
            {% endfor %}
        )
        and block_time >= timestamp '{{ project_deployed_on }}'
        {% if is_incremental() %}
        and {{ incremental_predicate('block_time') }}
        {% endif %}
        and topic0 is null 
)
select 
    cast(tt.blockchain as varchar) as blockchain
    , cast(tt.project as varchar) as project
    , cast(tt.version as varchar) as version
    , date_trunc( 'month', tt.block_time) as block_month
    , date_trunc( 'day', tt.block_time) as block_date
    , tt.block_time as block_time
    , cast(tt.block_number as uint256) as block_number
    , cast(tt.token_bought_amount_raw as uint256) as token_bought_amount_raw
    , cast(tt.token_sold_amount_raw as uint256) as token_sold_amount_raw
    , token_bought_address
    , token_sold_address
    , cast(tt.taker as varbinary) as taker
    , cast(tt.maker as varbinary) as maker
    , project_contract_address
    , tt.tx_hash
    , cast(coalesce(et.evt_index, tt.swap_number) as uint256) as evt_index 


    
from trace_trades tt
left join evt_trades et 
on (
    tt.tx_hash=et.tx_hash 
    and tt.block_time=et.block_time 
    and tt.swap_number=et.swap_number
   )
where 1=1      

