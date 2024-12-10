{{
    config(
        schema = 'fluid_ethereum',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

with 
decoded_events as (
    select
        '1' as version,
        t.evt_block_number as block_number,
        t.evt_block_time as block_time,
        t.to as taker,
        cast(null as varbinary) as maker,
        t.amountOut as token_bought_amount_raw,
        t.amountIn as token_sold_amount_raw,
        case when swap0to1 
            then p.borrow_token
            else p.supply_token
        end as token_bought_address,
        case when not(swap0to1)
            then p.borrow_token
            else p.supply_token
        end as token_sold_address,
        t.contract_address as project_contract_address,
        t.evt_tx_hash as tx_hash,
        t.evt_index
    from {{ source('fluid_ethereum', 'FluidDexT1_evt_Swap') }} t
        inner join {{ ref('fluid_ethereum_pools') }} p
            on t.contract_address = p.dex
    {% if is_incremental() %}
    where {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}
)


SELECT
    'ethereum' as blockchain,
    'fluid' as project,
    dexs.version,
    cast(date_trunc('month', dexs.block_time) as date) as block_month,
    cast(date_trunc('day', dexs.block_time) as date) as block_date,
    dexs.block_time,
    dexs.block_number,
    dexs.token_bought_amount_raw,
    dexs.token_sold_amount_raw,
    dexs.token_bought_address,
    dexs.token_sold_address,
    dexs.taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    dexs.evt_index
FROM decoded_events dexs 
