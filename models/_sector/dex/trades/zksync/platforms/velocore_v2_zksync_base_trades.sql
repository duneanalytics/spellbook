{{
    config(
        schema = 'velocore_v2_zksync',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

--docs: https://docs.velocore.xyz/technical-docs/events-and-chart-integration#swap

{% set dex_eth = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' %}
{% set native_eth = '0x000000000000000000000000000000000000800A' %}

with dexs as (
    select
        t.evt_block_time as block_time,
        t.evt_block_number as block_number,
        if(t.delta[1] < 0, 'buy', 'sell') as trade_type,
        if(t.delta[1] < 0, t.user, t.pool) as taker,
        if(t.delta[1] < 0, t.pool, t.user) as maker,
        if(t.delta[1] < 0, t.delta[1], t.delta[2]) as token_bought_amount_raw,
        if(t.delta[1] < 0, t.delta[2], t.delta[1]) as token_sold_amount_raw,
        if(
            t.delta[1] < 0,
            bytearray_substring(t.tokenRef[1], 13, 20),
            bytearray_substring(t.tokenRef[2], 13, 20)
        ) as token_bought_address,
        if(
            t.delta[1] < 0,
            bytearray_substring(t.tokenRef[2], 13, 20),
            bytearray_substring(t.tokenRef[1], 13, 20)
        ) as token_sold_address,
        t.contract_address as project_contract_address, -- t.pool ???
        t.evt_tx_hash as tx_hash,
        t.evt_index
    from {{ source('velocore_v2_zksync', 'VaultStorage_evt_Swap') }} as t
    where 1=1
        -- velocore v2 allows for swaps between more than 2 tokens,
        -- including sudo-swap style NFT pools (ERC721 & ERC1155)
        -- but this design doesn't fit into current dex.trades model
        -- therefore, only include 2-token swaps comprised of ETH and/or ERC20:
        and cardinality(t.tokenRef) = 2
        and bytearray_substring(t.tokenRef[1], 1, 1) in (0xee, 0x00)
        and bytearray_substring(t.tokenRef[2], 1, 1) in (0xee, 0x00)
        {% if is_incremental() %}
        and {{ incremental_predicate('t.evt_block_time') }}
        {% endif %}
)

select
    'zksync' as blockchain,
    'velocore' as project,
    '2' as version,
    cast(date_trunc('month', dexs.block_time) as date) as block_month,
    cast(date_trunc('day', dexs.block_time) as date) as block_date,
    dexs.block_time,
    dexs.block_number,
    abs(dexs.token_bought_amount_raw) as token_bought_amount_raw,
    dexs.token_sold_amount_raw,
    if(dexs.token_bought_address = {{ dex_eth}}, {{ native_eth }}, dexs.token_bought_address) as token_bought_address,
    if(dexs.token_sold_address = {{ dex_eth }}, {{ native_eth }}, dexs.token_sold_address) as token_sold_address,
    dexs.taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    dexs.evt_index
from dexs
