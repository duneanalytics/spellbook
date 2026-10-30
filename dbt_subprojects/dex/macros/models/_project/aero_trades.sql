{% macro aero_trades(blockchain, cl_factory) %}
{# Keep factory lookups at full history. The namespace also contains older deployments. #}
{% set pool_types = [
    ('stable', 'StablePool', 'StablePoolFactory', '0x78589e876edcebdce72ee3436ab3c9ad3a7cef2d'),
    ('volatile', 'VolatilePool', 'VolatilePoolFactory', '0x8e6a9fcd1d638cc760ffba397605fe3355de76d2'),
    ('cl', 'CLPool', 'CLFactory', cl_factory)
] %}
with
{% for version, pool, factory, factory_address in pool_types %}
{{ version }}_pools as (
    select pool, token0, token1
    from {{ source('aero_multichain', factory ~ '_evt_PoolCreated') }}
    where chain = '{{ blockchain }}'
        and contract_address = {{ factory_address }}
),

{{ version }}_swaps as (
    select
        contract_address,
        evt_tx_hash,
        evt_index,
        evt_block_time,
        evt_block_number,
        {% if version == 'cl' %}
        amount0,
        amount1,
        recipient
        {% else %}
        -- Both tokens can have outputs (including input-token dust). Use net pool deltas
        -- so the V2 direction macro identifies the economically bought and sold tokens.
        if(amount0In > amount0Out, amount0In - amount0Out, uint256 '0') as amount0In,
        if(amount1In > amount1Out, amount1In - amount1Out, uint256 '0') as amount1In,
        if(amount0Out > amount0In, amount0Out - amount0In, uint256 '0') as amount0Out,
        if(amount1Out > amount1In, amount1Out - amount1In, uint256 '0') as amount1Out,
        "to"
        {% endif %}
    from {{ source('aero_multichain', pool ~ '_evt_Swap') }}
    where chain = '{{ blockchain }}'
        {% if is_incremental() -%}
        and {{ incremental_predicate('evt_block_time') }}
        {% else -%}
        and evt_block_time >= timestamp '2026-10-21'
        {% endif -%}
),

{{ version }}_trades as (
    {% if version == 'cl' %}
    {{ uniswap_compatible_v3_trades(
        blockchain = blockchain,
        project = 'aero',
        version = version,
        Pair_evt_Swap = version ~ '_swaps',
        Factory_evt_PoolCreated = version ~ '_pools',
        optional_columns = []
    ) }}
    {% else %}
    {{ uniswap_compatible_v2_trades(
        blockchain = blockchain,
        project = 'aero',
        version = version,
        Pair_evt_Swap = version ~ '_swaps',
        Factory_evt_PairCreated = version ~ '_pools',
        pair_column_name = 'pool'
    ) }}
    {% endif %}
){% if not loop.last %},{% endif %}
{% endfor %}

{% for version, pool, factory, factory_address in pool_types %}
select
    blockchain,
    project,
    version,
    block_month,
    block_date,
    block_time,
    block_number,
    token_bought_amount_raw,
    token_sold_amount_raw,
    token_bought_address,
    token_sold_address,
    taker,
    maker,
    project_contract_address,
    tx_hash,
    evt_index
from {{ version }}_trades
{% if not loop.last %}union all{% endif %}
{% endfor %}
{% endmacro %}
