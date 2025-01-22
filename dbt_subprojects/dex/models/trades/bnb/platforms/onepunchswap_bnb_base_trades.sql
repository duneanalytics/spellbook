{{
    config(
        schema = 'onepunchswap_bnb',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{%
    set config_sources = [
        {'version': 'normal', 'source': source('onepunch_normal_bnb', 'DexLiquidityProvider_evt_QuoteAccepted')},
        {'version': 'quick', 'source': source('onepunch_quick_bnb', 'QuickLiquidityProvider_evt_QuoteAccepted')},
    ]
%}

{% set burn = '0x0000000000000000000000000000000000000000' %}
{% set wbnb = '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c' %}

with dexs as (
    {% for src in config_sources %}
        select
            '{{ src["version"] }}' as version,
            t.evt_block_number as block_number,
            t.evt_block_time as block_time,
            cast(json_extract_scalar(t.quoteInfo, '$.toAmount') as uint256) as token_bought_amount_raw,
            cast(json_extract_scalar(t.quoteInfo, '$.fromAmount') as uint256) as token_sold_amount_raw,
            case
                when from_hex(json_extract_scalar(t.quoteInfo, '$.toAsset')) = {{ burn }} then {{ wbnb }}
                else from_hex(json_extract_scalar(t.quoteInfo, '$.toAsset'))
            end as token_bought_address,
            case
                when from_hex(json_extract_scalar(t.quoteInfo, '$.fromAsset')) = {{ burn }} then {{ wbnb }}
                else from_hex(json_extract_scalar(t.quoteInfo, '$.fromAsset'))
            end as token_sold_address,
            t.evt_tx_hash as tx_hash,
            t.evt_index,
            t.user as taker,
            cast(null as varbinary) as maker,
            t.contract_address as project_contract_address
        from {{ src["source"] }} t
        {% if is_incremental() %}
        where {{ incremental_predicate('t.evt_block_time') }}
        {% endif %}

        {% if not loop.last %}
        union all
        {% endif %}
    {% endfor%}
)

select
    'bnb' as blockchain,
    'onepunchswap' as project,
    dexs.version,
    CAST(date_trunc('month', dexs.block_time) as date) as block_month,
    CAST(date_trunc('day', dexs.block_time) as date) as block_date,
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
from dexs
