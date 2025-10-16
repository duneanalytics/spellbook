{{
    config(
        schema = 'oneinch',
        alias = 'intent_accounts',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'address'],
    )
}}

{% set meta = oneinch_meta_cfg_macro() %}
{% set date_from = meta['streams']['lo']['start']['fusion'] %}
{%- set legacy = 'Settlement' -%}



with

legacy as (
    select
        blockchain
        , block_date
        , block_number
        , tx_hash
        , call_to as call_from
        , min_by(call_from, call_trace_address) as executor_address
    from (
        {% for blockchain in oneinch_lo_cfg_contracts_macro()[legacy]['blockchains'] %}
            select *
            from {{ ref('oneinch_' + blockchain + '_lo_raw_calls') }}
            where true
                and auxiliary
                and contract_name = '{{ legacy }}'
                and block_date >= timestamp '{{ date_from }}' -- it is only needed for simple/easy dates
                {% if is_incremental() %}and {{ incremental_predicate('block_time') }}{% endif %}
            {% if not loop.last %}union all{% endif %}
        {% endfor %}
    )
    group by 1, 2, 3, 4, 5
)

, intents as (
    select
        blockchain
        , tx_from
        , coalesce(executor_address, call_from) as executor_address
        , min(block_time) as first_time
        , max(block_time) as last_time
    from {{ ref('oneinch_lo') }}
    left join legacy using(blockchain, block_date, block_number, tx_hash, call_from)
    where true
        and block_date >= timestamp '{{ date_from }}' -- it is only needed for simple/easy dates
        {% if is_incremental() %}and {{ incremental_predicate('block_time') }}{% endif %}
    group by 1, 2, 3
)

-- output --

select
    blockchain
    , tx_from as account_address
    , resolver_address
    , resolver_name
    , count(distinct executor_address) as executors
    , min(first_time) as first_time
    , max(last_time) as last_time
from intents
join {{ ref('oneinch_intent_executors') }} using(blockchain, executor_address)
group by 1, 2, 3, 4