{{
    config(
        schema='boost',
        alias='completors',
        materialized='incremental',
        file_format='delta',
        incremental_strategy='merge',
        unique_key=['claimer_address']
    )
}}

{% set network_to_fees_logic = {
    'arbitrum': 'sum(t.gas_used / 1e18 * t.effective_gas_price) as arbitrum_fee_eth',
    'base': 'sum((t.gas_used * t.gas_price + t.l1_fee) / 1e18) as base_fee_eth',
    'ethereum': 'sum(t.gas_used / 1e18 * t.gas_price) as ethereum_fee_eth',
    'optimism': 'sum((t.gas_used * t.gas_price + t.l1_fee) / 1e18) as optimism_fee_eth',
    'polygon': 'sum(t.gas_used / 1e18 * t.gas_price) as polygon_fee_matic',
    'zora': 'sum(t.gas_used * t.gas_price) / 1e18 as zora_fee_eth',
} %}

with
{% if is_incremental() %}
existing_completors as (
    select
        c.claimer_address
    from {{ this }} c
),

changed_claimers as (
    select distinct
        bc.claimer_address
    from {{ ref("boost_claimed") }} bc
    where {{ incremental_predicate('bc.block_time') }}

    union

    {% for network in network_to_fees_logic.keys() %}
    select distinct
        t."from" as claimer_address
    from {{ source(network, 'transactions') }} t
    inner join existing_completors ec
        on ec.claimer_address = t."from"
    where {{ incremental_predicate('t.block_time') }}
    {% if not loop.last %}
    union
    {% endif %}
    {% endfor %}
),
{% endif %}

boost_completors as (
    select
        bc.claimer_address,
        min(bc.block_time) as first_time_on_boost,
        min_by(bc.boost_id, bc.block_time) as first_boost_completed,
        count(distinct bc.claim_tx_hash) as total_boost_completed,
        sum(bc.reward_usd) as total_reward_earned_usd
    from {{ ref("boost_claimed") }} bc
    {% if is_incremental() %}
    inner join changed_claimers cc
        on bc.claimer_address = cc.claimer_address
    {% endif %}
    group by bc.claimer_address
),
{% for network, fee_logic in network_to_fees_logic.items() %}
{{ network }}_transactions as (
    select
        u.claimer_address,
        count(distinct t.hash) as {{ network }}_tx_count,
        {{ fee_logic }},
        min(t.block_time) as first_time_on_{{ network }}
    from boost_completors u
    left join {{ source(network, 'transactions') }} t
        on u.claimer_address = t."from"
    group by u.claimer_address
){% if not loop.last %}, {% endif %}
{% endfor %}

select
    u.*,
    {% for network in network_to_fees_logic.keys() %}
    {{ network }}_tx_count,
    {% if network == 'polygon' %}
    polygon_fee_matic,
    {% else %}
    {{ network }}_fee_eth,
    {% endif %}
    first_time_on_{{ network }} {% if not loop.last %}, {% endif %}
    {% endfor %}
from boost_completors u
{% for network in network_to_fees_logic.keys() %}
join {{ network }}_transactions {{ network }}
    on u.claimer_address = {{ network }}.claimer_address
{% endfor %}
