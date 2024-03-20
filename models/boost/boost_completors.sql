{{
    config(
        schema='boost',
        alias='completors',
        materialized='table',
    )
}}

{% set network_to_fees_logic = {
    'arbitrum': 'sum(gas_used / 1e18 * effective_gas_price) arbitrum_fee_eth',
    'base': 'sum((gas_used * gas_price + l1_fee) / 1e18) base_fee_eth',
    'ethereum': 'sum(gas_used / 1e18 * gas_price) ethereum_fee_eth',
    'optimism': 'sum((gas_used * gas_price + l1_fee) / 1e18) optimism_fee_eth',
    'polygon': 'sum(gas_used / 1e18 * gas_price) polygon_fee_matic',
} %}

with boost_completors as (
    select 
        claimer_address,
        min(block_time) first_time_on_boost,
        min_by(boost_id, block_time) first_boost_completed,
        count(distinct claim_tx_hash) total_boost_completed,
        sum(reward_usd) as total_reward_earned_usd
    from {{ref("boost_claimed")}}
    group by 1
),
{% for network, fee_logic in network_to_fees_logic.items() %}
{{ network }}_transactions as (
    select 
        u.claimer_address,
        count(distinct t.hash) {{ network }}_tx_count,
        {{ fee_logic }},
        min(t.block_time) first_time_on_{{ network }}
    from boost_completors u
    left join {{source(network, 'transactions')}} t
    on u.claimer_address = t."from"
    group by 1
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
