{{
    config(
        unique_key='boost_address',
        schema='boost',
    )
}}

{% set network_to_fees_logic = {
    'arbitrum': 'sum(effective_gas_price * gas_used / 1e18) arbitrum_fee_eth',
    'base': 'sum((gas_used * gas_price + l1_fee) / 1e18) base_fee_eth',
    'ethereum': 'sum(gas_price * gas_used / 1e18) ethereum_fee_eth',
    'optimism': 'sum((gas_used * gas_price + l1_fee) / 1e18) optimism_fee_eth',
    'polygon': 'sum(gas_price * gas_used / 1e18) polygon_fee_matic',
} %}

with boost_completors as (
    select 
        claimer_address,
        min(block_time) first_time_on_boost,
        min_by(quest_id, block_time) first_boost_completed,
        count(distinct tx_hash) total_boost_completed,
        sum(reward_usd) as total_reward_earned_usd
    from dune.boost_xyz.result_dashboard_quest_claims
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
    left join {{ network }}.transactions t
    on u.claimer_address = t."from"
    group by 1
)
{% if not loop.last %}, {% endif %}
{% endfor %}

select 
    *
from boost_completors u 
{% for network in network_to_fees_logic.keys() %}
join {{ network }}_transactions {{network}}
on u.claimer_address = {{network}}.claimer_address
{% endfor %}
