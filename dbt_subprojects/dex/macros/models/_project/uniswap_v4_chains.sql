{% macro uniswap_v4_chains() %}
{#- single owner of the Uniswap V4 chain list. Used by the aggregator-hook
    registry (uniswap_v4_aggregator_hooks) and the cross-chain aggregator
    union (uniswap_v4_aggregator_base_trades) — when onboarding a new V4
    chain, adding it here covers both, alongside the per-chain models. -#}
{{ return(['arbitrum','avalanche_c','base','blast','bnb','celo','ethereum','ink','monad','optimism','polygon','tempo','unichain','worldchain','zora']) }}
{% endmacro %}
