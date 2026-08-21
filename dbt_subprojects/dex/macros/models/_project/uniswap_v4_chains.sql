{% macro uniswap_v4_chains() %}
{#- Chain list for the leftover cross-chain aggregator-hook registry
    (uniswap_v4_aggregator_hooks) and the cross-chain aggregator union
    (uniswap_v4_aggregator_base_trades). Per-chain swaps ref
    uniswap_v4_{chain}_aggregator_hooks, not this list, so adding a chain
    here does not rebuild dex_{other_chain}.base_trades. Onboarding still
    needs the per-chain models. -#}
{{ return(['arbitrum','avalanche_c','base','blast','bnb','celo','ethereum','ink','monad','optimism','polygon','tempo','unichain','worldchain','xlayer','zora']) }}
{% endmacro %}
