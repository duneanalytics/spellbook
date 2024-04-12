{% macro all_evm_chains() %}
   {{ return([
        'ethereum', 
        'optimism', 
        'arbitrum', 
        'avalanche_c', 
        'polygon', 
        'bnb', 
        'gnosis', 
        'fantom', 
        'celo', 
        'base', 
        'zksync', 
        'zora',
        'scroll'
    ]) }}
{% endmacro %}

{% macro all_evm_testnet_chains() %}
   {{ return(['goerli']) }}
{% endmacro %}

{% macro all_evm_mainnets_testnets_chains() %}
    {% set mainnet_chains = all_evm_chains() %}
    {% set testnet_chains = all_evm_testnet_chains() %}
    {% set mainnets_testnets_chains = [] %}

    {% for chain in mainnet_chains %}
        {% do mainnets_testnets_chains.append(chain) %}
    {% endfor %}

    {% for chain in testnet_chains %}
        {% do mainnets_testnets_chains.append(chain) %}
    {% endfor %}

    {{ return(mainnets_testnets_chains) }}
{% endmacro %}
