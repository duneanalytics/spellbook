{{
    config(
        schema = 'bungee',
        alias = 'bridges',
        materialized = 'view',
        post_hook = '{{ expose_spells(\'[
            "ethereum", "zkevm", "scroll", "blast", "linea", "mantle", "optimism",
            "gnosis", "arbitrum", "zksync", "base", "bnb", "polygon",
            "avalanche_c", "fantom"
        ]\',
        "project", "bungee", \'["lequangphu"]\') }}'
    )
}}

{% set chains = [
    'ethereum', 'zkevm', 'scroll', 'blast', 'linea', 'mantle', 'optimism',
    'gnosis', 'arbitrum', 'zksync', 'base', 'bnb', 'polygon',
    'avalanche_c', 'fantom'
] %}

with bungee_bridges as (
    {% for chain in chains %}
    select
        contract_address,
        evt_tx_hash,
        evt_index,
        evt_block_time,
        evt_block_number,
        amount,
        token,
        toChainId,
        bridgeName,
        sender,
        receiver,
        metadata,
        source_chain,
        transfer_id,
        amount_usd
    from {{ ref( 'bungee_' ~ chain ~ '_bridges' ) }}
    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
)

select *
from bungee_bridges