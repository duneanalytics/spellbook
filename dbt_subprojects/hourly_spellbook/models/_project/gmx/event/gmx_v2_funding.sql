{{ config(
        schema='gmx_v2',
        alias = 'funding'
        , post_hook='{{ hide_spells() }}'
        )
}}

{%- set chains = [
    'arbitrum',
    'avalanche_c',
    'megaeth'
] -%}

{%- for chain in chains -%}
SELECT 
    blockchain,
    block_time,
    block_date,
    block_number,
    tx_hash,
    index,
    contract_address,
    tx_from,
    tx_to,
    event_name,
    msg_sender,
    market,
    market_name,
    funding_factor_per_second_raw,
    funding_factor_per_second
FROM {{ ref('gmx_v2_' ~ chain ~ '_funding') }}
{% if not loop.last %}
UNION ALL
{% endif %}
{%- endfor -%}
