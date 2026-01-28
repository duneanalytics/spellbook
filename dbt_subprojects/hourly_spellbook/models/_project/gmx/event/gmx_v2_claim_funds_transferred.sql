{{ config(
        schema='gmx_v2',
        alias = 'claim_funds_transferred'
        , post_hook='{{ hide_spells() }}'
        )
}}

{%- set chains = [
    'arbitrum',
    'avalanche_c',
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

    from_account,
    to_account,
    token,
    distribution_id,
    amount,
    next_amount
FROM {{ ref('gmx_v2_' ~ chain ~ '_claim_funds_transferred') }}
{% if not loop.last %}
UNION ALL
{% endif %}
{%- endfor -%}
