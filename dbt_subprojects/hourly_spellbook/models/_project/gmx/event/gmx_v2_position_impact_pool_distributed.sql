{{ config(
        schema='gmx_v2',
        alias = 'position_impact_pool_distributed',
        post_hook='{{ expose_spells(blockchains = \'["arbitrum", "avalanche_c"]\',
                                    spell_type = "project",
                                    spell_name = "gmx",
                                    contributors = \'["ai_data_master","gmx-io"]\') }}'
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
    market,
    distribution_amount,
    next_position_impact_pool_amount
FROM {{ ref('gmx_v2_' ~ chain ~ '_position_impact_pool_distributed') }}
{% if not loop.last %}
UNION ALL
{% endif %}
{%- endfor -%}