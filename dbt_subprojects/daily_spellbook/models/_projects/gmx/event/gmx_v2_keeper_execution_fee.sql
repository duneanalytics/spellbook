{{ config(
        schema='gmx_v2',
        alias = 'keeper_execution_fee',
        post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c"]\',
                                    "project",
                                    "gmx",
                                    \'["ai_data_master","gmx-io"]\') }}'
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
    keeper,
    execution_fee_amount
FROM {{ ref('gmx_v2_' ~ chain ~ '_keeper_execution_fee') }}
{% if not loop.last %}
UNION ALL
{% endif %}
{%- endfor -%}

