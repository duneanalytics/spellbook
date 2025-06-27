{{ config(
        schema='gmx_v2',
        alias = 'grant_role',
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
    account,
    role_key
FROM {{ ref('gmx_v2_' ~ chain ~ '_grant_role') }}
{% if not loop.last %}
UNION ALL
{% endif %}
{%- endfor -%}
