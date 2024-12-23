{{ config(
        schema='gmx_v2',
        alias = 'glv_created',
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

    glv_token,
    long_token,
    short_token,
    salt,
    glv_type,
    market_token_symbol,
    market_token_decimals

FROM {{ ref('gmx_v2_' ~ chain ~ '_glv_created') }}
{% if not loop.last %}
UNION ALL
{% endif %}
{%- endfor -%}


