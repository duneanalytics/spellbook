{{ config(
        schema='gmx_v2',
        alias = 'market_created',
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
    block_number,
    tx_hash,
    index,
    contract_address,
    event_name,
    msg_sender,
    topic1, 
    topic2,    
    market_token,
    index_token,
    long_token,
    short_token,
    salt,
    spot_only,
    market_token_symbol,
    market_token_decimals
FROM {{ ref('gmx_v2_' ~ chain ~ '_market_created') }}
{% if not loop.last %}
UNION ALL
{% endif %}
{%- endfor -%}