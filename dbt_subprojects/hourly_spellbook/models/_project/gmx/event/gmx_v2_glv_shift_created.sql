{{ config(
        schema='gmx_v2',
        alias = 'glv_shift_created',
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

    from_market,
    to_market,
    glv,
    market_token_amount,
    min_market_tokens,
    updated_at_time,
    "key"
    
FROM {{ ref('gmx_v2_' ~ chain ~ '_glv_shift_created') }}
{% if not loop.last %}
UNION ALL
{% endif %}
{%- endfor -%}


