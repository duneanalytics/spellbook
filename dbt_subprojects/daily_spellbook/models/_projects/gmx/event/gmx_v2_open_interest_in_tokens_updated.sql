{{ config(
        schema='gmx_v2',
        alias = 'open_interest_in_tokens_updated',
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
    tx_index,
    tx_from,
    tx_to,
    event_name,
    msg_sender,
    market,
    collateral_token,
    is_long,
    next_value,
    delta
FROM {{ ref('gmx_v2_' ~ chain ~ '_open_interest_in_tokens_updated') }}
{% if not loop.last %}
UNION ALL
{% endif %}
{%- endfor -%}

