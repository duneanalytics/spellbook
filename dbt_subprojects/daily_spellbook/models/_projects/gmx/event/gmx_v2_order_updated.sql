{{ config(
        schema='gmx_v2',
        alias = 'order_updated',
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
    key,
    market,
    account,
    size_delta_usd,
    acceptable_price_raw,
    trigger_price_raw,
    min_output_amount_raw,
    updated_at_time

FROM {{ ref('gmx_v2_' ~ chain ~ '_order_updated') }}
{% if not loop.last %}
UNION ALL
{% endif %}
{%- endfor -%}