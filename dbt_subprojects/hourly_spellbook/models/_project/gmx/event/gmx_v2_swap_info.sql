{{ config(
        schema='gmx_v2',
        alias = 'swap_info',
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
    market,
    receiver,
    token_in,
    token_out,
    token_in_price,
    token_out_price,
    amount_in,
    amount_in_after_fees,
    amount_out,
    price_impact_usd,
    price_impact_amount,
    token_in_price_impact_amount,
    order_key
FROM {{ ref('gmx_v2_' ~ chain ~ '_swap_info') }}
{% if not loop.last %}
UNION ALL
{% endif %}
{%- endfor -%}
