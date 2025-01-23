{{ config(
        schema='gmx_v2',
        alias = 'swap_fees_collected',
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
    ui_fee_receiver,
    market,
    token,
    token_price,
    fee_receiver_amount,
    fee_amount_for_pool,
    amount_after_fees,
    ui_fee_receiver_factor,
    ui_fee_amount,
    trade_key,
    swap_fee_type,
    action_type
FROM {{ ref('gmx_v2_' ~ chain ~ '_swap_fees_collected') }}
{% if not loop.last %}
UNION ALL
{% endif %}
{%- endfor -%}