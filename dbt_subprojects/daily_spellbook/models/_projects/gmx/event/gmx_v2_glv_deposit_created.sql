{{ config(
        schema='gmx_v2',
        alias = 'glv_deposit_created',
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
    receiver,
    callback_contract,
    market,
    glv,
    initial_long_token,
    initial_short_token,
    ui_fee_receiver,
    long_token_swap_path,
    short_token_swap_path,
    initial_long_token_amount,
    initial_short_token_amount,
    min_glv_tokens,
    updated_at_time,
    execution_fee,
    callback_gas_limit,
    market_token_amount,
    should_unwrap_native_token,
    is_market_token_deposit,
    "key"

FROM {{ ref('gmx_v2_' ~ chain ~ '_glv_deposit_created') }}
{% if not loop.last %}
UNION ALL
{% endif %}
{%- endfor -%}

