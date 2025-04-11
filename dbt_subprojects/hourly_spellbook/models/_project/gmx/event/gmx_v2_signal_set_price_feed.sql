{{ config(
        schema='gmx_v2',
        alias = 'signal_set_price_feed',
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
    token,
    price_feed,
    price_feed_multiplier,
    price_feed_heartbeat_duration,
    stable_price
FROM {{ ref('gmx_v2_' ~ chain ~ '_signal_set_price_feed') }}
{% if not loop.last %}
UNION ALL
{% endif %}
{%- endfor -%}
