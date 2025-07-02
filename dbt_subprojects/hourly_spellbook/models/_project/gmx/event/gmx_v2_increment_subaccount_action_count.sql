{{ config(
        schema='gmx_v2',
        alias = 'increment_subaccount_action_count',
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
    subaccount,
    next_value,
    action_type
FROM {{ ref('gmx_v2_' ~ chain ~ '_increment_subaccount_action_count') }}
{% if not loop.last %}
UNION ALL
{% endif %}
{%- endfor -%}

