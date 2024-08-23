{{
  config(
    schema = 'gmx_v2_arbitrum',
    alias = 'order_executed',
    materialized = 'table',
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "project",
                                "gmx",
                                \'["ai_data_master","gmx-io"]\') }}'
    )
}}

{%- set event_name = 'OrderExecuted' -%}
{%- set blockchain_name = 'arbitrum' -%}
{%- set bytes32 = [
    ['key', 'key'],
] -%}
{%- set addresses = [
    ['account', 'account'],
] -%}
{%- set unsigned_integers = [
    ['secondaryOrderType', 'secondary_order_type'],
] -%}

SELECT
    -- Main Variables
    '{{ blockchain_name }}' as blockchain,
    block_time,
    block_date,
    block_number, 
    tx_hash,
    index,
    tx_index,
    tx_from,
    tx_to,    
    contract_address,
    varbinary_substring(topic2, 13, 20) as account,
    '{{ event_name }}' as event_name,

    -- Extracting Bytes32
    {% for var in bytes32 -%}
        {{ process_variable(var[0], var[1], 'bytes32', 64, 32) }},
    {% endfor -%} 

    -- Extracting Address
    {% for var in addresses -%}
        {{ process_variable(var[0], var[1], 'address', 52, 20) }},
    {% endfor -%}    
    
    -- Extracting Unsigned Integers
    {% for var in unsigned_integers -%}
        {{ process_variable(var[0], var[1], 'unsigned_integer', 64, 32) }}{%- if not loop.last -%},{%- endif %}
    {% endfor %}    

FROM
    {{ source(blockchain_name, 'logs') }}
WHERE
    contract_address = 0xc8ee91a54287db53897056e12d9819156d3822fb
    AND topic1 = keccak(to_utf8('{{ event_name }}'))