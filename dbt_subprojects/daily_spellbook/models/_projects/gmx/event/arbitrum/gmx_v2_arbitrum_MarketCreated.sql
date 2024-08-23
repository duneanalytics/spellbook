{{
  config(
    schema = 'gmx_v2_arbitrum',
    alias = 'market_created',
    materialized = 'table',
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "project",
                                "gmx",
                                \'["ai_data_master","gmx-io"]\') }}'
    )
}}

{%- set event_name = 'MarketCreated' -%}
{%- set blockchain_name = 'arbitrum' -%}
{%- set addresses = [
    ['marketToken','market_token'],
    ['indexToken','index_token'],
    ['longToken','long_token'],
    ['shortToken','short_token']
] -%}
{%- set bytes32 = [
    ['salt','salt']
] %}

WITH market_created_events AS (
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

        -- Extracting Addresses
        {% for var in addresses -%}
            {{ process_variable(var[0], var[1], 'address', 52, 20) }},
        {% endfor -%}      
    
        -- Extracting Bytes32
        {% for var in bytes32 -%}
            {{ process_variable(var[0], var[1], 'bytes32', 64, 32) }}{%- if not loop.last -%},{%- endif %}
        {% endfor %}        

    FROM
      {{ source(blockchain_name, 'logs') }}
    WHERE
      contract_address = 0xc8ee91a54287db53897056e12d9819156d3822fb
      AND topic1 = keccak(to_utf8('{{ event_name }}'))
)

SELECT 
    MCE.*
    , CASE 
        WHEN index_token = 0x0000000000000000000000000000000000000000 THEN true
        ELSE false
    END AS spot_only
    ,'GM' AS market_token_symbol
    , 18 AS market_token_decimals  
FROM market_created_events AS MCE