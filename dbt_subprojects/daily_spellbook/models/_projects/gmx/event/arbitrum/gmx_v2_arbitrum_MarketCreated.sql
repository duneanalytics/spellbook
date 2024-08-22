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

-- get tokens metadata
, relevant_erc20_tokens AS (
    SELECT 
        contract_address, 
        symbol,
        decimals
    FROM 
        {{ ref('gmx_v2_arbitrum_erc20') }}
)

SELECT 
    MCE.*
    , CASE 
        WHEN MCE.index_token = 0x0000000000000000000000000000000000000000 THEN true
        ELSE false
    END AS spot_only
    ,'GM' AS market_token_symbol
    , 18 AS market_token_decimals
    , ERC20_IT.symbol AS index_token_symbol
    , ERC20_IT.decimals AS index_token_decimals  
    , ERC20_LT.symbol AS long_token_symbol
    , ERC20_LT.decimals AS long_token_decimals  
    , ERC20_ST.symbol AS short_token_symbol
    , ERC20_ST.decimals AS short_token_decimals    
FROM market_created_events AS MCE
LEFT JOIN relevant_erc20_tokens AS ERC20_IT
    ON ERC20_IT.contract_address = MCE.index_token
LEFT JOIN relevant_erc20_tokens AS ERC20_LT
    ON ERC20_LT.contract_address = MCE.long_token 
LEFT JOIN relevant_erc20_tokens AS ERC20_ST
    ON ERC20_ST.contract_address = MCE.short_token
