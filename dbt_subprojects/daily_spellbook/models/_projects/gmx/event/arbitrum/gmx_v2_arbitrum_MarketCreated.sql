{{
  config(
    schema = 'gmx_v2_arbitrum',
    alias = 'market_created',
    materialized = 'table'
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
        '{{ event_name }}' as eventName,

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

-- Filter relevant tokens
, relevant_erc20_tokens AS (
    SELECT 
        contract_address, 
        symbol,
        decimals
    FROM 
        {{ source('tokens', 'erc20') }}
    WHERE 
        blockchain = '{{ blockchain_name }}'
        AND contract_address IN (
            SELECT indexToken
            FROM market_created_events
            UNION DISTINCT
            SELECT longToken
            FROM market_created_events
            UNION DISTINCT
            SELECT shortToken
            FROM market_created_events
        )
)

SELECT 
    GCA.*
    ,'GM' AS marketTokenSymbol
    , 18 AS marketTokenDecimals
    , ERC20_IT.symbol AS indexTokenSymbol
    , ERC20_IT.decimals AS indexTokenDecimals  
    , ERC20_LT.symbol AS longTokenSymbol
    , ERC20_LT.decimals AS longTokenDecimals  
    , ERC20_ST.symbol AS shortTokenSymbol
    , ERC20_ST.decimals AS shortTokenDecimals      
FROM market_created_events AS GCA
LEFT JOIN relevant_erc20_tokens AS ERC20_IT
    ON ERC20_IT.contract_address = GCA.indexToken
LEFT JOIN relevant_erc20_tokens AS ERC20_LT
    ON ERC20_LT.contract_address = GCA.longToken 
LEFT JOIN relevant_erc20_tokens AS ERC20_ST
    ON ERC20_ST.contract_address = GCA.shortToken 
