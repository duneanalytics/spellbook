{{ config(
        schema = 'tokens_ethereum',
        alias ='wsteth_exchange_rates',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(blockchains = \'["ethereum"]\',
                                    spell_type = "sector",
                                    spell_name = "tokens",
                                    contributors = \'["Henrystats"]\') }}'
)
}}

{% set token_address = '0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0' %}
{% set token_symbol = 'wstETH' %}
{% set underlying_token_address = '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2' %}
{% set underlying_token_symbol = 'WETH' %}

WITH 

get_rates as (
    SELECT 
        evt_block_time as block_time,
        evt_block_number as block_number,
        evt_index, 
        evt_tx_hash as tx_hash,
        (postTotalPooledEther/1e18)/(totalShares/1e18) as rate
    FROM 
    {{source('lido_ethereum','LegacyOracle_evt_PostTotalShares')}}
    WHERE evt_block_time <= date('2023-05-16')
    {% if is_incremental() %}
    AND {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    UNION ALL 

    SELECT 
        evt_block_time as block_time,
        evt_block_number as block_number,
        evt_index, 
        evt_tx_hash as tx_hash,
        (postTotalEther/1e18)/(postTotalShares/1e18) as rate
    FROM 
    {{source('lido_ethereum','steth_evt_TokenRebased')}}
    WHERE 1 = 1 
    {% if is_incremental() %}
    AND {{ incremental_predicate('evt_block_time') }}
    {% endif %}
)

SELECT 
    'ethereum' as blockchain,
    gr.block_time,
    gr.block_number,
    gr.tx_hash,
    gr.evt_index,
    'Lido' as project,
    {{token_address}} as token_address,
    '{{token_symbol}}' as token_symbol,
    'LST' as token_type,
    {{underlying_token_address}} as underlying_token_address,
    '{{underlying_token_symbol}}' as underlying_token_symbol,
    gr.rate, 
    gr.rate * p.price as price_usd 
FROM 
get_rates gr 
LEFT JOIN 
{{ source('prices', 'usd_with_native') }} p 
    ON p.contract_address = {{underlying_token_address}}
    AND p.blockchain = 'ethereum'
    AND p.minute = date_trunc('minute', gr.block_time)
    {% if is_incremental() %}
    AND {{ incremental_predicate('minute') }}
    {% endif %}