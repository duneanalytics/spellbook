{{ config(
    alias = 'top_minters',
    materialized='incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['minter', 'nft_contract_address'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "nft",
                                \'["Henrystats"]\') }}'
    )
}}

WITH 

src_data as (
    {% if not is_incremental() %}
    SELECT 
        nft_contract_address,
        buyer as minter, 
        SUM(amount_original) as eth_spent, 
        COUNT(*) as no_minted,
        NOW() - Interval '5 Minutes' as last_updated
    FROM 
    {{ ref('nft_mints') }}
    WHERE blockchain = 'ethereum'
    AND currency_symbol IN ('WETH', 'ETH')
    AND amount_original IS NOT NULL
    AND block_time < NOW() - Interval '5 Minutes'
    GROUP BY 1, 2, 5
    {% endif %}
    -- incremental run
    {% if is_incremental() %}
    SELECT 
        nft_contract_address,
        buyer as minter, 
        SUM(amount_original) as eth_spent, 
        COUNT(*) as no_minted,
        NOW() - Interval '5 Minutes' as last_updated
    FROM 
    {{ ref('nft_mints') }}
    WHERE block_time >= (SELECT MAX(last_updated) FROM {{this}})
    AND blockchain = 'ethereum'
    AND currency_symbol IN ('WETH', 'ETH')
    AND amount_original IS NOT NULL
    AND block_time < NOW() - Interval '5 Minutes'
    GROUP BY 1, 2, 5 

    UNION ALL 

    SELECT 
        nft_contract_address,
        minter,
        eth_spent,
        no_minted,
        last_updated
    FROM 
    {{this}}
    {% endif %}
), 

combined as (
    SELECT 
        nft_contract_address, 
        minter, 
        SUM(eth_spent) as eth_spent, 
        SUM(no_minted) as no_minted, 
        last_updated
    FROM 
    src_data
    GROUP BY 1, 2, 5
)

SELECT 
    ROW_NUMBER() OVER (PARTITION BY nft_contract_address ORDER BY no_minted DESC) as rank_,
    * 
FROM 
combined