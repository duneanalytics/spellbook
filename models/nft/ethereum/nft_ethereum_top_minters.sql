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

WITH src_data as
(
    SELECT 
        src.nft_contract_address,
        src.buyer as minter,
        SUM(src.amount_original) as eth_spent,
        COUNT(1) as no_minted,
        MAX(src.block_time) as last_updated
    FROM
        {{ ref('nft_mints') }} src
    WHERE
        1 = 1
        {% if is_incremental() %}
        AND block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        AND blockchain = 'ethereum'
        AND currency_symbol IN ('WETH', 'ETH')
        AND amount_original IS NOT NULL
    GROUP BY 1, 2
)

SELECT 
    * 
FROM 
    src_data
;