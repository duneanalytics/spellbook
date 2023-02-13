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
        {% if is_incremental() %} SUM(src.amount_original + prev.eth_spent) as eth_spent, {% else %} SUM(src.amount_original) as eth_spent, {% endif %}
        {% if is_incremental() %} COUNT(src.nft_contract_address + prev.no_minted) as no_minted, {% else %} COUNT(src.nft_contract_address) as no_minted, {% endif %}
        MAX(src.block_time) as last_updated
    FROM
        {{ ref('nft_mints') }} src
    {% if is_incremental() %}
    LEFT JOIN
        {{this}} prev
        ON prev.minter = src.minter 
        AND prev.nft_contract_address = src.nft_contract_address
    {% endif %}
    WHERE
        1 = 1
        {% if is_incremental() %}
        AND block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        AND blockchain = 'ethereum'
        AND currency_symbol IN ('WETH', 'ETH')
        AND amount_original IS NOT NULL
    GROUP BY 1, 2
), 
combined as
(
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
    * 
FROM 
    combined
;