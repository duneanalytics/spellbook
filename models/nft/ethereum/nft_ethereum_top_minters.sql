{{ config(
    tags=['dunesql'],
    alias = alias('top_minters'),
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

{% if is_incremental() %}
WITH weekly_unique_minter_nft_address AS
(
    SELECT DISTINCT
        nft_contract_address,
        buyer
    FROM
        {{ ref('nft_mints') }}
    WHERE
        block_time >= date_trunc('day', now() - interval '7' day)
        AND blockchain = 'ethereum'
        AND currency_symbol IN ('WETH', 'ETH')
        AND amount_original IS NOT NULL
)
SELECT
    src.nft_contract_address,
    src.buyer as minter,
    SUM(src.amount_original) as eth_spent,
    COUNT(*) as no_minted,
    MAX(src.block_time) as last_updated
FROM
    {{ ref('nft_mints') }} src
INNER JOIN
    weekly_unique_minter_nft_address
    ON src.nft_contract_address = weekly_unique_minter_nft_address.nft_contract_address
    AND src.buyer = weekly_unique_minter_nft_address.buyer
WHERE
    src.blockchain = 'ethereum'
    AND src.currency_symbol IN ('WETH', 'ETH')
    AND src.amount_original IS NOT NULL
GROUP BY
    1, 2
{% else %}
SELECT
    nft_contract_address,
    buyer as minter,
    SUM(amount_original) as eth_spent,
    COUNT(*) as no_minted,
    MAX(block_time) as last_updated
FROM
    {{ ref('nft_mints') }}
WHERE
    blockchain = 'ethereum'
    AND currency_symbol IN ('WETH', 'ETH')
    AND amount_original IS NOT NULL
GROUP BY
    1, 2
{% endif %}