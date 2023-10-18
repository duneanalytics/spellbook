{{ config(
	    tags=['legacy'],
        materialized = 'view',
        alias = alias('erc721_latest', legacy_model=True),
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                            "sector",
                                            "balances",
                                            \'["hildobby","soispoke","dot2dotseurat"]\') }}'
        )
}}
SELECT
    'ethereum' as blockchain,
    b.wallet_address,
    b.token_address,
    b.tokenId,
    nft_tokens.name as collection,
    b.updated_at
FROM {{ ref('transfers_ethereum_erc721_rolling_day_legacy') }} b
LEFT JOIN {{ ref('tokens_nft_legacy') }} nft_tokens ON nft_tokens.contract_address = b.token_address
AND nft_tokens.blockchain = 'ethereum'
LEFT JOIN {{ ref('balances_ethereum_erc721_noncompliant_legacy') }}  as nc
    ON b.token_address = nc.token_address
WHERE recency_index = 1
AND amount = 1
AND nc.token_address IS NULL 