{{ config(
	    tags=['legacy'],
        materialized = 'view',
        alias = alias('erc1155_latest', legacy_model=True),
        post_hook='{{ expose_spells_hide_trino(\'["ethereum"]\',
                                            "sector",
                                            "balances",
                                            \'["soispoke"]\') }}'
        )
}}
SELECT
    'ethereum' as blockchain,
    b.wallet_address,
    b.token_address,
    b.tokenId,
    b.amount,
    nft_tokens.name as collection,
    b.updated_at
FROM {{ ref('transfers_ethereum_erc1155_rolling_day_legacy') }} b
LEFT JOIN {{ ref('tokens_nft_legacy') }} nft_tokens ON nft_tokens.contract_address = b.token_address
AND nft_tokens.blockchain = 'ethereum'
WHERE recency_index = 1
AND amount > 0