{{ config(
        alias = 'erc721_latest',
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['token_address', 'tokenId'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                            "sector",
                                            "balances",
                                            \'["hildobby","soispoke","dot2dotseurat","denz-e"]\') }}'
        )
}}

WITH

    erc721_transfers AS (
        SELECT
            tr.blockchain,
            tr.to AS wallet_address,
            tr.contract_address AS token_address,
            tr.token_id AS tokenId,
            nft_tokens.name AS collection,
            cast(current_timestamp AS timestamp) AS updated_at,
            row_number() over (partition by tr.contract_address, tr.token_id order by tr.block_time desc, tr.evt_index desc) as recency_index
        FROM {{ ref('nft_transfers') }} tr
        LEFT JOIN {{ ref('tokens_nft') }} nft_tokens ON nft_tokens.contract_address = tr.contract_address
            AND nft_tokens.blockchain = 'ethereum'
        LEFT JOIN {{ ref('balances_ethereum_erc721_noncompliant') }} nc ON nc.token_address = tr.contract_address
        WHERE TRUE
            AND tr.blockchain = 'ethereum'
            AND tr.token_standard = 'erc721'
            AND nc.token_address IS NULL
            {% if is_incremental() %}
            AND {{incremental_predicate('tr.block_time')}}
            {% endif %}

    )

    SELECT
        blockchain,
        wallet_address,
        token_address,
        tokenId,
        collection,
        updated_at
    FROM erc721_transfers
    WHERE recency_index = 1