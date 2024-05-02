{{ config(
        
        schema = 'nft_solana',
        alias='wash_trades',
        partition_by=['block_month'],
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = ['DBT_INTERNAL_DEST.block_time >= date_trunc(\'day\', now() - interval \'7\' day)'],
        unique_key = ['unique_trade_id']
)
}}

WITH filter_1 AS (
    SELECT tx_id
    , inner_instruction_index
    , outer_instruction_index
    , true AS same_buyer_seller
    FROM {{ ref('nft_solana_trades') }} nftt
    WHERE (tx_id, inner_instruction_index, outer_instruction_index) IS NOT NULL
    AND nftt.buyer=nftt.seller
    {% if is_incremental() %}
    AND {{ incremental_predicate('nftt.block_time') }}
    {% endif %}
    )

, filter_2 AS (
    SELECT nftt.tx_id
    , nftt.inner_instruction_index
    , nftt.outer_instruction_index
    , true AS back_and_forth_trade
    FROM {{ ref('nft_solana_trades') }} nftt
    INNER JOIN {{ ref('nft_solana_trades') }} filter_baf
        ON filter_baf.seller=nftt.buyer
        AND filter_baf.buyer=nftt.seller
        AND ((filter_baf.account_mint=nftt.account_mint AND nftt.account_mint IS NOT NULL)
            OR (filter_baf.account_merkle_tree=nftt.account_merkle_tree
            AND filter_baf.leaf_id=nftt.leaf_id
            AND nftt.account_merkle_tree IS NOT NULL))
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('nftt.block_time') }}
    {% endif %}
    GROUP BY 1, 2, 3
    )

, filter_3_bought AS (
    SELECT nftt.tx_id
    , nftt.inner_instruction_index
    , nftt.outer_instruction_index
    , CASE WHEN COUNT(filter_bought_3x.block_slot) > 2
        THEN true
        ELSE false
        END AS bought_3x
    FROM {{ ref('nft_solana_trades') }} nftt
    INNER JOIN {{ ref('nft_solana_trades') }} filter_bought_3x
        ON ((filter_bought_3x.account_mint=nftt.account_mint AND nftt.account_mint IS NOT NULL)
            OR (filter_bought_3x.account_merkle_tree=nftt.account_merkle_tree
            AND filter_bought_3x.leaf_id=nftt.leaf_id
            AND nftt.account_merkle_tree IS NOT NULL))
        AND filter_bought_3x.buyer=nftt.buyer
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('nftt.block_time') }}
    {% endif %}
    GROUP BY 1, 2, 3
    )

, filter_3_sold AS (
    SELECT nftt.tx_id
    , nftt.inner_instruction_index
    , nftt.outer_instruction_index
    , CASE WHEN COUNT(filter_sold_3x.block_slot) > 2
        THEN true
        ELSE false
        END AS sold_3x
    FROM {{ ref('nft_solana_trades') }} nftt
    INNER JOIN {{ ref('nft_solana_trades') }} filter_sold_3x
        ON ((filter_sold_3x.account_mint=nftt.account_mint AND nftt.account_mint IS NOT NULL)
            OR (filter_sold_3x.account_merkle_tree=nftt.account_merkle_tree
            AND filter_sold_3x.leaf_id=nftt.leaf_id
            AND nftt.account_merkle_tree IS NOT NULL))
        AND filter_sold_3x.seller=nftt.seller
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('nftt.block_time') }}
    {% endif %}
    GROUP BY 1, 2, 3
    )

, filter_4 AS (
    SELECT nftt.tx_id
    , nftt.inner_instruction_index
    , nftt.outer_instruction_index
    , CASE WHEN filter_funding_buyer.first_funded_by = filter_funding_seller.first_funded_by
        OR filter_funding_buyer.first_funded_by = nftt.seller
        OR filter_funding_seller.first_funded_by = nftt.buyer
        THEN true
        ELSE false
        END AS first_funded_by_same_wallet
    , filter_funding_buyer.first_funded_by AS buyer_first_funded_by
    , filter_funding_seller.first_funded_by AS seller_first_funded_by
    FROM {{ ref('nft_solana_trades') }} nftt
    INNER JOIN {{ ref('addresses_events_solana_first_funded_by') }} filter_funding_buyer
        ON filter_funding_buyer.address=nftt.buyer
        AND filter_funding_buyer.first_funded_by NOT IN (SELECT address FROM cex_solana.addresses)
    INNER JOIN {{ ref('addresses_events_solana_first_funded_by') }} filter_funding_seller
        ON filter_funding_seller.address=nftt.seller
        AND filter_funding_seller.first_funded_by NOT IN (SELECT address FROM cex_solana.addresses)
        AND (nftt.tx_id, nftt.inner_instruction_index, nftt.outer_instruction_index) IS NOT NULL
        AND nftt.buyer IS NOT NULL
        AND nftt.seller IS NOT NULL
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('nftt.block_time') }}
    {% endif %}
    )

SELECT nftt.blockchain
, nftt.project
, nftt.version
, tx_id
, inner_instruction_index
, outer_instruction_index
, nftt.trade_category
, nftt.buyer
, nftt.seller
, nftt.project_program_id
, nftt.account_mint
, nftt.leaf_id
, nftt.aggregator_name
, nftt.aggregator_address
, nftt.block_time
, nftt.block_date
, nftt.block_month
, nftt.block_slot
, buyer_first_funded_by
, seller_first_funded_by
, CASE WHEN filter_1.same_buyer_seller
    THEN true
    ELSE false
    END AS filter_1_same_buyer_seller
, CASE WHEN filter_2.back_and_forth_trade
    THEN true
    ELSE false
    END AS filter_2_back_and_forth_trade
, CASE WHEN filter_3_bought.bought_3x
    OR filter_3_sold.sold_3x
    THEN true
    ELSE false
    END AS filter_3_bought_or_sold_3x
, CASE WHEN filter_4.first_funded_by_same_wallet
    THEN true
    ELSE false
    END AS filter_4_first_funded_by_same_wallet
, CASE WHEN filter_1.same_buyer_seller
    OR filter_2.back_and_forth_trade
    OR filter_3_bought.bought_3x
    OR filter_3_sold.sold_3x
    OR filter_4.first_funded_by_same_wallet
    THEN true
    ELSE false
    END AS is_wash_trade
FROM {{ ref('nft_solana_trades') }} nftt
LEFT JOIN filter_1 USING (tx_id, inner_instruction_index, outer_instruction_index)
LEFT JOIN filter_2 USING (tx_id, inner_instruction_index, outer_instruction_index)
LEFT JOIN filter_3_bought USING (tx_id, inner_instruction_index, outer_instruction_index)
LEFT JOIN filter_3_sold USING (tx_id, inner_instruction_index, outer_instruction_index)
LEFT JOIN filter_4 USING (tx_id, inner_instruction_index, outer_instruction_index)