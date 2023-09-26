{% macro nft_wash_trades(blockchain, first_funded_by) %}
{%- set token_standard_name = 'bep' if blockchain == 'bnb' else 'erc' -%}

WITH filter_1 AS (
    SELECT unique_trade_id
    , true AS same_buyer_seller
    FROM {{ ref('nft_trades') }} nftt
    WHERE nftt.blockchain='{{blockchain}}'
        AND nftt.unique_trade_id IS NOT NULL
        AND nftt.buyer=nftt.seller
        {% if is_incremental() %}
        AND nftt.block_time >= date_trunc('day', NOW() - interval '7' day)
        {% endif %}
    )

, filter_2 AS (
    SELECT nftt.unique_trade_id
    , true AS back_and_forth_trade
    FROM {{ ref('nft_trades') }} nftt
    INNER JOIN {{ ref('nft_trades') }} filter_baf
        ON filter_baf.seller=nftt.buyer
        AND filter_baf.buyer=nftt.seller
        AND filter_baf.nft_contract_address=nftt.nft_contract_address
        AND filter_baf.token_id=nftt.token_id
        AND nftt.blockchain='{{blockchain}}'
        AND nftt.unique_trade_id IS NOT NULL
        {% if is_incremental() %}
        AND nftt.block_time >= date_trunc('day', NOW() - interval '7' day)
        AND filter_baf.block_time >= date_trunc('day', NOW() - interval '7' day)
        {% endif %}
    GROUP BY nftt.unique_trade_id
    )

, filter_3_bought AS (
    SELECT nftt.unique_trade_id
    , CASE WHEN COUNT(filter_bought_3x.block_number) > 2
        THEN true
        ELSE false
        END AS bought_3x
    FROM {{ ref('nft_trades') }} nftt
    INNER JOIN {{ ref('nft_trades') }} filter_bought_3x
        ON filter_bought_3x.nft_contract_address=nftt.nft_contract_address
        AND filter_bought_3x.token_id=nftt.token_id
        AND filter_bought_3x.buyer=nftt.buyer
        AND filter_bought_3x.token_standard IN ('{{token_standard_name}}' || '721', '{{token_standard_name}}' || '20')
        AND nftt.blockchain='{{blockchain}}'
        AND nftt.unique_trade_id IS NOT NULL
        AND 0x29469395eaf6f95920e59f858042f0e28d98a20b NOT IN (nftt.buyer, nftt.seller)
        {% if is_incremental() %}
        AND nftt.block_time >= date_trunc('day', NOW() - interval '7' day)
        AND filter_bought_3x.block_time >= date_trunc('day', NOW() - interval '7' day)
        {% endif %}
    GROUP BY nftt.unique_trade_id
    )

, filter_3_sold AS (
    SELECT nftt.unique_trade_id
    , CASE WHEN COUNT(filter_sold_3x.block_number) > 2
        THEN true
        ELSE false
        END AS sold_3x
    FROM {{ ref('nft_trades') }} nftt
    INNER JOIN {{ ref('nft_trades') }} filter_sold_3x
        ON filter_sold_3x.nft_contract_address=nftt.nft_contract_address
        AND filter_sold_3x.token_id=nftt.token_id
        AND filter_sold_3x.seller=nftt.seller
        AND filter_sold_3x.token_standard IN ('{{token_standard_name}}' || '721', '{{token_standard_name}}' || '20')
        AND nftt.blockchain='{{blockchain}}'
        AND nftt.unique_trade_id IS NOT NULL
        {% if is_incremental() %}
        AND nftt.block_time >= date_trunc('day', NOW() - interval '7' day)
        AND filter_sold_3x.block_time >= date_trunc('day', NOW() - interval '7' day)
        {% endif %}
    GROUP BY nftt.unique_trade_id
    )

, filter_4 AS (
    SELECT nftt.unique_trade_id
    , CASE WHEN filter_funding_buyer.first_funded_by = filter_funding_seller.first_funded_by
        OR filter_funding_buyer.first_funded_by = nftt.seller
        OR filter_funding_seller.first_funded_by = nftt.buyer
        THEN true
        ELSE false
        END AS first_funded_by_same_wallet
    , filter_funding_buyer.first_funded_by AS buyer_first_funded_by
    , filter_funding_seller.first_funded_by AS seller_first_funded_by
    FROM {{ ref('nft_trades') }} nftt
    INNER JOIN {{ first_funded_by }} filter_funding_buyer
        ON filter_funding_buyer.address=nftt.buyer
        AND filter_funding_buyer.first_funded_by NOT IN (SELECT DISTINCT address FROM {{ ref('labels_bridges') }})
        AND filter_funding_buyer.first_funded_by NOT IN (SELECT DISTINCT address FROM {{ ref('labels_cex') }})
        AND filter_funding_buyer.first_funded_by NOT IN (SELECT DISTINCT contract_address FROM {{ ref('tornado_cash_withdrawals') }})
        {% if is_incremental() %}
        AND filter_funding_buyer.block_time >= date_trunc('day', NOW() - interval '7' day)
        {% endif %}
    INNER JOIN {{ first_funded_by }} filter_funding_seller
        ON filter_funding_seller.address=nftt.seller
        AND filter_funding_seller.first_funded_by NOT IN (SELECT DISTINCT address FROM {{ ref('labels_bridges') }})
        AND filter_funding_seller.first_funded_by NOT IN (SELECT DISTINCT address FROM {{ ref('labels_cex') }})
        AND filter_funding_seller.first_funded_by NOT IN (SELECT DISTINCT contract_address FROM {{ ref('tornado_cash_withdrawals') }})
        AND nftt.blockchain='{{blockchain}}'
        AND nftt.unique_trade_id IS NOT NULL
        AND nftt.buyer IS NOT NULL
        AND nftt.seller IS NOT NULL
        {% if is_incremental() %}
        AND nftt.block_time >= date_trunc('day', NOW() - interval '7' day)
        AND filter_funding_seller.block_time >= date_trunc('day', NOW() - interval '7' day)
        {% endif %}
    )

, filter_5 AS (
    SELECT unique_trade_id
    , true AS flashloan
    FROM {{ ref('nft_trades') }} nftt
    INNER JOIN {{ ref('dex_flashloans') }} df ON df.blockchain='{{blockchain}}'
        AND df.block_time=nftt.block_time
        AND df.tx_hash=nftt.tx_hash
        AND nftt.blockchain='{{blockchain}}'
        AND nftt.unique_trade_id IS NOT NULL
        {% if is_incremental() %}
        AND nftt.block_time >= date_trunc('day', NOW() - interval '7' day)
        {% endif %}
    )

SELECT nftt.blockchain
, nftt.project
, nftt.version
, nftt.nft_contract_address
, nftt.token_id
, nftt.token_standard
, nftt.trade_category
, nftt.buyer
, nftt.seller
, nftt.project_contract_address
, nftt.aggregator_name
, nftt.aggregator_address
, nftt.tx_from
, nftt.tx_to
, nftt.block_time
, CAST(date_trunc('day', nftt.block_time) AS date) AS block_date
, CAST(date_trunc('month', nftt.block_time) AS date) AS block_month
, nftt.block_number
, nftt.tx_hash
, nftt.unique_trade_id
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
, CASE WHEN filter_5.flashloan
    THEN true
    ELSE false
    END AS filter_5_flashloan
, CASE WHEN filter_1.same_buyer_seller
    OR filter_2.back_and_forth_trade
    OR filter_3_bought.bought_3x
    OR filter_3_sold.sold_3x
    OR filter_4.first_funded_by_same_wallet
    OR filter_5.flashloan
    THEN true
    ELSE false
    END AS is_wash_trade
FROM {{ ref('nft_trades') }} nftt
LEFT JOIN filter_1 ON nftt.unique_trade_id=filter_1.unique_trade_id
LEFT JOIN filter_2 ON nftt.unique_trade_id=filter_2.unique_trade_id
LEFT JOIN filter_3_bought ON nftt.unique_trade_id=filter_3_bought.unique_trade_id
LEFT JOIN filter_3_sold ON nftt.unique_trade_id=filter_3_sold.unique_trade_id
LEFT JOIN filter_4 ON nftt.unique_trade_id=filter_4.unique_trade_id
LEFT JOIN filter_5 ON nftt.unique_trade_id=filter_5.unique_trade_id
WHERE nftt.blockchain='{{blockchain}}'
    AND nftt.unique_trade_id IS NOT NULL
    {% if is_incremental() %}
    AND nftt.block_time >= date_trunc('day', NOW() - interval '7' day)
    {% endif %}

{% endmacro %}
