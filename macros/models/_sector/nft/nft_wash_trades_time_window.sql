{% macro nft_wash_trades_time_window(blockchain, first_funded_by) %}
{%- set token_standard_name = 'bep' if blockchain == 'bnb' else 'erc' -%}
{%- set increment_window = 7 -%} -- 7 is the maximum time since last refresh, in days
{%- set time_window = 30 -%} -- 30 is the time window for wash trade cycle, in days

WITH filter_1 AS ( -- self-trades
    SELECT unique_trade_id
    , true AS same_buyer_seller
    FROM {{ ref('nft_trades') }} nftt
    WHERE nftt.blockchain='{{blockchain}}'
        AND nftt.unique_trade_id IS NOT NULL
        AND nftt.buyer=nftt.seller
        {% if is_incremental() %}
        AND nftt.block_time >= date_trunc('day', NOW() - interval '{{increment_window}}' day)
        {% endif %}
    )

, filter_2 AS ( -- pairs of trades A->B B->A, at least one on {{blockchain}}, within {{time_window}} days
    SELECT unique_trade_id
        , true AS back_and_forth_trade
    FROM (
        SELECT *
        FROM {{ ref('nft_trades') }} nftt
        WHERE nftt.nft_contract_address IS NOT NULL
            AND nftt.token_id IS NOT NULL
            {% if is_incremental() %}
            AND nftt.block_time >= date_trunc('day', NOW() - interval '{{increment_window + time_window}}' day)
            {% endif %}
        )
    MATCH_RECOGNIZE (
        PARTITION BY nft_contract_address, token_id
        ORDER BY block_time
        MEASURES
            CLASSIFIER() <> 'X' AND blockchain = '{{blockchain}}' AS on_this_chain
        ALL ROWS PER MATCH
        AFTER MATCH SKIP TO NEXT ROW
        PATTERN ( (A_B_THIS_CHAIN X* B_A) | (A_B_OTHER_CHAIN X* B_A_THIS_CHAIN) )
        SUBSET A_B = (A_B_THIS_CHAIN, A_B_OTHER_CHAIN)
        DEFINE
            A_B_THIS_CHAIN AS blockchain = '{{blockchain}}',
            B_A AS seller = A_B_THIS_CHAIN.buyer
                AND buyer = A_B_THIS_CHAIN.seller
                AND block_time - A_B_THIS_CHAIN.block_time <= interval '{{time_window}}' day,
            A_B_OTHER_CHAIN AS blockchain <> '{{blockchain}}',
            B_A_THIS_CHAIN AS blockchain = '{{blockchain}}'
                AND seller = A_B_OTHER_CHAIN.buyer
                AND buyer = A_B_OTHER_CHAIN.seller
                AND block_time - A_B_OTHER_CHAIN.block_time <= interval '{{time_window}}' day,
            X AS (seller <> A_B.buyer OR buyer <> A_B.seller)
                AND block_time - A_B.block_time <= interval '{{time_window}}' day
    )
    WHERE on_this_chain AND unique_trade_id IS NOT NULL
    GROUP BY unique_trade_id
)

, filter_3_bought AS ( -- 3+ buys by the same wallet, at least one on {{blockchain}}, within {{time_window}} days
    SELECT unique_trade_id -- no need to group by unique_trade_id, the pattern always matches the whole partition
        , chain = 'THIS_CHAIN' AND buys_count > 2 AND in_time_window AS bought_3x
    FROM (
        SELECT *
        FROM {{ ref('nft_trades') }} nftt
        WHERE nftt.nft_contract_address IS NOT NULL
            AND nftt.token_id IS NOT NULL
            AND 0x29469395eaf6f95920e59f858042f0e28d98a20b NOT IN (nftt.buyer, nftt.seller)
            AND nftt.token_standard IN ('{{token_standard_name}}' || '721', '{{token_standard_name}}' || '20')
            {% if is_incremental() %}
            AND nftt.block_time >= date_trunc('day', NOW() - interval '{{increment_window + time_window}}' day)
            {% endif %}
        )
    MATCH_RECOGNIZE (
        PARTITION BY nft_contract_address, token_id, buyer
        ORDER BY block_time
        MEASURES
            CLASSIFIER() AS chain,
            CLASSIFIER() = 'THIS_CHAIN'
                AND (block_time - PREV(block_time, 2) <= interval '{{time_window}}' day
                    OR NEXT(block_time) - PREV(block_time) <= interval '{{time_window}}' day
                    OR NEXT(block_time, 2) - block_time <= interval '{{time_window}}' day) AS in_time_window, -- 3 consecutive buys within the time window
            FINAL count(*) AS buys_count
        ALL ROWS PER MATCH
        PATTERN ( (THIS_CHAIN | OTHER_CHAIN)* )
        DEFINE
            THIS_CHAIN AS blockchain = '{{blockchain}}',
            OTHER_CHAIN AS blockchain <> '{{blockchain}}'
        )
    WHERE unique_trade_id IS NOT NULL
    )

, filter_3_sold AS ( -- 3+ sells by the same wallet, at least one on {{blockchain}}, within {{time_window}} days
    SELECT unique_trade_id -- no need to group by unique_trade_id, the pattern always matches the whole partition
        , chain = 'THIS_CHAIN' AND sells_count > 2 AND in_time_window AS sold_3x
    FROM (
        SELECT *
        FROM {{ ref('nft_trades') }} nftt
        WHERE nftt.nft_contract_address IS NOT NULL
            AND nftt.token_id IS NOT NULL
            AND 0x29469395eaf6f95920e59f858042f0e28d98a20b NOT IN (nftt.buyer, nftt.seller)
            AND nftt.token_standard IN ('{{token_standard_name}}' || '721', '{{token_standard_name}}' || '20')
            {% if is_incremental() %}
            AND nftt.block_time >= date_trunc('day', NOW() - interval '{{increment_window + time_window}}' day)
            {% endif %}
        )
    MATCH_RECOGNIZE (
        PARTITION BY nft_contract_address, token_id, seller
        ORDER BY block_time
        MEASURES
            CLASSIFIER() AS chain,
            CLASSIFIER() = 'THIS_CHAIN'
                AND (block_time - PREV(block_time, 2) <= interval '{{time_window}}' day
                    OR NEXT(block_time) - PREV(block_time) <= interval '{{time_window}}' day
                    OR NEXT(block_time, 2) - block_time <= interval '{{time_window}}' day) AS in_time_window, -- 3 consecutive sells within the time window
            FINAL count(*) AS sells_count
        ALL ROWS PER MATCH
        PATTERN ( (THIS_CHAIN | OTHER_CHAIN)* )
        DEFINE
            THIS_CHAIN AS blockchain = '{{blockchain}}',
            OTHER_CHAIN AS blockchain <> '{{blockchain}}'
        )
    WHERE unique_trade_id IS NOT NULL
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
        AND filter_funding_buyer.block_time >= date_trunc('day', NOW() - interval '{{increment_window}}' day)
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
        AND nftt.block_time >= date_trunc('day', NOW() - interval '{{increment_window}}' day)
        AND filter_funding_seller.block_time >= date_trunc('day', NOW() - interval '{{increment_window}}' day)
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
        AND nftt.block_time >= date_trunc('day', NOW() - interval '{{increment_window}}' day)
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
    AND nftt.block_time >= date_trunc('day', NOW() - interval '{{increment_window}}' day)
    {% endif %}

{% endmacro %}