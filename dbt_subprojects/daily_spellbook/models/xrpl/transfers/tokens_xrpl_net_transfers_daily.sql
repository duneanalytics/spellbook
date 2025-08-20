{{
    config(
        schema = 'tokens_xrpl',
        alias = 'net_transfers_daily',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'block_date'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    )
}}

WITH raw_transfers AS (
    -- Outgoing transfers (sent = negative)
    SELECT
        blockchain
        , block_date
        , from_address AS address
        , 'sent' AS transfer_direction
        , (SUM(amount_usd) * -1) AS transfer_amount_usd
    FROM {{ ref('tokens_xrpl_transfers') }}
    WHERE amount_usd IS NOT NULL
        {% if is_incremental() %}
        AND {{ incremental_predicate('block_date') }}
        {% endif %}
    GROUP BY blockchain, block_date, from_address, 'sent'

    UNION ALL

    -- Incoming transfers (received = positive)
    SELECT
        blockchain
        , block_date
        , to_address AS address
        , 'received' AS transfer_direction
        , SUM(amount_usd) AS transfer_amount_usd
    FROM {{ ref('tokens_xrpl_transfers') }}
    WHERE amount_usd IS NOT NULL
        {% if is_incremental() %}
        AND {{ incremental_predicate('block_date') }}
        {% endif %}
    GROUP BY blockchain, block_date, to_address, 'received'
),

net_transfers AS (
    SELECT
        blockchain
        , block_date
        , address
        , SUM(CASE WHEN transfer_direction = 'sent' THEN transfer_amount_usd ELSE 0 END) AS transfer_amount_usd_sent
        , SUM(CASE WHEN transfer_direction = 'received' THEN transfer_amount_usd ELSE 0 END) AS transfer_amount_usd_received
        , SUM(transfer_amount_usd) AS net_transfer_amount_usd
    FROM raw_transfers
    GROUP BY blockchain, block_date, address
)

SELECT
    blockchain
    , block_date
    , SUM(net_transfer_amount_usd) AS net_transfer_amount_usd
FROM net_transfers
WHERE net_transfer_amount_usd > 0
GROUP BY blockchain, block_date