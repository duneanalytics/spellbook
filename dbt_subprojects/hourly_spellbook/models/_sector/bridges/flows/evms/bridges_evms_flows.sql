{{ config(
    schema = 'bridges_evms'
    , alias = 'flows'
    , materialized = 'view'
    , post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "blast", "bnb", "ethereum", "hyperevm", "ink", "lens", "linea", "optimism", "plasma", "polygon", "scroll", "unichain", "worldchain", "zksync", "zora", "fantom", "gnosis", "nova", "opbnb", "berachain", "corn", "flare", "sei", "boba", "mantle"]\',
                                "sector",
                                "bridges",
                                \'["hildobby"]\') }}'
    )
}}

WITH latest_deposits AS (
    SELECT bridge_name
    , bridge_version
    , deposit_chain
    , withdrawal_chain
    , withdrawal_chain_id
    , bridge_transfer_id
    , block_date
    , block_time
    , block_number
    , deposit_amount
    , deposit_amount_raw
    , deposit_amount_usd
    , sender
    , recipient
    , deposit_token_standard
    , deposit_token_address
    , tx_from
    , tx_hash
    , duplicate_index
    FROM (
        SELECT bridge_name
        , bridge_version
        , deposit_chain
        , withdrawal_chain
        , withdrawal_chain_id
        , bridge_transfer_id
        , block_date
        , block_time
        , block_number
        , deposit_amount_raw
        , deposit_amount
        , deposit_amount_usd
        , sender
        , recipient
        , deposit_token_standard
        , deposit_token_address
        , tx_from
        , tx_hash
        , duplicate_index
        , ROW_NUMBER() OVER (
            PARTITION BY bridge_name, bridge_version, deposit_chain, withdrawal_chain, bridge_transfer_id 
            ORDER BY duplicate_index DESC) AS rn
        FROM {{ ref('bridges_evms_deposits') }}
    )
    WHERE rn = 1
    )

SELECT deposit_chain
, w.deposit_chain_id
, withdrawal_chain
, d.withdrawal_chain_id
, bridge_name
, bridge_version
, d.block_date AS deposit_block_date
, d.block_time AS deposit_block_time
, d.block_number AS deposit_block_number
, w.block_date AS withdrawal_block_date
, w.block_time AS withdrawal_block_time
, w.block_number AS withdrawal_block_number
, d.deposit_amount_raw
, d.deposit_amount
, w.withdrawal_amount_raw
, w.withdrawal_amount
, COALESCE(d.deposit_amount_usd, w.withdrawal_amount_usd) AS amount_usd
, COALESCE(d.sender, w.sender) AS sender
, COALESCE(w.recipient, d.recipient) AS recipient
, d.deposit_token_standard
, w.withdrawal_token_standard
, d.deposit_token_address
, w.withdrawal_token_address
, d.tx_from AS deposit_tx_from
, d.tx_hash AS deposit_tx_hash
, w.tx_hash AS withdrawal_tx_hash
, bridge_transfer_id
, d.duplicate_index
FROM {{ ref('bridges_evms_withdrawals') }} w
FULL OUTER JOIN latest_deposits d USING (bridge_name, bridge_version, deposit_chain, withdrawal_chain, bridge_transfer_id)
