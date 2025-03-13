{{ config(
        schema = 'fantasy',
        alias = 'events',
        materialized = 'view',
        post_hook = '{{ expose_spells(
                        blockchains = \'["blast"]\',
                        spell_type = "project",
                        spell_name = "events",
                        contributors = \'["hildobby"]\') }}'
        )
}}

WITH fantasy_configs AS (
    SELECT evt_block_time AS block_time
    , evt_block_number AS block_number
    , evt_block_date AS block_date
    , mintConfigId AS config_id
    , requiresWhitelist AS whitelist
    , maxPacksPerAddress AS max_pack_per_address
    , fixedPrice/POWER(10, 18) AS token_amount
    , CAST(NULL AS varbinary) AS traded_with
    , paymentToken AS token_address
    , maxPacks AS max_packs
    , cardsPerPack AS cards_per_pack
    , collection
    , expirationTimestamp AS expiration
    , merkleRoot AS merkle_root
    , evt_tx_from AS tx_from
    , evt_tx_to AS tx_to
    , evt_tx_hash AS tx_hash
    , evt_index
    , contract_address
    , CAST(NULL AS boolean) AS is_wash_trade
    FROM {{ source('fantasy_blast', 'Minter_evt_NewMintConfig')}}
    )

-- Mints
 SELECT block_time
, block_number
, block_date
, evt_type
, user_address
, whitelist
, collection
, cards_minted
, cards_burned
, minted_ids
, burned_ids
, traded_ids
, traded_with
, tx_from
, tx_to
, tx_hash
, tx_index
, contract_address
, is_wash_trade
, token_symbol
, token_address
, token_amount
, price_usd
, heroes_revenue
, heroes_revenue_usd
, to_fantasy_treasury
, to_fantasy_treasury_usd
, tactics_bought
FROM (
    SELECT m.evt_block_time AS block_time
    , m.evt_block_number AS block_number
    , m.evt_block_date AS block_date
    , 'Mint' AS evt_type
    , m.buyer AS user_address
    , c.whitelist
    , c.collection
    , CAST(m.lastTokenId-m.firstTokenId+1 AS double) AS cards_minted
    , CAST(0 AS double) AS cards_burned
    , filter(ARRAY[
        m.firstTokenId, m.firstTokenId+1, m.firstTokenId+2, m.firstTokenId+3, m.firstTokenId+4, m.firstTokenId+5, m.firstTokenId+6, m.firstTokenId+7, m.firstTokenId+8, m.firstTokenId+9
    , m.firstTokenId+10, m.firstTokenId+11, m.firstTokenId+12, m.firstTokenId+13, m.firstTokenId+14, m.firstTokenId+15, m.firstTokenId+16, m.firstTokenId+17, m.firstTokenId+18, m.firstTokenId+19
    , m.firstTokenId+20, m.firstTokenId+21, m.firstTokenId+22, m.firstTokenId+23, m.firstTokenId+24, m.firstTokenId+25, m.firstTokenId+26, m.firstTokenId+27, m.firstTokenId+28, m.firstTokenId+29
    , m.firstTokenId+30, m.firstTokenId+31, m.firstTokenId+32, m.firstTokenId+33, m.firstTokenId+34, m.firstTokenId+35, m.firstTokenId+36, m.firstTokenId+37, m.firstTokenId+38, m.firstTokenId+39
    , m.firstTokenId+40, m.firstTokenId+41, m.firstTokenId+42, m.firstTokenId+43, m.firstTokenId+44, m.firstTokenId+45, m.firstTokenId+46, m.firstTokenId+47, m.firstTokenId+48, m.firstTokenId+49
    , m.firstTokenId+50, m.firstTokenId+51, m.firstTokenId+52, m.firstTokenId+53, m.firstTokenId+54, m.firstTokenId+55, m.firstTokenId+56, m.firstTokenId+57, m.firstTokenId+58, m.firstTokenId+59
    , m.firstTokenId+60, m.firstTokenId+61, m.firstTokenId+62, m.firstTokenId+63, m.firstTokenId+64, m.firstTokenId+65, m.firstTokenId+66, m.firstTokenId+67, m.firstTokenId+68, m.firstTokenId+69
    , m.firstTokenId+70, m.firstTokenId+71, m.firstTokenId+72, m.firstTokenId+73, m.firstTokenId+74, m.firstTokenId+75, m.firstTokenId+76, m.firstTokenId+77, m.firstTokenId+78, m.firstTokenId+79
    , m.firstTokenId+80, m.firstTokenId+81, m.firstTokenId+82, m.firstTokenId+83, m.firstTokenId+84, m.firstTokenId+85, m.firstTokenId+86, m.firstTokenId+87, m.firstTokenId+88, m.firstTokenId+89
    , m.firstTokenId+90, m.firstTokenId+91, m.firstTokenId+92, m.firstTokenId+93, m.firstTokenId+94, m.firstTokenId+95, m.firstTokenId+96, m.firstTokenId+97, m.firstTokenId+98, m.firstTokenId+99
        ], x -> x <= lastTokenId) AS minted_ids
    , CAST(NULL AS ARRAY<UINT256>) AS burned_ids
    , CAST(NULL AS DOUBLE) AS traded_ids
    , CAST(NULL AS varbinary) AS traded_with
    , m.evt_tx_from AS tx_from
    , m.evt_tx_to AS tx_to
    , m.evt_tx_hash AS tx_hash
    , m.evt_tx_index AS tx_index
    , m.contract_address
    , CAST(NULL AS boolean) AS is_wash_trade
    , 'WETH' AS token_symbol
    , 0x4300000000000000000000000000000000000004 AS token_address
    , m.price/POWER(10, 18) AS token_amount
    , m.price/POWER(10, 18)*pu.price AS price_usd
    , 0.1*m.price/POWER(10, 18) AS heroes_revenue
    , 0.1*m.price/POWER(10, 18)*pu.price AS heroes_revenue_usd
    , 0.9*m.price/POWER(10, 18) AS to_fantasy_treasury
    , 0.9*m.price/POWER(10, 18)*pu.price AS to_fantasy_treasury_usd
    , 0 AS tactics_bought
    , RANK() OVER (PARTITION BY m.mintConfigId ORDER BY c.block_number, c.evt_index DESC) AS rank
    FROM {{ source('fantasy_blast', 'Minter_evt_Mint')}} m
    INNER JOIN fantasy_configs c ON m.mintConfigId=c.config_id
        AND c.block_number < m.evt_block_number
    LEFT JOIN {{ source('prices', 'usd') }} pu ON pu.blockchain='blast'
        AND pu.contract_address=0x4300000000000000000000000000000000000004
        AND pu.minute=date_trunc('minute', m.evt_block_time)
    )
WHERE rank = 1

UNION ALL

-- Level Ups
SELECT evt_block_time AS block_time
, evt_block_number AS block_number
, evt_block_date AS block_date
, 'Level Up' AS evt_type
, caller AS user_address
, CAST(NULL AS boolean) AS whitelist
, collection
, CAST(1 AS double) AS cards_minted
, CAST(cardinality(burntTokenIds) AS double) AS cards_burned
, ARRAY[mintedTokenId] AS minted_ids
, burntTokenIds AS burned_ids
, CAST(NULL AS DOUBLE) AS traded_ids
, CAST(NULL AS varbinary) AS traded_with
, evt_tx_from AS tx_from
, evt_tx_to AS tx_to
, evt_tx_hash AS tx_hash
, evt_tx_index AS tx_index
, contract_address
, CAST(NULL AS boolean) AS is_wash_trade
, CAST(NULL AS varchar) AS token_symbol
, CAST(NULL AS varbinary) AS token_address
, 0 AS token_amount
, 0 AS price_usd
, 0 AS heroes_revenue
, 0 AS heroes_revenue_usd
, 0 AS to_fantasy_treasury
, 0 AS to_fantasy_treasury_usd
, 0 AS tactics_bought
FROM {{ source('fantasy_blast', 'Minter_evt_LevelUp')}}

UNION ALL

-- Burns to Draw
SELECT evt_block_time AS block_time
, evt_block_number AS block_number
, evt_block_date AS block_date
, 'Burn to Draw' AS evt_type
, caller AS user_address
, CAST(NULL AS boolean) AS whitelist
, collection
, CAST(cardinality(mintedTokenIds) AS double) AS cards_minted
, CAST(cardinality(burntTokenIds) AS double) AS cards_burned
, mintedTokenIds AS minted_ids
, burntTokenIds AS burned_ids
, CAST(NULL AS DOUBLE) AS traded_ids
, CAST(NULL AS varbinary) AS traded_with
, evt_tx_from AS tx_from
, evt_tx_to AS tx_to
, evt_tx_hash AS tx_hash
, evt_tx_index AS tx_index
, contract_address
, CAST(NULL AS boolean) AS is_wash_trade
, CAST(NULL AS varchar) AS token_symbol
, CAST(NULL AS varbinary) AS token_address
, 0 AS token_amount
, 0 AS price_usd
, 0 AS heroes_revenue
, 0 AS heroes_revenue_usd
, 0 AS to_fantasy_treasury
, 0 AS to_fantasy_treasury_usd
, 0 AS tactics_bought
FROM {{ source('fantasy_blast', 'Minter_evt_BurnToDraw')}}

UNION ALL

-- Trades
SELECT nftt.block_time
, nftt.block_number
, nftt.block_date
, 'Trade' AS evt_type
, nftt.tx_from AS user_address
, false AS whitelist
, nftt.nft_contract_address AS collection
, CAST(0 AS double) AS cards_minted
, CAST(0 AS double) AS cards_burned
, CAST(NULL AS ARRAY<UINT256>) AS minted_ids
, CAST(NULL AS ARRAY<UINT256>) AS burned_ids
, nftt.token_id AS traded_ids
, CASE WHEN nftt.trade_category='Buy' THEN nftt.seller ELSE nftt.buyer END AS traded_with
, nftt.tx_from
, nftt.tx_to
, nftt.tx_hash
, txs.index AS tx_index
, nftt.project_contract_address AS contract_address
, wt.is_wash_trade
, nftt.currency_symbol AS token_symbol
, nftt.currency_contract AS token_address
, nftt.amount_original AS token_amount
, nftt.amount_usd AS price_usd
, royalty_fee_amount AS heroes_revenue
, royalty_fee_amount_usd AS heroes_revenue_usd
, platform_fee_amount AS to_fantasy_treasury
, platform_fee_amount_usd AS to_fantasy_treasury_usd
, 0 AS tactics_bought
FROM {{ source('nft', 'trades') }} nftt
INNER JOIN {{ source('blast', 'transactions') }} txs ON txs.block_number=nftt.block_number
    AND txs.hash=nftt.tx_hash
LEFT JOIN {{source('nft_blast', 'wash_trades')}} wt ON wt.project = 'fantasy'
    AND wt.block_number=nftt.block_number
    AND nftt.unique_trade_id=wt.unique_trade_id
WHERE nftt.blockchain = 'blast'
AND nftt.project = 'fantasy'

UNION ALL

-- Tactics Purchases
SELECT block_time
, block_number
, block_date
, 'Tactics Purchase' AS evt_type
, tx_from AS user_address
, false AS whitelist
, CAST(NULL AS varbinary) AS collection
, CAST(0 AS double) AS cards_minted
, CAST(0 AS double) AS cards_burned
, CAST(NULL AS ARRAY<UINT256>) AS minted_ids
, CAST(NULL AS ARRAY<UINT256>) AS burned_ids
, CAST(NULL AS DOUBLE) AS traded_ids
, CAST(NULL AS varbinary) AS traded_with
, tx_from
, tx_to
, tx_hash
, tx_index
, CAST(NULL AS varbinary) AS contract_address
, CAST(NULL AS boolean) AS is_wash_trade
, symbol AS token_symbol
, contract_address AS token_address
, amount AS token_amount
, amount_usd AS price_usd
, 0.015*amount AS heroes_revenue
, 0.015*amount_usd AS heroes_revenue_usd
, 0.06*amount AS to_fantasy_treasury
, 0.06*amount_usd AS to_fantasy_treasury_usd
, ROUND(amount_usd/19.99) AS tactics_bought
FROM {{ source('tokens_blast', 'transfers') }} tt
WHERE block_number >= 4917909
AND to = 0x4af1f00f50efbfcdf7a8f2ac02e9bc24825438ac
AND contract_address = 0x4300000000000000000000000000000000000004
AND amount > 0

UNION ALL

-- Batch Burn
SELECT evt_block_time AS block_time
, evt_block_number AS block_number
, evt_block_date AS block_date
, 'Batch Burn' AS evt_type
, caller AS user_address
, false AS whitelist
, NULL AS collection
, CAST(0 AS double) AS cards_minted
, CARDINALITY(burntTokenIds) AS cards_burned
, CAST(NULL AS ARRAY<UINT256>) AS minted_ids
, burntTokenIds AS burned_ids
, CAST(NULL AS DOUBLE) AS traded_ids
, NULL AS traded_with
, evt_tx_from AS tx_from
, evt_tx_to AS tx_to
, evt_tx_hash AS tx_hash
, evt_tx_index AS tx_index
, contract_address
, NULL AS is_wash_trade
, NULL AS token_symbol
, NULL AS token_address
, 0 AS token_amount
, 0 AS price_usd
, 0 AS heroes_revenue
, 0 AS heroes_revenue_usd
, 0 AS to_fantasy_treasury
, 0 AS to_fantasy_treasury_usd
, 0 AS tactics_bought
FROM {{ source('fantasy_blast', 'minter_evt_batchburn')}}