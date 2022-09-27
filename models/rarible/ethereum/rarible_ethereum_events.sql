{{ config(
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "rarible",
                                \'["hildobby"]\') }}'
    )
}}

WITH rarible_all_trades AS (
    -- Get data from various Rarible contracts deployed over time
    -- October 2019 -> Summer 2020
    SELECT 'v1' AS version
    , evt_block_time AS block_time
    , evt_block_time AS block_number
    , tokenId AS token_id
    , 'erc721' AS token_standard
    , 1 AS number_of_items
    , 'Buy' AS trade_category
    , 'Trade' AS evt_type
    , seller
    , buyer
    , price/POWER(10, 18) AS amount_original
    , price AS amount_raw
    , 'ETH' AS currency_symbol
    , '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS currency_contract
    , contract_address AS project_contract_address
    , token AS nft_contract_address
    , evt_tx_hash AS tx_hash
    FROM {{ source('rarible_ethereum','TokenSale_evt_Buy') }}
    UNION
    -- May 2020 -> September 2020
    SELECT 'v1' AS version
    , evt_block_time AS block_time
    , evt_block_time AS block_number
    , tokenId AS token_id
    , 'erc1155' AS token_standard
    , 1 AS number_of_items
    , 'Buy' AS trade_category
    , 'Trade' AS evt_type
    , owner AS seller
    , buyer
    , price/POWER(10, 18) AS amount_original
    , price AS amount_raw
    , 'ETH' AS currency_symbol
    , '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS currency_contract
    , contract_address AS project_contract_address
    , token AS nft_contract_address
    , evt_tx_hash AS tx_hash
    FROM {{ source('rarible_v1_ethereum','ERC1155Sale_v1_evt_Buy') }}
    UNION
    SELECT 'v1' AS version
    , evt_block_time AS block_time
    , evt_block_time AS block_number
    , tokenId AS token_id
    , 'erc721' AS token_standard
    , 1 AS number_of_items
    , 'Buy' AS trade_category
    , 'Trade' AS evt_type
    , seller
    , buyer
    , price/POWER(10, 18) AS amount_original
    , price AS amount_raw
    , 'ETH' AS currency_symbol
    , '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS currency_contract
    , contract_address AS project_contract_address
    , token AS nft_contract_address
    , evt_tx_hash AS tx_hash
    FROM {{ source('rarible_v1_ethereum','ERC721Sale_v1_evt_Buy') }}
    UNION
    -- September 2020 -> November 2020
    SELECT 'v1' AS version
    , evt_block_time AS block_time
    , evt_block_time AS block_number
    , tokenId AS token_id
    , 'erc721' AS token_standard
    , 1 AS number_of_items
    , 'Buy' AS trade_category
    , 'Trade' AS evt_type
    , seller
    , buyer
    , price/POWER(10, 18) AS amount_original
    , price AS amount_raw
    , 'ETH' AS currency_symbol
    , '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS currency_contract
    , contract_address AS project_contract_address
    , token AS nft_contract_address
    , evt_tx_hash AS tx_hash
    FROM {{ source('rarible_v1_ethereum','ERC721Sale_v2_evt_Buy') }}
    UNION
    SELECT 'v1' AS version
    , evt_block_time AS block_time
    , evt_block_time AS block_number
    , tokenId AS token_id
    , 'erc1155' AS token_standard
    , 1 AS number_of_items
    , 'Buy' AS trade_category
    , 'Trade' AS evt_type
    , owner AS seller
    , buyer
    , price/POWER(10, 18) AS amount_original
    , price AS amount_raw
    , 'ETH' AS currency_symbol
    , '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS currency_contract
    , contract_address AS project_contract_address
    , token AS nft_contract_address
    , evt_tx_hash AS tx_hash
    FROM {{ source('rarible_v1_ethereum','ERC1155Sale_v2_evt_Buy') }}
    UNION
    -- November 2020 -> June 2021
    SELECT 'v1' AS version
    , s.evt_block_time AS block_time
    , s.evt_block_time AS block_number
    , s.selltokenId AS token_id
    , CASE WHEN nft.evt_index IS NOT NULL THEN 'erc721' ELSE 'erc1155' END AS token_standard
    , 1 AS number_of_items
    , 'Buy' AS trade_category
    , 'Trade' AS evt_type
    , s.owner AS seller
    , s.buyer
    , (s.buyValue * s.amount / s.sellValue)/POWER(10, t.decimals) AS amount_original
    , s.buyValue * s.amount / s.sellValue AS amount_raw
    , CASE WHEN buyToken = '0x0000000000000000000000000000000000000000' THEN 'ETH'
        ELSE t.symbol
        END AS currency_symbol
    , CASE WHEN buyToken = '0x0000000000000000000000000000000000000000' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        ELSE buyToken
        END AS currency_contract
    , s.contract_address AS project_contract_address
    , s.sellToken AS nft_contract_address
    , s.evt_tx_hash AS tx_hash
    FROM {{ source('rarible_ethereum','ExchangeV1_evt_Buy') }} s
    LEFT JOIN {{ ref('tokens_ethereum_erc20') }} t ON (s.buyToken='0x0000000000000000000000000000000000000000' AND t.contract_address='0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2') OR (t.contract_address=s.buyToken)
    LEFT JOIN {{ source('erc721_ethereum','evt_transfer') }} nft ON nft.evt_block_time=s.evt_block_time AND nft.evt_tx_hash=s.evt_tx_hash AND nft.contract_address=s.sellToken AND nft.tokenId=s.selltokenId
    UNION
    -- June 2021 onwards (ETH Buys)
    SELECT 'v2' AS version
    , s.evt_block_time AS block_time
    , s.evt_block_time AS block_number
    , ROUND(bytea2numeric_v2(substring(get_json_object(s.leftAsset, '$.data'), 67, 64)), 0) AS token_id
    , CASE WHEN nft.evt_index IS NOT NULL THEN 'erc721' ELSE 'erc1155' END AS token_standard
    , 1 AS number_of_items
    , 'Buy' AS trade_category
    , 'Trade' AS evt_type
    , s.leftMaker AS seller
    , s.rightMaker AS buyer
    , s.newLeftFill/POWER(10, 18) AS amount_original
    , s.newLeftFill AS amount_raw
    , 'ETH' AS currency_symbol
    , '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' AS currency_contract
    , s.contract_address AS project_contract_address
    , '0x' || substring(get_json_object(s.leftAsset, '$.data'), 27, 40) AS nft_contract_address
    , s.evt_tx_hash AS tx_hash
    FROM {{ source('rarible_ethereum','ExchangeV2_evt_Match') }} s
    LEFT JOIN {{ source('erc721_ethereum','evt_transfer') }} nft ON nft.evt_block_time=s.evt_block_time AND nft.evt_tx_hash=s.evt_tx_hash AND nft.contract_address='0x' || substring(get_json_object(s.leftAsset, '$.data'), 27, 40)
    WHERE get_json_object(s.rightAsset, '$.assetClass') = '0xaaaebeba' -- ETH
    AND get_json_object(s.leftAsset, '$.assetClass') IN ('0x973bb640', '0x73ad2146')
    UNION
    -- June 2021 onwards (ERC20 Accepted Offers)
    SELECT 'v2' AS version
    , s.evt_block_time AS block_time
    , s.evt_block_time AS block_number
    , ROUND(bytea2numeric_v2(substring(get_json_object(s.leftAsset, '$.data'), 67, 64)), 0) AS token_id
    , CASE WHEN nft.evt_index IS NOT NULL THEN 'erc721' ELSE 'erc1155' END AS token_standard
    , 1 AS number_of_items
    , 'Accepted Offer' AS trade_category
    , 'Trade' AS evt_type
    , s.rightMaker AS seller
    , s.leftMaker AS buyer
    , s.newRightFill/POWER(10, t.decimals) AS amount_original
    , s.newRightFill AS amount_raw
    , t.symbol AS currency_symbol
    , '0x' || substring(get_json_object(s.leftAsset, '$.data'), 27, 40) AS currency_contract
    , s.contract_address AS project_contract_address
    , '0x' || substring(get_json_object(s.rightAsset, '$.data'), 27, 40) AS nft_contract_address
    , s.evt_tx_hash AS tx_hash
    FROM {{ source('rarible_ethereum','ExchangeV2_evt_Match') }} s
    LEFT JOIN {{ ref('tokens_ethereum_erc20') }} t ON t.contract_address='0x' || substring(get_json_object(s.leftAsset, '$.data'), 27, 40)
    LEFT JOIN {{ source('erc721_ethereum','evt_transfer') }} nft ON nft.evt_block_time=s.evt_block_time AND nft.evt_tx_hash=s.evt_tx_hash AND nft.contract_address='0x' || substring(get_json_object(s.rightAsset, '$.data'), 27, 40)
    WHERE get_json_object(s.leftAsset, '$.assetClass') = '0x8ae85d84'
    AND get_json_object(s.rightAsset, '$.assetClass') IN ('0x973bb640', '0x73ad2146')
    UNION
    -- June 2021 onwards (ERC20s Buy)
    SELECT 'v2' AS version
    , s.evt_block_time AS block_time
    , s.evt_block_time AS block_number
    , ROUND(bytea2numeric_v2(substring(get_json_object(s.leftAsset, '$.data'), 67, 64)), 0) AS token_id
    , CASE WHEN nft.evt_index IS NOT NULL THEN 'erc721' ELSE 'erc1155' END AS token_standard
    , 1 AS number_of_items
    , 'Buy' AS trade_category
    , 'Trade' AS evt_type
    , s.rightMaker AS seller
    , s.leftMaker AS buyer
    , s.newRightFill/POWER(10, t.decimals) AS amount_original
    , s.newRightFill AS amount_raw
    , t.symbol AS currency_symbol
    , '0x' || substring(get_json_object(s.leftAsset, '$.data'), 27, 40) AS currency_contract
    , s.contract_address AS project_contract_address
    , '0x' || substring(get_json_object(s.rightAsset, '$.data'), 27, 40) AS nft_contract_address
    , s.evt_tx_hash AS tx_hash
    FROM {{ source('rarible_ethereum','ExchangeV2_evt_Match') }} s
    LEFT JOIN {{ ref('tokens_ethereum_erc20') }} t ON t.contract_address='0x' || substring(get_json_object(s.leftAsset, '$.data'), 27, 40)
    LEFT JOIN {{ source('erc721_ethereum','evt_transfer') }} nft ON nft.evt_block_time=s.evt_block_time AND nft.evt_tx_hash=s.evt_tx_hash AND nft.contract_address='0x' || substring(get_json_object(s.rightAsset, '$.data'), 27, 40)
    WHERE get_json_object(s.leftAsset, '$.assetClass') = '0x8ae85d84'
    AND get_json_object(s.rightAsset, '$.assetClass') IN ('0x973bb640', '0x73ad2146')
    )

SELECT distinct 'ethereum' AS blockchain
, 'rarible' AS project
, rat.version
, date_trunc('day', rat.block_time) AS block_date
, rat.block_time
, rat.block_number
, ROUND(rat.token_id, 0) AS token_id
, nft.name AS collection
, rat.amount_original*pu.price AS amount_usd
, rat.token_standard
, CASE WHEN agg.name IS NOT NULL THEN 'Bundle Trade' ELSE 'Single Item Trade' END AS trade_type
, rat.number_of_items
, rat.trade_category
, rat.evt_type
, rat.seller
, rat.buyer
, rat.amount_original
, rat.amount_raw
, rat.currency_symbol
, rat.currency_contract
, rat.project_contract_address
, rat.nft_contract_address
, agg.name AS aggregator_name
, agg.contract_address AS aggregator_address
, rat.tx_hash
, et.from AS tx_from
, et.to AS tx_to
, COALESCE(SUM(traces.value), SUM(erc.value)) AS platform_fee_amount_raw
, COALESCE(SUM(traces.value)/POWER(10, 18), SUM(erc.value)/POWER(10, tok.decimals)) AS platform_fee_amount
, COALESCE(pu.price*SUM(traces.value)/POWER(10, 18), pu.price*SUM(erc.value)/POWER(10, tok.decimals)) AS platform_fee_amount_usd
, COALESCE(SUM(traces.value), SUM(erc.value))/rat.amount_raw platform_fee_percentage
, NULL AS royalty_fee_amount_raw
, NULL AS royalty_fee_amount
, NULL AS royalty_fee_amount_usd
, NULL AS royalty_fee_percentage
, NULL AS royalty_fee_receive_address
, NULL AS royalty_fee_currency_symbol
, 'ethereumrarible' || rat.version || rat.tx_hash || rat.nft_contract_address || rat.token_id || rat.seller || rat.buyer AS unique_trade_id
FROM rarible_all_trades rat
LEFT JOIN {{ source('ethereum','transactions') }} et ON et.block_time=rat.block_time AND et.hash=rat.tx_hash
LEFT JOIN {{ ref('nft_ethereum_aggregators') }} agg ON et.to=agg.contract_address
LEFT JOIN {{ source('prices','usd') }} pu ON pu.minute=date_trunc('minute', rat.block_time) AND pu.contract_address=rat.currency_contract
LEFT JOIN {{ ref('tokens_ethereum_nft') }} nft ON nft.contract_address=rat.nft_contract_address
LEFT JOIN {{ ref('tokens_ethereum_erc20') }} tok ON tok.contract_address=rat.nft_contract_address
LEFT JOIN {{ source('ethereum','traces') }} traces ON traces.block_time=rat.block_time AND traces.tx_hash=rat.tx_hash AND traces.to='0x1cf0df2a5a20cd61d68d4489eebbf85b8d39e18a'
LEFT JOIN {{ source('erc20_ethereum','evt_transfer') }} erc ON erc.evt_block_time=rat.block_time AND erc.evt_tx_hash=rat.tx_hash AND erc.to='0x1cf0df2a5a20cd61d68d4489eebbf85b8d39e18a'