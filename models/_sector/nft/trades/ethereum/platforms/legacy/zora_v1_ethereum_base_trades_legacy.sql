{{ config(
    schema = 'zora_v1_ethereum',
    alias = alias('base_trades', legacy_model=True),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id'],
    )
}}

SELECT
     date_trunc('day',bf.evt_block_time) AS block_date
    , bf.evt_block_time AS block_time
    , bf.evt_block_number AS block_number
    , bf.contract_address AS project_contract_address
    , bf.evt_tx_hash AS tx_hash
    , '0xabefbc9fd2f806065b4f3c237d4b59d9a97bcac7' AS nft_contract_address -- hardcoded ZORA address
    , bf.tokenId AS nft_token_id
    , CAST(1 as INT) as nft_amount
    , 'Offer accepted' AS trade_category
    , CASE WHEN mt.from = mint.to
        THEN 'primary'
        ELSE 'secondary'
        END AS trade_type
    , get_json_object(bf.bid, '$.bidder') AS buyer
    , mt.from AS seller
    , CAST(get_json_object(bf.bid, '$.amount') as DECIMAL(38)) AS price_raw
    , get_json_object(bf.bid, '$.currency') AS currency_contract
    , CAST(0 as DECIMAL(38)) AS platform_fee_amount_raw
    , CAST(0 as DECIMAL(38)) AS royalty_fee_amount_raw
    , CAST(NULL as VARCHAR(1)) AS platform_fee_address
    , CAST(NULL as VARCHAR(1)) AS royalty_fee_address
    , bf.evt_index as sub_tx_trade_id
FROM {{ source('zora_ethereum','Market_evt_BidFinalized') }} bf
LEFT JOIN {{ source('zora_ethereum','Media_evt_Transfer') }} mint
    ON mint.from = '0x0000000000000000000000000000000000000000'
    AND mint.tokenId = bf.tokenId
LEFT JOIN {{ source('zora_ethereum','Media_evt_Transfer') }} mt
    ON bf.evt_block_number = mt.evt_block_number
    AND bf.evt_tx_hash = mt.evt_tx_hash
    AND bf.tokenId = mt.tokenId
    AND get_json_object(bf.bid, '$.bidder') = mt.to
WHERE get_json_object(bf.bid, '$.bidder') != '0xe468ce99444174bd3bbbed09209577d25d1ad673'   -- these are sells through the auction house which are included in V2
{% if is_incremental() %}
AND bf.evt_block_time >= date_trunc("day", now() - interval '1 week')
AND mt.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}
