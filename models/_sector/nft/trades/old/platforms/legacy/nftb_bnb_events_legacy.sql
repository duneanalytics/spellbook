{{ config(
	tags=['legacy'],
	
    schema = 'nftb_bnb',
    alias = alias('events', legacy_model=True),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'tx_hash', 'evt_index']
    )
}}

{%- set project_start_date = '2021-04-30' %}


WITH mints as
(SELECT tr.contract_address as project_contract_address
       ,tr.evt_tx_hash as tx_hash
       ,tr.evt_index
       ,tr.evt_block_time as block_time
       ,tr.evt_block_number as block_number
    --   ,"from"
    --   ,to
       ,tr.tokenId as token_id
       ,NULL as buyer
       ,NULL as seller
       ,'Mint' as evt_type
       ,CAST(1 AS DECIMAL(38,0)) AS number_of_items
       ,NULL as trade_type
       ,NULL as trade_category
       ,'BNB' as currency_symbol
       ,'0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c' as currency_contract
       ,'0x836eb8202d4bc2ed14d1d2861e441c69228155cc' AS nft_contract_address -- generic contract address for all nfts on NFTb; no individual contract addreses
       ,_recipient as royalty_fee_receive_address
        ,CAST(0 as DECIMAL(38,0)) AS amount_raw
       ,CASE WHEN cardinality(_royaltyAmounts)>1 THEN CAST (10 AS DOUBLE)
              WHEN cardinality(_royaltyAmounts)=1 THEN CAST (element_at(_royaltyAmounts, 1) AS DOUBLE)
              ELSE CAST (0 AS DOUBLE)
              END AS royalty_fee_percentage

FROM {{source('nftb_bnb', 'NFT_evt_Transfer')}} tr

INNER JOIN {{source('nftb_bnb', 'NFT_evt_Mint')}} m
ON tr.evt_tx_hash=m.evt_tx_hash
WHERE tr.evt_block_time >= '{{project_start_date}}'
{% if is_incremental() %}
AND tr.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}
)

,burns as
(SELECT tr.contract_address as project_contract_address
       ,tr.evt_tx_hash as tx_hash
       ,tr.evt_index
       ,tr.evt_block_time as block_time
       ,tr.evt_block_number as block_number
    --   ,"from"
    --   ,to
       ,tokenId as token_id
       ,NULL as buyer
       ,NULL as seller
       ,'Burn' as evt_type
       ,CAST(1 AS DECIMAL(38,0)) AS number_of_items
       ,NULL as trade_type
       ,NULL as trade_category
       ,'BNB' as currency_symbol
       ,'0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c' as currency_contract
       ,'0x836eb8202d4bc2ed14d1d2861e441c69228155cc' AS nft_contract_address  -- generic contract address for all nfts on NFTb; no individual contract addreses
       ,"from" as royalty_fee_receive_address
       ,CAST(0 as DECIMAL(38,0)) AS amount_raw
       ,CAST (0 as DOUBLE) as royalty_fee_percentage
FROM {{source('nftb_bnb', 'NFT_evt_Transfer')}} tr
WHERE 1=1
AND to='0x0000000000000000000000000000000000000000'
AND  tr.evt_block_time >= '{{project_start_date}}'
{% if is_incremental() %}
AND tr.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}

)

,trades as
(SELECT  tr.contract_address as project_contract_address
       ,tr.evt_tx_hash as tx_hash
       ,tr.evt_index
       ,tr.evt_block_time as block_time
       ,tr.evt_block_number as block_number
     -- ,tr."from"
      --,tr.to
       ,tr.tokenId as token_id
       ,tr.to as buyer
       ,CASE WHEN tr.from= '0xdc2f08364ebc6cebe0b487fc47823b1e83ce8550' --- claim contract address
             THEN CONCAT('0x',substr(l1.topic3, 27, 40))
             WHEN tr.from= '0xe22c90e7816db4344f33c651c7b0a01fcd51a327' -- buy function contract address (explicitly stated)
             THEN CONCAT('0x',substr(l2.topic3, 27, 40))
             WHEN tr.from='0xebd4232e4c1999bc9562802eae01b431d5053e65' -- auction settled contract address (withdraw function)
             THEN CONCAT('0x',substr(l3.topic3, 27, 40))
             ELSE tr.from
        END  as seller

       ,'Trade' as evt_type
       ,CAST(1 AS DECIMAL(38,0)) AS number_of_items
       ,'Single Item Trade' as trade_type
       ,CASE WHEN tr.from='0xebd4232e4c1999bc9562802eae01b431d5053e65'
              THEN 'Auction Settled'
              WHEN tr.from='0xdc2f08364ebc6cebe0b487fc47823b1e83ce8550'
              THEN 'Claim'
              ELSE 'Buy' END as trade_category
       ,'BNB' as currency_symbol
       ,'0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c' as currency_contract
       ,'0x836eb8202d4bc2ed14d1d2861e441c69228155cc' AS nft_contract_address
       ,CAST (element_at(r.output_0, 1) AS STRING) as royalty_fee_receive_address
       ,CASE WHEN tr1.value=0 AND tra1.address='0xebd4232e4c1999bc9562802eae01b431d5053e65' --seller contract address with withdraw function is called (auction settled)
             THEN tra1.value
             WHEN tr1.value=0 AND tra1.address='0xdc2f08364ebc6cebe0b487fc47823b1e83ce8550' -- seller contract address with claim function is called
             THEN tra1.value
             ELSE tr1.value END AS amount_raw
       ,CASE WHEN cardinality(r.output_1)>1 THEN CAST (10 AS DOUBLE)
              WHEN cardinality(r.output_1)=1 THEN CAST (element_at(r.output_1, 1) AS DOUBLE)
              ELSE CAST (0 AS DOUBLE)
              END AS royalty_fee_percentage

FROM {{ source('nftb_bnb', 'NFT_evt_Transfer')}} tr
INNER JOIN {{ source('nftb_bnb', 'NFT_call_royaltyInfo')}} r
ON r.call_tx_hash=tr.evt_tx_hash
INNER JOIN {{ source('bnb','transactions')}} as tr1
ON tr1.hash=tr.evt_tx_hash
{% if not is_incremental() %}
AND tr1.block_time >= '{{project_start_date}}'
{% endif %}
{% if is_incremental() %}
AND tr1.block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}
LEFT JOIN (SELECT
                   bt.from as address
                  ,tx_hash
                  ,SUM(CAST (value AS DECIMAL(38,0))) AS value
            FROM {{ source ('bnb','traces')}} bt
            WHERE 1=1
            AND (bt.from='0xebd4232e4c1999bc9562802eae01b431d5053e65' --seller contract address when withdraw function is called
            OR
            bt.from='0xdc2f08364ebc6cebe0b487fc47823b1e83ce8550'-- seller contract address when claim function is called
            )
            AND bt.input='0x'
            AND bt.call_type='call'
            {% if not is_incremental() %}
            AND bt.block_time >= '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND bt.block_time >= date_trunc("day", now() - interval '1 week')
            {% endif %}
            GROUP BY 1,2
            ) tra1
ON tra1.tx_hash=tr.evt_tx_hash
LEFT JOIN {{ source('bnb','logs')}} l1
ON l1.tx_hash=r.call_tx_hash
AND l1.contract_address='0xdc2f08364ebc6cebe0b487fc47823b1e83ce8550'
AND l1.topic1='0x7a9edcf72fda2a090305f15fe2f1d8d881c849c4a142e8847734fe93542c64ef' --func sig for claim function call
{% if not is_incremental() %}
AND l1.block_time >= '{{project_start_date}}'
{% endif %}
{% if is_incremental() %}
AND l1.block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}

LEFT JOIN {{ source('bnb','logs')}} l2
ON l2.tx_hash=r.call_tx_hash
AND l2.contract_address='0xe22c90e7816db4344f33c651c7b0a01fcd51a327'
AND l2.topic1='0x7df6bafa53a0e01e6995efd8c0c627e532d2fb130178b2261d619f256db0d65a' --func sig for buy function call
{% if not is_incremental() %}
AND l2.block_time >= '{{project_start_date}}'
{% endif %}
{% if is_incremental() %}
AND l2.block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}

LEFT JOIN {{ source('bnb','logs')}} l3
ON l3.tx_hash=r.call_tx_hash
AND l3.contract_address='0xebd4232e4c1999bc9562802eae01b431d5053e65'
AND l3.topic1='0x3966e47923a4243aaa12fcf3bb231f645e3c8c5e70985cd00c689ec364cf4da0' --func sig for withdraw function call (Auction Settled)
{% if not is_incremental() %}
AND l3.block_time >= '{{project_start_date}}'
{% endif %}
{% if is_incremental() %}
AND l3.block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}

WHERE tr.evt_block_time >= '{{project_start_date}}'
{% if is_incremental() %}
AND tr.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}
)

,all_events AS
(SELECT * FROM mints
UNION ALL
SELECT * FROM burns
UNION ALL
SELECT * FROM trades
)


SELECT 'bnb' as blockchain
      ,'nftb' as project
      ,'v1' as version
      ,ae.block_time as block_time
      ,date_trunc('day', ae.block_time) as block_date
      ,ae.block_number as block_number
      ,ae.token_id
      ,nft_token.name  as collection
      ,amount_raw
      ,amount_raw/pow(10,p.decimals) as amount_original
      ,(amount_raw/pow(10,p.decimals)) * p.price as amount_usd
      ,CASE
            WHEN erc721.evt_index IS NOT NULL THEN 'erc721'
            ELSE 'erc1155'
        END as token_standard
        ,ae.trade_type
        ,ae.number_of_items
        ,ae.trade_category
        ,ae.evt_type
        ,ae.buyer
        ,ae.seller
        ,ae.currency_symbol
        ,ae.currency_contract
        ,ae.nft_contract_address
        ,ae.project_contract_address

       , agg.name as aggregator_name
       , agg.contract_address as aggregator_address
        ,ae.tx_hash
        ,btx.from as tx_from
        ,btx.to as tx_to


        ,CAST (0.1 *(ae.amount_raw -(ae.royalty_fee_percentage/100 * ae.amount_raw)) AS DOUBLE) as platform_fee_amount_raw
        ,CAST (0.1 * (ae.amount_raw -(ae.royalty_fee_percentage/100 * ae.amount_raw))/POW(10,p.decimals) AS DOUBLE) as platform_fee_amount
        ,CAST ((0.1 * (ae.amount_raw -(ae.royalty_fee_percentage/100 * ae.amount_raw))/POW(10,p.decimals)) * p.price AS DOUBLE) as platform_fee_amount_usd
        ,CAST ((0.1 * (ae.amount_raw-(ae.royalty_fee_percentage/100 * ae.amount_raw))/ae.amount_raw) * 100 AS DOUBLE)  as platform_fee_percentage

        ,ae.royalty_fee_percentage/100 * ae.amount_raw as royalty_fee_amount_raw
        ,(ae.royalty_fee_percentage/100 * ae.amount_raw)/pow(10,p.decimals) as royalty_fee_amount
        ,(ae.royalty_fee_percentage/100 * ae.amount_raw)/pow(10,p.decimals) * p.price as royalty_fee_amount_usd
        ,ae.royalty_fee_percentage
        ,'BNB' as royalty_fee_currency_symbol
        ,COALESCE(ae.royalty_fee_receive_address,ae.seller) as royalty_fee_receive_address
        ,ae.evt_index
        ,'bnb-nftb-v1' || '-' || ae.block_number || '-' || ae.tx_hash || '-' ||  ae.evt_index AS unique_trade_id
FROM all_events ae

INNER JOIN {{ source('bnb','transactions') }} btx
ON btx.block_time = ae.block_time
AND btx.hash = ae.tx_hash
{% if not is_incremental() %}
AND btx.block_time >= '{{project_start_date}}'
{% endif %}
{% if is_incremental() %}
AND btx.block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}

LEFT JOIN {{ source('prices','usd') }} p
ON p.blockchain = 'bnb'
AND p.minute = date_trunc('minute', ae.block_time)
AND p.contract_address = '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'
{% if not is_incremental() %}
AND p.minute >= '{{project_start_date}}'
{% endif %}
{% if is_incremental() %}
AND p.minute >= date_trunc("day", now() - interval '1 week')
{% endif %}

LEFT JOIN {{ source('erc721_ethereum','evt_transfer') }} erc721
ON erc721.evt_block_time = ae.block_time
AND erc721.evt_tx_hash = ae.tx_hash
AND erc721.contract_address = ae.nft_contract_address
AND erc721.tokenId = ae.token_id
AND erc721.to = ae.buyer
{% if not is_incremental() %}
AND erc721.evt_block_time >= '{{project_start_date}}'
{% endif %}
{% if is_incremental() %}
AND erc721.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}


LEFT JOIN {{ ref('tokens_bnb_nft_legacy') }} nft_token
ON nft_token.contract_address = ae.nft_contract_address

LEFT JOIN {{ ref('nft_bnb_aggregators_legacy')}} agg
ON agg.contract_address = btx.`to`






