{{ config(
    schema = 'nftb_bnb',
    alias = 'base_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_hash','sub_tx_trade_id']
    )
}}

{%- set project_start_date = '2021-04-30' %}


WITH trades as
(SELECT  tr.contract_address as project_contract_address
       ,tr.evt_tx_hash as tx_hash
       ,tr.evt_index
       ,tr.evt_block_time as block_time
       ,tr.evt_block_number as block_number
     -- ,tr."from"
      --,tr.to
       ,tr.tokenId as token_id
       ,tr.to as buyer
       ,CASE WHEN tr."from"= 0xdc2f08364ebc6cebe0b487fc47823b1e83ce8550 --- claim contract address
             THEN bytearray_substring(l1.topic2,13,20)
             WHEN tr."from"= 0xe22c90e7816db4344f33c651c7b0a01fcd51a327 -- buy function contract address (explicitly stated)
             THEN bytearray_substring(l2.topic2,13,20)
             WHEN tr."from"=0xebd4232e4c1999bc9562802eae01b431d5053e65 -- auction settled contract address (withdraw function)
             THEN bytearray_substring(l3.topic2,13,20)
             ELSE tr."from"
        END as seller

       ,'Trade' as evt_type
       ,uint256 '1' AS number_of_items
       ,'secondary' as trade_type
       ,CASE WHEN tr."from"=0xebd4232e4c1999bc9562802eae01b431d5053e65
              THEN 'Auction Settled'
              WHEN tr."from"=0xdc2f08364ebc6cebe0b487fc47823b1e83ce8550
              THEN 'Claim'
              ELSE 'Buy' END as trade_category
       ,'BNB' as currency_symbol
       ,0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c as currency_contract
       ,0x836eb8202d4bc2ed14d1d2861e441c69228155cc AS nft_contract_address
       ,element_at(r.output_0, 1) as royalty_fee_receive_address
       ,CASE WHEN tr1.value=0 AND tra1.address=0xebd4232e4c1999bc9562802eae01b431d5053e65 --seller contract address with withdraw function is called (auction settled)
             THEN tra1.value
             WHEN tr1.value=0 AND tra1.address=0xdc2f08364ebc6cebe0b487fc47823b1e83ce8550 -- seller contract address with claim function is called
             THEN tra1.value
             ELSE cast(tr1.value as uint256) END AS amount_raw
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
AND tr1.block_time >= TIMESTAMP '{{project_start_date}}'
{% endif %}
{% if is_incremental() %}
AND tr1.block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}
LEFT JOIN (SELECT
                   bt."from" as address
                  ,tx_hash
                  ,cast(SUM(value) as uint256) AS value
            FROM {{ source ('bnb','traces')}} bt
            WHERE 1=1
            AND (bt."from"=0xebd4232e4c1999bc9562802eae01b431d5053e65 --seller contract address when withdraw function is called
            OR
            bt."from"=0xdc2f08364ebc6cebe0b487fc47823b1e83ce8550 -- seller contract address when claim function is called
            )
            AND bt.input=0x
            AND bt.call_type='call'
            {% if not is_incremental() %}
            AND bt.block_time >= TIMESTAMP '{{project_start_date}}'
            {% endif %}
            {% if is_incremental() %}
            AND bt.block_time >= date_trunc('day', now() - interval '7' day)
            {% endif %}
            GROUP BY 1,2
            ) tra1
ON tra1.tx_hash=tr.evt_tx_hash
LEFT JOIN {{ source('bnb','logs')}} l1
ON l1.tx_hash=r.call_tx_hash
AND l1.contract_address=0xdc2f08364ebc6cebe0b487fc47823b1e83ce8550
AND l1.topic0=0x7a9edcf72fda2a090305f15fe2f1d8d881c849c4a142e8847734fe93542c64ef --func sig for claim function call
{% if not is_incremental() %}
AND l1.block_time >= TIMESTAMP '{{project_start_date}}'
{% endif %}
{% if is_incremental() %}
AND l1.block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}

LEFT JOIN {{ source('bnb','logs')}} l2
ON l2.tx_hash=r.call_tx_hash
AND l2.contract_address=0xe22c90e7816db4344f33c651c7b0a01fcd51a327
AND l2.topic0=0x7df6bafa53a0e01e6995efd8c0c627e532d2fb130178b2261d619f256db0d65a --func sig for buy function call
{% if not is_incremental() %}
AND l2.block_time >= TIMESTAMP '{{project_start_date}}'
{% endif %}
{% if is_incremental() %}
AND l2.block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}

LEFT JOIN {{ source('bnb','logs')}} l3
ON l3.tx_hash=r.call_tx_hash
AND l3.contract_address=0xebd4232e4c1999bc9562802eae01b431d5053e65
AND l3.topic0=0x3966e47923a4243aaa12fcf3bb231f645e3c8c5e70985cd00c689ec364cf4da0 --func sig for withdraw function call (Auction Settled)
{% if not is_incremental() %}
AND l3.block_time >= TIMESTAMP '{{project_start_date}}'
{% endif %}
{% if is_incremental() %}
AND l3.block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}

WHERE tr.evt_block_time >= TIMESTAMP '{{project_start_date}}'
{% if is_incremental() %}
AND tr.evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}
)

,base_trades as (

SELECT 'bnb' as blockchain
    ,'nftb' as project
    ,'v1' as project_version
    ,ae.block_time as block_time
    ,cast(date_trunc('day', ae.block_time) as date) as block_date
    ,cast(date_trunc('month', ae.block_time) as date) as block_month
    ,ae.block_number as block_number
    ,ae.token_id as nft_token_id
    ,amount_raw as price_raw
    ,ae.trade_type
    ,ae.number_of_items as nft_amount
    ,ae.trade_category
    ,ae.buyer
    ,ae.seller
    ,ae.currency_contract
    ,ae.nft_contract_address
    ,ae.project_contract_address
    ,ae.tx_hash
    ,CAST (0.1 *(ae.amount_raw -(ae.royalty_fee_percentage/100 * ae.amount_raw)) AS uint256) as platform_fee_amount_raw
    ,cast(ae.royalty_fee_percentage/100 * ae.amount_raw as uint256) as royalty_fee_amount_raw
    ,COALESCE(ae.royalty_fee_receive_address,ae.seller) as royalty_fee_address
    ,cast(null as varbinary) as platform_fee_address
    ,ae.evt_index as sub_tx_trade_id
FROM trades ae
)
-- this will be removed once tx_from and tx_to are available in the base event tables
{{ add_nft_tx_data('base_trades', 'bnb') }}



