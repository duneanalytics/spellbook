{{ config(
    schema = 'tofu_arbitrum',
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'bundle_index' ]
    )
}}

{%- set ARETH_ERC20_ADDRESS = '0x82af49447d8a07e3bd95bd0d56f35241523fbab1' %}
{% set project_start_date = '2021-12-09' %}

WITH tff AS (
    SELECT call_block_time,
           call_tx_hash,
           fee_rate,
           royalty_rate,
           fee_address,
           royalty_address,
           bundle_size,
           get_json_object(t, '$.token')   as token,
           get_json_object(t, '$.tokenId') as token_id,
           get_json_object(t, '$.amount')  as amount,
           i as bundle_index
    FROM (SELECT call_block_time,
                 call_tx_hash,
                 get_json_object(get_json_object(detail, '$.settlement'), '$.feeRate') / 1000000     as fee_rate,
                 get_json_object(get_json_object(detail, '$.settlement'), '$.royaltyRate') / 1000000 as royalty_rate,
                 get_json_object(get_json_object(detail, '$.settlement'), '$.feeAddress')            as fee_address,
                 get_json_object(get_json_object(detail, '$.settlement'), '$.royaltyAddress')        as royalty_address,
                 posexplode(from_json(get_json_object(detail, '$.bundle'), 'array<string>'))         as (i,t),
                 json_array_length(get_json_object(detail, '$.bundle'))                              as bundle_size
          FROM {{ source('tofunft_arbitrum', 'MarketNG_call_run') }}
          WHERE call_success = true
              {% if is_incremental() %}
              and call_block_time >= date_trunc("day", now() - interval '1 week')
              {% endif %}
         ) as tmp
),
     tfe as (
         select evt_tx_hash,
                evt_block_time,
                evt_block_number,
                evt_index,
                get_json_object(inventory, '$.seller')   as seller,
                get_json_object(inventory, '$.buyer')    as buyer,
                get_json_object(inventory, '$.kind')     as kind,
                get_json_object(inventory, '$.price')    as price,
                CASE WHEN get_json_object(inventory, '$.currency') = '0x0000000000000000000000000000000000000000'
                  THEN '{{ARETH_ERC20_ADDRESS}}'
                  ELSE get_json_object(inventory, '$.currency')
                END as currency,
                (get_json_object(inventory, '$.currency') = '0x0000000000000000000000000000000000000000') as native_eth,
                contract_address
         from {{ source('tofunft_arbitrum', 'MarketNG_evt_EvInventoryUpdate') }}
         where get_json_object(inventory, '$.status') = '1'
              {% if is_incremental() %}
              and evt_block_time >= date_trunc("day", now() - interval '1 week')
              {% endif %}
     )
SELECT 'arbitrum'                                 as blockchain
     , 'tofu'                                as project
     , 'v1'                                  as version
     , date_trunc('day', tfe.evt_block_time) as block_date
     , tfe.evt_block_time                    as block_time
     , tfe.evt_block_number                  as block_number
     , tff.token_id                          as token_id
     , nft.standard                          as token_standard
     , nft.name                              as collection
     , case
           when tff.bundle_size = 1 then 'Single Item Trade'
           else 'Bundle Trade'
    end                                      as trade_type
     , CAST(tff.amount AS DECIMAL(38,0))     as number_of_items
     , 'Trade'                               as evt_type
     , tfe.seller                            as seller
     , tfe.buyer                             as buyer
     , case
           when tfe.kind = '1' then 'Buy'
           when tfe.kind = '2' then 'Sell'
           else 'Auction'
    end                                      as trade_category
     , CAST(tfe.price AS DECIMAL(38,0))      as amount_raw
     , tfe.price / power(10, pu.decimals)    as amount_original
     , pu.price * tfe.price / power(10, pu.decimals) as amount_usd
     , case
           when tfe.native_eth THEN 'ARETH'
           else pu.symbol
       end                                                                          as currency_symbol
     , tfe.currency                                                                 as currency_contract
     , tfe.contract_address                                                         as project_contract_address
     , tff.token                                                                    as nft_contract_address
     , agg.name                                                                     as aggregator_name
     , agg.contract_address                                                         as aggregator_address
     , tfe.evt_tx_hash                                                              as tx_hash
     , tx.from                                                                      as tx_from
     , tx.to                                                                        as tx_to
     , CAST(tfe.price * tff.fee_rate AS DOUBLE)                                     as platform_fee_amount_raw
     , CAST(tfe.price * tff.fee_rate / power(10, pu.decimals) AS DOUBLE)            as platform_fee_amount
     , CAST(pu.price * tfe.price * tff.fee_rate / power(10, pu.decimals) AS DOUBLE) as platform_fee_amount_usd
     , CAST(100 * tff.fee_rate AS DOUBLE)                                           as platform_fee_percentage
     , tfe.price * tff.royalty_rate                                                 as royalty_fee_amount_raw
     , tfe.price * tff.royalty_rate / power(10, pu.decimals)                        as royalty_fee_amount
     , pu.price * tfe.price * tff.royalty_rate / power(10, pu.decimals)             as royalty_fee_amount_usd
     , CAST(100 * tff.royalty_rate AS DOUBLE)                                       as royalty_fee_percentage
     , tff.royalty_address                                                          as royalty_fee_receive_address
     , case
           when tfe.native_eth THEN 'ARETH'
           else pu.symbol
      end                                    as royalty_fee_currency_symbol
    , tff.bundle_index
    , concat('arbitrum-tofu-v1-', tfe.evt_block_number, tfe.evt_tx_hash, tfe.evt_index, tff.bundle_index) as unique_trade_id
    , tfe.evt_index
FROM tfe
         INNER JOIN tff
              ON tfe.evt_tx_hash = tff.call_tx_hash
                  AND tfe.evt_block_time = tff.call_block_time
         LEFT JOIN {{ source('arbitrum', 'transactions') }} tx
                   ON tx.block_time = tfe.evt_block_time
                       AND tx.hash = tfe.evt_tx_hash
                       {% if not is_incremental() %}
                       AND tx.block_time >= '{{project_start_date}}'
                       {% endif %}
                       {% if is_incremental() %}
                       and tx.block_time >= date_trunc("day", now() - interval '1 week')
                       {% endif %}
         LEFT JOIN {{ ref('tokens_nft_legacy') }} nft
                   ON tff.token = nft.contract_address
                   AND nft.blockchain = 'arbitrum'
         LEFT JOIN {{ source('prices', 'usd') }} pu
                   ON pu.blockchain = 'arbitrum'
                       AND pu.minute = date_trunc('minute', tfe.evt_block_time)
                       AND pu.contract_address = tfe.currency
                       {% if not is_incremental() %}
                       AND pu.minute >= '{{project_start_date}}'
                       {% endif %}
                       {% if is_incremental() %}
                       AND pu.minute >= date_trunc("day", now() - interval '1 week')
                       {% endif %}
         LEFT JOIN {{ ref('nft_aggregators_legacy')}} agg
                   ON agg.contract_address = tx.`to`
                   AND agg.blockchain = 'arbitrum'
