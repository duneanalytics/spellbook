-- Tofu NFT trades (re-usable macro for all chains)
{% macro tofu_v1_events(blockchain,MarketNG_call_run, MarketNG_evt_EvInventoryUpdate, raw_transactions, project_start_date, NATIVE_ERC20_REPLACEMENT, NATIVE_SYMBOL_REPLACEMENT) %}

WITH tff AS (
    SELECT call_block_time,
           call_tx_hash,
           fee_rate,
           royalty_rate,
           fee_address,
           royalty_address,
           bundle_size,
           from_hex(json_extract_scalar(bundle_item, '$.token'))   as token,
           cast(json_extract_scalar(bundle_item, '$.tokenId') as uint256) as token_id,
           cast(json_extract_scalar(bundle_item, '$.amount') as uint256)  as amount,
           bundle_index,
           row_number() over (partition by call_tx_hash order by bundle_index) as ordering
    FROM (SELECT call_block_time,
                 call_tx_hash,
                 cast(json_extract_scalar(json_extract_scalar(detail, '$.settlement'), '$.feeRate') as uint256)/ pow(10,6)     as fee_rate,
                 cast(json_extract_scalar(json_extract_scalar(detail, '$.settlement'), '$.royaltyRate') as uint256)/ pow(10,6)  as royalty_rate,
                 from_hex(json_extract_scalar(json_extract_scalar(detail, '$.settlement'), '$.feeAddress'))            as fee_address,
                 from_hex(json_extract_scalar(json_extract_scalar(detail, '$.settlement'), '$.royaltyAddress'))        as royalty_address,
                 cardinality(cast(json_extract(detail, '$.bundle') as ARRAY<VARCHAR>))                             as bundle_size
                 ,bundle_item
                 ,bundle_index
          FROM {{ MarketNG_call_run }}
          CROSS JOIN unnest(cast(json_extract(detail, '$.bundle') as ARRAY<VARCHAR>)) WITH ORDINALITY AS foo(bundle_item,bundle_index)
          WHERE call_success = true
              {% if is_incremental() %}
              and call_block_time >= date_trunc('day', now() - interval '7' day)
              {% endif %}
         ) as tmp
),
     tfe as (
         select evt_tx_hash,
                evt_block_time,
                evt_block_number,
                evt_index,
                from_hex(json_extract_scalar(inventory, '$.seller'))   as seller,
                from_hex(json_extract_scalar(inventory, '$.buyer'))    as buyer,
                json_extract_scalar(inventory, '$.kind')     as kind,
                cast(json_extract_scalar(inventory, '$.price') as uint256)    as price,
                CASE WHEN from_hex(json_extract_scalar(inventory, '$.currency')) = 0x0000000000000000000000000000000000000000
                  THEN {{NATIVE_ERC20_REPLACEMENT}}
                  ELSE from_hex(json_extract_scalar(inventory, '$.currency'))
                END as currency,
                (from_hex(json_extract_scalar(inventory, '$.currency')) = 0x0000000000000000000000000000000000000000) as native_eth,
                contract_address,
                row_number() over (partition by evt_tx_hash order by evt_index) as ordering
         from {{ MarketNG_evt_EvInventoryUpdate }}
         where json_extract_scalar(inventory, '$.status') = '1'
              {% if is_incremental() %}
              and evt_block_time >= date_trunc('day', now() - interval '7' day)
              {% endif %}
     )
SELECT '{{blockchain}}'                                 as blockchain
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
     , tff.amount    as number_of_items
     , 'Trade'                               as evt_type
     , tfe.seller                            as seller
     , tfe.buyer                             as buyer
     , case
           when tfe.kind = '1' then 'Buy'
           when tfe.kind = '2' then 'Sell'
           else 'Auction'
    end                                      as trade_category
     , tfe.price     as amount_raw
     , tfe.price / power(10, pu.decimals)    as amount_original
     , pu.price * tfe.price / power(10, pu.decimals) as amount_usd
     , case
           when tfe.native_eth THEN '{{NATIVE_SYMBOL_REPLACEMENT}}'
           else pu.symbol
       end                                                                          as currency_symbol
     , tfe.currency                                                                 as currency_contract
     , tfe.contract_address                                                         as project_contract_address
     , tff.token                                                                    as nft_contract_address
     , agg.name                                                                     as aggregator_name
     , agg.contract_address                                                         as aggregator_address
     , tfe.evt_tx_hash                                                              as tx_hash
     , tx."from"                                                                    as tx_from
     , tx.to                                                                        as tx_to
     , CAST(tfe.price * tff.fee_rate AS uint256)                                    as platform_fee_amount_raw
     , CAST(tfe.price * tff.fee_rate / power(10, pu.decimals) AS DOUBLE)            as platform_fee_amount
     , CAST(pu.price * tfe.price * tff.fee_rate / power(10, pu.decimals) AS DOUBLE) as platform_fee_amount_usd
     , CAST(100 * tff.fee_rate AS DOUBLE)                                           as platform_fee_percentage
     , CAST(tfe.price * tff.royalty_rate  AS uint256)                               as royalty_fee_amount_raw
     , tfe.price * tff.royalty_rate / power(10, pu.decimals)                        as royalty_fee_amount
     , pu.price * tfe.price * tff.royalty_rate / power(10, pu.decimals)             as royalty_fee_amount_usd
     , CAST(100 * tff.royalty_rate AS DOUBLE)                                       as royalty_fee_percentage
     , tff.royalty_address                                                          as royalty_fee_receive_address
     , case
           when tfe.native_eth THEN 'ARETH'
           else pu.symbol
      end                                    as royalty_fee_currency_symbol
    , tff.bundle_index
    , concat('{{blockchain}}-tofu-v1-', cast(tfe.evt_tx_hash as varchar), cast(tfe.evt_index as varchar), cast(tff.bundle_index as varchar)) as unique_trade_id
    , tfe.evt_index
FROM tfe
         INNER JOIN tff
              ON tfe.evt_tx_hash = tff.call_tx_hash
                  AND tfe.evt_block_time = tff.call_block_time
                  AND tfe.ordering = tff.ordering
         LEFT JOIN {{ raw_transactions }} tx
                   ON tx.block_time = tfe.evt_block_time
                       AND tx.hash = tfe.evt_tx_hash
                       {% if not is_incremental() %}
                       AND tx.block_time >= {{project_start_date}}
                       {% endif %}
                       {% if is_incremental() %}
                       and tx.block_time >= date_trunc('day', now() - interval '7' day)
                       {% endif %}
         LEFT JOIN {{ ref('tokens_nft') }} nft
                   ON nft.blockchain = '{{blockchain}}'
                   AND tff.token = nft.contract_address
         LEFT JOIN {{ source('prices', 'usd') }} pu
                   ON pu.blockchain = '{{blockchain}}'
                       AND pu.minute = date_trunc('minute', tfe.evt_block_time)
                       AND pu.contract_address = tfe.currency
                       {% if not is_incremental() %}
                       AND pu.minute >= {{project_start_date}}
                       {% endif %}
                       {% if is_incremental() %}
                       AND pu.minute >= date_trunc('day', now() - interval '7' day)
                       {% endif %}
         LEFT JOIN {{ ref('nft_aggregators')}} agg
                   ON agg.contract_address = tx."to"
                   AND agg.blockchain = '{{blockchain}}'
{% endmacro %}
