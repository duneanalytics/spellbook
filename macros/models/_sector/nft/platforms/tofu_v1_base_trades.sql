-- Tofu NFT trades (re-usable macro for all chains)
{% macro tofu_v1_base_trades(blockchain,MarketNG_call_run, MarketNG_evt_EvInventoryUpdate, raw_transactions, project_start_date, NATIVE_ERC20_REPLACEMENT, NATIVE_SYMBOL_REPLACEMENT) %}

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
              {{incremental_predicate('call_block_time')}}
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
              {{incremental_predicate('evt_block_time')}}
              {% endif %}
     )

base_trades as (
SELECT '{{blockchain}}'                      as blockchain
     , 'tofu'                                as project
     , 'v1'                                  as project_version
     , tfe.evt_block_time                    as block_time
     , date_trunc('day',tfe.evt_block_time)   as block_date
     , date_trunc('month',tfe.evt_block_time) as block_month
     , tfe.evt_block_number                  as block_number
     , tfe.evt_tx_hash                       as tx_hash
     , tfe.contract_address                  as project_contract_address
     , case
           when tfe.kind = '1' then 'Buy'
           when tfe.kind = '2' then 'Sell'
           else 'Auction'
       end                                   as trade_category
     , 'secondary'                           as trade_type
     , tfe.buyer                             as buyer
     , tfe.seller                            as seller
     , tff.token                             as nft_contract_address
     , tff.token_id                          as nft_token_id
     , tff.amount                            as nft_amount
     , tfe.price                             as price_raw
     , tfe.currency                          as currency_contract
     , CAST(tfe.price * tff.fee_rate AS uint256)            as platform_fee_amount_raw
     , CAST(tfe.price * tff.royalty_rate  AS uint256)       as royalty_fee_amount_raw
     , cast(null as varbinary)                              as platform_fee_address
     , tff.royalty_address                                  as royalty_fee_address
     , row_number() over ( partition by tfe.evt_tx_hash order by tfe.evt_index, tff.bundle_index) as sub_tx_trade_id
    FROM tfe
    INNER JOIN tff
      ON tfe.evt_tx_hash = tff.call_tx_hash
          AND tfe.evt_block_time = tff.call_block_time
          AND tfe.ordering = tff.ordering
)
{{add_nft_tx_data('base_trades','arbitrum')}}
{% endmacro %}
