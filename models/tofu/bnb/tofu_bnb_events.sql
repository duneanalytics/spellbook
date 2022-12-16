{{ config(
    alias = 'events',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id'],
    post_hook='{{ expose_spells(\'["bnb"]\',
                                "project",
                                "tofu",
                                \'["theachenyj"]\') }}')
}}

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
           get_json_object(t, '$.amount')  as amount
    FROM (SELECT call_block_time,
                 call_tx_hash,
                 get_json_object(get_json_object(detail, '$.settlement'), '$.feeRate') / 1000000     as fee_rate,
                 get_json_object(get_json_object(detail, '$.settlement'), '$.royaltyRate') / 1000000 as royalty_rate,
                 get_json_object(get_json_object(detail, '$.settlement'), '$.feeAddress')            as fee_address,
                 get_json_object(get_json_object(detail, '$.settlement'), '$.royaltyAddress')        as royalty_address,
                 explode(from_json(get_json_object(detail, '$.bundle'), 'array<string>'))            as t,
                 json_array_length(get_json_object(detail, '$.bundle'))                              as bundle_size
          FROM {{ source('tofu_nft_bnb', 'MarketNG_call_run') }}
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
                get_json_object(inventory, '$.seller')   as seller,
                get_json_object(inventory, '$.buyer')    as buyer,
                get_json_object(inventory, '$.kind')     as kind,
                get_json_object(inventory, '$.price')    as price,
                get_json_object(inventory, '$.currency') as currency,
                contract_address
         from {{ source('tofu_nft_bnb', 'MarketNG_evt_EvInventoryUpdate') }}
         where get_json_object(inventory, '$.status') = '1'
              {% if is_incremental() %}
              and evt_block_time >= date_trunc("day", now() - interval '1 week')
              {% endif %}
     )
SELECT 'bnb'                                 as blockchain
     , 'tofu'                                as project
     , 'v1'                                  as version
     , date_trunc('day', tfe.evt_block_time) as block_date
     , tfe.evt_block_time                    as block_time
     , tfe.evt_block_number                  as block_number
     , tff.token_id                          as token_id
     , case
           when erct.evt_block_time is not null then 'bep721'
           else 'bep1155' end                as token_standard
     , nft.name                              as collection
     , case
           when tff.bundle_size = 1 then 'Single Item Trade'
           else 'Bundle Trade'
    end                                      as trade_type
     , tff.amount                            as number_of_items
     , 'Trade'                               as evt_type
     , tfe.seller                            as seller
     , tfe.buyer                             as buyer
     , case
           when tfe.kind = '1' then 'Sell'
           when tfe.kind = '2' then 'Buy'
           else 'Acution'
    end                                      as trade_category
     , tfe.price                         as amount_raw
     , case
           when tfe.currency = '0x0000000000000000000000000000000000000000'
               Then tfe.price / power(10, 18)
           else tfe.price / power(10, pu.decimals)
    end                                      as amount_original
     , case
           when tfe.currency = '0x0000000000000000000000000000000000000000'
               Then pu.price * tfe.price / power(10, 18)
           else pu.price * tfe.price / power(10, pu.decimals)
    end                                      as amount_usd
     , case
           when tfe.currency = '0x0000000000000000000000000000000000000000' THEN 'BNB'
           else pu.symbol
    end                                      as currency_symbol
     , tfe.currency                          as currency_contract
     , tfe.contract_address                  as project_contract_address
     , tff.token                             as nft_contract_address
     , agg.name                              as aggregator_name
     , agg.contract_address                  as aggregator_address
     , tfe.evt_tx_hash                       as tx_hash
     , tx.from                               as tx_from
     , tx.to                                 as tx_to
     , tfe.price * tff.fee_rate              as platform_fee_amount_raw
     , case
           when tfe.currency = '0x0000000000000000000000000000000000000000'
               Then tfe.price * tff.fee_rate / power(10, 18)
           else tfe.price * tff.fee_rate / power(10, pu.decimals)
    end                                      as platform_fee_amount
     , case
           when tfe.currency = '0x0000000000000000000000000000000000000000'
               Then pu.price * tfe.price * tff.fee_rate / power(10, 18)
           else pu.price * tfe.price * tff.fee_rate / power(10, pu.decimals)
    end                                      as platform_fee_amount_usd
     , tff.fee_rate                          as platform_fee_percentage
     , tfe.price * tff.royalty_rate          as royalty_fee_amount_raw
     , case
           when tfe.currency = '0x0000000000000000000000000000000000000000'
               Then tfe.price * tff.royalty_rate / power(10, 18)
           else tfe.price * tff.royalty_rate / power(10, pu.decimals)
    end                                      as royalty_fee_amount
     , case
           when tfe.currency = '0x0000000000000000000000000000000000000000'
               Then pu.price * tfe.price * tff.royalty_rate / power(10, 18)
           else pu.price * tfe.price * tff.royalty_rate / power(10, pu.decimals)
    end                                      as royalty_fee_amount_usd
     , tff.royalty_rate                      as royalty_fee_percentage
     , tff.fee_address                       as royalty_fee_receive_address
     , case
           when tfe.currency = '0x0000000000000000000000000000000000000000' THEN 'BNB'
           else pu.symbol
    end                                      as royalty_fee_currency_symbol
     , 'bnb-tofu-v1' || tfe.evt_tx_hash || '-' || tfe.seller || '-' || tfe.buyer || '-' || tff.token || '-' ||
       tff.token_id                          AS unique_trade_id
FROM tfe
         JOIN tff
              ON tfe.evt_tx_hash = tff.call_tx_hash
                  AND tff.call_block_time = tfe.evt_block_time
         LEFT JOIN {{ source('bnb', 'transactions') }} as tx
                   ON tx.block_time = tfe.evt_block_time
                       AND tx.hash = tfe.evt_tx_hash
                       {% if is_incremental() %}
                       and tx.block_time >= date_trunc("day", now() - interval '1 week')
                       {% endif %}
         LEFT JOIN {{ source('erc721_bnb', 'evt_Transfer') }} as erct
                   ON erct.evt_block_time = tfe.evt_block_time
                       AND tff.token = erct.contract_address
                       AND erct.evt_tx_hash = tfe.evt_tx_hash
                       AND tff.token_id = erct.tokenId
                       AND erct.from = tfe.seller
                       AND erct.to = tfe.buyer
                       {% if is_incremental() %}
                       and erct.evt_block_time >= date_trunc("day", now() - interval '1 week')
                       {% endif %}
         LEFT JOIN {{ ref('tokens_bnb_nft') }} AS nft
                   ON tff.token = nft.contract_address
         LEFT JOIN {{ source('prices', 'usd') }} as pu
                   ON pu.blockchain = 'bnb'
                       AND pu.minute = date_trunc('minute', tfe.evt_block_time)
                       AND (pu.contract_address = tfe.currency
                           OR (pu.contract_address = '0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c'
                               AND tfe.currency = '0x0000000000000000000000000000000000000000'))
                       {% if is_incremental() %}
                       AND pu.minute >= date_trunc("day", now() - interval '1 week')
                       {% endif %}
         LEFT JOIN {{ ref('nft_bnb_aggregators')}} as agg
                   ON agg.contract_address = tx.`to`
