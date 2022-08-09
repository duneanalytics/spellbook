CREATE OR REPLACE FUNCTION opensea.insert_trades (p_start_ts timestamptz, p_end_ts timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN

with iv_raw as (
    select a.call_block_time as block_time
          ,concat('\x',substr((a."rightOrder"->'makerAssetData')::text,36,40))::bytea as nft_contract_address
          ,case when length(a."rightOrder"->>'makerAssetData') = 650 then bytea2numeric(concat('\x00',substr((a."rightOrder"->'makerAssetData')::text,332,64))::bytea)::text
                else bytea2numeric(concat('\x',substr((a."rightOrder"->'makerAssetData')::text,76,64))::bytea)::text
           end as nft_token_id
          ,case when length(a."rightOrder"->>'makerAssetData') = 650 then 'erc1155'
                else 'erc721' -- 138
           end as erc_standard
          ,'Single Item Trade' as trade_type
          ,(a."rightOrder"->>'makerAddress')::bytea as seller
          ,(a."leftOrder"->>'makerAddress')::bytea as buyer
          ,"paymentTokenAddress" as currency_contract
          ,least("output_matchedFillResults"->'left'->'takerFeePaid', "output_matchedFillResults"->'right'->'makerFeePaid')::numeric as nft_item_count
          ,least("output_matchedFillResults"->'left'->'makerFeePaid', "output_matchedFillResults"->'right'->'takerFeePaid')::numeric as original_amount_raw
          ,("feeData"->0->>'recipient')::bytea as fee_receive_address
          ,("feeData"->0->>'paymentTokenAmount')::numeric as fee_amount_raw
          ,("feeData"->1->>'recipient')::bytea as royalty_receive_address
          ,("feeData"->1->>'paymentTokenAmount')::numeric as royalty_amount_raw
          ,a.call_tx_hash as tx_hash
          ,a.call_block_number as block_number
          ,a.contract_address as exchange_contract_address
          ,a.call_trace_address
      from opensea_polygon_v2."ZeroExFeeWrapper_call_matchOrders" a
     where 1=1
       and call_success
       and a.call_block_time >= p_start_ts
       and a.call_block_time < p_end_ts
)
,rows as (
    INSERT INTO opensea.trades (
           block_time
          ,nft_contract_address
          ,nft_token_id
          ,erc_standard
          ,platform
          ,platform_version
          ,trade_type
          ,number_of_items
          ,seller
          ,buyer
          ,currency_contract
          ,original_currency
          ,original_amount
          ,original_amount_raw
          ,usd_amount
          ,fee_receive_address
          ,fee_amount
          ,fee_amount_raw
          ,fee_usd_amount
          ,royalty_receive_address
          ,royalty_amount
          ,royalty_amount_raw
          ,royalty_usd_amount
          ,exchange_contract_address
          ,block_number
          ,tx_hash
          ,tx_from
          ,tx_to
          ,trade_id
    )
    select a.block_time 
          ,nft_contract_address
          ,nft_token_id
          ,erc_standard
          ,'OpenSea' as platform
          ,'1' as platform_version
          ,trade_type
          ,nft_item_count as number_of_items
          ,seller
          ,buyer
          ,currency_contract
          ,p.symbol as origianl_currency
          ,original_amount_raw / 10^p.decimals as original_amount
          ,original_amount_raw
          ,original_amount_raw / 10^p.decimals * p.price as usd_amount
          ,fee_receive_address
          ,fee_amount_raw / 10^p.decimals as fee_amount
          ,fee_amount_raw
          ,fee_amount_raw / 10^p.decimals * p.price as fee_usd_amount
          ,royalty_receive_address
          ,royalty_amount_raw / 10^p.decimals as royalty_amount
          ,royalty_amount_raw
          ,royalty_amount_raw / 10^p.decimals * p.price as royalty_usd_amount
          ,a.exchange_contract_address
          ,a.block_number
          ,a.tx_hash
          ,t."from" as tx_from
          ,t."to" as tx_to
          ,row_number() over (partition by a.tx_hash order by call_trace_address) as trade_id
      from iv_raw a
           inner join polygon.transactions t on t.hash = a.tx_hash
                                             and t.block_time >= p_start_ts
                                             and t.block_time < p_end_ts
           left join prices.usd p on p.minute = date_trunc('minute', a.block_time)
                                  and p.contract_address = a.currency_contract
                                  and p.minute >= p_start_ts
                                  and p.minute < p_end_ts
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- fill histroy
SELECT opensea.insert_trades('2021-06-01','2021-07-01');
SELECT opensea.insert_trades('2021-07-01','2021-08-01');
SELECT opensea.insert_trades('2021-08-01','2021-09-01');
SELECT opensea.insert_trades('2021-09-01','2021-10-01');
SELECT opensea.insert_trades('2021-10-01','2021-11-01');
SELECT opensea.insert_trades('2021-11-01','2021-12-01');
SELECT opensea.insert_trades('2021-12-01','2022-01-01');
SELECT opensea.insert_trades('2022-01-01','2022-02-01');
SELECT opensea.insert_trades('2022-02-01','2022-03-01');
SELECT opensea.insert_trades('2022-03-01','2022-04-01');
SELECT opensea.insert_trades('2022-04-01','2022-05-01');
SELECT opensea.insert_trades('2022-05-01','2022-06-01');
SELECT opensea.insert_trades('2022-06-01','2022-07-01');
SELECT opensea.insert_trades('2022-07-01','2022-08-01');
SELECT opensea.insert_trades('2022-08-01','2022-09-01');

-- cronjob
INSERT INTO cron.job (schedule, command)
VALUES ('*/20 * * * *', $$
    SELECT opensea.insert_trades(
            (SELECT MAX(block_time) - interval '6 hours' FROM opensea.trades WHERE platform='OpenSea' AND platform_version = '1')
           ,(SELECT NOW() - interval '20 minutes')
        );
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule; 
