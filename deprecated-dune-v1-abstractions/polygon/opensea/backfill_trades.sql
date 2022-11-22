CREATE OR REPLACE FUNCTION opensea.backfill_trades() RETURNS boolean
LANGUAGE plpgsql AS $function$
BEGIN

-- backfill usd_price, fee_usd_price, royalty_usd_amount
update opensea.trades
   set usd_amount = new.new_usd_amount
      ,fee_usd_amount = new.new_fee_usd_amount
      ,royalty_usd_amount = new.new_royalty_usd_amount
  from (select a.tx_hash
              ,a.trade_id
              ,a.original_amount_raw / 10^p.decimals * p.price as new_usd_amount
              ,a.fee_amount_raw / 10^p.decimals * p.price as new_fee_usd_amount
              ,a.royalty_amount_raw / 10^p.decimals * p.price as new_royalty_usd_amount
          from opensea.trades a
               inner join prices.usd p on p.contract_address = a.currency_contract
                            		   and p.minute = date_trunc('minute', a.block_time)
         where a.usd_amount is null
        ) as new
  where trades.tx_hash = new.tx_hash
    and trades.trade_id = new.trade_id
;

RETURN TRUE;
END
$function$;

-- historical fill
SELECT opensea.backfill_trades();

-- insert into cronjob
INSERT INTO cron.job (schedule, command)
VALUES ('*/35 * * * *', $$
    SELECT opensea.backfill_trades();
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;