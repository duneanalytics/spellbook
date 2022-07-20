CREATE OR REPLACE FUNCTION seaport.backfill_transactions() RETURNS boolean
LANGUAGE plpgsql AS $function$
BEGIN

-- backfill usd_price, fee_usd_price, royalty_usd_amount
update seaport.transactions
   set usd_amount = new.new_usd_amount
      ,fee_usd_amount = new.new_fee_usd_amount
      ,royalty_usd_amount = new.new_royalty_usd_amount
  from (select a.tx_hash
              ,a.original_amount_raw / 10^t.decimals * p.price as new_usd_amount
              ,a.fee_amount_raw / 10^t.decimals * p.price as new_fee_usd_amount
              ,a.royalty_amount_raw / 10^t.decimals * p.price as new_royalty_usd_amount
          from seaport.transactions a
               inner join prices.usd p on p.contract_address = a.currency_contract
                            		   and date_trunc('minute', a.block_time) = p."minute"
                                       and p.minute >= '2022-05-15'
               inner join erc20.tokens t on t.contract_address = a.currency_contract
         where a.usd_amount is null
        ) as new
  where transactions.tx_hash = new.tx_hash
;

RETURN TRUE;
END
$function$;

-- historical fill
SELECT seaport.backfill_transactions();

-- insert into cronjob
INSERT INTO cron.job (schedule, command)
VALUES ('*/35 * * * *', $$
    SELECT seaport.backfill_transactions();
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;