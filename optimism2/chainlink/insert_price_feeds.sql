CREATE OR REPLACE FUNCTION chainlink.insert_price_feeds(start_block_time timestamptz, end_block_time timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT INTO chainlink.view_price_feeds (
        hour,
    	feed_name,
    	price,
    	proxy,
    	address,
    	underlying_token_address
    )
    
    -- code here

ON CONFLICT (hour,feed_name,proxy,address,underlying_token_address)
    DO UPDATE SET
        price = EXCLUDED.price
	
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;
