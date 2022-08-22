CREATE OR REPLACE FUNCTION ovm2.insert_l1_gas_price_oracle_updates(start_block numeric, end_block numeric) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT INTO ovm2.l1_gas_price_oracle_updates (
        block_number,
        l1_gas_price,
        block_time
    )

	WITH gs AS (
	SELECT
	generate_series(start_block,end_block +1,1) AS bn
	)
	

	,updates AS (
		WITH oracle_reads  AS (
		SELECT block_number, "block_time",
		    bytea2numeric(data)/1e9 AS l1_gas_price
		    FROM optimism.logs l
		    WHERE topic1 ='\x351fb23757bb5ea0546c85b7996ddd7155f96b939ebaa5ff7bc49c75f27f2c44'
		    AND contract_address = '\x420000000000000000000000000000000000000f'
			AND block_number IN (SELECT bn FROM gs)
			AND block_number >= start_block

		UNION ALL
		SELECT 0,'11-11-2021'::date,1 --backfill block 1
		WHERE 1 >= start_block --only backfill if needed
		)
		, start_off AS ( --default first block to the most recent L1 Gas Price (handle for edge case of no updates)
		SELECT start_block-1 AS block_number, time AS block_time, --start number minus 1 since we increment it later
			(SELECT l1_gas_price FROM ovm2.l1_gas_price_oracle_updates
			    WHERE block_number <= start_block AND (l1_gas_price IS NOT NULL) ORDER BY block_number DESC LIMIT 1) AS l1_gas_price
		FROM optimism.blocks b
		WHERE b."number" = start_block
		AND NOT EXISTS (SELECT 1 FROM oracle_reads oru WHERE oru.block_number <= start_block) --if there's no block on or before the start
		)
		
	SELECT block_number, block_time, l1_gas_price FROM oracle_reads
	UNION ALL
	SELECT block_number, block_time, l1_gas_price FROM start_off

	)

	, events AS (
	SELECT
	block_number, "block_time",
	l1_gas_price,
	count(l1_gas_price) OVER (ORDER BY block_number) AS grp
	FROM (
	SELECT gs.bn AS block_number, "block_time",
	    CASE WHEN gs.bn = 1 THEN 1 ELSE l1_gas_price END AS l1_gas_price
	    FROM gs
	    LEFT JOIN updates u
	    ON gs.bn = u.block_number + 1  --add 1 since the new gas price takes effect in the next block
		AND u.l1_gas_price IS NOT NULL
	    ) p

	)

	--https://dba.stackexchange.com/questions/186218/carry-over-long-sequence-of-missing-values-with-postgres

	SELECT e_list.block_number, e_list.l1_gas_price, b.time --grab actual block time
	FROM (
	    SELECT block_number
		, first_value(l1_gas_price) OVER (PARTITION BY grp ORDER BY block_number) AS l1_gas_price
		, first_value(block_time) OVER (PARTITION BY grp ORDER BY block_number) AS block_time

		FROM events
		) e_list
	INNER JOIN optimism.blocks b
		ON b."number" = e_list.block_number
	
	WHERE (block_number IS NOT NULL) AND (l1_gas_price IS NOT NULL) AND (block_time IS NOT NULL)

	ON CONFLICT (block_number) DO UPDATE SET l1_gas_price = EXCLUDED.l1_gas_price, block_time = EXCLUDED.block_time
	RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- Get the table started
SELECT ovm2.insert_l1_gas_price_oracle_updates(1,1000)
WHERE NOT EXISTS (
    SELECT *
    FROM ovm2.l1_gas_price_oracle_updates
    WHERE block_number = 1000
);

SELECT ovm2.insert_l1_gas_price_oracle_updates(1000,100000)
WHERE NOT EXISTS (
    SELECT *
    FROM ovm2.l1_gas_price_oracle_updates
    WHERE block_number = 100000
);

INSERT INTO cron.job (schedule, command)
VALUES ('* * * * *', $$
 SELECT ovm2.insert_l1_gas_price_oracle_updates(
        (SELECT MAX(block_number) FROM ovm2.l1_gas_price_oracle_updates WHERE block_time > NOW() - interval '1 month'),
        (SELECT MAX(number) FROM optimism.blocks WHERE "time" > NOW() - interval '1 month')
        );
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
