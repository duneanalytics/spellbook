CREATE OR REPLACE FUNCTION ovm2.insert_get_contracts(start_blocktime timestamp, end_blocktime timestamp, creator_list bytea[] = NULL::bytea[]) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT INTO ovm2.get_contracts (
        contract_address,
      contract_project,
      token_symbol, 
      contract_name,
      creator_address, 
      created_time,
      contract_creator_if_factory
    )

--This is used for backfilling
WITH creator_rows AS (

SELECT UNNEST(creator_list::bytea[]) AS creators

)

, contract_creators AS (
SELECT COALESCE(a."from",w.creator_address) AS creator_address, w.project
FROM (
SELECT "from" FROM optimism."transactions" t
WHERE "to" IS NULL
AND success = true
AND t."block_time" >= start_blocktime
AND t."block_time" < end_blocktime 
AND 1 = (
    CASE WHEN NOT EXISTS (SELECT creators FROM creator_rows WHERE creators IS NOT NULL) THEN 1 --when no input, search everything
        WHEN t."from" IN (SELECT creators FROM creator_rows) THEN 1--when input, limit to these contracts (i.e. updated mapping)
        ELSE 0 END
        )
GROUP BY 1
) a
FULL OUTER JOIN ovm2.contract_creator_address_list w
ON w.creator_address = a."from"
WHERE 
(
(w.project != 'EXCLUDE') OR (w.project IS NULL)
)

AND 1 = (
    CASE WHEN NOT EXISTS (SELECT creators FROM creator_rows WHERE creators IS NOT NULL) THEN 1 --when no input, search everythin
        WHEN a."from" IN (SELECT creators FROM creator_rows) AND
            (w.creator_address IN (SELECT creators FROM creator_rows)
            OR w.creator_address IS NULL) THEN 1--Either a match or not in the static creator list
        ELSE 0 END
        )
)

,erc20_tokens AS (
SELECT e.contract_address, CASE WHEN tl.symbol IS NULL THEN 'Other ERC20' ELSE tl.symbol END AS symbol
FROM (
    SELECT contract_address FROM erc20."ERC20_evt_Transfer"
	WHERE "evt_block_time" >= start_blocktime - interval '1 day'
	AND "evt_block_time" < end_blocktime
	GROUP BY 1
    UNION ALL
    SELECT "contract_address" FROM erc20."tokens"
    ) e
LEFT JOIN erc20."tokens" tl
    ON tl."contract_address" = e."contract_address"
GROUP BY 1,2
)

,nft_tokens AS (
SELECT e.contract_address, CASE WHEN tl."project_name" IS NULL THEN fallback ELSE tl."project_name" END AS symbol
FROM (
    SELECT contract_address, 'Other ERC721' AS fallback FROM erc721."ERC721_evt_Transfer"
	WHERE "evt_block_time" >= start_blocktime - interval '1 day'
	AND "evt_block_time" < end_blocktime
	GROUP BY 1,2
    UNION ALL
    SELECT "contract_address", 'Other ERC721' AS fallback FROM erc721."tokens"
    UNION ALL
    SELECT contract_address, 'Other ERC1155' AS fallback FROM erc1155."ERC1155_evt_TransferBatch"
	WHERE "evt_block_time" >= start_blocktime - interval '1 day'
	AND "evt_block_time" < end_blocktime
	GROUP BY 1,2
    UNION ALL
    SELECT contract_address, 'Other ERC1155' AS fallback FROM erc1155."ERC1155_evt_TransferSingle" GROUP BY 1,2
    
    ) e
LEFT JOIN erc721."tokens" tl
    ON tl."contract_address" = e."contract_address"
GROUP BY 1,2
)

,get_contracts AS (
    WITH 
    creator_contracts AS
    (
    SELECT
    con.creator_address, con.contract_factory, contract_address, project, con.block_time AS created_time
    FROM (
        WITH base_level AS (
            SELECT "from" AS creator_address, NULL::bytea AS contract_factory, "address" AS contract_address, "block_time"
            FROM optimism.traces r
            WHERE "success" AND "tx_success"
            AND r."type" = 'create'
		AND r."block_time" >= start_blocktime
                AND r."block_time" < end_blocktime 
            )
        , second_level AS (
        SELECT
        COALESCE(b1.creator_address,b.creator_address) AS creator_address,
            CASE WHEN b1.creator_address IS NULL THEN NULL
            ELSE b.creator_address END AS contract_factory
        ,b.contract_address, b.block_time
            FROM base_level b
            LEFT JOIN base_level b1
                ON b.creator_address = b1.contract_address
        )
        , third_level AS (
        SELECT
        COALESCE(b1.creator_address,b.creator_address) AS creator_address,
        CASE WHEN b1.creator_address IS NULL THEN b.contract_factory
            ELSE b.creator_address END AS contract_factory
        ,b.contract_address, b.block_time
            FROM second_level b
            LEFT JOIN base_level b1
                ON b.creator_address = b1.contract_address
        )
        
        , fourth_level AS (
        SELECT
        COALESCE(b1.creator_address,b.creator_address) AS creator_address,
        CASE WHEN b1.creator_address IS NULL THEN b.contract_factory
            ELSE b.creator_address END AS contract_factory
        ,b.contract_address, b.block_time
            FROM third_level b
            LEFT JOIN base_level b1
                ON b.creator_address = b1.contract_address
        )
        
        , fifth_level AS (
        SELECT
        COALESCE(b1.creator_address,b.creator_address) AS creator_address,
        CASE WHEN b1.creator_address IS NULL THEN b.contract_factory
            ELSE b.creator_address END AS contract_factory
        ,b.contract_address, b.block_time
            FROM fourth_level b
            LEFT JOIN base_level b1
                ON b.creator_address = b1.contract_address
        
            )
        
        SELECT * FROM fifth_level -- check for contract factories 4 layers down --as of 3/12, this ran through all contracts
    ) con
    LEFT JOIN contract_creators cc
    ON con.creator_address = cc.creator_address
 WHERE contract_address IS NOT NULL   
    
    )

SELECT
COALESCE(c.contract_address,erc20.contract_address,snx.address) AS contract_address,
    contract_factory,
    CASE WHEN snx.address IS NOT NULL THEN 'Synthetix' ELSE
        INITCAP(REPLACE(c.project,'_',' '))
    END AS contract_project,
    COALESCE(erc20.symbol,nft.symbol) AS erc20_symbol, COALESCE(c.contract_name,snx.contract_name) AS contract_name, creator_address,
    created_time
    
FROM (
    SELECT cc.creator_address, contract_factory, cc.contract_address, COALESCE(cc.project, oc.namespace) AS project, oc.name AS contract_name, cc.created_time FROM creator_contracts cc
        LEFT JOIN optimism."contracts" oc
            ON oc."address" = cc.contract_address
	WHERE 1 = ( --1 if we're re-running, 0 if it already exists
                CASE
			WHEN NOT EXISTS (SELECT creators FROM creator_rows WHERE creators IS NOT NULL)
			AND NOT EXISTS (SELECT 1 FROM ovm2.get_contracts WHERE contract_address = address) 
			THEN 1 --when no input or doesn't already exist, search everything
                WHEN cc.creator_address IN (SELECT creators FROM creator_rows) THEN 1--when input, limit to these contracts (i.e. updated mapping)
                ELSE 0 END
                )

    UNION ALL --other decoded contracts
    SELECT NULL::bytea AS creator_address, NULL::bytea AS contract_factory, "address" AS contract_address, namespace AS project, name, created_at AS contract_name
    FROM optimism."contracts"
    WHERE "address" NOT IN (SELECT contract_address FROM creator_contracts)-- WHERE address IS NOT NULL)
	AND 1 = ( --1 if we're re-running, 0 if it already exists
                CASE
			WHEN NOT EXISTS (SELECT creators FROM creator_rows WHERE creators IS NOT NULL)
			AND NOT EXISTS (SELECT 1 FROM ovm2.get_contracts WHERE contract_address = address) 
			THEN 1 --when no input or doesn't already exist, search everything
                WHEN cc.creator_address IN (SELECT creators FROM creator_rows) THEN 1--when input, limit to these contracts (i.e. updated mapping)
                ELSE 0 END
                )
    GROUP BY 1,2,3,4,5,6
    
    UNION ALL --ovm 1.0 contracts
    
    SELECT creator_address::bytea, NULL::bytea AS contract_factory, "contract_address","contract_project" AS project,"contract_name" AS name, created_time::timestamptz FROM ovm1.op_ovm1_contracts d
    WHERE contract_address NOT IN (SELECT contract_address FROM creator_contracts)
	AND NOT EXISTS (SELECT 1 FROM ovm2.get_contracts gc WHERE gc.contract_address = d.address AND gc.contract_name = d.contract_name) 
    GROUP BY 1,2,3,4,5,6
    
    UNION ALL --synthetix genesis contracts

    SELECT NULL::bytea AS creator_address, NULL::bytea AS contract_factory, snx.contract_address AS contract_address, 'Synthetix' AS contract_project, contract_name, '07-06-2021 00:00:00'::timestamp
    FROM ovm1.synthetix_genesis_contracts snx
        WHERE address NOT IN (SELECT contract_address FROM creator_contracts)
	AND NOT EXISTS (SELECT 1 FROM ovm2.get_contracts gc WHERE gc.contract_address = snx.contract_address AND 'Synthetix' = contract_project) 
	
    GROUP BY 1,2,3,4,5,6
        
    UNION ALL --other missing genesis contracts
    
    SELECT NULL::bytea AS creator_address, NULL::bytea AS contract_factory, contract_address::bytea, contract_project::text, contract_name, '07-06-2021 00:00:00'
        FROM (
            values
            ('\x8be60b5031c0686e48a079c81822173bfa1268da','Synthetix',NULL,NULL)
            ,('\xc16251b5401087902e0956a2968CB3e0e4a52760','Celer',NULL,NULL)
            ) a (contract_address,contract_project,erc20_symbol,contract_name)
            WHERE contract_address::bytea NOT IN (SELECT contract_address FROM creator_contracts)
		AND NOT EXISTS (SELECT 1 FROM ovm2.get_contracts gc WHERE gc.contract_address = a.contract_address::bytea) 
    ) c
LEFT JOIN ovm1.synthetix_genesis_contracts snx --TODO: could replace this will all predeploys
    ON c.contract_address = snx.address
FULL OUTER JOIN erc20_tokens erc20 -- b/c we want to get all ERC20s that aren't in this list too.
    ON COALESCE(c.contract_address,snx.address) = erc20.contract_address
FULL OUTER JOIN nft_tokens nft
    ON COALESCE(c.contract_address,snx.address) = nft.contract_address
    
)

SELECT c.contract_address, INITCAP(COALESCE(c.contract_project,ovm1c.contract_project)) AS contract_project ,c.erc20_symbol, COALESCE(c.contract_name) AS contract_name,
COALESCE(c.creator_address,ovm1c.creator_address) AS creator_address, COALESCE(c.created_time,ovm1c.created_time::timestamp) AS created_time, contract_factory AS contract_creator_if_factory
FROM (
--grab the first non-null value for each (i.e. if we have the contract via both contract mapping and optimism.contracts)
    SELECT 
    contract_address,
    (ARRAY_AGG(contract_project) FILTER (WHERE contract_project IS NOT NULL))[1] AS contract_project,
    (ARRAY_AGG(erc20_symbol) FILTER (WHERE erc20_symbol IS NOT NULL))[1] AS erc20_symbol,
    (ARRAY_AGG(contract_name) FILTER (WHERE contract_name IS NOT NULL))[1] AS contract_name,
    (ARRAY_AGG(creator_address) FILTER (WHERE creator_address IS NOT NULL))[1] AS creator_address,
    (ARRAY_AGG(created_time) FILTER (WHERE created_time IS NOT NULL))[1] AS created_time,
    (ARRAY_AGG(contract_factory) FILTER (WHERE contract_factory IS NOT NULL))[1] AS contract_factory

    FROM get_contracts
    WHERE contract_address IS NOT NULL
    
    GROUP BY 1
    ) c
    LEFT JOIN ovm1.op_ovm1_contracts ovm1c
        ON c."contract_address" = ovm1c."contract_address" --fill in any missing contract creators


	ON CONFLICT (contract_address) DO UPDATE SET
	  contract_project = EXCLUDED.contract_project,
	  token_symbol = EXCLUDED.token_symbol,
	  contract_name = EXCLUDED.contract_name,
	  creator_address = EXCLUDED.creator_address,
	  created_time = EXCLUDED.created_time,
	  contract_creator_if_factory = EXCLUDED.contract_creator_if_factory

	RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- Get the table started
SELECT ovm2.insert_get_contracts('07-06-2021'::timestamp,NOW())
WHERE NOT EXISTS (
    SELECT *
    FROM ovm2.get_contracts
);


INSERT INTO cron.job (schedule, command)
VALUES ('14,29,44,59 * * * *', $$
 SELECT ovm2.insert_get_contracts(
        (SELECT MAX("time") FROM optimism.blocks WHERE block_time > NOW() - interval '1 week'),
        (SELECT MAX("time") FROM optimism.blocks WHERE "time" > NOW() - interval '1 week'),
	 NULL::bytea[]
        );
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
