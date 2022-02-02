CREATE OR REPLACE FUNCTION ovm2.insert_get_contracts(start_blocktime timestamp, end_blocktime timestamp) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN

WITH rows AS (
    INSERT INTO dune_user_generated.ovm2_get_contracts (
        contract_address,
      contract_project,
      erc20_symbol, 
      contract_name,
      creator_address, 
      created_time,
      contract_creator_if_factory
    )

WITH contract_creators AS (
SELECT COALESCE(a."from",w.creator_address) AS creator_address, w.project
FROM (
SELECT "from" FROM optimism."transactions" t
WHERE "to" IS NULL
AND success = true
AND t."block_time" >= start_blocktime
AND t."block_time" < end_blocktime 
AND 1 = (
    CASE WHEN creators IS NULL THEN 1 --when no input, search everythin
        WHEN t."from" IN (creators) THEN 1--when input, limit to these contracts (i.e. updated mapping)
        ELSE 0 END
        )
GROUP BY 1
) a
FULL OUTER JOIN dune_user_generated.contract_creator_address_list w
ON w.creator_address = a."from"
WHERE 
(
(w.project != 'EXCLUDE') OR (w.project IS NULL)
)

AND 1 = (
    CASE WHEN creators IS NULL THEN 1 --when no input, search everythin
        WHEN a."from" IN (creators) AND
            (w.creator_address IN (creators)
            OR w.creator_address IS NULL) THEN 1--Either a match or not in the static creator list
        ELSE 0 END
        )
)


,get_contracts AS (
    WITH creator_contracts AS
    (
    SELECT
    con.creator_address, con.contract_factory, contract_address, project, con.block_time AS created_time
    FROM
    (
    WITH main_creates AS (
        SELECT
        --top_creator = creator of the contract (i.e. who created the factory if factory)
        --contract_creator = creator of the base-level contract (i.e. factory if factory)
        tx."from" AS creator_address, NULL::bytea AS contract_factory, r.address AS contract_address, tx.block_time
        FROM (
            SELECT t.* FROM optimism."transactions" t
            WHERE t."from" IN (SELECT creator_address FROM contract_creators)
            AND t."to" IS NULL
            AND t.success = true
                AND t."block_time" >= start_blocktime
                AND t."block_time" < end_blocktime 
            ) tx
        INNER JOIN optimism."traces" r
            ON tx.hash = r.tx_hash
            AND tx.block_time = r.block_time
        WHERE r."type" = 'create'
            AND r."block_time" >= start_blocktime
            AND r."block_time" < end_blocktime 
        GROUP BY 1,2,3,4
        )
    , to_factories AS (
            SELECT
            mc.creator_address, tx."to" AS contract_factory, r.address AS contract_address, tx.block_time --use factory to assign project
            FROM (
                SELECT t.* FROM optimism."transactions" t
                WHERE t."to" IN (SELECT contract_address FROM main_creates) --AND t."from" IN (SELECT creator_address FROM contract_creators)
                AND t.success = true
                AND t."block_time" >= start_blocktime
                AND t."block_time" < end_blocktime 
                ) tx
            INNER JOIN optimism."traces" r
                ON tx.hash = r.tx_hash
                AND tx.block_time = r.block_time
            INNER JOIN main_creates mc
                ON mc.contract_address = tx."to"
            WHERE r."type" = 'create'
                AND r."block_time" >= start_blocktime
                AND r."block_time" < end_blocktime 
            --GROUP BY 1,2,3,4
        )

    , to_factories_1 AS ( --contracts created by factories created by factories
            SELECT
            tf.creator_address, tx."to" AS contract_factory, r.address AS contract_address, tx.block_time --use factory to assign project
            FROM (
                SELECT t.* FROM optimism."transactions" t
                WHERE t."to" IN (SELECT contract_address FROM to_factories) --AND t."from" IN (SELECT creator_address FROM contract_creators)
                AND t.success = true
                    AND t."block_time" >= start_blocktime
                    AND t."block_time" < end_blocktime 
                ) tx
            INNER JOIN optimism."traces" r
                ON tx.hash = r.tx_hash
                AND tx.block_time = r.block_time
            INNER JOIN to_factories tf
                ON tf.contract_address = tx."to"
            WHERE r."type" = 'create'
                AND r."block_time" >= start_blocktime
                AND r."block_time" < end_blocktime 
            GROUP BY 1,2,3,4
        )
        
        SELECT * FROM (
            SELECT creator_address, contract_factory, contract_address,block_time--, NULL::bytea AS factory
            FROM main_creates
            UNION ALL
            SELECT creator_address, contract_factory, contract_address,block_time--, factory
            FROM to_factories
            UNION ALL
            SELECT creator_address, contract_factory, contract_address,block_time--, factory 1 layer deeper
            FROM to_factories_1
            ) uni
        GROUP BY 1,2,3,4 --remove dupes
    
    ) con
    LEFT JOIN contract_creators cc
    ON con.creator_address = cc.creator_address
    
    
    )
    
    ,erc20_tokens AS (
    SELECT e.contract_address, CASE WHEN tl.symbol IS NULL THEN 'erc20' ELSE tl.symbol END AS symbol
    FROM (
        SELECT contract_address FROM erc20."ERC20_evt_Transfer" WHERE contract_address IN (SELECT contract_address FROM creator_contracts)
            GROUP BY 1
        UNION ALL
        SELECT "contract_address" FROM erc20."tokens" WHERE contract_address IN (SELECT contract_address FROM creator_contracts)
        ) e
    LEFT JOIN erc20."tokens" tl
        ON tl."contract_address" = e."contract_address"
    GROUP BY 1,2
    )
    
    ,erc721_tokens AS (
    SELECT e.contract_address, CASE WHEN tl."project_name" IS NULL THEN 'erc721' ELSE tl."project_name" END AS symbol
    FROM (
        SELECT contract_address FROM erc721."ERC721_evt_Transfer" WHERE contract_address IN (SELECT contract_address FROM creator_contracts)
            GROUP BY 1
        UNION ALL
        SELECT "contract_address" FROM erc721."tokens" WHERE contract_address IN (SELECT contract_address FROM creator_contracts)
        ) e
    LEFT JOIN erc721."tokens" tl
        ON tl."contract_address" = e."contract_address"
    GROUP BY 1,2
    )
    
SELECT
COALESCE(c.contract_address,erc20.contract_address,snx.address) AS contract_address,
    contract_factory,
    CASE WHEN snx.address IS NOT NULL THEN 'Synthetix' ELSE c.project END AS contract_project,
    COALESCE(erc20.symbol,erc721.symbol) AS erc20_symbol, COALESCE(c.contract_name,snx.contract_name) AS contract_name, creator_address,
    created_time
    
FROM (
    SELECT cc.creator_address, contract_factory, cc.contract_address, COALESCE(cc.project, oc.namespace) AS project, oc.name AS contract_name, cc.created_time FROM creator_contracts cc
        LEFT JOIN optimism."contracts" oc
            ON oc."address" = cc.contract_address
        WHERE 1 = (
                CASE WHEN creators IS NULL THEN 1 --when no input, search everythin
                WHEN cc.creator_address IN (creators) THEN 1--when input, limit to these contracts (i.e. updated mapping)
                ELSE 0 END
                )

    UNION ALL --other decoded contracts
    SELECT NULL::bytea AS creator_address, NULL::bytea AS contract_factory, "address" AS contract_address, INITCAP(REPLACE(namespace,'_',' ')) AS project, name, created_at AS contract_name
    FROM optimism."contracts"
    WHERE "address" NOT IN (SELECT contract_address FROM creator_contracts)
    AND 1 = (
                CASE WHEN creators IS NULL THEN 1 --when no input, search everythin
                --WHEN cc.creator_address IN (creators) THEN 1--when input, limit to these contracts (i.e. updated mapping)
                ELSE 0 END
                )
    GROUP BY 1,2,3,4,5,6
    
    UNION ALL --ovm 1.0 contracts
    
    SELECT creator_address::bytea, NULL::bytea AS contract_factory, "contract_address","contract_project" AS project,"contract_name" AS name, created_time::timestamptz FROM dune_user_generated.oe_ovm1_contracts
    WHERE contract_address NOT IN (SELECT contract_address FROM creator_contracts)
    AND 1 = (
                CASE WHEN creators IS NULL THEN 1 --when no input, search everythin
                WHEN creator_address IN (creators) THEN 1--when input, limit to these contracts (i.e. updated mapping)
                ELSE 0 END
                )
    GROUP BY 1,2,3,4,5,6
    
    UNION ALL --synthetix genesis contracts

    SELECT NULL::bytea AS creator_address, NULL::bytea AS contract_factory, address AS contract_address, 'Synthetix' AS contract_project, contract_name, '07-06-2021 00:00:00'::timestamp
    FROM dune_user_generated.synthetix_genesis_contracts
        WHERE address NOT IN (SELECT contract_address FROM creator_contracts)
        AND 1 = (
                CASE WHEN creators IS NULL THEN 1 --when no input, search everythin
                --WHEN cc.creator_address IN (creators) THEN 1--when input, limit to these contracts (i.e. updated mapping)
                ELSE 0 END
                )
    GROUP BY 1,2,3,4,5,6
        
    UNION ALL --other missing genesis contracts
    
    SELECT NULL::bytea AS creator_address, NULL::bytea AS contract_factory, contract_address::bytea, contract_project::text, contract_name, '07-06-2021 00:00:00'
        FROM (
            values
            ('\x8be60b5031c0686e48a079c81822173bfa1268da','Synthetix',NULL,NULL)
            ,('\xc16251b5401087902e0956a2968CB3e0e4a52760','Celer',NULL,NULL)
            ) a (contract_address,contract_project,erc20_symbol,contract_name)
            WHERE contract_address::bytea NOT IN (SELECT contract_address FROM creator_contracts)
            AND 1 = (
                CASE WHEN creators IS NULL THEN 1 --when no input, search everythin
                --WHEN cc.creator_address IN (creators) THEN 1--when input, limit to these contracts (i.e. updated mapping)
                ELSE 0 END
                )
    ) c
LEFT JOIN dune_user_generated.synthetix_genesis_contracts snx --TODO: could replace this will all predeploys
    ON c.contract_address = snx.address
FULL OUTER JOIN erc20_tokens erc20 -- b/c we want to get all ERC20s that aren't in this list too.
    ON COALESCE(c.contract_address,snx.address) = erc20.contract_address
FULL OUTER JOIN erc721_tokens erc721
    ON COALESCE(c.contract_address,snx.address) = erc721.contract_address

WHERE c."created_time" >= start_blocktime
    AND c."created_time" < end_blocktime 
    
)


SELECT c.contract_address, COALESCE(c.contract_project,ovm1c.contract_project) AS contract_project ,c.erc20_symbol, COALESCE(c.contract_name) AS contract_name,
COALESCE(c.creator_address,ovm1c.creator_address) AS creator_address, COALESCE(c.created_time,ovm1c.created_time::timestamp) AS created_time, contract_factory AS contract_creator_if_factory
FROM (
    SELECT *,
    ROW_NUMBER() OVER (PARTITION BY contract_address ORDER BY contract_project ASC NULLS LAST, erc20_symbol ASC NULLS LAST) AS contract_rank --to ensure no dupes
    FROM get_contracts
    WHERE contract_address IS NOT NULL
    ) c
    LEFT JOIN dune_user_generated."oe_ovm1_contracts" ovm1c
        ON c."contract_address" = ovm1c."contract_address" --fill in any missing contract creators

WHERE contract_rank = 1

	ON CONFLICT (contract_address) DO UPDATE SET
  contract_project = EXCLUDED.contract_project,
  erc20_symbol = EXCLUDED.erc20_symbol,
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
        (SELECT MAX("time") FROM optimism.blocks WHERE "time" > NOW() - interval '1 week')
        );
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
