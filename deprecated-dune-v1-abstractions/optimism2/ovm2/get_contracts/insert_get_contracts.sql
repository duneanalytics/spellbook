CREATE OR REPLACE FUNCTION ovm2.insert_get_contracts(start_blocktime timestamptz, end_blocktime timestamptz, creator_list bytea[] = NULL::bytea[]) RETURNS integer
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
      contract_creator_if_factory,
      is_self_destruct,
      creation_tx_hash
    )

--This is used for backfilling
WITH
  creator_rows AS (
      SELECT UNNEST(creator_list:: bytea []) AS creators
  )

, base_level AS (
SELECT *
	FROM (
	-- On Normal Runs, grab the entire time window
	SELECT
	  "from" AS creator_address,
	  NULL:: bytea AS contract_factory,
	  "address" AS contract_address,
	  "block_time",
	  r.tx_hash,
	  trace_address[1] AS trace_element
	FROM
	  optimism.traces r
	WHERE
	  "success"
	  AND "tx_success"
	  AND r."type" = 'create'
	  AND r."block_time" >= start_blocktime
	  AND r."block_time" < end_blocktime
	AND NOT EXISTS (SELECT creators FROM creator_rows)
	
	-- On update runs, grab any contracts we have info to update
	UNION ALL -- grab any contracts that we have info to update
	
        SELECT "creator_address", "contract_creator_if_factory", "contract_address", "created_time", tx_hash, trace_address[1] AS trace_element
        FROM ovm2."get_contracts" gc
            INNER JOIN optimism.traces r ON r.address = gc.contract_address AND r.tx_hash = gc.creation_tx_hash
         WHERE ( "creator_address" IN (SELECT creators FROM creator_rows) OR "contract_creator_if_factory" IN (SELECT creators FROM creator_rows) ) 
            AND "success" AND "tx_success" AND r."type" = 'create'
	--start and end will reflect contract creation times
	AND r."block_time" >= start_blocktime
	  AND r."block_time" < end_blocktime
		
	) uni
GROUP BY 1,2,3,4,5,6
	
	
	  )
  , tokens AS (
    SELECT
      e.contract_address,
      tl.symbol AS symbol
    FROM base_level e
	INNER JOIN erc20."tokens" tl ON tl."contract_address" = e."contract_address"
    GROUP BY
      1, 2
    
    UNION ALL
 
    SELECT
      e.contract_address,
      tl."project_name" AS symbol
    FROM
      base_level e
      INNER JOIN nft."tokens" tl ON tl."contract_address" = e."contract_address"
    GROUP BY
      1, 2
  )
  , get_contracts AS (
    WITH
      creator_contracts AS (
        SELECT
          con.creator_address,
          con.contract_factory,
          con.contract_address,
          COALESCE(cc.project,ccf.project) AS project,
          con.block_time AS created_time,
          -- Check if the contract is an immediate self-destruct contract
          CASE
            WHEN EXISTS (
              SELECT
                1
              FROM
                optimism.traces sd
              WHERE
                con.tx_hash = sd.tx_hash
                AND con.trace_element = sd.trace_address[1]
                AND sd."type" = 'suicide'
                AND sd."block_time" >= start_blocktime
                AND sd."block_time" < end_blocktime
            ) THEN true
            ELSE false
          END AS is_self_destruct,
          tx_hash AS creation_tx_hash
        FROM
          (
            WITH
              second_level AS (
                    SELECT
                      COALESCE(b1.creator_address, b.creator_address) AS creator_address,
                      CASE
                        WHEN b1.creator_address IS NULL THEN NULL
                        ELSE b.creator_address
                      END AS contract_factory,
                      b.contract_address,
                      b.block_time,
                      b.tx_hash,
                      b.trace_element
                    FROM
                      base_level b
                      LEFT JOIN base_level b1 ON b.creator_address = b1.contract_address
                  ),
                  third_level AS (
                    SELECT
                      COALESCE(b1.creator_address, b.creator_address) AS creator_address,
                      CASE
                        WHEN b1.creator_address IS NULL THEN b.contract_factory
                        ELSE b.creator_address
                      END AS contract_factory,
                      b.contract_address,
                      b.block_time,
                      b.tx_hash,
                      b.trace_element
                    FROM
                      second_level b
                      LEFT JOIN base_level b1 ON b.creator_address = b1.contract_address
                  ),
                  fourth_level AS (
                    SELECT
                      COALESCE(b1.creator_address, b.creator_address) AS creator_address,
                      CASE
                        WHEN b1.creator_address IS NULL THEN b.contract_factory
                        ELSE b.creator_address
                      END AS contract_factory,
                      b.contract_address,
                      b.block_time,
                      b.tx_hash,
                      b.trace_element
                    FROM
                      third_level b
                      LEFT JOIN base_level b1 ON b.creator_address = b1.contract_address
                  ),
                  fifth_level AS (
                    SELECT
                      COALESCE(b1.creator_address, b.creator_address) AS creator_address,
                      CASE
                        WHEN b1.creator_address IS NULL THEN b.contract_factory
                        ELSE b.creator_address
                      END AS contract_factory,
                      b.contract_address,
                      b.block_time,
                      b.tx_hash,
                      b.trace_element
                    FROM
                      fourth_level b
                      LEFT JOIN base_level b1 ON b.creator_address = b1.contract_address
                  )
                SELECT
                  *
                FROM
                  fifth_level -- check for contract factories 4 layers down --as of 3/12, this ran through all contracts
              ) con
              LEFT JOIN ovm2."contract_creator_address_list" cc ON con.creator_address = cc.creator_address
              LEFT JOIN ovm2."contract_creator_address_list" ccf ON con.contract_factory = ccf.creator_address
            WHERE
              contract_address IS NOT NULL
          )
        SELECT
          c.contract_address AS contract_address,
          contract_factory,
          c.project AS contract_project,
          tokens.symbol AS token_symbol,
          contract_name,
          creator_address,
          created_time,
          is_self_destruct,
          creation_tx_hash
        FROM
          (
            SELECT
              cc.creator_address,
              contract_factory,
              cc.contract_address,
              COALESCE(cc.project, oc.namespace) AS project,
              oc.name AS contract_name,
              cc.created_time,
              COALESCE(is_self_destruct, false) AS is_self_destruct,
              'creator contracts' as source,
              creation_tx_hash
            FROM
              creator_contracts cc
              LEFT JOIN optimism."contracts" oc ON oc."address" = cc.contract_address
            WHERE cc.contract_address IN (SELECT contract_address FROM creator_contracts)

            UNION ALL
            --other decoded contracts / Only pull if there's a match to avoid unnecessary overwriting
            SELECT
              NULL:: bytea AS creator_address,
              NULL:: bytea AS contract_factory,
              "address" AS contract_address,
              namespace AS project,
              name,
              created_at AS created_time,
              COALESCE(is_self_destruct, false) AS is_self_destruct,
              'decoded contracts' as source,
              creation_tx_hash
            FROM
              optimism."contracts" oc
              INNER JOIN creator_contracts cc ON oc."address" = cc.contract_address --enforce match
            WHERE oc.address IN (SELECT contract_address FROM creator_contracts)

            GROUP BY
              1, 2, 3, 4, 5, 6, 7, 8, 9
            UNION ALL
            --ovm 1.0 contracts
            SELECT
              creator_address:: bytea,
              NULL:: bytea AS contract_factory,
              "contract_address",
              "contract_project" AS project,
              "contract_name" AS name,
              created_time:: timestamptz AS created_time,
              false AS is_self_destruct,
              'ovm1 contracts' as source,
              NULL::bytea AS creation_tx_hash
            FROM
              ovm1.op_ovm1_contracts d
            WHERE
              NOT EXISTS (
                SELECT
                  1
                FROM
                  ovm2.get_contracts gc
                WHERE
                  gc.contract_address = d.contract_address
                  AND (
                      (gc.contract_project = d.contract_project) OR (gc.contract_project IS NULL)
                      )
              )
              OR contract_address IN (SELECT contract_address FROM creator_contracts)
            GROUP BY
              1, 2, 3, 4, 5, 6, 7, 8, 9
            UNION ALL
            --synthetix genesis contracts
            SELECT
              NULL:: bytea AS creator_address,
              NULL:: bytea AS contract_factory,
              snx.contract_address AS contract_address,
              'Synthetix' AS contract_project,
              contract_name,
              '07-06-2021 00:00:00':: timestamptz AS created_time,
              false AS is_self_destruct,
              'synthetix contracts' as source,
              NULL::bytea AS creation_tx_hash
            FROM
              ovm1.synthetix_genesis_contracts snx
            WHERE
              NOT EXISTS (
                SELECT
                  1
                FROM
                  ovm2.get_contracts gc
                WHERE
                  gc.contract_address = snx.contract_address
                  AND 'Synthetix' = contract_project
              )
              OR contract_address IN (SELECT contract_address FROM creator_contracts)

              ) c
              LEFT JOIN tokens
              ON c.contract_address = tokens.contract_address
              GROUP BY
              1, 2, 3, 4, 5, 6, 7, 8, 9
              
          )
        SELECT
          c.contract_address,
          INITCAP(
            REPLACE(
              --Priority order: Override name, Mapped vs Dune, Raw/Actual names
              COALESCE(
                co.project_name,
                dnm.mapped_name,
                c.contract_project,
                ovm1c.contract_project
              ),
              '_',
              ' '
            )
          ) AS contract_project,
          c.token_symbol,
          COALESCE(co.contract_name, c.contract_name) AS contract_name,
          COALESCE(c.creator_address, ovm1c.creator_address) AS creator_address,
          COALESCE(c.created_time, ovm1c.created_time:: timestamptz) AS created_time,
          contract_factory AS contract_creator_if_factory,
          COALESCE(is_self_destruct, false) AS is_self_destruct,
          creation_tx_hash
        FROM
          (
            --grab the first non-null value for each (i.e. if we have the contract via both contract mapping and optimism.contracts)
            SELECT
              contract_address,
              (
                ARRAY_AGG(contract_project) FILTER (
                  WHERE
                    contract_project IS NOT NULL
                )
              ) [1] AS contract_project,
              (
                ARRAY_AGG(token_symbol) FILTER (
                  WHERE
                    token_symbol IS NOT NULL
                )
              ) [1] AS token_symbol,
              (
                ARRAY_AGG(contract_name) FILTER (
                  WHERE
                    contract_name IS NOT NULL
                )
              ) [1] AS contract_name,
              (
                ARRAY_AGG(creator_address) FILTER (
                  WHERE
                    creator_address IS NOT NULL
                )
              ) [1] AS creator_address,
              (
                ARRAY_AGG(created_time) FILTER (
                  WHERE
                    created_time IS NOT NULL
                )
              ) [1] AS created_time,
              (
                ARRAY_AGG(contract_factory) FILTER (
                  WHERE
                    contract_factory IS NOT NULL
                )
              ) [1] AS contract_factory,
              (
                ARRAY_AGG(is_self_destruct) FILTER (
                  WHERE
                    is_self_destruct IS NOT NULL
                )
              ) [1] AS is_self_destruct,

              (
                ARRAY_AGG(creation_tx_hash) FILTER (
                  WHERE
                    creation_tx_hash IS NOT NULL
                )
              ) [1] AS creation_tx_hash
              
            FROM
              get_contracts
            WHERE
              contract_address IS NOT NULL
            GROUP BY
              1
          ) c
          LEFT JOIN ovm1.op_ovm1_contracts ovm1c ON c."contract_address" = ovm1c."contract_address" --fill in any missing contract creators
          LEFT JOIN ovm2.project_name_mappings dnm --fix names for decoded contracts
          ON LOWER(c.contract_project) = LOWER(dune_name)
          LEFT JOIN ovm2.contract_overrides co --override contract maps
          ON c."contract_address" = co."contract_address"
	

	ON CONFLICT (contract_address) DO UPDATE SET
	
	  contract_project = EXCLUDED.contract_project,
	  token_symbol = EXCLUDED.token_symbol,
	  contract_name = EXCLUDED.contract_name
	
	RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- Get the table started
SELECT ovm2.insert_get_contracts('01-01-2021'::timestamptz,NOW())
WHERE NOT EXISTS (
    SELECT *
    FROM ovm2.get_contracts
);


INSERT INTO cron.job (schedule, command)
VALUES ('14,29,44,59 * * * *', $$
 SELECT ovm2.insert_get_contracts(
        (SELECT MAX("created_time") FROM ovm2.get_contracts)::timestamptz,
        (SELECT MAX("time") FROM optimism.blocks WHERE "time" > NOW() - interval '1 month')::timestamptz,
	 NULL::bytea[]
        );
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
