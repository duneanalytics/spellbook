INSERT INTO cron.job (schedule, command)
VALUES ('14,29,44,59 * * * *', $$
 SELECT ovm2.insert_get_contracts(
        (SELECT MAX("time") FROM optimism.blocks WHERE block_time > NOW() - interval '1 week'),
        (SELECT '07-06-2021'::timestamp ),
	
	
	    (
    SELECT array_agg(creator_address) FROM
    (
    SELECT gc.creator_address
    
    FROM dune_user_generated.ovm2_get_contracts gc
    LEFT JOIN dune_user_generated."contract_creator_address_list" cc
        ON cc."creator_address" = gc.creator_address
    
    WHERE
    (
        gc.contract_project IS NULL -- Check if we have mappings now
        OR 
         (  (
            COALESCE(contract_name,token_symbol) IS NULL --Check if we have a contract name 
            OR token_symbol IN ('erc20','erc721') --Check if we have a symbol now
            )
            )
            AND ( -- Check if there's any reason to believe that we have an update
                contract_address IN (SELECT "address" FROM optimism."contracts") --is it now in the contracts table
                OR
                contract_address IN (SELECT "contract_address" FROM erc20."tokens")--is it now in the erc20 table
                OR
                contract_address IN (SELECT "contract_address" FROM erc721."tokens")--is it now in the erc721 table
                )
    )
    OR cc.project != gc."contract_project"
    GROUP BY 1

    ) a
    LIMIT 1
    
    )
	
	
);
	
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
