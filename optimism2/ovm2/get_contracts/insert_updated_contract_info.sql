-- Scan for contracts that have a missing contract name or missing project name mapping and see if we have updated data to fill in.


CREATE OR REPLACE VIEW ovm2.view_get_contracts_contracts_to_update AS

SELECT gc.creator_address, MIN(created_time) AS min_created_time, MAX(created_time) AS max_created_time
    
    FROM ovm2.get_contracts gc
    LEFT JOIN ovm2.contract_creator_address_list cc
        ON cc."creator_address" = gc.creator_address
		OR cc."creator_address" = gc.contract_creator_if_factory
	LEFT JOIN ovm2.project_name_mappings pnmc
	ON pnmc.mapped_name = gc.contract_project
	LEFT JOIN ovm2.project_name_mappings pnm
	ON pnm.dune_name = gc.contract_project
    
    WHERE
	is_self_destruct = false
    AND 
    (
		(
			(
				(gc.contract_project IS NULL AND cc.project IS NOT NULL) -- Check if we have new mappings
				OR 
				-- check empty or default mapping to see if we have updates (i.e. names, tokens)
				(
    				(
    				COALESCE(contract_name,token_symbol) IS NULL --Check if we have a contract name 
    				OR upper(token_symbol) IN ('ERC20','ERC721','OTHER ERC20','OTHER ERC721','OTHER ERC1155','OTHER NFT') --Check if we have a symbol now
    				)
    			AND ( -- For null values, Check if there's any reason to believe that we have an update
    				contract_address IN (SELECT "address" FROM optimism."contracts") --is it now in the contracts table
    				OR
    				contract_address IN (SELECT "contract_address" FROM erc20."tokens")--is it now in the erc20 table
    				OR
    				contract_address IN (SELECT "contract_address" FROM nft."tokens")--is it now in the nft table
    				)
                )
            )
		)
	    OR (lower(cc.project) != lower(COALESCE(pnmc."mapped_name",gc."contract_project")) ) --check if the name is now updates
	    OR pnm.dune_name IS NOT NULL -- currently mapped to an old name version
	)
	and gc.creator_address IS NOT NULL
    GROUP BY 1
;


-- INSERT INTO cron.job (schedule, command)
-- VALUES ('11,44 * * * *', $$

--  SELECT ovm2.insert_get_contracts(
-- 	(SELECT MIN(min_created_time) FROM  ovm2.view_get_contracts_contracts_to_update), --start time
--         (SELECT MAX(max_created_time) FROM  ovm2.view_get_contracts_contracts_to_update), --end time (max time)
-- 	(SELECT array_agg(creator_address) FROM ovm2.view_get_contracts_contracts_to_update LIMIT 1)
-- 	)
-- 	WHERE EXISTS (
-- 		SELECT * FROM ovm2.view_get_contracts_contracts_to_update LIMIT 1 --only run if there are contracts to update.
-- 	);
	
-- $$)
-- ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
