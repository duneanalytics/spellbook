CREATE OR REPLACE FUNCTION ovm2.update_decoded_contracts() RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT INTO ovm2.get_contracts (
      contract_address,
      contract_project
    )
    
    WITH to_update AS (
    SELECT
    c.address AS contract_address,
        INITCAP(
                REPLACE(
                  COALESCE( pnmc.mapped_name, pnm.mapped_name, c.namespace),
                  '_',' '
                )
        ) AS contract_project
        
        FROM ovm2.get_contracts gc
        LEFT JOIN ovm2.contract_creator_address_list cc
            ON cc."creator_address" = gc.creator_address
    		OR cc."creator_address" = gc.contract_creator_if_factory
    	
    	INNER JOIN optimism.contracts c
    	    ON c."address" = gc.contract_address
    	    AND gc.contract_project IS NULL
    	    AND cc."creator_address" IS NULL
    	LEFT JOIN ovm2.project_name_mappings pnmc
    	    ON pnmc.mapped_name = gc.contract_project
    	LEFT JOIN ovm2.project_name_mappings pnm
    	    ON pnm.dune_name = gc.contract_project
        
        WHERE cc."creator_address" IS NULL
        )
        SELECT
        contract_address, contract_project
        FROM to_update
        
        
        ON CONFLICT (contract_address) DO UPDATE SET
	        contract_project = EXCLUDED.contract_project
    	
    	RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

--This insert is in the insert_updated_contract_info.sql file to enforce ordering.
--SELECT ovm2.update_decoded_contracts();
