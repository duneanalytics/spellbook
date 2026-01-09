-- Example usage of the EIP-8021 calldata parser macro
-- This demonstrates how to extract builder codes from transaction calldata
-- 
-- The macro call_data_8021('calldata_column') returns a ROW with:
-- - original_tx_data: Original calldata without EIP-8021 suffix (varbinary)
-- - schema_type: 'Schema 0: Canonical Registry' or 'Schema 1: Custom Registry'
-- - codes_hex: Raw hex codes (varchar)
-- - custom_registry_address: Registry address for Schema 1 only (varbinary)
-- - codes_readable: Human-readable codes (varchar)
-- - codes_array: Codes split by comma (array of varchar)
-- - erc_8021_suffix: The EIP-8021 magic bytes suffix (varbinary)

WITH sample_calldata AS (
    -- Example from the Linear issue
    SELECT 0x722c6182000000000000000000000000000000000000000000000000000000000000000163625f77616c6c6574090080218021802180218021802180218021802180218021 AS calldata
),

parsed AS (
    SELECT 
        {{ call_data_8021('calldata') }} AS eip8021_data
    FROM sample_calldata
    WHERE {{ has_eip_8021_suffix('calldata') }}
)

SELECT 
    eip8021_data.original_tx_data,
    eip8021_data.schema_type,
    eip8021_data.codes_hex,
    eip8021_data.custom_registry_address,
    eip8021_data.codes_readable,
    eip8021_data.codes_array,
    eip8021_data.erc_8021_suffix
FROM parsed
-- This test should return exactly one row - if zero rows, test fails
WHERE eip8021_data.codes_readable != 'cb_wallet'
