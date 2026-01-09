-- Example usage of the EIP-8021 calldata parser macro
-- This test verifies that the macro runs correctly and parses the example calldata

WITH sample_calldata AS (
    -- Example from the Linear issue (corrected: 63 bytes total)
    -- Structure: original_data (36 bytes) + codes (9 bytes) + codes_length (1 byte) + schema (1 byte) + magic (16 bytes)
    SELECT 0x722c6182000000000000000000000000000000000000000000000000000000000000000163625f77616c6c6574090080218021802180218021802180218021 AS calldata
),

parsed AS (
    SELECT 
        {{ call_data_8021('calldata') }} AS eip8021_data
    FROM sample_calldata
    WHERE {{ has_eip_8021_suffix('calldata') }}
)

-- Test passes if the macro returns a non-null result with the expected codes_readable
-- The test returns 0 rows (passes) if codes_readable = 'cb_wallet', or 1 row (fails) otherwise
SELECT 
    eip8021_data.codes_readable AS actual_codes,
    'cb_wallet' AS expected_codes
FROM parsed
WHERE eip8021_data IS NULL 
   OR eip8021_data.codes_readable IS NULL 
   OR eip8021_data.codes_readable != 'cb_wallet'
