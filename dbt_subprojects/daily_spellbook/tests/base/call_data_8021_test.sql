-- Test for EIP-8021 calldata parser macro
-- This test validates the call_data_8021 macro with the example from the Linear issue

WITH test_data AS (
    -- Test case: Schema 0 (Canonical Registry) with codes "cb_wallet" (example from issue)
    -- Structure: original_data (36 bytes) + codes (9 bytes) + codes_length (1 byte) + schema (1 byte) + magic (16 bytes)
    SELECT 
        0x722c6182000000000000000000000000000000000000000000000000000000000000000163625f77616c6c6574090080218021802180218021802180218021802180218021 AS calldata,
        0x722c61820000000000000000000000000000000000000000000000000000000000000001 AS expected_original,
        'Schema 0: Canonical Registry' AS expected_schema_type,
        'cb_wallet' AS expected_codes_readable,
        0x80218021802180218021802180218021 AS expected_suffix
),

test_results AS (
    SELECT 
        {{ call_data_8021('calldata') }} AS result,
        expected_original,
        expected_schema_type,
        expected_codes_readable,
        expected_suffix
    FROM test_data
)

-- Return only failing test cases
SELECT 
    COALESCE(result.codes_readable, 'NULL') AS result_codes_readable,
    expected_codes_readable,
    COALESCE(result.schema_type, 'NULL') AS result_schema_type,
    expected_schema_type
FROM test_results
WHERE result IS NULL
   OR result.codes_readable IS NULL
   OR result.codes_readable != expected_codes_readable
   OR result.schema_type != expected_schema_type
   OR result.erc_8021_suffix != expected_suffix
   OR result.original_tx_data != expected_original
