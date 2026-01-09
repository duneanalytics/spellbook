-- Test for EIP-8021 calldata parser macro
-- This test validates the call_data_8021 macro with sample calldata

WITH test_data AS (
    SELECT 
        calldata,
        CAST(expected AS ROW(
            original_tx_data varbinary,
            schema_type varchar,
            codes_hex varchar,
            custom_registry_address varbinary,
            codes_readable varchar,
            codes_array array(varchar),
            erc_8021_suffix varbinary
        )) AS expected
    FROM (
        VALUES
        -- Test case 1: Schema 0 (Canonical Registry) with codes "cb_wallet" (example from issue)
        -- Structure: original_data (36 bytes) + codes (9 bytes) + codes_length (1 byte) + schema (1 byte) + magic (16 bytes)
        (
            0x722c6182000000000000000000000000000000000000000000000000000000000000000163625f77616c6c6574090080218021802180218021802180218021802180218021,
            ROW(
                0x722c61820000000000000000000000000000000000000000000000000000000000000001,  -- original tx data (36 bytes)
                'Schema 0: Canonical Registry',
                '63625f77616c6c6574',  -- codes_hex ("cb_wallet" in hex = 9 bytes)
                CAST(NULL AS varbinary),  -- no registry for Schema 0
                'cb_wallet',  -- codes_readable
                ARRAY['cb_wallet'],  -- codes_array
                0x80218021802180218021802180218021  -- magic suffix (16 bytes)
            )
        ),
        -- Test case 2: Schema 0 with simple single code "test"
        -- Structure: original_data (3 bytes) + codes (4 bytes) + codes_length (1 byte) + schema (1 byte) + magic (16 bytes)
        (
            0xabcdef74657374040080218021802180218021802180218021,
            ROW(
                0xabcdef,  -- original tx data (3 bytes)
                'Schema 0: Canonical Registry',
                '74657374',  -- codes_hex ("test" in hex)
                CAST(NULL AS varbinary),  -- no registry for Schema 0
                'test',
                ARRAY['test'],
                0x80218021802180218021802180218021  -- magic suffix
            )
        ),
        -- Test case 3: Schema 0 with comma-separated codes "one,two"
        -- Structure: original_data (2 bytes) + codes (7 bytes) + codes_length (1 byte) + schema (1 byte) + magic (16 bytes)
        (
            0x12346f6e652c74776f070080218021802180218021802180218021,
            ROW(
                0x1234,  -- original tx data (2 bytes)
                'Schema 0: Canonical Registry',
                '6f6e652c74776f',  -- codes_hex ("one,two" in hex)
                CAST(NULL AS varbinary),  -- no registry for Schema 0
                'one,two',
                ARRAY['one', 'two'],
                0x80218021802180218021802180218021  -- magic suffix
            )
        ),
        -- Test case 4: Schema 1 (Custom Registry) with codes "app" and a registry address
        -- Structure: original_data (2 bytes) + codes (3 bytes) + registry_address (20 bytes) + codes_length (1 byte) + schema (1 byte) + magic (16 bytes)
        (
            0xaabb6170701234567890123456789012345678901234567890030180218021802180218021802180218021,
            ROW(
                0xaabb,  -- original tx data (2 bytes)
                'Schema 1: Custom Registry',
                '617070',  -- codes_hex ("app" in hex = 3 bytes)
                0x1234567890123456789012345678901234567890,  -- custom registry address (20 bytes)
                'app',  -- codes_readable
                ARRAY['app'],  -- codes_array
                0x80218021802180218021802180218021  -- magic suffix (16 bytes)
            )
        )
    ) AS temp (calldata, expected)
),

test_results AS (
    SELECT 
        {{ call_data_8021('calldata') }} AS result,
        expected
    FROM test_data
)

-- Return only failing test cases (rows where result != expected)
-- Use explicit null-safe comparisons
SELECT 
    to_hex(result.original_tx_data) AS result_original_tx_data,
    to_hex(expected.original_tx_data) AS expected_original_tx_data,
    result.schema_type AS result_schema_type,
    expected.schema_type AS expected_schema_type,
    result.codes_hex AS result_codes_hex,
    expected.codes_hex AS expected_codes_hex,
    result.codes_readable AS result_codes_readable,
    expected.codes_readable AS expected_codes_readable
FROM test_results
WHERE NOT (result.original_tx_data = expected.original_tx_data)
   OR NOT (result.schema_type = expected.schema_type)
   OR NOT (result.codes_hex = expected.codes_hex)
   OR NOT (COALESCE(result.codes_readable, '') = COALESCE(expected.codes_readable, ''))
   OR NOT (result.erc_8021_suffix = expected.erc_8021_suffix)
   OR NOT (result.custom_registry_address IS NOT DISTINCT FROM expected.custom_registry_address)
