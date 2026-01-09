{% macro call_data_8021(calldata_field) %}
{#
    Extracts EIP-8021 builder code label from the end of transaction/UserOperation calldata.
    This standard allows transactions to embed builder identification codes in calldata.
    
    Only processes transactions where calldata ends with magic bytes: 0x80218021802180218021802180218021
    
    Returns a ROW with:
    - original_tx_data: Original calldata without EIP-8021 suffix (varbinary)
    - schema_type: 'Schema 0: Canonical Registry', 'Schema 1: Custom Registry', or 'Unknown Schema'
    - codes_hex: Raw hex codes (varchar)
    - custom_registry_address: Registry address for Schema 1 only (varbinary)
    - codes_readable: Human-readable codes (varchar)
    - codes_array: Codes split by comma (array of varchar)
    - erc_8021_suffix: The EIP-8021 magic bytes suffix (varbinary)
    
    Usage:
        SELECT {{ call_data_8021('data') }}.*
        FROM table
        WHERE bytearray_substring(data, bytearray_length(data) - 15, 16) = 0x80218021802180218021802180218021

    Reference: EIP-8021 specification
#}
CASE
    -- Only process calldata that ends with the EIP-8021 magic bytes
    WHEN bytearray_substring({{ calldata_field }}, bytearray_length({{ calldata_field }}) - 15, 16) = 0x80218021802180218021802180218021
    THEN CAST(
        ROW(
            -- 1) original_tx_data: Extract original calldata (everything before the EIP-8021 suffix)
            bytearray_substring(
                {{ calldata_field }},
                1,
                bytearray_length({{ calldata_field }})
                - 16  -- magic bytes (16 bytes)
                - 1   -- schema_id (1 byte)
                - 1   -- codes_length (1 byte)
                - CASE 
                    WHEN bytearray_to_integer(bytearray_substring({{ calldata_field }}, bytearray_length({{ calldata_field }}) - 16, 1)) = 1 
                    THEN 20  -- registry address for Schema 1 (20 bytes)
                    ELSE 0 
                  END
                - bytearray_to_integer(bytearray_substring({{ calldata_field }}, bytearray_length({{ calldata_field }}) - 17, 1))  -- codes_length bytes
            ),

            -- 2) schema_type: Decode schema ID
            CASE
                WHEN bytearray_to_integer(bytearray_substring({{ calldata_field }}, bytearray_length({{ calldata_field }}) - 16, 1)) = 0 
                    THEN 'Schema 0: Canonical Registry'
                WHEN bytearray_to_integer(bytearray_substring({{ calldata_field }}, bytearray_length({{ calldata_field }}) - 16, 1)) = 1 
                    THEN 'Schema 1: Custom Registry'
                ELSE 'Unknown Schema'
            END,

            -- 3) codes_hex: Raw hex codes
            to_hex(
                bytearray_substring(
                    {{ calldata_field }},
                    bytearray_length({{ calldata_field }})
                        - 16  -- magic bytes
                        - 1   -- schema_id
                        - 1   -- codes_length
                        - CASE 
                            WHEN bytearray_to_integer(bytearray_substring({{ calldata_field }}, bytearray_length({{ calldata_field }}) - 16, 1)) = 1 
                            THEN 20  -- registry address for Schema 1
                            ELSE 0 
                          END
                        - bytearray_to_integer(bytearray_substring({{ calldata_field }}, bytearray_length({{ calldata_field }}) - 17, 1))  -- codes
                        + 1,  -- start position (1-indexed)
                    bytearray_to_integer(bytearray_substring({{ calldata_field }}, bytearray_length({{ calldata_field }}) - 17, 1))  -- length
                )
            ),

            -- 4) custom_registry_address: Registry address (Schema 1 only)
            -- Registry is located between codes and codes_length byte
            -- Position: len - 16 (magic) - 1 (schema) - 1 (codes_length) - 20 (registry) + 1 = len - 37
            CASE
                WHEN bytearray_to_integer(bytearray_substring({{ calldata_field }}, bytearray_length({{ calldata_field }}) - 16, 1)) = 1 
                THEN bytearray_substring(
                    {{ calldata_field }},
                    bytearray_length({{ calldata_field }}) - 16 - 1 - 1 - 20 + 1,  -- start of registry
                    20
                )
                ELSE NULL
            END,

            -- 5) codes_readable: Human-readable codes (UTF-8 decoded)
            try(from_utf8(
                bytearray_substring(
                    {{ calldata_field }},
                    bytearray_length({{ calldata_field }})
                        - 16  -- magic bytes
                        - 1   -- schema_id
                        - 1   -- codes_length
                        - CASE 
                            WHEN bytearray_to_integer(bytearray_substring({{ calldata_field }}, bytearray_length({{ calldata_field }}) - 16, 1)) = 1 
                            THEN 20  -- registry address for Schema 1
                            ELSE 0 
                          END
                        - bytearray_to_integer(bytearray_substring({{ calldata_field }}, bytearray_length({{ calldata_field }}) - 17, 1))  -- codes
                        + 1,  -- start position (1-indexed)
                    bytearray_to_integer(bytearray_substring({{ calldata_field }}, bytearray_length({{ calldata_field }}) - 17, 1))  -- length
                )
            )),

            -- 6) codes_array: Codes split by comma
            split(
                try(from_utf8(
                    bytearray_substring(
                        {{ calldata_field }},
                        bytearray_length({{ calldata_field }})
                            - 16  -- magic bytes
                            - 1   -- schema_id
                            - 1   -- codes_length
                            - CASE 
                                WHEN bytearray_to_integer(bytearray_substring({{ calldata_field }}, bytearray_length({{ calldata_field }}) - 16, 1)) = 1 
                                THEN 20  -- registry address for Schema 1
                                ELSE 0 
                              END
                            - bytearray_to_integer(bytearray_substring({{ calldata_field }}, bytearray_length({{ calldata_field }}) - 17, 1))  -- codes
                            + 1,  -- start position (1-indexed)
                        bytearray_to_integer(bytearray_substring({{ calldata_field }}, bytearray_length({{ calldata_field }}) - 17, 1))  -- length
                    )
                )),
                ','
            ),

            -- 7) erc_8021_suffix: The magic bytes
            bytearray_substring({{ calldata_field }}, bytearray_length({{ calldata_field }}) - 15, 16)
        )
        AS ROW(
            original_tx_data varbinary,
            schema_type varchar,
            codes_hex varchar,
            custom_registry_address varbinary,
            codes_readable varchar,
            codes_array array(varchar),
            erc_8021_suffix varbinary
        )
    )
    ELSE NULL
END
{% endmacro %}


{% macro has_eip_8021_suffix(calldata_field) %}
{#
    Check if calldata ends with EIP-8021 magic bytes.
    
    Usage:
        SELECT *
        FROM table
        WHERE {{ has_eip_8021_suffix('data') }}
#}
bytearray_substring({{ calldata_field }}, bytearray_length({{ calldata_field }}) - 15, 16) = 0x80218021802180218021802180218021
{% endmacro %}
