{% macro
    angstrom_bundle_volume_events(    
        blockchain = null,
        project = null,
        version = null
    )
%}


WITH
    tx_data AS (
        SELECT 
            block_number,
            block_time,
            tx_hash,
            index AS tx_index,
            to AS angstrom_address,
            input AS tx_input
        FROM ethereum.transactions
        WHERE to = 0xb9c4cE42C2e29132e207d29Af6a7719065Ca6AeC AND varbinary_substring(input, 1, 4) = 0x09c5eabe
    ),
    tob_orders AS (
        SELECT 
            
    )



{% endmacro %}