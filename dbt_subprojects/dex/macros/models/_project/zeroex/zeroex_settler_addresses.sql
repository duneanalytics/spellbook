{% macro zeroex_settler_addresses(blockchain) %}
-- Macro to extract 0x Protocol settler addresses
-- This macro identifies and extracts settler addresses from the logs table
-- Returns a CTE with settler address details including activation timestamps and token IDs

WITH 
-- Step 1: Extract addresses from the logs table where 0x Protocol settler events occurred
-- This CTE identifies the settler addresses and their activation timestamps
tbl_addresses AS (
     SELECT
            tx_hash,
            bytearray_substring(topic3,13,20) as settler_address, -- Extract settler address from topic3
            varbinary_to_integer(varbinary_ltrim(topic1)) as token_id, -- Extract token ID from topic1
            block_time as begin_block_time,
            block_number as begin_block_number
    FROM
        {{ source(blockchain, 'logs') }}
    WHERE
        -- Filter for the 0x Protocol registry contract
        contract_address = 0x00000000000004533Fe15556B1E086BB1A72cEae
        -- Filter for the specific event signature (ProtocolFeeCollectorAddress event)
        AND topic0 = 0xaa94c583a45742b26ac5274d230aea34ab334ed5722264aa5673010e612bc0b2
),

-- Step 2: Calculate the active time periods for each settler address
-- For each token_id, determine when the next settler address was activated
tbl_end_times AS (
    SELECT
        tx_hash,
        settler_address,
        token_id,
        begin_block_time,
        begin_block_number,
        -- Use LEAD window function to find the next activation time for the same token_id
        LEAD(begin_block_time) OVER (PARTITION BY token_id ORDER BY begin_block_time) AS end_block_time,
        LEAD(begin_block_number) OVER (PARTITION BY token_id ORDER BY begin_block_time) AS end_block_number
    FROM
        tbl_addresses
),

-- Step 3: Filter out null settler addresses
-- Only keep records where a valid settler address was specified
result_0x_settler_addresses AS (
    SELECT *
    FROM tbl_end_times
    WHERE settler_address != 0x0000000000000000000000000000000000000000
)

-- Return the final dataset with all 0x Protocol settler addresses
SELECT * FROM result_0x_settler_addresses
{% endmacro %} 