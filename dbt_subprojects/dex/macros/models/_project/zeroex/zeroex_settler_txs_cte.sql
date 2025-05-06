{% macro zeroex_settler_txs_cte(blockchain, start_date) %}
-- Macro to process 0x Protocol settler transactions
-- This macro identifies and extracts transactions related to the 0x Protocol settler contracts
-- Returns a CTE with transaction details including block information, method IDs, and trader addresses

WITH 
-- Use the settler addresses from the incremental view
result_0x_settler_addresses AS (
    SELECT *
    FROM {{ ref('zeroex_' ~ blockchain ~ '_settler_addresses') }}
),

-- Step 4: Pre-filter traces to reduce data volume before joining
-- This optimization reduces the amount of data processed in the subsequent join
filtered_traces AS (
    SELECT
        tx_hash,
        block_number,
        block_time,
        "from",
        "to",
        trace_address,
        varbinary_substring(input,1,4) AS method_id, -- Extract method ID from input data
        input
    FROM
        {{ source(blockchain, 'traces') }} AS tr
    WHERE
        -- Filter for specific method signatures used by 0x Protocol
        (varbinary_position(input,0x1fff991f) <> 0 OR varbinary_position(input,0xfd3ad6d4) <> 0)
        -- Apply time-based filtering for incremental loads
        {% if is_incremental() %}
            AND {{ incremental_predicate('block_time') }}
        {% else %}
            AND block_time >= DATE '{{start_date}}'
        {% endif %}
),

-- Step 5: Join traces with settler addresses to identify relevant transactions
-- This CTE contains the core transaction data for 0x Protocol trades
settler_trace_data AS (
    SELECT
        tr.tx_hash,
        tr.block_number,
        tr.block_time,
        tr."to" AS contract_address,
        tr.method_id,
        -- Extract tracker information from input data
        varbinary_substring(tr.input,varbinary_position(tr.input,0xfd3ad6d4)+132,32) AS tracker,
        -- Determine the taker address based on input data pattern
        CASE 
            WHEN varbinary_substring(tr.input,17,6) in (0x,0x000000000000) 
            THEN tr."from"
            ELSE first_value(varbinary_substring(tr.input,17,20)) OVER (PARTITION BY tr.tx_hash ORDER BY tr.trace_address DESC) 
        END AS taker,
        a.settler_address,
        tr.trace_address,
        -- Identify CoW Protocol trades by checking for the CoW address in the input
        CASE WHEN varbinary_position(tr.input,0x9008d19f58aabd9ed0d60971565aa8510560ab41) <> 0 THEN 1 END AS cow_trade,
        tr.input
    FROM
        filtered_traces AS tr
    JOIN
        result_0x_settler_addresses a 
        -- Join condition: trace destination matches settler address and occurs after activation
        ON a.settler_address = tr."to" AND tr.block_time > a.begin_block_time
    WHERE
        -- Include known settler addresses or specific 0x Protocol contract addresses
        (a.settler_address IS NOT NULL OR tr."to" in (
            0x0000000000001fF3684f28c67538d4D072C22734,
            0x0000000000005E88410CcDFaDe4a5EfaE4b49562,
            0x000000000000175a8b9bC6d539B3708EEd92EA6c
        ))
),

-- Step 6: Final processing to extract and format transaction details
-- This CTE contains the final dataset with all necessary trade information
settler_txs AS (
    SELECT
        tx_hash,
        block_time,
        block_number,
        method_id,
        contract_address,
        settler_address,
        -- Extract ZeroEx ID (zid) from tracker data
        varbinary_substring(tracker,2,12) AS zid,
        -- Extract tag based on method ID
        CASE
            WHEN method_id = 0x1fff991f THEN varbinary_substring(tracker,14,3)
            WHEN method_id = 0xfd3ad6d4 THEN varbinary_substring(tracker,13,3)
        END AS tag,
        -- Assign row numbers for transactions with the same hash
        ROW_NUMBER() OVER (PARTITION BY tx_hash ORDER BY varbinary_substring(tracker,2,12)) AS rn,
        -- Assign row numbers for CoW Protocol trades
        CASE WHEN cow_trade = 1 THEN ROW_NUMBER() OVER (PARTITION BY tx_hash ORDER BY trace_address) END AS cow_rn,
        -- Handle special cases for taker addresses
        CASE 
            WHEN taker IN (
                0x0000000000001ff3684f28c67538d4d072c22734, 
                0x0000000000005E88410CcDFaDe4a5EfaE4b49562,
                0x000000000000175a8b9bC6d539B3708EEd92EA6c
            ) 
            THEN varbinary_substring(input, varbinary_length(input) - 19, 20) 
            ELSE taker 
        END AS taker 
    FROM
        settler_trace_data
    WHERE 
        -- Filter out transactions with empty ZeroEx IDs
        varbinary_substring(tracker,2,12) != 0x000000000000000000000000
)

-- Return the final dataset with all 0x Protocol settler transactions
SELECT * FROM settler_txs
{% endmacro %}
