{% macro zeroex_settler_txs_cte(blockchain, start_date) %}
WITH tbl_addresses AS (
     SELECT
            tx_hash,
            bytearray_substring(topic3,13,20) as settler_address,
            varbinary_to_integer(varbinary_ltrim(topic1)) as token_id,
            block_time as begin_block_time,
            block_number as begin_block_number
    FROM
        {{ source(blockchain, 'logs') }}
    WHERE
        contract_address = 0x00000000000004533Fe15556B1E086BB1A72cEae
        AND topic0 = 0xaa94c583a45742b26ac5274d230aea34ab334ed5722264aa5673010e612bc0b2
),

tbl_end_times AS (
    SELECT
        *,
        LEAD(begin_block_time) OVER (PARTITION BY token_id ORDER BY begin_block_time) AS end_block_time,
        LEAD(begin_block_number) OVER (PARTITION BY token_id ORDER BY begin_block_time) AS end_block_number
    FROM
        tbl_addresses
),

result_0x_settler_addresses AS (
    SELECT
        *
    FROM
        tbl_end_times
    WHERE
        settler_address != 0x0000000000000000000000000000000000000000
),

settler_trace_data AS (
    SELECT
        tr.tx_hash,
        block_number,
        block_time,
        "to" AS contract_address,
        varbinary_substring(input,1,4) AS method_id,
        varbinary_substring(input,varbinary_position(input,0xfd3ad6d4)+132,32) tracker,
        case when varbinary_substring(input,17,6) in (0x,0x000000000000) then "from"
            else first_value(varbinary_substring(input,17,20)) over (partition by tr.tx_hash order by trace_address desc)
            end as taker,
        a.settler_address,
        trace_address,
        case when varbinary_position(input,0x9008d19f58aabd9ed0d60971565aa8510560ab41) <> 0 then 1 end as cow_trade,
        input
    FROM
        {{ source(blockchain, 'traces') }} AS tr
    JOIN
        result_0x_settler_addresses a ON a.settler_address = tr.to AND tr.block_time > a.begin_block_time
    WHERE
        (a.settler_address IS NOT NULL OR tr.to in (0x0000000000001fF3684f28c67538d4D072C22734,
                                                    0x0000000000005E88410CcDFaDe4a5EfaE4b49562,
                                                    0x000000000000175a8b9bC6d539B3708EEd92EA6c))
        AND (varbinary_position(input,0x1fff991f) <> 0 OR  varbinary_position(input,0xfd3ad6d4) <> 0 )
        {% if is_incremental() %}
            AND {{ incremental_predicate('block_time') }}
        {% else %}
            AND block_time >= DATE '{{start_date}}'
        {% endif %}
),

settler_txs AS (
    SELECT
        tx_hash,
        block_time,
        block_number,
        method_id,
        contract_address,
        settler_address,
        (varbinary_substring(tracker,2,12)) AS zid,
        CASE
            WHEN method_id = 0x1fff991f THEN (varbinary_substring(tracker,14,3))
            WHEN method_id = 0xfd3ad6d4 THEN (varbinary_substring(tracker,13,3))
        END AS tag,
        row_number() over (partition by tx_hash order by varbinary_substring(tracker,2,12)) rn,
        case when cow_trade = 1 then row_number() over (partition by tx_hash order by trace_address) end as cow_rn,
        case when taker in ( 0x0000000000001ff3684f28c67538d4d072c22734,
                                0x0000000000005E88410CcDFaDe4a5EfaE4b49562,
                                0x000000000000175a8b9bC6d539B3708EEd92EA6c )
                then varbinary_substring(input, varbinary_length(input) - 19, 20)
                else taker
            end as taker
    FROM
        settler_trace_data
    where (varbinary_substring(tracker,2,12)) != 0x000000000000000000000000
)

SELECT * FROM settler_txs
{% endmacro %}
