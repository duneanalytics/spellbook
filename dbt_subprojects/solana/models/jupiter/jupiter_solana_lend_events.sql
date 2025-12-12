 {{
  config(
        schema = 'jupiter_solana',
        alias = 'lend_events',
        partition_by = ['block_month'],
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy='merge',
        unique_key = ['tx_id','outer_instruction_index','supply_amount_nominal'],
        post_hook='{{ expose_spells(\'["jupiter"]\',
                                    "project",
                                    "jupiter_solana",
                                    \'["osk2020"]\') }}')
}}

/*

JLend Liquidity: jupeiUmn818Jg1ekPURTpr4mFo29p46vygyykFJ3wZC
JLend Liquidity solscan link: https://solscan.io/account/jupeiUmn818Jg1ekPURTpr4mFo29p46vygyykFJ3wZC

data, 

first 1-8 (8) bytes, discriminator: 
Operate: 0xd96ad06374972a87

next 9-24 (16) bytes:
Supply Amount

next 25-40 (16) bytes:
Borrow Amount

next 41-72 (32) bytes:
withdraw to

next 73-104 (32) bytes
borrow to

next 105 (1) byte
transfer type

*/

WITH lend_events AS (

SELECT
    tx_type,
    cast(date_trunc('month', block_time) as date) as block_month,
    block_time,
    tx_id,
    outer_instruction_index,
    account_arguments[4] AS contract_address,
    --handle negative numbers with casting below and int256
    CASE 
      WHEN varbinary_to_int256(varbinary_reverse(bytearray_substring(data, 9, 16))) >= CAST('170141183460469231731687303715884105728' AS INT256)
        THEN varbinary_to_int256(varbinary_reverse(bytearray_substring(data, 9, 16))) - CAST('340282366920938463463374607431768211456' AS INT256)
      ELSE varbinary_to_int256(varbinary_reverse(bytearray_substring(data, 9, 16))) 
    END AS supply_amount_raw,
    
    CASE 
      WHEN varbinary_to_int256(varbinary_reverse(bytearray_substring(data, 25, 16))) >= CAST('170141183460469231731687303715884105728' AS INT256)
        THEN varbinary_to_int256(varbinary_reverse(bytearray_substring(data, 25, 16))) - CAST('340282366920938463463374607431768211456' AS INT256)
      ELSE varbinary_to_int256(varbinary_reverse(bytearray_substring(data, 25, 16))) 
    END AS borrow_amount_raw,
    
    to_base58(bytearray_substring(data, 41, 32)) AS withdraw_to,
    to_base58(bytearray_substring(data, 73, 32)) AS borrow_to,
    
    CASE bytearray_substring(data, 105, 1)
        WHEN 0x00 THEN 'SKIP'
        WHEN 0x01 THEN 'DIRECT'
        WHEN 0x02 THEN 'CLAIM'
    END AS transfer_type,
    
    tx_signer
FROM
    {{ source('solana','instruction_calls') }}
LEFT JOIN
    (
        SELECT
            CASE executing_account
            WHEN 'jupgfSgfuAXv4B6R2Uxu85Z1qdzgju79s6MfZekN6XS' THEN 'Flashloan'
            WHEN 'jupr81YtYssSyPt8jbnGuiWon5f6x9TcDEFxYe3Bdzi' THEN 'Lend-Borrow'
            WHEN 'jup3YeL8QhtSx1e253b2FDvsMNC87fDrgQZivbrndc9' THEN 'Lend-Earn'
            ELSE executing_account
            END AS tx_type,
            tx_id,
            outer_instruction_index
        FROM
            {{ source('solana','instruction_calls') }}
        WHERE
            executing_account IN ('jupgfSgfuAXv4B6R2Uxu85Z1qdzgju79s6MfZekN6XS', 'jupr81YtYssSyPt8jbnGuiWon5f6x9TcDEFxYe3Bdzi', 'jup3YeL8QhtSx1e253b2FDvsMNC87fDrgQZivbrndc9')
        AND
            block_time > TIMESTAMP '2025-07-23' --Jupiter Lend starts 1-2 days later, no need for prior data
        AND
            tx_success = true
            {% if is_incremental() %}
        AND 
            {{ incremental_predicate('block_time') }}
            {% endif %}
        GROUP BY
            2, 3, 1
    ) t
USING
    (tx_id, outer_instruction_index)

WHERE
    executing_account = 'jupeiUmn818Jg1ekPURTpr4mFo29p46vygyykFJ3wZC'
AND
    varbinary_substring(data, 1, 8) = 0xd96ad06374972a87
AND
    tx_success = true
AND
    block_time > TIMESTAMP '2025-07-23' --Jupiter Lend starts 1-2 days later, no need for prior data
    {% if is_incremental() %}
AND 
    {{ incremental_predicate('block_time') }}
    {% endif %}
ORDER BY
    block_time DESC

),

nominal_amounts AS (

      SELECT
            '0.1.0' AS "version", --per PER Jupiter Official Docs, version listed: https://github.com/jup-ag/jupiter-lend/blob/main/target/idl/lending.json
            tx_type,
            block_month,
            block_time,
            tx_id,
            outer_instruction_index,
            contract_address,
            symbol,
            supply_amount_raw/POW(10, decimals) AS supply_amount_nominal,
            borrow_amount_raw/POW(10, decimals) AS borrow_amount_nominal,
            withdraw_to,
            borrow_to,
            transfer_type,
            tx_signer
      FROM
            lend_events le
      JOIN
            {{ source('tokens_solana', 'fungible') }} t
      ON
            le.contract_address = t.token_mint_address
),

prices AS (
    
    SELECT
        --n table
        '0.1.0' AS "version",
        tx_type,
        block_month,
        block_time,
        tx_id,
        outer_instruction_index,
        n.contract_address,
        symbol,
        supply_amount_nominal,
        borrow_amount_nominal,
        --prices.usd table
        price AS unit_price,
        supply_amount_nominal * price AS supply_usd,
        borrow_amount_nominal * price AS borrow_usd,
        --n table
        withdraw_to,
        borrow_to,
        transfer_type,
        tx_signer
    FROM
        nominal_amounts n
    LEFT JOIN
        (
            SELECT
                contract_address,
                "minute",
                price
            FROM
                {{ source('prices','usd_forward_fill') }}
            WHERE
                blockchain = 'solana'
            AND
                "minute" > TIMESTAMP '2025-07-23' --Jupiter Lend starts 1-2 days later, no need for prior data
            AND
                to_base58(contract_address) IN (SELECT contract_address FROM nominal_amounts)
                {% if is_incremental() %}
            AND 
                {{ incremental_predicate('minute') }}
                {% endif %}
        ) p
    ON
        n.contract_address = to_base58(p.contract_address)
    AND
        Date_Trunc('minute', n.block_time) = p.minute
)

SELECT
    *
FROM
    prices
ORDER BY
    block_time DESC