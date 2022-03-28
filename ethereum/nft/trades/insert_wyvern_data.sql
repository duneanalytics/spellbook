CREATE OR REPLACE FUNCTION nft.insert_wyvern_data(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN


WITH wyvern_calldata AS (
    SELECT 
        call_tx_hash,
        -- calldataBuy can be used to extract meaningful information about:
        CASE WHEN substring("calldataBuy",1,4) in ('\x68f0bcaa') THEN 'Bundle Trade' -- the trade type
             ELSE 'Single Item Trade'
        END AS trade_type,
        CASE WHEN substring("calldataBuy",1,4) in ('\xfb16a595','\x23b872dd') THEN 'erc721' -- the ERC standard
             WHEN substring("calldataBuy",1,4) in ('\x96809f90','\xf242432a') THEN 'erc1155' 
        END AS erc_standard,
        addrs [1] as exchange_contract_address,
        CASE WHEN substring("calldataBuy",1,4) in ('\xfb16a595','\x96809f90') THEN CAST(substr("calldataBuy", 81,20) as bytea) -- the NFT contract address
            WHEN  substring("calldataBuy",1,4) in ('\x23b872dd','\xf242432a') THEN addrs [5]
            ELSE addrs [5]
            END AS nft_contract_address,
        CASE -- Replace `ETH` with `WETH` for ERC20 lookup later
            WHEN addrs [7] = '\x0000000000000000000000000000000000000000' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE addrs [7]
        END AS currency_token,
        uints [5] as original_amount,
        addrs [2] as buyer,
        CAST(substr("calldataBuy", 49,20) as bytea) as buyer_when_aggr,
        addrs [9] AS seller,
        CASE WHEN substring("calldataBuy",1,4) in ('\xfb16a595','\x96809f90') THEN CAST(bytea2numericpy(substr("calldataBuy",101,32)) as text) -- the token ID
             WHEN  substring("calldataBuy",1,4) in ('\x23b872dd','\xf242432a') THEN CAST(bytea2numericpy(substr("calldataBuy", 69,32)) as text)
             END AS token_id,
        
        CASE -- call_trace_address will be used to extract information about royalty fees 
             WHEN call_trace_address::varchar = '{}' then '{3}' -- For bundle join
             ELSE call_trace_address::varchar 
        END as call_trace_address,
        array_agg(DISTINCT addrs [7]) AS original_currency_address
    FROM
        opensea."WyvernExchange_call_atomicMatch_"
    WHERE
        "call_success"
    AND call_block_time >= start_ts
    AND call_block_time < end_ts
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12    
),

-- Get value of Royalty Fees from ethereum.traces
royalty_fees as (
SELECT  
    CASE WHEN array_length(trace_address,1) = 1 then '{3}' -- For single row join
         WHEN array_length(trace_address,1) = 2 then '{'||trace_address[1]||'}'
         WHEN array_length(trace_address,1) = 3 then '{'||trace_address[1]||','||trace_address[2]||'}'
    END as trace_address,
    tx_hash,
    value AS fees,
    traces."from",
    traces."to"
FROM ethereum.traces
WHERE "from" in ('\x7Be8076f4EA4A4AD08075C2508e481d6C946D12b', '\x7f268357a8c2552623316e2562d90e642bb538e5')
AND "to" = '\x5b3256965e7c3cf26e11fcaf296dfc8807c01073' -- OpenSea Wallet
AND traces.block_time >= start_ts
AND traces.block_time < end_ts
        UNION ALL  
SELECT 
    '{3}' as trace_address,
    evt_tx_hash as tx_hash,
    value AS fees,
    "from",
    "to"
   FROM erc20."ERC20_evt_Transfer" erc
   WHERE "to" = '\x5b3256965e7c3cf26e11fcaf296dfc8807c01073'
   AND evt_block_time >= start_ts
   AND evt_block_time < start_ts
),

rows AS (
    INSERT INTO nft.wyvern_data(
        call_tx_hash,
        trade_type,
        erc_standard,
        exchange_contract_address,
        nft_contract_address,
        currency_token,
        original_amount,
        buyer,
        buyer_when_aggr,
        seller,
        token_id,
        call_trace_address,
        original_currency_address,
        fees,
        block_time,
        block_number,
        tx_from,
        tx_to
        )
    SELECT 
        call_tx_hash,
        trade_type,
        erc_standard,
        exchange_contract_address,
        nft_contract_address,
        currency_token,
        original_amount,
        buyer,
        buyer_when_aggr,
        seller,
        token_id,
        call_trace_address,
        original_currency_address,
        fees,
        tx.block_time AS block_time,
        tx.block_number,
        tx."from" AS tx_from,
        tx."to" AS tx_to
    FROM wyvern_calldata wc
    LEFT JOIN ethereum.transactions tx ON wc.call_tx_hash = tx.hash
        AND tx.block_time >= start_ts
        AND tx.block_time < end_ts
    LEFT JOIN royalty_fees rf ON rf.tx_hash = wc.call_tx_hash AND rf.trace_address = wc.call_trace_address
    WHERE
        NOT EXISTS (SELECT * -- Exclude OpenSea mint transactions
        FROM erc721."ERC721_evt_Transfer" erc721
        WHERE wc.call_tx_hash = erc721.evt_tx_hash
        AND erc721.evt_block_time >= start_ts
        AND erc721.evt_block_time < end_ts
        AND erc721."from" = '\x0000000000000000000000000000000000000000')
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

