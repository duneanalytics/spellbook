
 {{ config(schema = 'opensea_v1_ethereum', 
alias='fees') }}

SELECT  
    CASE WHEN size(trace_address) = 1 then array(3::bigint) -- for single row join
    WHEN size(trace_address) = 2 then array(trace_address[0]) 
    WHEN size(trace_address) = 3 then array(trace_address[0], trace_address[1])
    END as trace_address,
    tx_hash,
    SUM(value) AS fees,
    to,
    'ETH' as fee_currency_symbol
FROM ethereum.traces source_fees
WHERE 
FROM IN ('0x7be8076f4ea4a4ad08075c2508e481d6c946d12b','0x7f268357a8c2552623316e2562d90e642bb538e5')
AND to = '0x5b3256965e7c3cf26e11fcaf296dfc8807c01073' -- OpenSea Wallet
GROUP BY 1,2,4,5
                UNION ALL  
SELECT 
    array(3::bigint) as trace_address,
    evt_tx_hash as tx_hash,
    SUM(value) AS fees,
    to,
    erc20.symbol as fee_currency_symbol
   FROM erc20_ethereum.evt_transfer erc
   LEFT JOIN dbt_thomas_tokens_ethereum.erc20 erc20 ON erc20.contract_address =  erc.contract_address
   WHERE to = '0x5b3256965e7c3cf26e11fcaf296dfc8807c01073'
   AND evt_tx_hash = '0xaa68c271a72a2a280eb06d89506d1feb3de6a84f6f19d1aa001885d783d5b9c7'
   GROUP BY 1,2,4,5
