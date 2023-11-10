{{
  config(
    
    alias = 'erc20_bridged_mapping'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['l1_token', 'l2_token']
    , post_hook='{{ expose_spells(\'["optimism"]\',
                                "sector",
                                "tokens",
                                \'["msilb7"]\') }}'
  )
}}

SELECT l1_token, l2_token, l1_symbol, l1_decimals

FROM (

SELECT l1_token, l2_token
    , COALESCE(map.symbol, et.symbol) AS l1_symbol --select token factory, else eth
    , COALESCE(et.decimals, map.decimals) AS l1_decimals --select eth mapping, else token factory
    , ROW_NUMBER() OVER (PARTITION BY l1_token, l2_token
        ORDER BY COALESCE(et.decimals, map.decimals) ASC NULLS LAST, COALESCE(map.symbol, et.symbol) DESC NULLS LAST) AS rnk
FROM (

        SELECT _l1Token AS l1_token, _l2Token AS l2_token, NULL AS symbol, NULL AS decimals
            FROM {{source( 'optimism_ethereum', 'L1StandardBridge_evt_ERC20DepositInitiated' ) }}
        WHERE evt_tx_hash != 0x460965f169a99b3d372cd749621a3652ad232d1b1580fd77424eacc2e973b672 --bad event emitted
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
        GROUP BY 1,2

        UNION ALL
        SELECT _l1Token, _l2Token, NULL AS symbol, NULL AS decimals
            FROM {{source( 'optimism_ethereum', 'OVM_L1StandardBridge_evt_ERC20DepositInitiated' ) }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
        GROUP BY 1,2

        UNION ALL
        SELECT l1_token, l2_token, symbol, decimals FROM {{ ref('ovm_optimism_l2_token_factory') }}
        {% if is_incremental() %}
        WHERE call_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
        GROUP BY 1,2,3,4

        -- Manual adds
        UNION ALL SELECT 0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f AS l1_token,  0x8700daec35af8ff88c16bdf0418774cb3d7599b4 AS l2_token, NULL, NULL -- SNX
        UNION ALL SELECT 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee AS l1_token, 0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000 AS l2_token, NULL, NULL -- ETH
        UNION ALL SELECT 0x6b175474e89094c44da98b954eedeac495271d0f AS l1_token, 0xda10009cbd5d07dd0cecc66161fc93d7c9000da1 AS l2_token, NULL, NULL -- DAI
        UNION ALL SELECT 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS l1_token,  0x4200000000000000000000000000000000000006 AS l2_token, NULL, NULL --WETH

    ) map

LEFT JOIN {{ ref('tokens_ethereum_erc20') }} et
    ON et.contract_address = map.l1_token
) fin
WHERE rnk =1